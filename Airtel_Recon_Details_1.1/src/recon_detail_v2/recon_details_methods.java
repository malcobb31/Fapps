package recon_detail_v2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class recon_details_methods {

    private Connection conn;
    private String tableName;
    private String date;

    private String ipmTable, ipmSnoColumn, ipmDateColumn;
    private String urlTable, urlSnoColumn, urlDateColumn;

    private String dbUrl, dbUser, dbPass;

    // RANGE (IMPORTANT FIX)
    private int[] ipmRange;
    private int[] urlRange;

    private boolean debugEnabled = true;
    private String processName = "RECON";
    private String processVersion = "1.0";

    private DateTimeFormatter logFormat =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    public recon_details_methods(Connection conn, String tableName, String date,
                                 String ipmTable, String ipmSnoColumn, String ipmDateColumn,
                                 String urlTable, String urlSnoColumn, String urlDateColumn,
                                 String dbUrl, String dbUser, String dbPass) {

        this.conn = conn;
        this.tableName = tableName;
        this.date = date;

        this.ipmTable = ipmTable;
        this.ipmSnoColumn = ipmSnoColumn;
        this.ipmDateColumn = ipmDateColumn;

        this.urlTable = urlTable;
        this.urlSnoColumn = urlSnoColumn;
        this.urlDateColumn = urlDateColumn;

        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;
    }

    public void process() throws Exception {

        log("DEBUG", "START processing date: " + date, "", "START");

        SNORangeFinder finder = new SNORangeFinder();

        int[] ipmMinMax = finder.getStartEndIdOfTable(ipmTable, ipmSnoColumn, dbUrl, dbUser, dbPass);
        int[] urlMinMax = finder.getStartEndIdOfTable(urlTable, urlSnoColumn, dbUrl, dbUser, dbPass);

        if (ipmMinMax == null || urlMinMax == null) {
            throw new Exception("SNO RANGE FETCH FAILED");
        }

        // STORE RANGE (IMPORTANT)
        ipmRange = finder.getStartAndEndId(
                date, ipmMinMax[0], ipmMinMax[1],
                ipmTable, ipmDateColumn,
                "daily", "120", false,
                ipmSnoColumn, dbUrl, dbUser, dbPass, "%Y-%m-%d"
        );

        urlRange = finder.getStartAndEndId(
                date, urlMinMax[0], urlMinMax[1],
                urlTable, urlDateColumn,
                "daily", "120", false,
                urlSnoColumn, dbUrl, dbUser, dbPass, "%Y-%m-%d"
        );

        log("DEBUG", "Ranges IPM:" + ipmRange[0] + "-" + ipmRange[1] +
                " URL:" + urlRange[0] + "-" + urlRange[1], "", "SNO");

        // ================= COUNTS =================

        int total_ticket_count = getCount(
                "select count(*) from " + ipmTable +
                " where " + ipmSnoColumn + " between " + ipmRange[0] + " and " + ipmRange[1] +
                " and date(" + ipmDateColumn + ") = '" + date + "'" + " and rqstType='user'");

        int total_entries_received = getCount(
                "select count(*) from " + urlTable +
                " where provisioning_node like 'Netsweeper%'");

        // BLOCKING
        int bn = get("Block", "Netsweeper%");
        int bd = get("Block", "DNS%");
        int bi = get("Block", "Infoblock%");

        // UNBLOCKING
        int un = get("unBlock", "Netsweeper%");
        int ud = get("unBlock", "DNS%");
        int ui = get("unBlock", "Infoblock%");

        // STATUS BLOCKED
        int sbn = getStatus("Blocked", "Netsweeper");
        int sbd = getStatus("Blocked", "DNS");
        int sbi = getStatus("Blocked", "Infoblock");

        // STATUS UNBLOCKED
        int sun = getStatus("UnBlocked", "Netsweeper");
        int sud = getStatus("UnBlocked", "DNS");
        int sui = getStatus("UnBlocked", "Infoblock");

        // ALREADY EXISTS
        int aen = getStatus("Already Exists", "Netsweeper");
        int aed = getStatus("Already Exists", "DNS");
        int aei = getStatus("Already Exists", "Infoblock");

        // ================= NS LOOKUP FIELD NOT FOUND (NEW) =================

        int nsLookupFieldNetsweeper = getNsLookupFieldNotFoundCount("Netsweeper_ns_lookup_not_found", "Netsweeper");
        int nsLookupFieldDns = getNsLookupFieldNotFoundCount("DNS_ns_lookup_not_found", "DNS");
        int nsLookupFieldInfoblock = getNsLookupFieldNotFoundCount("Infoblock_ns_lookup_not_found", "Infoblock");

        // ================= GAP =================
        // Entries that failed ns lookup were never actually blocked, so they
        // are excluded from the "true" gap for that node's blocking count.

        int gbn = bn - sbn - aen - nsLookupFieldNetsweeper;
        int gun = un - sun;

        int gbd = bd - sbd - aed - nsLookupFieldDns;
        int gud = ud - sud;

        int gbi = bi - sbi - aei - nsLookupFieldInfoblock;
        int gui = ui - sui;

        int gapCount = gbn + gun + gbd + gud + gbi + gui;

        int totalEntries = total_entries_received;

        // ================= NS LOOKUP FAILED =================

        int nsLookupFailedCount = getNsLookupFailedCount();
        String nsLookupFailedUrls = getNsLookupFailedUrls();
        int nsLookupManualImplCount = getNsLookupManualImplementedCount();

        log("DEBUG",
                "Summary Gap:" + gapCount +
                " Total:" + totalEntries +
                " NS_Lookup_Failed_Count:" + nsLookupFailedCount +
                " NS_Lookup_Manual_Impl_Count:" + nsLookupManualImplCount +
                " NS_Lookup_Field_Netsweeper:" + nsLookupFieldNetsweeper +
                " NS_Lookup_Field_DNS:" + nsLookupFieldDns +
                " NS_Lookup_Field_Infoblock:" + nsLookupFieldInfoblock,
                "",
                "SUMMARY");

        upsert(total_ticket_count, total_entries_received,
                bn, bd, bi,
                un, ud, ui,
                sbn, sbd, sbi,
                sun, sud, sui,
                aen, aed, aei,
                gapCount,
                gbn, gun, gbd, gud, gbi, gui,
                totalEntries,
                nsLookupFailedCount,
                nsLookupFailedUrls,
                nsLookupManualImplCount,
                nsLookupFieldNetsweeper,
                nsLookupFieldDns,
                nsLookupFieldInfoblock);

        log("DEBUG", "END processing", "", "END");
    }

    // ================= FIXED METHODS =================

    private int get(String status, String node) throws Exception {

        String query = "select count(*) from " + urlTable +
                " where " + urlSnoColumn + " between " + urlRange[0] + " and " + urlRange[1] +
                " and date(" + urlDateColumn + ") = '" + date + "'" +
                " and actual_status='" + status + "' and provisioning_node like '" + node + "'";

        return getCount(query);
    }

    private int getStatus(String status, String node) throws Exception {

        String query = "select count(*) from " + urlTable +
                " where " + urlSnoColumn + " between " + urlRange[0] + " and " + urlRange[1] +
                " and date(" + urlDateColumn + ") = '" + date + "'" +
                " and status='" + status + "' and provisioning_node='" + node + "'";

        return getCount(query);
    }

    private int getNsLookupFailedCount() throws Exception {
        String query = "select count(*) from urlScan.url_details_ns_lookup where date(time) = '" + date + "'";
        return getCount(query);
    }

    private String getNsLookupFailedUrls() throws Exception {
        String query = "select url from urlScan.url_details_ns_lookup where date(time) = '" + date + "'";

        StringBuilder sb = new StringBuilder();

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            while (rs.next()) {
                String url = rs.getString(1);
                if (url != null) {
                    url = url.trim();
                }

                if (url != null && url.length() > 0) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(url);
                }
            }
        }

        return sb.toString();
    }

    private int getNsLookupManualImplementedCount() throws Exception {
        String query =
                "select count(distinct n.order_id) " +
                "from urlScan.url_details_ns_lookup n " +
                "where date(n.time) = '" + date + "' " +
                "and exists (" +
                "   select 1 from urlScan.ipm_requests i " +
                "   where i.rqstID = n.order_id " +
                "   and i.rqstStatus = 95" +
                ")";

        return getCount(query);
    }

    // NEW: counts rows in urlScan.url_details where the provisioning_node indicates
    // the ns lookup could not find the field, scoped to the matching authority.
    private int getNsLookupFieldNotFoundCount(String provisioningNode, String rqstAuthority) throws Exception {
        String query = "select count(*) from urlScan.url_details" +
                " where date(time) = '" + date + "'" +
                " and provisioning_node = '" + provisioningNode + "'" +
                " and rqstAuthority = '" + rqstAuthority + "'";

        return getCount(query);
    }

    private int getCount(String query) throws Exception {

        log("DEBUG", query, "", "QUERY");

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getInt(1);
            }
        }

        return 0;
    }

    private void upsert(
            int total_ticket_count, int total_entries_received,
            int bn, int bd, int bi,
            int un, int ud, int ui,
            int sbn, int sbd, int sbi,
            int sun, int sud, int sui,
            int aen, int aed, int aei,
            int gapCount,
            int gbn, int gun, int gbd, int gud, int gbi, int gui,
            int totalEntries,
            int nsLookupFailedCount,
            String nsLookupFailedUrls,
            int nsLookupManualImplCount,
            int nsLookupFieldNetsweeper,
            int nsLookupFieldDns,
            int nsLookupFieldInfoblock
    ) throws Exception {

        String sql = "INSERT INTO " + tableName +
                " (date, Total_tickets_Count, Total_Entries_received," +
                "Blocking_Entries_Netsweeper, Blocking_Entries_DNS, Blocking_Entries_Infoblox," +
                "Unblocking_Entries_Netsweeper, Unblocking_Entries_DNS, Unblocking_Entries_Infoblox," +
                "Status_Blocked_Netsweeper, Status_Blocked_DNS, Status_Blocked_Infoblox," +
                "Status_Unblocking_Netsweeper, Status_Unblocking_DNS, Status_Unblocking_Infoblox," +
                "Already_Exists_Entries_Netsweeper, Already_Exists_Entries_DNS, Already_Exists_Entries_Infoblox," +
                "Gap_Count," +
                "Gap_Count_Block_Netsweeper, Gap_Count_Unblock_Netsweeper," +
                "Gap_Count_Block_DNS, Gap_Count_Unblock_DNS," +
                "Gap_Count_Block_Infoblox, Gap_Count_Unblock_Infoblox," +
                "Total_Entries, " +
                "ns_lookup_failed_count, ns_lookup_failed_urls, ns_lookup_manual_impl_count, " +
                "ns_lookup_field_netsweeper, ns_lookup_field_dns, ns_lookup_field_infoblock)" +
                " VALUES (" + getPlaceholders(32) + ")";

        PreparedStatement ps = conn.prepareStatement(sql);

        int i = 1;

        ps.setString(i++, date);
        ps.setInt(i++, total_ticket_count);
        ps.setInt(i++, total_entries_received);

        ps.setInt(i++, bn);
        ps.setInt(i++, bd);
        ps.setInt(i++, bi);

        ps.setInt(i++, un);
        ps.setInt(i++, ud);
        ps.setInt(i++, ui);

        ps.setInt(i++, sbn);
        ps.setInt(i++, sbd);
        ps.setInt(i++, sbi);

        ps.setInt(i++, sun);
        ps.setInt(i++, sud);
        ps.setInt(i++, sui);

        ps.setInt(i++, aen);
        ps.setInt(i++, aed);
        ps.setInt(i++, aei);

        ps.setInt(i++, gapCount);

        ps.setInt(i++, gbn);
        ps.setInt(i++, gun);

        ps.setInt(i++, gbd);
        ps.setInt(i++, gud);

        ps.setInt(i++, gbi);
        ps.setInt(i++, gui);

        ps.setInt(i++, totalEntries);

        ps.setInt(i++, nsLookupFailedCount);
        ps.setString(i++, nsLookupFailedUrls == null ? "" : nsLookupFailedUrls);
        ps.setInt(i++, nsLookupManualImplCount);

        ps.setInt(i++, nsLookupFieldNetsweeper);
        ps.setInt(i++, nsLookupFieldDns);
        ps.setInt(i++, nsLookupFieldInfoblock);

        ps.executeUpdate();
        ps.close();
    }

    private String getPlaceholders(int count) {
        StringBuilder sb = new StringBuilder();
        for (int x = 0; x < count; x++) {
            if (x > 0) {
                sb.append(",");
            }
            sb.append("?");
        }
        return sb.toString();
    }

    private void log(String level, String message, String serverIP, String evtType) {

        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
            return;
        }

        String timestamp = LocalDateTime.now().format(logFormat);

        System.out.println("Time:" + timestamp +
                " [" + level + "] Process_Name:" + processName +
                " Process_Version:" + processVersion +
                " EVT_Type:" + evtType +
                " Description:" + message);
    }
}