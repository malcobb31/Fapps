package recon_detail_v1;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class recon_details_methods {

    private Connection conn;
    private String tableName;
    private String date;

    private String ipmTable, ipmSnoColumn, ipmDateColumn;
    private String urlTable, urlSnoColumn, urlDateColumn;

    private String dbUrl, dbUser, dbPass;

    //  RANGE (IMPORTANT FIX)
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

        //  STORE RANGE (IMPORTANT)
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
        	    "select count(*) from " + "urlScan.master_url_details" +
        	    " where status in ('Blocked','Block')"); // 06-07-2026 CHANGE
        
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

        // ================= GAP =================

        
//        int gbn = bn - sbn - aen;
//        int gbd = bd - sbd - aed;
//        int gbi = bi - sbi - aei;
        int gbn = bn - sbn - aen;
        int gun = un - sun;
        
        int gbd = bd - sbd - aed;
        int gud = ud - sud;

        int gbi = bi - sbi - aei;
        int gui = ui - sui;

        int gapCount = gbn + gun + gbd + gud + gbi + gui;

        int totalEntries = total_entries_received;

        log("DEBUG", "Summary Gap:" + gapCount + " Total:" + totalEntries, "", "SUMMARY");

        upsert(total_ticket_count, total_entries_received,
                bn, bd, bi,
                un, ud, ui,
                sbn, sbd, sbi,
                sun, sud, sui,
                aen, aed, aei,
                0,
                gbn, gun, gbd, gud, gbi, gui,
                0);

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

    private int getCount(String query) throws Exception {

        log("DEBUG", query, "", "QUERY");

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) return rs.getInt(1);
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
            int totalEntries
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
    		    "Total_Entries)" +
    		    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//        String sql = "INSERT INTO " + tableName +
//                " (date, Total_tickets_Count, Total_Entries_received," +
//                "Blocking_Entries_Netsweeper, Blocking_Entries_DNS, Blocking_Entries_Infoblock," +
//                "Unblocking_Entries_Netsweeper, Unblocking_Entries_DNS, Unblocking_Entries_Infoblock," +
//                "Status_Blocked_Netsweeper, Status_Blocked_DNS, Status_Blocked_InfoBlock," +
//                "Status_Unblocking_Netsweeper, Status_Unblocking_DNS, Status_Unblocking_Infoblock," +
//                "Already_Exists_Entries_Netsweeper, Already_Exists_Entries_DNS, Already_Exists_Entries_Infoblock," +
//                "Gap_Count, Gap_Count_Block_Netsweeper, Gap_Count_Block_Netsweeper," +
//                "Gap_Count_Block_DNS, Gap_Count_Unblock_DNS, Gap_Count_Block_Infoblock, Gap_Count_Unblock_Infoblock, Total_Entries)" +
//                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

        PreparedStatement ps = conn.prepareStatement(sql);

        int i = 1;

        ps.setString(i++, date);
        ps.setInt(i++, total_ticket_count);
        ps.setInt(i++, total_entries_received);

        ps.setInt(i++, bn); ps.setInt(i++, bd); ps.setInt(i++, bi);
        ps.setInt(i++, un); ps.setInt(i++, ud); ps.setInt(i++, ui);

        ps.setInt(i++, sbn); ps.setInt(i++, sbd); ps.setInt(i++, sbi);
        ps.setInt(i++, sun); ps.setInt(i++, sud); ps.setInt(i++, sui);

        ps.setInt(i++, aen); ps.setInt(i++, aed); ps.setInt(i++, aei);

        ps.setInt(i++, gapCount);

        ps.setInt(i++, gbn); ps.setInt(i++, gun);
        ps.setInt(i++, gbd); ps.setInt(i++, gud);
        ps.setInt(i++, gbi); ps.setInt(i++, gui);

        ps.setInt(i++, totalEntries);

        ps.executeUpdate();
        ps.close();
    }

    private void log(String level, String message, String serverIP, String evtType) {

        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(logFormat);

        System.out.println("Time:" + timestamp +
                " [" + level + "] Process_Name:" + processName +
                " Process_Version:" + processVersion +
                " EVT_Type:" + evtType +
                " Description:" + message);
    }
}