package opd_parser_final;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;

/**
 * OPD_Source - Data Retrieval for OPD Type Classification
 *
 * Methods:
 *   1. truncateMonthData()        - DELETE rows from dest table for the whole month
 *                                   (1st of month → processDate) using SNO binary-search,
 *                                   then reset AUTO_INCREMENT.
 *   2. copySourceData()           - INSERT full month range from source → dest with
 *                                   OPD_TYPE = 'Natural OPD', using SNO binary-search.
 *   3. getDestSnoRange()          - Return [minSno, maxSno] of the just-inserted month
 *                                   block in the dest table (used by UPDATE rules).
 *   4. getMRDSetFromCamp()        - Fetch distinct MRD_NO from Camp_count_detail
 *                                   where opd_visit = 'Y' for the whole processMonth.
 *   5. getMRDSetFromScreening()   - Fetch distinct MRD_NO from Free_Screening_Count
 *                                   where opd_visit = 'Y' for the whole processMonth.
 *
 * All table scans use SNORangeFinder binary-search to narrow the range first,
 * so full-table scans are avoided even on large tables.
 */
public class OPD_Source {

    PrintUtilMsg pu = new PrintUtilMsg();

    // =========================================================================
    // BINARY SEARCH HELPER
    // =========================================================================

    /**
     * Returns [startSno, endSno] for a given date in the given table/column.
     *
     * @param dateColumn  Column name (e.g. "VISIT_DATE", "Registered_date")
     * @param dateValue   YYYY-MM-DD
     * @param trendType   "daily" or "monthly"
     * @param table       Table to probe
     * @return int[2] { startSno, endSno }
     */
    private int[] getSnoRange(String dateColumn, String dateValue, String trendType,
                              String table, String dbURL, String dbName, String dbPass) {
        try {
            int[] tableRange = new SNORangeFinder().getStartEndIdOfTable(table, dbURL, dbName, dbPass);
            if (tableRange == null) {
                pu.sop("getSnoRange", "WARN: could not read min/max sno from " + table + " - using 0/MAX_INT");
                return new int[]{0, Integer.MAX_VALUE};
            }
            return new SNORangeFinder().getStartAndEndId(
                    dateValue,
                    tableRange[0], tableRange[1],
                    table,
                    "DATE_FORMAT(" + dateColumn + ",'%Y-%m-%d')",
                    trendType,
                    "120",
                    false,
                    "sno",
                    dbURL, dbName, dbPass);
        } catch (Exception ex) {
            pu.sop("getSnoRange", "EXCEPTION: " + ex.toString());
            ex.printStackTrace();
            return new int[]{0, Integer.MAX_VALUE};
        }
    }

    // =========================================================================
    // 1. TRUNCATE MONTH DATA FROM DEST TABLE
    // =========================================================================

    /**
     * Delete ALL rows from the destination table that belong to the current
     * processing month (1st-of-month → processDate, inclusive) and reset
     * AUTO_INCREMENT so the sequence stays clean for the re-insert.
     *
     * SNO binary-search is used to constrain the DELETE to a tight sno window
     * instead of a full-table scan.
     *
     * @param destTable   Destination table (e.g. OPD_Tracker_frequency_daily)
     * @param processDate YYYY-MM-DD  (last date to include; delete from 1st)
     * @param dbURL       JDBC URL
     * @param dbName      DB username
     * @param dbPass      DB password
     * @return true on success, false on any error
     */
    public boolean truncateMonthData(String destTable, String processDate,
                                     String dbURL, String dbName, String dbPass) {

        Connection conn = null;
        Statement  stmt = null;

        try {
            String firstDay = processDate.substring(0, 7) + "-01";

            // Binary-search SNO range: left edge = 1st of month, right edge = processDate
            int[] snoStart = getSnoRange("VISIT_DATE", firstDay,    "daily", destTable, dbURL, dbName, dbPass);
            int[] snoEnd   = getSnoRange("VISIT_DATE", processDate, "daily", destTable, dbURL, dbName, dbPass);

            int snoMin = snoStart[0];
            int snoMax = snoEnd[1];

            pu.sop("truncateMonthData", "Month range     : " + firstDay + " to " + processDate);
            pu.sop("truncateMonthData", "SNO scan window : " + snoMin   + " to " + snoMax);

            // The sno window narrows the scan; the date predicates ensure we
            // never accidentally delete rows outside the target month window
            // even if SNO boundaries are slightly generous.
            String deleteQuery =
                "DELETE FROM " + destTable
                + " WHERE sno >= " + snoMin
                + " AND sno <= " + snoMax
                + " AND DATE_FORMAT(VISIT_DATE,'%Y-%m-%d') >= '" + firstDay    + "'"
                + " AND DATE_FORMAT(VISIT_DATE,'%Y-%m-%d') <= '" + processDate + "'";

            pu.sop("truncateMonthData", "DELETE query: " + deleteQuery);

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement();

            int deleted = stmt.executeUpdate(deleteQuery);
            conn.commit();

            pu.sop("truncateMonthData", "Rows deleted: " + deleted);

            // ── RESET AUTO_INCREMENT ────────────────────────────────────────
            String maxSnoQuery = "SELECT COALESCE(MAX(sno), 0) AS maxSno FROM " + destTable;
            ResultSet rs = stmt.executeQuery(maxSnoQuery);
            long maxSno = 0;
            if (rs.next()) {
                maxSno = rs.getLong("maxSno");
            }
            rs.close();

            long nextSno = maxSno + 1;
            stmt.executeUpdate("ALTER TABLE " + destTable + " AUTO_INCREMENT = " + nextSno);
            conn.commit();

            pu.sop("truncateMonthData", "AUTO_INCREMENT reset to: " + nextSno);

            return true;

        } catch (Exception ex) {
            pu.sop("truncateMonthData", "EXCEPTION: " + ex.toString());
            ex.printStackTrace();
            try { if (conn != null) conn.rollback(); } catch (Exception ignored) {}
            return false;
        } finally {
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }
    }

    // =========================================================================
    // 2. COPY SOURCE DATA → DEST  (OPD_TYPE = 'Natural OPD')
    // =========================================================================

    /**
     * INSERT rows from sourceTable for the entire current month
     * (1st-of-month → processDate) into destTable with OPD_TYPE = 'Natural OPD'.
     *
     * The dest table has exactly the same columns as the source table plus one
     * extra column: OPD_TYPE.  Every row gets 'Natural OPD' as default; the 12
     * classification UPDATE rules will overwrite it in priority order.
     *
     * Binary-search on the source table keeps the scan tight.
     * Rows are inserted ORDER BY VISIT_DATE so the dest table is pre-sorted.
     *
     * @param sourceTable  Patient_Type_Age_wise_final (or equivalent)
     * @param destTable    OPD_Tracker_frequency_daily  (or equivalent)
     * @param processDate  YYYY-MM-DD
     * @param processMonth YYYY-MM
     * @param dbURL        JDBC URL
     * @param dbName       DB username
     * @param dbPass       DB password
     * @return rows inserted (>=0), or -1 on error
     */
    public int copySourceData(String sourceTable, String destTable,
                              String processDate, String processMonth,
                              String dbURL, String dbName, String dbPass) {

        Connection conn = null;
        Statement  stmt = null;
        int rowCount    = -1;

        try {
            String firstDay = processMonth + "-01";

            // ── SNO range on source table ───────────────────────────────────
            int[] snoLeft  = getSnoRange("VISIT_DATE", firstDay,    "daily", sourceTable, dbURL, dbName, dbPass);
            int[] snoRight = getSnoRange("VISIT_DATE", processDate, "daily", sourceTable, dbURL, dbName, dbPass);

            int snoMin = snoLeft[0];
            int snoMax = snoRight[1];

            pu.sop("copySourceData", "Source SNO range : " + snoMin + " to " + snoMax);
            pu.sop("copySourceData", "Month range      : " + firstDay + " to " + processDate);

            // ── INSERT with OPD_TYPE = 'Natural OPD' ───────────────────────
            String insertSql =
                "INSERT INTO " + destTable + " ("
                + "PATIENT_TYPE, REGISTERED_DATE, APPOINTMENT_TYPE, MRD_NO, PATIENT_NAME,"
                + "LOCATION_CODE, LOCATION, AGE, SEX, SPECIALITY_CODE, SPECIALITY_NAME,"
                + "TREATMENT_CODE, TREATMENT_NAME, ADDRESS1, ADDRESS2, AREA, WARD, CITY,"
                + "PINCODE, state, country, DOCTOR_CD, DOCTOR_NAME, OPTO_NAME,"
                + "APPOINTMENT_SOURCE, PATIENT_CATEGORY, PATIENT_SUBCATEGORY,"
                + "APPT_CreatedUser, REG_CreatedUser, VISIT_DATE, REGISTERED_DATETIME,"
                + "REMARKS, MAILID, MAXID, PatientDepartment, PatientDepartmentName,"
                + "CONTACT_DETAILS, CONTACT_DETAILS1, source, facebook_status,"
                + "google_adwords_status, website_status, whatsapp_status, campaign_name,"
                + "cid, campaign_list, rule_name, filter_query, LOCATION_NAME, cluster,"
                + "Region, OPD_TYPE"
                + ") "
                + "SELECT "
                + "PATIENT_TYPE, REGISTERED_DATE, APPOINTMENT_TYPE, MRD_NO, PATIENT_NAME,"
                + "LOCATION_CODE, LOCATION, AGE, SEX, SPECIALITY_CODE, SPECIALITY_NAME,"
                + "TREATMENT_CODE, TREATMENT_NAME, ADDRESS1, ADDRESS2, AREA, WARD, CITY,"
                + "PINCODE, state, country, DOCTOR_CD, DOCTOR_NAME, OPTO_NAME,"
                + "APPOINTMENT_SOURCE, PATIENT_CATEGORY, PATIENT_SUBCATEGORY,"
                + "APPT_CreatedUser, REG_CreatedUser,"
                + "STR_TO_DATE(VISIT_DATE,'%Y-%m-%d') AS VISIT_DATE,"
                + "REGISTERED_DATETIME,"
                + "REMARKS, MAILID, MAXID, PatientDepartment, PatientDepartmentName,"
                + "CONTACT_DETAILS, CONTACT_DETAILS1, source, facebook_status,"
                + "google_adwords_status, website_status, whatsapp_status, campaign_name,"
                + "cid, campaign_list, rule_name, filter_query, LOCATION_NAME, cluster,"
                + "Region, 'Natural OPD' AS OPD_TYPE "
                + "FROM " + sourceTable + " "
                + "WHERE sno >= " + snoMin + " AND sno <= " + snoMax + " "
                + "AND STR_TO_DATE(VISIT_DATE,'%Y-%m-%d') IS NOT NULL "
                + "AND DATE_FORMAT(VISIT_DATE,'%Y-%m-%d') >= '" + firstDay    + "' "
                + "AND DATE_FORMAT(VISIT_DATE,'%Y-%m-%d') <= '" + processDate + "' "
                + "AND APPOINTMENT_TYPE = 'REG' "
                + "ORDER BY STR_TO_DATE(VISIT_DATE,'%Y-%m-%d')";

            pu.sop("copySourceData", "INSERT executing...");

            conn     = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt     = conn.createStatement();
            rowCount = stmt.executeUpdate(insertSql);
            conn.commit();

            pu.sop("copySourceData", "Rows inserted: " + rowCount);

        } catch (Exception ex) {
            pu.sop("copySourceData", "EXCEPTION: " + ex.toString());
            ex.printStackTrace();
            rowCount = -1;
            try { if (conn != null) conn.rollback(); } catch (Exception ignored) {}
        } finally {
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }

        return rowCount;
    }

    // =========================================================================
    // 3. GET DEST TABLE SNO RANGE  (used by UPDATE rules for fast scans)
    // =========================================================================

    /**
     * After copySourceData() completes, call this to get the tight SNO window
     * for the month block that was just inserted into destTable.
     *
     * All 12 UPDATE rules add "AND sno >= X AND sno <= Y" to their WHERE clause
     * using these values, so MySQL can skip rows outside the window and avoid
     * full-table scans on the destination table.
     *
     * @param destTable   Destination table
     * @param processDate YYYY-MM-DD
     * @param dbURL       JDBC URL
     * @param dbName      DB username
     * @param dbPass      DB password
     * @return int[2] { snoMin, snoMax }; returns {0, MAX_INT} on error
     */
    public int[] getDestSnoRange(String destTable, String processDate,
                                 String dbURL, String dbName, String dbPass) {
        try {
            String firstDay = processDate.substring(0, 7) + "-01";

            int[] snoLeft  = getSnoRange("VISIT_DATE", firstDay,    "daily", destTable, dbURL, dbName, dbPass);
            int[] snoRight = getSnoRange("VISIT_DATE", processDate, "daily", destTable, dbURL, dbName, dbPass);

            int snoMin = snoLeft[0];
            int snoMax = snoRight[1];

            pu.sop("getDestSnoRange", "Dest SNO range: " + snoMin + " to " + snoMax);
            return new int[]{snoMin, snoMax};

        } catch (Exception ex) {
            pu.sop("getDestSnoRange", "EXCEPTION: " + ex.toString());
            ex.printStackTrace();
            return new int[]{0, Integer.MAX_VALUE};
        }
    }

    // =========================================================================
    // 4. GET MRD SET FROM CAMP  (opd_visit = 'Y')
    // =========================================================================

    /**
     * Fetch distinct MRD_NO values from Camp_count_detail for the WHOLE
     * processMonth (1st → processDate) where opd_visit = 'Y'.
     * Used to tag Camp (Out Reach) OPD type.
     *
     * Binary-search on Registered_date narrows the scan range.
     *
     * @param campTable    Camp_count_detail (or equivalent)
     * @param processDate  YYYY-MM-DD
     * @param processMonth YYYY-MM
     * @param dbURL        JDBC URL
     * @param dbName       DB username
     * @param dbPass       DB password
     * @return HashSet of uppercase-trimmed MRD_NO values; empty set on error
     */
    public HashSet<String> getMRDSetFromCamp(String campTable,
                                              String processDate, String processMonth,
                                              String dbURL, String dbName, String dbPass) {

        HashSet<String> mrdSet = new HashSet<String>();
        Connection conn = null;
        Statement  stmt = null;
        ResultSet  rs   = null;

        try {
            String firstDay = processMonth + "-01";

            int[] snoLeft  = getSnoRange("Registered_date", firstDay,    "daily", campTable, dbURL, dbName, dbPass);
            int[] snoRight = getSnoRange("Registered_date", processDate, "daily", campTable, dbURL, dbName, dbPass);

            int snoMin = snoLeft[0];
            int snoMax = snoRight[1];

            pu.sop("getMRDSetFromCamp", "SNO range: " + snoMin + " to " + snoMax);

            String query =
                "SELECT TRIM(UPPER(MRD_NO)) AS MRD_NO"
                + " FROM " + campTable
                + " WHERE sno >= " + snoMin + " AND sno <= " + snoMax
                + "   AND DATE_FORMAT(Registered_date,'%Y-%m') = '" + processMonth + "'"
                + "   AND UPPER(TRIM(opd_visit)) = 'Y'"
                + "   AND MRD_NO IS NOT NULL AND TRIM(MRD_NO) != ''";

            pu.sop("getMRDSetFromCamp", "Query: " + query);

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs   = stmt.executeQuery(query);

            rs.last();
            int total = rs.getRow();
            rs.beforeFirst();
            pu.sop("getMRDSetFromCamp", "Total camp MRD records found: " + total);

            while (rs.next()) {
                String mrd = rs.getString("MRD_NO");
                if (mrd != null && !mrd.trim().isEmpty()) {
                    mrdSet.add(mrd.trim().toUpperCase());
                }
            }

        } catch (Exception ex) {
            pu.sop("getMRDSetFromCamp", "EXCEPTION: " + ex.toString());
            ex.printStackTrace();
        } finally {
            try { if (rs   != null) rs.close();   } catch (Exception ignored) {}
            try { if (stmt != null) stmt.close();  } catch (Exception ignored) {}
            try { if (conn != null) conn.close();  } catch (Exception ignored) {}
        }

        return mrdSet;
    }

    // =========================================================================
    // 5. GET MRD SET FROM FREE SCREENING  (opd_visit = 'Y')
    // =========================================================================

    /**
     * Fetch distinct MRD_NO values from Free_Screening_Count for the WHOLE
     * processMonth (1st → processDate) where opd_visit = 'Y'.
     * Used to tag Attendant Activity OPD type.
     *
     * Binary-search on Registered_date narrows the scan range.
     *
     * @param screeningTable  Free_Screening_Count (or equivalent)
     * @param processDate     YYYY-MM-DD
     * @param processMonth    YYYY-MM
     * @param dbURL           JDBC URL
     * @param dbName          DB username
     * @param dbPass          DB password
     * @return HashSet of uppercase-trimmed MRD_NO values; empty set on error
     */
    public HashSet<String> getMRDSetFromScreening(String screeningTable,
                                                   String processDate, String processMonth,
                                                   String dbURL, String dbName, String dbPass) {

        HashSet<String> mrdSet = new HashSet<String>();
        Connection conn = null;
        Statement  stmt = null;
        ResultSet  rs   = null;

        try {
            String firstDay = processMonth + "-01";

            int[] snoLeft  = getSnoRange("Registered_date", firstDay,    "daily", screeningTable, dbURL, dbName, dbPass);
            int[] snoRight = getSnoRange("Registered_date", processDate, "daily", screeningTable, dbURL, dbName, dbPass);

            int snoMin = snoLeft[0];
            int snoMax = snoRight[1];

            pu.sop("getMRDSetFromScreening", "SNO range: " + snoMin + " to " + snoMax);

            String query =
                "SELECT TRIM(UPPER(MRD_NO)) AS MRD_NO"
                + " FROM " + screeningTable
                + " WHERE sno >= " + snoMin + " AND sno <= " + snoMax
                + "   AND DATE_FORMAT(Registered_date,'%Y-%m') = '" + processMonth + "'"
                + "   AND UPPER(TRIM(opd_visit)) = 'Y'"
                + "   AND MRD_NO IS NOT NULL AND TRIM(MRD_NO) != ''";

            pu.sop("getMRDSetFromScreening", "Query: " + query);

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs   = stmt.executeQuery(query);

            rs.last();
            int total = rs.getRow();
            rs.beforeFirst();
            pu.sop("getMRDSetFromScreening", "Total screening MRD records found: " + total);

            while (rs.next()) {
                String mrd = rs.getString("MRD_NO");
                if (mrd != null && !mrd.trim().isEmpty()) {
                    mrdSet.add(mrd.trim().toUpperCase());
                }
            }

        } catch (Exception ex) {
            pu.sop("getMRDSetFromScreening", "EXCEPTION: " + ex.toString());
            ex.printStackTrace();
        } finally {
            try { if (rs   != null) rs.close();   } catch (Exception ignored) {}
            try { if (stmt != null) stmt.close();  } catch (Exception ignored) {}
            try { if (conn != null) conn.close();  } catch (Exception ignored) {}
        }

        return mrdSet;
    }
}
