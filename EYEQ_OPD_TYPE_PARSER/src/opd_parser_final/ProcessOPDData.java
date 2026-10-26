package opd_parser_final;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashSet;

/**
 * ProcessOPDData - OPD Type Classification Engine
 *
 * Processing Flow
 * ──────────────
 *  STEP 1  Truncate (DELETE) the dest table for the whole month
 *          (1st-of-month → processDate) using SNO binary-search + date guard.
 *          AUTO_INCREMENT is reset after the delete.
 *
 *  STEP 2  Copy the full month range from the source table into the dest table
 *          with OPD_TYPE = 'Natural OPD' as the default for every row.
 *          Rows are inserted ORDER BY VISIT_DATE.
 *
 *  STEP 3  Load MRD HashSets from Camp_count_detail and Free_Screening_Count
 *          for the whole processMonth (where opd_visit = 'Y').
 *
 *  STEP 4  Fetch the SNO range of the just-inserted month block in the dest
 *          table (one binary-search, result reused by all 12 UPDATE rules).
 *
 *  STEP 5  Apply 12 UPDATE rules IN ORDER (lowest priority first, highest last)
 *          inside a single transaction.  Because each rule is a plain UPDATE
 *          with no "WHERE OPD_TYPE = ..." guard, later rules OVERWRITE earlier
 *          ones — which is exactly the intended "last-write-wins" priority.
 *
 * Rule priority (applied top → bottom; bottom = highest priority):
 *   Rule  1 → Natural OPD        (fallback – already set at copy time, but
 *                                  also re-applied here for any NULL stragglers)
 *   Rule  2 → New OPD DGHS       (PATIENT_CATEGORY = 'DGHS')
 *   Rule  3 → New OPD CGHS       (PATIENT_CATEGORY = 'CGHS')
 *   Rule  4 → New OPD ECHS       (PATIENT_CATEGORY = 'EX SERVICEMAN')
 *   Rule  5 → Senior Citizen     (MARKETING ACTIVITY / SENIOR CITIZEN ACTIVITY)
 *   Rule  6 → Corporate OPD      (CORPORATE (CASH) or CORPORATE (CASHLESS))
 *   Rule  7 → Referral           (MARKETING ACTIVITY / referral subcategories)
 *   Rule  8 → Surgical Coupon    (MARKETING ACTIVITY / SURGICAL COUPON PATIENTS)
 *   Rule  9 → Camp (In House)    (PATIENT_CATEGORY = 'CAMP PATIENT')
 *   Rule 10 → Camp (Out Reach)   (MRD in campSet OR PATIENT_CATEGORY = 'OUT - REACH CAMP')
 *   Rule 11 → Attendant Activity (MRD in screeningSet)
 *   Rule 12 → Old OPD            (APPOINTMENT_TYPE = 'REG' AND PATIENT_TYPE != 'NEW')
 *                                  � highest priority; wins over everything
 *
 * All UPDATE WHERE clauses include  "AND sno >= X AND sno <= Y"  so MySQL can
 * use the sno index to skip rows outside the current month window.
 */
public class ProcessOPDData {

    PrintUtilMsg pu = new PrintUtilMsg();

    // Subcategories that map to "Referral" OPD type
    private static final String[] REFERRAL_SUBCATEGORIES = {
        "REFERRAL ARMY CLINIC",
        "MAX ATTENDENT ACTIVITIES",
        "LASIK REFERRAL PROGRAM",
        "REFERENCE FROM AGENT",
        "INVITATION CARD ACTIVITY",
        "IN CENTER COUPON ACTIVITY",
        "REFERRAL THROUGH DOCTORS",
        "REFERRAL MARKETING",
        "REFERRAL FROM OPTICAL"
    };

    // =========================================================================
    // MAIN ENTRY POINT
    // =========================================================================

    public boolean processOPDData(String tableSource, String tableDest,
                                   String tableCamp,   String tableScreening,
                                   String processDate, String processMonth,
                                   String dbURL, String dbName, String dbPass) {

        Connection conn = null;
        Statement  stmt = null;
        boolean    status = false;

        try {

            // ─────────────────────────────────────────────────────────────────
            // STEP 1 : TRUNCATE DEST TABLE FOR THE WHOLE MONTH
            // ─────────────────────────────────────────────────────────────────
            pu.sop("processOPDData", "===== STEP 1: Truncate month data from dest table =====");

            boolean truncated = new OPD_Source().truncateMonthData(
                    tableDest, processDate, dbURL, dbName, dbPass);

            if (!truncated) {
                // Not fatal — dest might genuinely be empty for this month yet.
                // Log and continue; the insert will still work.
                pu.sop("processOPDData", "WARN: truncate returned false (table may already be empty for this month)");
            } else {
                pu.sop("processOPDData", "Month data truncated successfully");
            }

            // ─────────────────────────────────────────────────────────────────
            // STEP 2 : COPY FULL MONTH FROM SOURCE → DEST (OPD_TYPE = 'Natural OPD')
            // ─────────────────────────────────────────────────────────────────
            pu.sop("processOPDData", "===== STEP 2: Copy full month data from source table =====");

            int copied = new OPD_Source().copySourceData(
                    tableSource, tableDest, processDate, processMonth,
                    dbURL, dbName, dbPass);

            if (copied < 0) {
                pu.sop("processOPDData", "ERROR: copy failed - aborting");
                return false;
            }
            if (copied == 0) {
                pu.sop("processOPDData", "No source data found for month ending: " + processDate);
                return true;  // nothing to classify - not an error
            }
            pu.sop("processOPDData", "Rows copied: " + copied);

            // ─────────────────────────────────────────────────────────────────
            // STEP 3 : LOAD MRD SETS FROM CAMP & FREE SCREENING
            // ─────────────────────────────────────────────────────────────────
            pu.sop("processOPDData", "===== STEP 3: Load MRD sets =====");

            HashSet<String> campMRDSet = new OPD_Source().getMRDSetFromCamp(
                    tableCamp, processDate, processMonth, dbURL, dbName, dbPass);

            HashSet<String> screeningMRDSet = new OPD_Source().getMRDSetFromScreening(
                    tableScreening, processDate, processMonth, dbURL, dbName, dbPass);

            pu.sop("processOPDData", "Camp MRD count      : " + campMRDSet.size());
            pu.sop("processOPDData", "Screening MRD count : " + screeningMRDSet.size());

            // ─────────────────────────────────────────────────────────────────
            // STEP 4 : GET DEST SNO RANGE  (used by all UPDATE rules below)
            // ─────────────────────────────────────────────────────────────────
            pu.sop("processOPDData", "===== STEP 4: Fetch dest-table SNO range =====");

            int[] destSno = new OPD_Source().getDestSnoRange(
                    tableDest, processDate, dbURL, dbName, dbPass);

            int destSnoMin = destSno[0];
            int destSnoMax = destSno[1];

            pu.sop("processOPDData", "Dest SNO window: " + destSnoMin + " to " + destSnoMax);

            // ─────────────────────────────────────────────────────────────────
            // STEP 5 : APPLY 12 CLASSIFICATION RULES IN A SINGLE TRANSACTION
            // ─────────────────────────────────────────────────────────────────
            pu.sop("processOPDData", "===== STEP 5: Apply classification rules =====");

            // Pre-build IN-clause strings
            String campInClause      = campMRDSet.isEmpty()      ? "'__NO_MATCH__'" : buildInClause(campMRDSet);
            String screeningInClause = screeningMRDSet.isEmpty() ? "'__NO_MATCH__'" : buildInClause(screeningMRDSet);
            String referralInClause  = buildStringInClause(REFERRAL_SUBCATEGORIES);

            // SNO guard snippet (prepended to every WHERE clause for speed)
            // The date predicates are the authoritative filter; SNO just narrows the scan.
            String snoGuard = " sno >= " + destSnoMin + " AND sno <= " + destSnoMax + " AND ";

            // Date range for the month block
            String firstDay   = processMonth + "-01";
            String dateFrom   = firstDay    + " 00:00:00";

            // End = processDate + 1 day at midnight  (so VISIT_DATE < endDate covers processDate fully)
            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            java.util.Date pdDate = sdf.parse(processDate);
            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.setTime(pdDate);
            cal.add(java.util.Calendar.DATE, 1);
            String dateTo = sdf.format(cal.getTime()) + " 00:00:00";

            // Date guard snippet (date range applied in every WHERE)
            String dateGuard = " VISIT_DATE >= '" + dateFrom + "' AND VISIT_DATE < '" + dateTo + "' ";

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            conn.setAutoCommit(false);
            stmt = conn.createStatement();

            int totalUpdated = 0;

            // ── RULE 1 : Natural OPD (NULL fallback) ──────────────────────────
            // Ensures any row that somehow has NULL OPD_TYPE gets a value.
            // All other rows already have 'Natural OPD' from the INSERT.
//            totalUpdated += runUpdate(stmt, "Rule 1 - Natural OPD (NULL fallback)",
//                "UPDATE " + tableDest + " SET OPD_TYPE = 'Natural OPD' "
//                + "WHERE" + snoGuard + dateGuard
//                + "AND OPD_TYPE IS NULL");

            // ── RULE 2 : New OPD DGHS ─────────────────────────────────────────
            totalUpdated += runUpdate(stmt, "Rule 2 - New OPD DGHS",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'New OPD DGHS' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(PATIENT_CATEGORY)) = 'DGHS'");

            // ── RULE 3 : New OPD CGHS ─────────────────────────────────────────
            totalUpdated += runUpdate(stmt, "Rule 3 - New OPD CGHS",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'New OPD CGHS' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(PATIENT_CATEGORY)) = 'CGHS'");

            // ── RULE 4 : New OPD ECHS ─────────────────────────────────────────
            totalUpdated += runUpdate(stmt, "Rule 4 - New OPD ECHS",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'New OPD ECHS' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(PATIENT_CATEGORY)) = 'EX SERVICEMAN'");

            // ── RULE 5 : Senior Citizen Activity ──────────────────────────────
            totalUpdated += runUpdate(stmt, "Rule 5 - Senior Citizen Activity",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'Senior Citizen Activity' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(PATIENT_CATEGORY))    = 'MARKETING ACTIVITY' "
                + "AND UPPER(TRIM(PATIENT_SUBCATEGORY)) = 'SENIOR CITIZEN ACTIVITY'");

            // ── RULE 6 : Corporate OPD ────────────────────────────────────────
            totalUpdated += runUpdate(stmt, "Rule 6 - Corporate OPD",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'Corporate OPD' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(PATIENT_CATEGORY)) IN ('CORPORATE (CASH)','CORPORATE (CASHLESS)')");

            // ── RULE 7 : Referral ─────────────────────────────────────────────
            totalUpdated += runUpdate(stmt, "Rule 7 - Referral",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'Referral' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(PATIENT_CATEGORY))    = 'MARKETING ACTIVITY' "
                + "AND UPPER(TRIM(PATIENT_SUBCATEGORY)) IN (" + referralInClause + ")");

            // ── RULE 8 : Surgical Coupon ──────────────────────────────────────
            totalUpdated += runUpdate(stmt, "Rule 8 - Surgical Coupon",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'Surgical Coupon' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(PATIENT_CATEGORY))    = 'MARKETING ACTIVITY' "
                + "AND UPPER(TRIM(PATIENT_SUBCATEGORY)) = 'SURGICAL COUPON PATIENTS'");

            // ── RULE 9 : Camp (In House) ──────────────────────────────────────
            totalUpdated += runUpdate(stmt, "Rule 9 - Camp (In House)",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'Camp (In House)' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(PATIENT_CATEGORY)) = 'CAMP PATIENT'");

            // ── RULE 10 : Camp (Out Reach) ────────────────────────────────────
            // Matches rows whose MRD_NO is in the campSet loaded from
            // Camp_count_detail (opd_visit='Y'), OR whose PATIENT_CATEGORY is
            // 'OUT - REACH CAMP'.  This intentionally overwrites Camp (In House)
            // because Out Reach has higher priority.
            totalUpdated += runUpdate(stmt, "Rule 10 - Camp (Out Reach)",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'Camp (Out Reach)' "
                + "WHERE" + snoGuard + dateGuard
                + "AND (UPPER(TRIM(MRD_NO)) IN (" + campInClause + ") "
                + "     OR UPPER(TRIM(PATIENT_CATEGORY)) = 'OUT - REACH CAMP')");

            // ── RULE 11 : Attendant Activity ──────────────────────────────────
            // Matches rows whose MRD_NO is in the screeningSet loaded from
            // Free_Screening_Count (opd_visit='Y').
            totalUpdated += runUpdate(stmt, "Rule 11 - Attendant Activity",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'Attendant Activity' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(MRD_NO)) IN (" + screeningInClause + ")");

            // ── RULE 12 : Old OPD  (HIGHEST PRIORITY – applied last) ──────────
            // Any repeat patient (PATIENT_TYPE != 'NEW') with APPOINTMENT_TYPE='REG'
            // gets labelled Old OPD regardless of every other rule above.
            totalUpdated += runUpdate(stmt, "Rule 12 - Old OPD (highest priority)",
                "UPDATE " + tableDest + " SET OPD_TYPE = 'Old OPD' "
                + "WHERE" + snoGuard + dateGuard
                + "AND UPPER(TRIM(APPOINTMENT_TYPE)) = 'REG' "
                + "AND UPPER(TRIM(PATIENT_TYPE))     != 'NEW'");

            conn.commit();

            pu.sop("processOPDData", "===== ALL RULES APPLIED =====");
            pu.sop("processOPDData", "Total rows updated across all rules: " + totalUpdated);

            status = true;

        } catch (Exception ex) {
            pu.sop("processOPDData", "EXCEPTION: " + ex.toString());
            ex.printStackTrace();
            try { if (conn != null) conn.rollback(); } catch (Exception ignored) {}
            status = false;
        } finally {
            try { if (stmt != null) stmt.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }

        return status;
    }

    // =========================================================================
    // HELPERS
    // =========================================================================

    /**
     * Execute a single UPDATE, log the label / query / rows-affected / time.
     */
    private int runUpdate(Statement stmt, String label, String query) throws Exception {
        long start = System.currentTimeMillis();
        pu.sop("OPD_RULE", "[" + label + "] START");
        pu.sop("OPD_RULE", "QUERY: " + query);
        int updated = stmt.executeUpdate(query);
        long ms = System.currentTimeMillis() - start;
        pu.sop("OPD_RULE", "[" + label + "] Rows: " + updated + " | Time: " + ms + " ms");
        return updated;
    }

    /**
     * Build a comma-separated 'val1','val2',... string from a HashSet.
     * Single quotes inside values are escaped by doubling them.
     */
    private String buildInClause(HashSet<String> values) {
        StringBuilder sb = new StringBuilder();
        for (String v : values) {
            if (sb.length() > 0) sb.append(",");
            sb.append("'").append(v.replace("'", "''")).append("'");
        }
        return sb.toString();
    }

    /**
     * Build a comma-separated 'VAL1','VAL2',... string from a String array.
     * Values are upper-cased and single quotes are escaped.
     */
    private String buildStringInClause(String[] values) {
        StringBuilder sb = new StringBuilder();
        for (String v : values) {
            if (sb.length() > 0) sb.append(",");
            sb.append("'").append(v.toUpperCase().replace("'", "''")).append("'");
        }
        return sb.toString();
    }
}
