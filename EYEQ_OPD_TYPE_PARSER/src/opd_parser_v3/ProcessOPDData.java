package opd_parser_v3;

/**
 * ProcessOPDData - Main Processing Logic for OPD Classification
 *
 * Mirrors ProcessTCNData architecture exactly:
 *   - One entry method: processOPDData()
 *   - Delegates ALL data-access to OPD_Source
 *   - Runs rules 0-11 in strict sequence via labelled steps
 *   - Logs outcome (SUCCESS / FAILED) for every step
 *   - Returns boolean overall status to the caller (OPD_Main)
 *
 * Business Logic Flow:
 * ─────────────────────────────────────────────────────────────────────────
 * STEP 1  Delete current-month data from copy table + reset AUTO_INCREMENT
 * STEP 2  RULE-0  : Initialise all rows  → 'Natural OPD'  (mandatory first)
 * STEP 3  RULE-1  : Reg + non-New        → 'Old OPD'
 * STEP 4  RULE-2  : Free Screening JOIN  → 'Attendant Activity'
 * STEP 5  RULE-3  : Camp / OUT-REACH     → 'Camp (Out Reach)'
 * STEP 6  RULE-4  : Camp Patient         → 'Camp (In House)'
 * STEP 7  RULE-5  : Surgical Coupon      → 'Surgical Coupon'
 * STEP 8  RULE-6  : Referral sub-cats    → 'Referral'
 * STEP 9  RULE-7  : Corporate            → 'Corporate OPD'
 * STEP 10 RULE-8  : Senior Citizen       → 'Senior Citizen Activity'
 * STEP 11 RULE-9  : EX SERVICEMAN        → 'New OPD ECHS'
 * STEP 12 RULE-10 : CGHS                 → 'New OPD CGHS'
 * STEP 13 RULE-11 : DGHS                 → 'New OPD DGHS'
 * ─────────────────────────────────────────────────────────────────────────
 */
public class ProcessOPDData {

    PrintUtilMsg pu = new PrintUtilMsg();

    /**
     * Entry point called from OPD_Main.
     *
     * @param copyTable          Patient_Type_Age_wise_final_copy
     * @param campTable          Camp_count_detail
     * @param freeScreeningTable Free_Screening_Count
     * @param processDate        YYYY-MM-DD  (D-1)
     * @param processMonth       YYYY-MM
     * @param dbURL              JDBC URL
     * @param dbName             DB username
     * @param dbPass             DB password
     * @return true if every step completed without error
     */
    public boolean processOPDData(String copyTable, String campTable, String freeScreeningTable,
                                   String processDate, String processMonth,
                                   String dbURL, String dbName, String dbPass) {

        boolean overallStatus = true;
        OPD_Source src = new OPD_Source();
        pu.sop("processOPDData", "===== STEP 1: DELETE OLD DATA =====");

        boolean deleted = src.deleteMonthData(
                copyTable,
                processMonth,
                dbURL, dbName, dbPass
        );

        overallStatus &= deleted;
        pu.sop("processOPDData", "===== STEP 1: Copy Source Data =====");

        boolean copied = src.copyDataForDate(
                "Patient_Type_Age_wise_final",
                copyTable,
                processMonth,
                processDate,
                dbURL, dbName, dbPass
        );

        if (!copied) {
            pu.sop("processOPDData", "COPY FAILED � STOPPING PROCESS");
            return false;
        }
        
        
//        // ── STEP 3: RULE-1 — Old──────────────────────────────────────
//        pu.sop("processOPDData", "===== STEP 3: RULE-1  | Old OPD =====");
//        overallStatus &= step("RULE-1  | Old OPD",
//                src.applyOldOpd(copyTable, processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 4: RULE-2 — Attendant Activity ──────────────────────────────
//        pu.sop("processOPDData", "===== STEP 4: RULE-2  | Attendant Activity =====");
//        overallStatus &= step("RULE-2  | Attendant Activity",
//                src.applyAttendantActivity(copyTable, freeScreeningTable,
//                        processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 5: RULE-3 — Camp (Out Reach) ────────────────────────────────
//        pu.sop("processOPDData", "===== STEP 5: RULE-3  | Camp (Out Reach) =====");
//        overallStatus &= step("RULE-3  | Camp (Out Reach)",
//                src.applyCampOutReach(copyTable, campTable,
//                        processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 6: RULE-4 — Camp (In House) ─────────────────────────────────
//        pu.sop("processOPDData", "===== STEP 6: RULE-4  | Camp (In House) =====");
//        overallStatus &= step("RULE-4  | Camp (In House)",
//                src.applyCampInHouse(copyTable, processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 7: RULE-5 — Surgical Coupon ─────────────────────────────────
//        pu.sop("processOPDData", "===== STEP 7: RULE-5  | Surgical Coupon =====");
//        overallStatus &= step("RULE-5  | Surgical Coupon",
//                src.applySurgicalCoupon(copyTable, processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 8: RULE-6 — Referral ────────────────────────────────────────
//        pu.sop("processOPDData", "===== STEP 8: RULE-6  | Referral =====");
//        overallStatus &= step("RULE-6  | Referral",
//                src.applyReferral(copyTable, processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 9: RULE-7 — Corporate OPD ───────────────────────────────────
//        pu.sop("processOPDData", "===== STEP 9: RULE-7  | Corporate OPD =====");
//        overallStatus &= step("RULE-7  | Corporate OPD",
//                src.applyCorporateOpd(copyTable, processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 10: RULE-8 — Senior Citizen Activity ────────────────────────
//        pu.sop("processOPDData", "===== STEP 10: RULE-8  | Senior Citizen Activity =====");
//        overallStatus &= step("RULE-8  | Senior Citizen Activity",
//                src.applySeniorCitizenActivity(copyTable, processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 11: RULE-9 — New OPD ECHS ───────────────────────────────────
//        pu.sop("processOPDData", "===== STEP 11: RULE-9  | New OPD ECHS =====");
//        overallStatus &= step("RULE-9  | New OPD ECHS",
//                src.applyNewOpdEchs(copyTable, processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 12: RULE-10 — New OPD CGHS ──────────────────────────────────
//        pu.sop("processOPDData", "===== STEP 12: RULE-10 | New OPD CGHS =====");
//        overallStatus &= step("RULE-10 | New OPD CGHS",
//                src.applyNewOpdCghs(copyTable, processMonth, processDate, dbURL, dbName, dbPass));
//
//        // ── STEP 13: RULE-11 — New OPD DGHS ──────────────────────────────────
//        pu.sop("processOPDData", "===== STEP 13: RULE-11 | New OPD DGHS =====");
//        overallStatus &= step("RULE-11 | New OPD DGHS",
//                src.applyNewOpdDghs(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 3 � RULE-11
        pu.sop("processOPDData", "===== STEP 3: RULE-11 | New OPD DGHS =====");
        overallStatus &= step("RULE-11 | New OPD DGHS",
                src.applyNewOpdDghs(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 4 � RULE-10
        pu.sop("processOPDData", "===== STEP 4: RULE-10 | New OPD CGHS =====");
        overallStatus &= step("RULE-10 | New OPD CGHS",
                src.applyNewOpdCghs(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 5 � RULE-9
        pu.sop("processOPDData", "===== STEP 5: RULE-9  | New OPD ECHS =====");
        overallStatus &= step("RULE-9  | New OPD ECHS",
                src.applyNewOpdEchs(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 6 � RULE-8
        pu.sop("processOPDData", "===== STEP 6: RULE-8  | Senior Citizen Activity =====");
        overallStatus &= step("RULE-8  | Senior Citizen Activity",
                src.applySeniorCitizenActivity(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 7 � RULE-7
        pu.sop("processOPDData", "===== STEP 7: RULE-7  | Corporate OPD =====");
        overallStatus &= step("RULE-7  | Corporate OPD",
                src.applyCorporateOpd(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 8 � RULE-6
        pu.sop("processOPDData", "===== STEP 8: RULE-6  | Referral =====");
        overallStatus &= step("RULE-6  | Referral",
                src.applyReferral(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 9 � RULE-5
        pu.sop("processOPDData", "===== STEP 9: RULE-5  | Surgical Coupon =====");
        overallStatus &= step("RULE-5  | Surgical Coupon",
                src.applySurgicalCoupon(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 10 � RULE-4
        pu.sop("processOPDData", "===== STEP 10: RULE-4  | Camp (In House) =====");
        overallStatus &= step("RULE-4  | Camp (In House)",
                src.applyCampInHouse(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 11 � RULE-3
        pu.sop("processOPDData", "===== STEP 11: RULE-3  | Camp (Out Reach) =====");
        overallStatus &= step("RULE-3  | Camp (Out Reach)",
                src.applyCampOutReach(copyTable, campTable,
                        processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 12 � RULE-2
        pu.sop("processOPDData", "===== STEP 12: RULE-2  | Attendant Activity =====");
        overallStatus &= step("RULE-2  | Attendant Activity",
                src.applyAttendantActivity(copyTable, freeScreeningTable,
                        processMonth, processDate, dbURL, dbName, dbPass));

        // STEP 13 � RULE-1
        pu.sop("processOPDData", "===== STEP 13: RULE-1  | Old OPD =====");
        overallStatus &= step("RULE-1  | Old OPD",
                src.applyOldOpd(copyTable, processMonth, processDate, dbURL, dbName, dbPass));

        // ===== FINAL STATUS =====
        pu.sop("processOPDData", overallStatus
                ? "ALL STEPS COMPLETED SUCCESSFULLY"
                : "ONE OR MORE STEPS FAILED � check logs above");

        return overallStatus;
    }

    // =========================================================================
    // Private step helper  (mirrors ProcessTCNData's executeStep pattern)
    // =========================================================================

    /**
     * Evaluates the result of a single rule execution and logs it.
     *
     * @param label       Human-readable rule label for logging
     * @param rowsAffected Return value from OPD_Source method (-1 = error)
     * @return true if rows >= 0, false on error
     */
    private boolean step(String label, int rowsAffected) {
        if (rowsAffected < 0) {
            pu.sop("step", "FAILED : " + label);
            return false;
        }
        pu.sop("step", "SUCCESS: " + label + " | Rows affected: " + rowsAffected);
        return true;
    }
}
