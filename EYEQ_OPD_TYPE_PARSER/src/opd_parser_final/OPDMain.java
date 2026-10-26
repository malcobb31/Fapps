package opd_parser_final;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * OPDMain - Entry point for OPD Type Classification Processing
 *
 * Command Line Arguments:
 * -----------------------------------------------------------------------
 * args[0]  - DB URL                  e.g. jdbc:mysql://localhost:3306/mydb
 * args[1]  - DB Username             e.g. root
 * args[2]  - DB Password             e.g. secret
 * args[3]  - Source table            e.g. Patient_Type_Age_wise_final
 * args[4]  - Destination table       e.g. OPD_Tracker_frequency_daily
 * args[5]  - Camp table              e.g. Camp_count_detail
 * args[6]  - Free Screening table    e.g. Free_Screening_Count
 * args[7]  - Process Date (YYYY-MM-DD)  -- leave blank or omit for auto D-1
 *
 * Example:
 *   java -cp "OPDProcessing.jar;mysql-connector-j-9.4.0.jar" call_processing_v2.OPDMain \
 *        "jdbc:mysql://localhost:3306/mydb" root secret \
 *        Patient_Type_Age_wise_final OPD_Tracker_frequency_daily \
 *        Camp_count_detail Free_Screening_Count 2025-10-28
 * -----------------------------------------------------------------------
 *
 * Processing Flow:
 *   1. Delete current-day data from destination table
 *   2. Copy source rows for processDate → destination, OPD_TYPE = 'Natural OPD'
 *   3. Apply override rules in REVERSE priority order (rule 14 first, rule 1 last)
 *      so that higher-priority rules overwrite lower ones, ending with Natural OPD
 *      for anything that matched no rule.
 *   4. Final result is ordered by VISIT_DATE.
 *
 * Override order (applied top-to-bottom; last write wins = highest priority):
 *   Step 1  → Old OPD          (PATIENT_TYPE != 'New')
 *   Step 2  → Attendant Activity (MRD in Free_Screening_Count with opd_visit='Y')
 *   Step 3  → Camp (Out Reach)  (MRD in Camp_count_detail opd_visit='Y'
 *                                 OR PATIENT_CATEGORY = 'OUT - REACH CAMP')
 *   Step 4  → Camp (In House)   (PATIENT_CATEGORY = 'Camp Patient')
 *   Step 5  → Surgical Coupon   (PATIENT_CATEGORY='MARKETING ACTIVITY'
 *                                 AND PATIENT_SUBCATEGORY='SURGICAL COUPON PATIENTS')
 *   Step 6  → Referral          (PATIENT_CATEGORY='MARKETING ACTIVITY'
 *                                 AND PATIENT_SUBCATEGORY IN (referral list))
 *   Step 7  → Corporate OPD     (PATIENT_CATEGORY IN ('CORPORATE (CASH)','CORPORATE (CASHLESS)'))
 *   Step 8  → Senior Citizen    (PATIENT_CATEGORY='MARKETING ACTIVITY'
 *                                 AND PATIENT_SUBCATEGORY='SENIOR CITIZEN ACTIVITY')
 *   Step 9  → New OPD ECHS      (PATIENT_CATEGORY = 'EX SERVICEMAN')
 *   Step 10 → New OPD CGHS      (PATIENT_CATEGORY = 'CGHS')
 *   Step 11 → New OPD DGHS      (PATIENT_CATEGORY = 'DGHS')
 *   Step 12 → Natural OPD       (everything remaining; already set at copy time)
 * -----------------------------------------------------------------------
 */
public class OPDMain {

    public static void main(String[] args) {

        System.out.println("========================================================");
        System.out.println("     OPD TYPE CLASSIFICATION PROCESSING - STARTED");
        System.out.println("========================================================");
        System.out.println("Number of arguments: " + args.length + "\n");

        for (int i = 0; i < args.length; i++) {
            System.out.println("Argument [" + i + "]: " + args[i]);
        }
        System.out.println("========================================================\n");

        if (args.length < 7) {
            System.out.println("===================== ERROR ============================");
            System.out.println("INSUFFICIENT ARGUMENTS!");
            System.out.println("Required (minimum 7):");
            System.out.println("  <DB_URL> <DB_User> <DB_Password>");
            System.out.println("  <Source_Table> <Dest_Table>");
            System.out.println("  <Camp_Table> <FreeScreening_Table>");
            System.out.println("  [Process_Date YYYY-MM-DD]  -- optional, defaults to D-1");
            System.out.println("\nExample:");
            System.out.println("  java -cp \"OPDProcessing.jar;mysql-connector-j-9.4.0.jar\" "
                    + "call_processing_v2.OPDMain "
                    + "\"jdbc:mysql://localhost:3306/mydb\" root secret "
                    + "Patient_Type_Age_wise_final OPD_Tracker_frequency_daily "
                    + "Camp_count_detail Free_Screening_Count 2025-10-28");
            System.out.println("========================================================");
            return;
        }

        try {

            // ── STEP 1: Determine Process Date ──────────────────────────────
            String processDate;
            if (args.length >= 8 && args[7] != null && !args[7].trim().isEmpty()) {
                processDate = args[7].trim();
                System.out.println(">>> Using PROVIDED date : " + processDate);
            } else {
                processDate = getYesterdayDate();
                System.out.println(">>> Using AUTO D-1 date : " + processDate);
            }

            if (!isValidDateFormat(processDate)) {
                System.out.println("===================== ERROR ============================");
                System.out.println("INVALID DATE FORMAT! Expected: YYYY-MM-DD  Got: " + processDate);
                System.out.println("========================================================");
                return;
            }

            // ── STEP 2: Extract month ───────────────────────────────────────
            String processMonth = processDate.substring(0, 7);
            System.out.println(">>> Process Month       : " + processMonth);

            // ── STEP 3: Parse arguments ─────────────────────────────────────
            String dbURL          = args[0];
            String dbName         = args[1];
            String dbPass         = args[2];
            String tableSource    = args[3];
            String tableDest      = args[4];
            String tableCamp      = args[5];
            String tableScreening = args[6];

            System.out.println(">>> DB URL              : " + dbURL);
            System.out.println(">>> DB Name/User        : " + dbName);
            System.out.println(">>> DB Password         : " + maskPassword(dbPass));
            System.out.println("\n>>> Source Table        : " + tableSource);
            System.out.println(">>> Destination Table   : " + tableDest);
            System.out.println(">>> Camp Table          : " + tableCamp);
            System.out.println(">>> Free Screening Table: " + tableScreening);
            System.out.println(">>> Process Date        : " + processDate);
            System.out.println(">>> Process Month       : " + processMonth);

            System.out.println("\n========================================================");
            System.out.println("     INITIATING OPD TYPE CLASSIFICATION");
            System.out.println("========================================================\n");

            // ── STEP 4: Invoke processing ───────────────────────────────────
            boolean status = new ProcessOPDData().processOPDData(
                    tableSource, tableDest, tableCamp, tableScreening,
                    processDate, processMonth,
                    dbURL, dbName, dbPass);

            // ── STEP 5: Display result ──────────────────────────────────────
            System.out.println("\n========================================================");
            if (status) {
                System.out.println("     \u2713 OPD CLASSIFICATION COMPLETED SUCCESSFULLY");
                System.out.println("========================================================");
                System.out.println("Process Date  : " + processDate);
                System.out.println("Process Month : " + processMonth);
                System.out.println("Dest Table    : " + tableDest);
            } else {
                System.out.println("     \u2717 OPD CLASSIFICATION FAILED");
                System.out.println("========================================================");
                System.out.println("Check logs above for error details.");
            }
            System.out.println("========================================================\n");

        } catch (Exception ex) {
            System.out.println("\n===================== EXCEPTION ========================");
            System.out.println("EXCEPTION in OPDMain: " + ex.toString());
            System.out.println("========================================================");
            ex.printStackTrace();
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static String getYesterdayDate() {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE, -1);
            return sdf.format(calendar.getTime());
        } catch (Exception ex) {
            System.out.println("ERROR in getYesterdayDate: " + ex.toString());
            return null;
        }
    }

    private static boolean isValidDateFormat(String date) {
        try {
            if (date == null || date.isEmpty()) return false;
            if (date.length() != 10 || date.charAt(4) != '-' || date.charAt(7) != '-') return false;
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            sdf.setLenient(false);
            sdf.parse(date);
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    private static String maskPassword(String password) {
        if (password == null || password.isEmpty()) return "****";
        if (password.length() <= 2) return "**";
        return password.charAt(0) + "****" + password.charAt(password.length() - 1);
    }
}
