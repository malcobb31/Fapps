package opd_parser_v3;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * OPD_Main - Entry point for OPD Classification Processing
 *
 * Mirrors Main_v2 architecture exactly:
 *   - Validates argument count
 *   - Auto-resolves D-1 date when not provided
 *   - Validates date format
 *   - Derives processMonth from processDate
 *   - Delegates to ProcessOPDData
 *   - Prints clear SUCCESS / FAILED banner
 *
 * Command Line Arguments:
 * ─────────────────────────────────────────────────────────────────────────
 * Required (8):
 *   args[0]  DB URL                    jdbc:mysql://localhost:3306/mydb
 *   args[1]  DB Username               root
 *   args[2]  DB Password               secret
 *   args[3]  Copy table                Patient_Type_Age_wise_final_copy
 *   args[4]  Camp table                Camp_count_detail
 *   args[5]  Free Screening table      Free_Screening_Count
 *   args[6]  Destination table         (same as copy table in most deployments)
 *   args[7]  Process Date YYYY-MM-DD   leave blank or omit → auto D-1
 *
 * Example — explicit date:
 *   java -cp "TCNProcessing.jar;mysql-connector-j-9.4.0.jar" opd_parser_v2.OPD_Main \
 *        "jdbc:mysql://localhost:3306/mydb" root secret \
 *        Patient_Type_Age_wise_final_copy Camp_count_detail Free_Screening_Count \
 *        Patient_Type_Age_wise_final_copy 2025-01-31
 *
 * Example — auto date (D-1):
 *   java -cp "TCNProcessing.jar;mysql-connector-j-9.4.0.jar" opd_parser_v2.OPD_Main \
 *        "jdbc:mysql://localhost:3306/mydb" root secret \
 *        Patient_Type_Age_wise_final_copy Camp_count_detail Free_Screening_Count \
 *        Patient_Type_Age_wise_final_copy
 * ─────────────────────────────────────────────────────────────────────────
 */
public class OPD_Main {

    public static void main(String[] args) {

        System.out.println("========================================================");
        System.out.println("     OPD CLASSIFICATION PROCESSING - STARTED");
        System.out.println("========================================================");
        System.out.println("Number of arguments: " + args.length + "\n");

        for (int i = 0; i < args.length; i++) {
            System.out.println("Argument [" + i + "]: " + args[i]);
        }
        System.out.println("========================================================\n");

        // ── Argument validation ───────────────────────────────────────────────
        if (args.length < 7) {
            System.out.println("===================== ERROR ============================");
            System.out.println("INSUFFICIENT ARGUMENTS!");
            System.out.println("Required (minimum 7):");
            System.out.println("  <DB_URL> <DB_User> <DB_Password>");
            System.out.println("  <CopyTable> <CampTable> <FreeScreeningTable> <DestTable>");
            System.out.println("  [Process_Date YYYY-MM-DD]  -- optional, defaults to D-1");
            System.out.println("\nExample:");
            System.out.println("  java -cp \"TCNProcessing.jar;mysql-connector-j-9.4.0.jar\" "
                    + "opd_parser_v2.OPD_Main "
                    + "\"jdbc:mysql://localhost:3306/mydb\" root secret "
                    + "Patient_Type_Age_wise_final_copy Camp_count_detail Free_Screening_Count "
                    + "Patient_Type_Age_wise_final_copy 2025-01-31");
            System.out.println("========================================================");
            return;
        }

        try {
            // ── STEP 1: Resolve Process Date (mirrors Main_v2 exactly) ────────
            String processDate;
            if (args.length >= 8 && args[7] != null && !args[7].trim().isEmpty()) {
                processDate = args[7].trim();
                System.out.println(">>> Using PROVIDED date: " + processDate);
            } else {
                processDate = getYesterdayDate();
                System.out.println(">>> Using AUTO D-1 date: " + processDate);
            }

            if (!isValidDateFormat(processDate)) {
                System.out.println("===================== ERROR ============================");
                System.out.println("INVALID DATE FORMAT! Expected: YYYY-MM-DD  Got: " + processDate);
                System.out.println("========================================================");
                return;
            }
            System.out.println(">>> Process Date (D-1) : " + processDate);

            // ── STEP 2: Derive month ───────────────────────────────────────────
            String processMonth = processDate.substring(0, 7);
            System.out.println(">>> Process Month      : " + processMonth);

            // ── STEP 3: Parse arguments ────────────────────────────────────────
            String dbURL              = args[0];
            String dbName             = args[1];
            String dbPass             = args[2];
            String copyTable          = args[3];
            String campTable          = args[4];
            String freeScreeningTable = args[5];
            String destTable          = args[6];

            System.out.println(">>> DB URL              : " + dbURL);
            System.out.println(">>> DB Name / User      : " + dbName);
            System.out.println(">>> DB Password         : " + maskPassword(dbPass));
            System.out.println("\n>>> Source / Work Tables:");
            System.out.println("    - Copy Table        : " + copyTable);
            System.out.println("    - Camp Table        : " + campTable);
            System.out.println("    - Free Screening    : " + freeScreeningTable);
            System.out.println(">>> Destination Table   : " + destTable);

            System.out.println("\n========================================================");
            System.out.println("     INITIATING OPD CLASSIFICATION PROCESSING");
            System.out.println("========================================================\n");

            // ── STEP 4: Invoke service layer ───────────────────────────────────
            boolean status = new ProcessOPDData().processOPDData(
                    copyTable, campTable, freeScreeningTable,
                    processDate, processMonth,
                    dbURL, dbName, dbPass);

            // ── STEP 5: Display result banner (mirrors Main_v2 exactly) ───────
            System.out.println("\n========================================================");
            if (status) {
                System.out.println("     \u2713 OPD CLASSIFICATION COMPLETED SUCCESSFULLY");
                System.out.println("========================================================");
                System.out.println("Process Date  : " + processDate);
                System.out.println("Process Month : " + processMonth);
                System.out.println("Copy Table    : " + copyTable);
                System.out.println("Dest Table    : " + destTable);
            } else {
                System.out.println("     \u2717 OPD CLASSIFICATION FAILED");
                System.out.println("========================================================");
                System.out.println("Check logs above for error details.");
            }
            System.out.println("========================================================\n");

        } catch (Exception ex) {
            System.out.println("\n===================== EXCEPTION ========================");
            System.out.println("EXCEPTION in OPD_Main: " + ex.toString());
            System.out.println("========================================================");
            ex.printStackTrace();
        }
    }

    // =========================================================================
    // Helpers  (identical to Main_v2)
    // =========================================================================

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
