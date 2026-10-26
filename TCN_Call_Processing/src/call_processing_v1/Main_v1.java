package call_processing_v1;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * TCNMain - Entry point for TCN Call Data Processing
 *
 * Command Line Arguments:
 * -----------------------------------------------------------------------
 * args[0]  - DB URL          e.g. jdbc:mysql://localhost:3306/mydb
 * args[1]  - DB Username     e.g. root
 * args[2]  - DB Password     e.g. secret
 * args[3]  - TCN_Inbound_Call table name
 * args[4]  - TCN_Manual_Call_final table name
 * args[5]  - Patient_Type_Age_wise_final table name  (OPD)
 * args[6]  - TCN_Inbound_Call_tmp_extra2 table name
 * args[7]  - Destination table name
 * args[8]  - Process Date (YYYY-MM-DD)  -- leave blank or omit for auto D-1
 *
 * Example:
 *   java -cp "TCNProcessing.jar;mysql-connector-j-9.4.0.jar" com.fapps.main.TCNMain \
 *        "jdbc:mysql://localhost:3306/mydb" root secret \
 *        TCN_Inbound_Call TCN_Manual_Call_final \
 *        Patient_Type_Age_wise_final TCN_Inbound_Call_tmp_extra2 \
 *        TCN_Call_Output 2025-10-28
 * -----------------------------------------------------------------------
 */
public class Main_v1 {

    public static void main(String[] args) {

        System.out.println("========================================================");
        System.out.println("     TCN CALL DATA PROCESSING - STARTED");
        System.out.println("========================================================");
        System.out.println("Number of arguments: " + args.length + "\n");

        for (int i = 0; i < args.length; i++) {
            System.out.println("Argument [" + i + "]: " + args[i]);
        }
        System.out.println("========================================================\n");

        if (args.length < 8) {
            System.out.println("===================== ERROR ============================");
            System.out.println("INSUFFICIENT ARGUMENTS!");
            System.out.println("Required (minimum 8):");
            System.out.println("  <DB_URL> <DB_User> <DB_Password>");
            System.out.println("  <TCN_Inbound_Table> <TCN_Manual_Table>");
            System.out.println("  <OPD_Table> <Extra2_Table> <Dest_Table>");
            System.out.println("  [Process_Date YYYY-MM-DD]  -- optional, defaults to D-1");
            System.out.println("\nExample:");
            System.out.println("  java -cp \"TCNProcessing.jar;mysql-connector-j-9.4.0.jar\" "
                    + "com.fapps.main.TCNMain "
                    + "\"jdbc:mysql://localhost:3306/mydb\" root secret "
                    + "TCN_Inbound_Call TCN_Manual_Call_final "
                    + "Patient_Type_Age_wise_final TCN_Inbound_Call_tmp_extra2 "
                    + "TCN_Call_Output 2025-10-28");
            System.out.println("========================================================");
            return;
        }

        try {

            // STEP 1: Determine Process Date
            String processDate;
            if (args.length >= 9 && args[8] != null && !args[8].trim().isEmpty()) {
                processDate = args[8].trim();
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
            System.out.println(">>> Process Date (D-1): " + processDate);

            // STEP 2: Extract month
            String processMonth = processDate.substring(0, 7);
            System.out.println(">>> Process Month      : " + processMonth);

            // STEP 3: Parse arguments
            String dbURL    = args[0];
            String dbName   = args[1];
            String dbPass   = args[2];

            String tableInbound = args[3];
            String tableManual  = args[4];
            String tableOPD     = args[5];
            String tableExtra2  = args[6];
            String destTable    = args[7];

            System.out.println(">>> DB URL              : " + dbURL);
            System.out.println(">>> DB Name/User        : " + dbName);
            System.out.println(">>> DB Password         : " + maskPassword(dbPass));
            System.out.println("\n>>> Source Tables:");
            System.out.println("    - Inbound Call      : " + tableInbound);
            System.out.println("    - Manual Call       : " + tableManual);
            System.out.println("    - OPD               : " + tableOPD);
            System.out.println("    - Extra2 (Group)    : " + tableExtra2);
            System.out.println(">>> Destination Table   : " + destTable);

            System.out.println("\n========================================================");
            System.out.println("     INITIATING DATA PROCESSING");
            System.out.println("========================================================\n");

            // STEP 4: Call ProcessTCNData
            boolean status = new ProcessTCNData().processTCNData(
                    tableInbound, tableManual, tableOPD, tableExtra2,
                    destTable, processDate, processMonth, dbURL, dbName, dbPass);

            // STEP 5: Display result
            System.out.println("\n========================================================");
            if (status) {
                System.out.println("     \u2713 DATA PROCESSING COMPLETED SUCCESSFULLY");
                System.out.println("========================================================");
                System.out.println("Process Date  : " + processDate);
                System.out.println("Process Month : " + processMonth);
                System.out.println("Dest Table    : " + destTable);
            } else {
                System.out.println("     \u2717 DATA PROCESSING FAILED");
                System.out.println("========================================================");
                System.out.println("Check logs above for error details.");
            }
            System.out.println("========================================================\n");

        } catch (Exception ex) {
            System.out.println("\n===================== EXCEPTION ========================");
            System.out.println("EXCEPTION in TCNMain: " + ex.toString());
            System.out.println("========================================================");
            ex.printStackTrace();
        }
    }

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
