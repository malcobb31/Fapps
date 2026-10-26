package call_processing_v2;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
public class Main_v2 {

    private static final String            processName    = "TCN_Call_Processing";
    private static final String            processVersion = "2.0";
    private static       boolean           debugEnabled   = false;
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp)
                .append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(processName).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(processVersion)
                .append(" Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                .append(" Description: ").append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }

    public static void main(String[] args) {

        log("INFO", "TCN CALL DATA PROCESSING - STARTED", "", "PROCESS_START");
        log("INFO", "Number of arguments received: " + args.length, "", "PROCESS_START");

        for (int i = 0; i < args.length; i++) {
            log("DEBUG", "Argument [" + i + "]: " + args[i], "", "PROCESS_START");
        }

        if (args.length < 8) {
            log("ERROR", "INSUFFICIENT ARGUMENTS. Required minimum 8: "
                    + "<DB_URL> <DB_User> <DB_Password> "
                    + "<TCN_Inbound_Table> <TCN_Manual_Table> "
                    + "<OPD_Table> <Extra2_Table> <Dest_Table> "
                    + "[Process_Date YYYY-MM-DD]. "
                    + "Example: java -cp TCNProcessing.jar;mysql-connector-j-9.4.0.jar "
                    + "com.fapps.main.TCNMain "
                    + "jdbc:mysql://localhost:3306/mydb root secret "
                    + "TCN_Inbound_Call TCN_Manual_Call_final "
                    + "Patient_Type_Age_wise_final TCN_Inbound_Call_tmp_extra2 "
                    + "TCN_Call_Output 2025-10-28",
                    "", "VALIDATION");
            return;
        }

        try {

            // STEP 1: Determine Process Date
            String processDate;
            if (args.length >= 9 && args[8] != null && !args[8].trim().isEmpty()) {
                processDate = args[8].trim();
                log("INFO", "Using PROVIDED process date: " + processDate, "", "DATE_COMPUTE");
            } else {
                processDate = getYesterdayDate();
                log("INFO", "No date provided. Using AUTO D-1 date: " + processDate, "", "DATE_COMPUTE");
            }

            if (!isValidDateFormat(processDate)) {
                log("ERROR", "INVALID DATE FORMAT. Expected: YYYY-MM-DD. Received: " + processDate,
                        "", "VALIDATION");
                return;
            }
            log("INFO", "Process Date validated: " + processDate, "", "DATE_COMPUTE");

            // STEP 2: Extract month
            String processMonth = processDate.substring(0, 7);
            log("INFO", "Process Month derived: " + processMonth, "", "DATE_COMPUTE");

            // STEP 3: Parse arguments
            String dbURL    = args[0];
            String dbName   = args[1];
            String dbPass   = args[2];

            String tableInbound = args[3];
            String tableManual  = args[4];
            String tableOPD     = args[5];
            String tableExtra2  = args[6];
            String destTable    = args[7];

            log("INFO", "DB URL: " + dbURL + " DB User: " + dbName
                    + " DB Password: " + maskPassword(dbPass), "", "PROCESS_START");
            log("INFO", "Source tables - Inbound: " + tableInbound
                    + " Manual: " + tableManual
                    + " OPD: " + tableOPD
                    + " Extra2: " + tableExtra2, "", "PROCESS_START");
            log("INFO", "Destination table: " + destTable, "", "PROCESS_START");

            log("INFO", "Initiating data processing for date: " + processDate
                    + " month: " + processMonth, "", "PROCESS_START");

            // STEP 4: Call ProcessTCNData
            boolean status = new ProcessTCNData().processTCNData(
                    tableInbound, tableManual, tableOPD, tableExtra2,
                    destTable, processDate, processMonth, dbURL, dbName, dbPass);

            // STEP 5: Display result
            if (status) {
                log("INFO", "DATA PROCESSING COMPLETED SUCCESSFULLY. Process_Date: " + processDate
                        + " Process_Month: " + processMonth
                        + " Dest_Table: " + destTable, "", "PROCESS_END");
            } else {
                log("ERROR", "DATA PROCESSING FAILED. Check preceding log entries for error details."
                        + " Process_Date: " + processDate
                        + " Dest_Table: " + destTable, "", "PROCESS_END");
            }

        } catch (Exception ex) {
            log("ERROR", "Unhandled exception in Main_v2.main(). Error: " + ex.toString(),
                    "", "PROCESS_END");
        }
    }

    private static String getYesterdayDate() {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.DATE, -1);
            return sdf.format(calendar.getTime());
        } catch (Exception ex) {
            log("ERROR", "Failed to compute yesterday date. Error: " + ex.toString(),
                    "", "DATE_COMPUTE");
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
