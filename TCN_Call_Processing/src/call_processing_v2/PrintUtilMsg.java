package call_processing_v2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PrintUtilMsg {

    public static String imsiToDis = "";
    public static String msgType   = "";

    private static final String              processName    = "TCN_Call_Processing";
    private static final String              processVersion = "2.0";
    private              boolean             debugEnabled   = false;
    private static final DateTimeFormatter   logFormat      =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private void log(String level, String message, String serverIP, String evtType) {
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

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }

    public void sop(String tagName, String msg) {
        String className                  = null;
        int lineNumFromWhereThisFunCalled = 0;
        try {
            throw new Exception("");
        } catch (Exception e) {
            className                  = e.getStackTrace()[1].getClassName();
            lineNumFromWhereThisFunCalled = e.getStackTrace()[1].getLineNumber();
        }
        String formatted = "[" + tagName + "][" + className + "][" + lineNumFromWhereThisFunCalled + "] " + msg;
        log("INFO", formatted, "", "APP_LOG");
    }

    private String getDateTime() {
        java.text.DateFormat dateFormat = new java.text.SimpleDateFormat("dd-MM-yyyy HH:mm:ss SSS");
        java.util.Date date             = new java.util.Date();
        return dateFormat.format(date);
    }

    public void printMap(String TAG, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            log("DEBUG",
                "possible event [" + entry.getKey() + "], possible action [" + entry.getValue() + "]",
                "", "MAP_PRINT");
        }
    }

    // public String normalizePhoneNumber(String phoneNumber) {
    //     if (phoneNumber == null || phoneNumber.trim().isEmpty()) {
    //         return phoneNumber;
    //     }
    //
    //     // Remove all spaces, + symbols, and hyphens
    //     String cleaned = phoneNumber.replaceAll("[\\s+\\-]", "");
    //
    //     // Get last 10 digits
    //     if (cleaned.length() >= 10) {
    //         return cleaned.substring(cleaned.length() - 10);
    //     }
    //
    //     // If less than 10 digits, return as-is
    //     return cleaned;
    // }

}
