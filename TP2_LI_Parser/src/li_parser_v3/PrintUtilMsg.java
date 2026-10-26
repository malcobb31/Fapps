package li_parser_v3;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PrintUtilMsg {

    // =========================================================================
    // LOGGING INFRASTRUCTURE
    // =========================================================================

    private static final String PROCESS_NAME    = "li_parser_v3";
    private static final String PROCESS_VERSION = "1.0";
    private static final boolean debugEnabled   = false;
    private static final DateTimeFormatter LOG_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String SERVER_IP;
    static {
        String ip;
        try {
            ip = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            ip = "UNKNOWN";
        }
        SERVER_IP = ip;
    }

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(LOG_FORMAT);

        StringBuilder logBuilder = new StringBuilder(512);
        logBuilder.append("Time:").append(timestamp)
                  .append(" [").append(capitalize(level)).append("] ")
                  .append("Process_Name: ").append(PROCESS_NAME).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(PROCESS_VERSION)
                  .append(" Log_Level: EVT_LOG ")
                  .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                  .append(" Description: ").append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    // =========================================================================
    // PUBLIC UTILITY METHODS (delegating to log())
    // =========================================================================

    public static String imsiToDis = "";
    public static String msgType   = "";

    public void sop(String tagName, String msg) {
        log("INFO",
            "[" + tagName + "] " + msg,
            SERVER_IP, "APP_LOG");
    }

    public void eventLog(String level, String evtType, String desc) {
        log(level, desc, SERVER_IP, evtType);
    }

    public void printMap(String TAG, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            log("DEBUG",
                "[" + TAG + "] possible event [" + entry.getKey()
                + "], possible action [" + entry.getValue() + "]",
                SERVER_IP, "APP_LOG");
        }
    }
}
