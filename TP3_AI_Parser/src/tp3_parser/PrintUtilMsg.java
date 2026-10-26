package tp3_parser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PrintUtilMsg {

    public static String imsiToDis = "";
    public static String msgType   = "";

    private static final String processName    = "TP3_CSV_PIPELINE";
    private static final String processVersion = "2.0";
    private static final boolean debugEnabled  = true;

    private static final DateTimeFormatter logFormat =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    // =========================================================================
    // Core structured log method — all logging must go through this
    // =========================================================================

    /**
     * Emit a structured log line.
     *
     * @param level     - INFO, DEBUG, WARN, ERROR
     * @param message   - Human-readable description
     * @param serverIP  - Server/host identifier (pass null or "" if not applicable)
     * @param evtType   - Short event-type tag (e.g. FILE_PROC, DB_INSERT, VALIDATION)
     */
    public void log(String level, String message, String serverIP, String evtType) {
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
                  .append(" Log_Level: EVT_LOG")
                  .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                  .append(" Description: ").append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    // =========================================================================
    // Convenience overloads
    // =========================================================================

    /** Log without serverIP context. */
    public void log(String level, String message, String evtType) {
        log(level, message, "", evtType);
    }

    // =========================================================================
    // Retained public API for Map printing (non-logging utility)
    // =========================================================================

    public void printMap(String TAG, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            log("DEBUG",
                "Tag: " + TAG + " possible event [" + entry.getKey() + "], possible action [" + entry.getValue() + "]",
                "MAP_PRINT");
        }
    }

    // =========================================================================
    // Private helpers
    // =========================================================================

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }
}
