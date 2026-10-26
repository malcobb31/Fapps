package li_parser_v3;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * TP2_Logger - Structured Event Logger for TP2
 *
 * Outputs logs in the required format:
 * Time:YYYY-MM-DD HH:MM:SS [LEVEL] Process_Name: TP2 Server_IP: <IP>
 * Process_Version: 1.0 Log_Level: EVT_LOG EVT_Type: <EVENT_TYPE> Description: <message>
 */
public class TP2_Logger {

    // =========================================================================
    // LOGGING INFRASTRUCTURE
    // =========================================================================

    private static final String PROCESS_NAME    = "TP2";
    private static final String PROCESS_VERSION = "1.0";
    private static final boolean debugEnabled   = false;
    private static final DateTimeFormatter LOG_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private final String serverIP;

    // ===== SUPPORTED EVENT TYPES =====
    public static final String EVT_PROCESS_START  = "PROCESS_START";
    public static final String EVT_FILE_DETECTED  = "FILE_DETECTED";
    public static final String EVT_CSV_SPLIT      = "CSV_SPLIT";
    public static final String EVT_ROUTING        = "ROUTING_DECISION";
    public static final String EVT_TP1_CALL       = "TP1_CALL";
    public static final String EVT_INSERT_SUCCESS = "INSERT_SUCCESS";
    public static final String EVT_INSERT_FAILURE = "INSERT_FAILURE";
    public static final String EVT_FILE_MOVED     = "FILE_MOVED";
    public static final String EVT_FILE_NOT_MOVED = "FILE_NOT_MOVED";
    public static final String EVT_ERROR          = "ERROR";

    public TP2_Logger() {
        String ip;
        try {
            ip = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            ip = "UNKNOWN";
        }
        this.serverIP = ip;
    }

    // =========================================================================
    // PUBLIC API
    // =========================================================================

    /**
     * Log an INFO-level structured event.
     *
     * @param evtType     One of the EVT_* constants defined above
     * @param description Human-readable message
     */
    public void info(String evtType, String description) {
        log("INFO", description, serverIP, evtType);
    }

    /**
     * Log a WARN-level structured event.
     */
    public void warn(String evtType, String description) {
        log("WARN", description, serverIP, evtType);
    }

    /**
     * Log an ERROR-level structured event.
     */
    public void error(String evtType, String description) {
        log("ERROR", description, serverIP, evtType);
    }

    // =========================================================================
    // PRIVATE CORE
    // =========================================================================

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
}
