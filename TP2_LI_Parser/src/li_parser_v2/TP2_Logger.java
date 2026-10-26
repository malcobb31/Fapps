package li_parser_v2;

import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * TP2_Logger - Structured Event Logger for TP2
 *
 * Outputs logs in the exact required format:
 * Time:YYYY-MM-DD HH:MM:SS [LEVEL] Process_Name: TP2 Server_IP: <IP>
 * Process_Version: 1.0 Log_Level: EVT_LOG EVT_Type: <EVENT_TYPE> Description: <message>
 */
public class TP2_Logger {

    private static final String PROCESS_NAME    = "TP2";
    private static final String PROCESS_VERSION = "1.0";
    private static final String LOG_LEVEL       = "EVT_LOG";

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
            ip = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            ip = "UNKNOWN";
        }
        this.serverIP = ip;
    }

    /**
     * Log an INFO-level structured event.
     *
     * @param evtType     One of the EVT_* constants defined above
     * @param description Human-readable message
     */
    public void info(String evtType, String description) {
        log("INFO", evtType, description);
    }

    /**
     * Log a WARN-level structured event.
     */
    public void warn(String evtType, String description) {
        log("WARN", evtType, description);
    }

    /**
     * Log an ERROR-level structured event.
     */
    public void error(String evtType, String description) {
        log("ERROR", evtType, description);
    }

    // ===== PRIVATE CORE =====

    private void log(String level, String evtType, String description) {
        String timestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String line = String.format(
            "Time:%s [%s] Process_Name: %s Server_IP: %s Process_Version: %s Log_Level: %s EVT_Type: %s Description: %s",
            timestamp, level, PROCESS_NAME, serverIP, PROCESS_VERSION, LOG_LEVEL, evtType, description
        );
        System.out.println(line);
    }
}
