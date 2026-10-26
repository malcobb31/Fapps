package li_parser_v3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MySQLConnection {

    // =========================================================================
    // LOGGING INFRASTRUCTURE
    // =========================================================================

    private static final String PROCESS_NAME    = "MySQLConnection";
    private static final String PROCESS_VERSION = "3.0";
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
    // BUSINESS LOGIC
    // =========================================================================

    public Connection getConnection(String DB_URL, String USER, String PASS) {
        Connection conn = null;
        log("DEBUG",
            "Attempting MySQL connection to URL: " + DB_URL + " as user: " + USER,
            SERVER_IP, "DB_CONNECT");
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            conn.setAutoCommit(false);
            log("INFO",
                "MySQL connection established successfully. URL: " + DB_URL,
                SERVER_IP, "DB_CONNECT");
            return conn;
        } catch (Exception e) {
            log("ERROR",
                "MySQL connection failed. URL: " + DB_URL + ", user: " + USER
                + ". Cause: " + e.getMessage(),
                SERVER_IP, "DB_CONNECT");
            return null;
        }
    }
}
