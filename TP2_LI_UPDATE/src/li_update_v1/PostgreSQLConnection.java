package li_update_v1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * PostgreSQLConnection - Database Connection Handler
 * 
 * Establishes connection to PostgreSQL database
 */
public class PostgreSQLConnection {

    private static final boolean           debugEnabled   = false;
    private static final String            processName    = "TP2";
    private static final String            processVersion = "1.0";
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * Get PostgreSQL database connection
     * 
     * @param DB_URL - Database URL (jdbc:postgresql://host:port/database)
     * @param USER - Database username
     * @param PASS - Database password
     * @return Connection object or null if failed
     */
    public Connection getConnection(String DB_URL, String USER, String PASS) {
        Connection conn = null;
        
        try {
            // Explicitly load PostgreSQL driver
            Class.forName("org.postgresql.Driver");

            // Create connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            conn.setAutoCommit(false);

            log("INFO",
                "PostgreSQL connection established - url=" + DB_URL + " | user=" + USER,
                null, "PROCESS_START");

            return conn;

        } catch (ClassNotFoundException e) {
            log("ERROR",
                "PostgreSQL JDBC driver not found on classpath - " + e.getMessage(),
                null, "ERROR");
            return null;
        } catch (Exception e) {
            log("ERROR",
                "Failed to establish PostgreSQL connection - url=" + DB_URL
                + " | user=" + USER + " - " + e.getMessage(),
                null, "ERROR");
            return null;
        }
    }

    // =========================================================================
    // LOGGING
    // =========================================================================

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
}
