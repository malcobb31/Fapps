package ntp12_process_v5;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DBUtil {

    private static final String processName = "user-migration-parser";
    private static final String processVersion = "1.0";
    private static final boolean debugEnabled = true;
    private static final DateTimeFormatter logFormat =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    public static Connection getPostgresConnection(String url, String user, String pass) {
        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);
            log("INFO", "PostgreSQL connection established | URL: " + url + " | User: " + user, null, "DB_CONNECT");
            return conn;
        } catch (ClassNotFoundException e) {
            log("ERROR", "PostgreSQL JDBC driver not found on classpath | Error: " + e.getMessage(), null, "DB_CONNECT");
            return null;
        } catch (Exception e) {
            log("ERROR", "Failed to connect to PostgreSQL | URL: " + url +
                    " | User: " + user + " | Error: " + e.getMessage(), null, "DB_CONNECT");
            return null;
        }
    }

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
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }
}