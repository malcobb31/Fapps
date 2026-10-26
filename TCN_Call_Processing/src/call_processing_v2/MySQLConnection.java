package call_processing_v2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MySQLConnection {

    private static final String            processName    = "TCN_Call_Processing";
    private static final String            processVersion = "2.0";
    private              boolean           debugEnabled   = false;
    private static final DateTimeFormatter logFormat      =
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

    public Connection getConnection(String DB_URL, String USER, String PASS) {
        Connection conn = null;
        try {
            // Use 5.1.x driver - compatible with Java 7
            Class.forName("com.mysql.jdbc.Driver");

            log("DEBUG", "Attempting DB connection to: " + DB_URL + " as user: " + USER,
                    "", "DB_CONNECT");

            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            conn.setAutoCommit(false);

            log("INFO", "DB connection established successfully. URL: " + DB_URL,
                    "", "DB_CONNECT");

            return conn;
        } catch (ClassNotFoundException e) {
            log("ERROR", "MySQL JDBC Driver not found. Ensure mysql-connector jar is on classpath. Error: "
                    + e.getMessage(), "", "DB_CONNECT");
            return null;
        } catch (Exception e) {
            log("ERROR", "Database connection failed. URL: " + DB_URL
                    + " User: " + USER + " Error: " + e.getMessage(), "", "DB_CONNECT");
            return null;
        }
    }
}



//package call_processing_v1;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//
//public class MySQLConnection {
//
//    public Connection getConnection(String DB_URL, String USER, String PASS) {
//        Connection conn = null;
//        try {
//            // Explicitly load MySQL driver
//            Class.forName("com.mysql.cj.jdbc.Driver");
//
//            // Create connection
//            conn = DriverManager.getConnection(DB_URL, USER, PASS);
//            conn.setAutoCommit(false);
//            // System.out.println(" MySQL Connection Established Successfully!");
//            return conn;
//
//        } catch (ClassNotFoundException e) {
//            System.err.println(" MySQL JDBC Driver not found!");
//            e.printStackTrace();
//            return null;
//        } catch (Exception e) {
//            System.err.println(" Database connection failed!");
//            e.printStackTrace();
//            return null;
//        }
//    }
//}
