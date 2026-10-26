package Mysql_DailyV1;

import java.sql.*;
import java.util.logging.Logger;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Mysql_daily_methods5 {

    private static final Logger logger = Logger.getLogger(Mysql_daily_methods5.class.getName());
    private static final String PROCESS_NAME = "MySQL_Monitoring";
    private static final String PROCESS_VERSION = "1.1.1";

    public static boolean main(String dbUrl, String dbUser, String dbPass, String serverIP) {

        Connection conn = null;
        Statement stmt = null;

        try {
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            stmt = conn.createStatement();

            log("MySQL_Version", getVersion(stmt), serverIP, "EVT_Log");

            log("Max_Connections", getGlobal(stmt, "max_connections") + "", serverIP, "EVT_Log");
            log("Max_Allowed_Packet", getGlobal(stmt, "max_allowed_packet") + "", serverIP, "EVT_Log");
            log("Open_Files_Limit", getGlobal(stmt, "open_files_limit") + "", serverIP, "EVT_Log");
            log("Table_Open_Cache", getGlobal(stmt, "table_open_cache") + "", serverIP, "EVT_Log");
            log("Query_Cache_Size", getGlobal(stmt, "query_cache_size") + "", serverIP, "EVT_Log");
            log("Query_Cache_Limit", getGlobal(stmt, "query_cache_limit") + "", serverIP, "EVT_Log");
            log("Innodb_Buffer_Pool_Size", getGlobal(stmt, "innodb_buffer_pool_size") + "", serverIP, "EVT_Log");

            log("Uptime", getStatus(stmt, "Uptime") + "", serverIP, "EVT_Log");
            log("Open_Tables", getStatus(stmt, "Open_tables") + "", serverIP, "EVT_Log");

            log("Select_Scan", getStatus(stmt, "Select_scan") + "", serverIP, "EVT_Log");

            log("Threads_Cached", getStatus(stmt, "Threads_cached") + "", serverIP, "Critical");

            log("Thread_Cache_Size", getGlobal(stmt, "thread_cache_size") + "", serverIP, "EVT_Log");

        } catch (Exception e) {
            logger.severe("Exception occurred: " + e.getMessage());
        } finally {

            try {
                if (stmt != null) stmt.close();
            } catch (SQLException e) {
                logger.severe(e.getMessage());
            }

            try {
                if (conn != null) conn.close();
            } catch (SQLException e) {
                logger.severe(e.getMessage());
            }
        }

        return true;
    }

    private static void log(String evtType, String description, String serverIP, String logLevel) {

        String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

        logger.info(String.format(
            "Time:%s [Debug] Process_Name: %s Server_IP:%s Process_Version: %s Log_Level: %s EVT_Type:%s Description: %s",
            time, PROCESS_NAME, serverIP, PROCESS_VERSION, logLevel, evtType, description
        ));
    }

    static String getVersion(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SHOW GLOBAL VARIABLES LIKE 'version%'");
        StringBuffer sb = new StringBuffer();

        while (rs.next()) {
            sb.append(rs.getString(1)).append(" = ").append(rs.getString(2)).append("; ");
        }

        rs.close();
        return sb.toString();
    }

    static long getGlobal(Statement stmt, String key) throws SQLException {
        ResultSet rs = stmt.executeQuery("SHOW GLOBAL VARIABLES LIKE '" + key + "'");
        long val = 0;

        if (rs.next()) {
            try {
                val = Long.parseLong(rs.getString(2));
            } catch (Exception ignored) {}
        }

        rs.close();
        return val;
    }

    static long getStatus(Statement stmt, String key) throws SQLException {
        ResultSet rs = stmt.executeQuery("SHOW GLOBAL STATUS LIKE '" + key + "'");
        long val = 0;

        if (rs.next()) {
            try {
                val = Long.parseLong(rs.getString(2));
            } catch (Exception ignored) {}
        }

        rs.close();
        return val;
    }
}