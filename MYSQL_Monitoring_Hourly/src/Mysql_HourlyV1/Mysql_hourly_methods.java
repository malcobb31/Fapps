package Mysql_HourlyV1;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.Logger;

public class Mysql_hourly_methods {

    private static final Logger logger = Logger.getLogger(Mysql_hourly_methods.class.getName());
    private static final String PROCESS_NAME = "MySQL_Monitoring";
    private static final String PROCESS_VERSION = "1.1.1";

    public static boolean main(String dbUrl, String dbUser, String dbPass, String serverIP) {
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
             Statement stmt = conn.createStatement()) {

            // Max Used Memory (Critical > 70%)
            long maxUsedMemory = calculateMemory(stmt, false);
            double usagePercent = getMemoryUsagePercent(maxUsedMemory);
            String logLevel = usagePercent > 70.0 ? "Critical" : "EVT_Log";
            log("Max_Used_Memory", maxUsedMemory / (1024 * 1024) + " MB", serverIP, logLevel);

            // Max Peak Memory (Critical > 70%)
            long maxPeakMemory = calculateMemory(stmt, true);
            double peakPercent = getMemoryUsagePercent(maxPeakMemory);
            logLevel = peakPercent > 70.0 ? "Critical" : "EVT_Log";
            log("Max_Peak_Memory", maxPeakMemory / (1024 * 1024) + " MB", serverIP, logLevel);

            // Top Tables
//            printTopTables(stmt, serverIP);

            // Slow Queries (Critical > 10)
            long slowQueries = getStatus(stmt, "Slow_queries");
            logLevel = slowQueries > 10 ? "Critical" : "EVT_Log";
            log("Slow_Queries", slowQueries + "", serverIP, logLevel);

            // Lock Waits (Critical > 1)
            long lockWaits = getStatus(stmt, "Innodb_row_lock_waits");
            logLevel = lockWaits > 1 ? "Critical" : "EVT_Log";
            log("Innodb_Row_Lock_Waits", lockWaits + "", serverIP, logLevel);

            // Opened Tables
            log("Opened_Tables", getStatus(stmt, "Opened_tables") + "", serverIP, "EVT_Log");

            // Select_full_join (Always Critical)
            log("Select_Full_Join", getStatus(stmt, "Select_full_join") + "", serverIP, "Critical");

            // Qcache_hits (Always Critical)
            log("Qcache_Hits", getStatus(stmt, "Qcache_hits") + "", serverIP, "Critical");

            // Threads_Created vs Thread_Cache_Size (Dynamic log level)
            long threadsCreated = getStatus(stmt, "Threads_created");
            long threadCacheSize = getGlobal(stmt, "thread_cache_size");
            logLevel = threadsCreated >= threadCacheSize ? "Critical" : "Major";
            log("Threads_Created", threadsCreated + "", serverIP, logLevel);

            // Threads_connected
            log("Threads_Connected", getStatus(stmt, "Threads_connected") + "", serverIP, "EVT_Log");

            // Threads_running
            log("Threads_Running", getStatus(stmt, "Threads_running") + "", serverIP, "EVT_Log");

            // Open_Tables
            log("Open_Tables", getStatus(stmt, "Open_tables") + "", serverIP, "EVT_Log");

            // Threads_Cached
//            log("Threads_Cached", getStatus(stmt, "Threads_cached") + "", serverIP, "Critical");

        } catch (Exception e) {
            logger.severe("Exception occurred: " + e.getMessage());
        }
        return true;
    }

    private static void log(String evtType, String description, String serverIP, String logLevel) {
        String time = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        logger.info(String.format(
            "Time:%s [Debug] Process_Name: %s Server_IP:%s Process_Version: %s Log_Level: %s EVT_Type:%s Description: %s",
            time, PROCESS_NAME, serverIP, PROCESS_VERSION, logLevel, evtType, description
        ));
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

    static long calculateMemory(Statement stmt, boolean isPeak) throws SQLException {
        String[] serverKeys = {
                "key_buffer_size", "max_heap_table_size", "tmp_table_size",
                "innodb_buffer_pool_size", "innodb_additional_mem_pool_size",
                "innodb_log_buffer_size", "query_cache_size", "aria_pagecache_buffer_size"
        };
        String[] threadKeys = {
                "read_buffer_size", "read_rnd_buffer_size", "sort_buffer_size",
                "thread_stack", "max_allowed_packet", "join_buffer_size"
        };

        long heap = Math.max(getGlobal(stmt, "max_heap_table_size"), getGlobal(stmt, "tmp_table_size"));
        long serverBuffer = getGlobal(stmt, "key_buffer_size") + heap;
        for (int i = 3; i < serverKeys.length; i++) serverBuffer += getGlobal(stmt, serverKeys[i]);

        long perThread = 0;
        for (String key : threadKeys) perThread += getGlobal(stmt, key);

        long connections = isPeak ? getGlobal(stmt, "max_connections") : getStatus(stmt, "Max_used_connections");
        long performanceSchemaMemory = getPerformanceSchemaMemory(stmt);

        return serverBuffer + (perThread * connections) + performanceSchemaMemory;
    }

    static long getPerformanceSchemaMemory(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("SHOW ENGINE PERFORMANCE_SCHEMA STATUS");
        long mem = 0;
        while (rs.next()) {
            if ("performance_schema".equalsIgnoreCase(rs.getString("Type")) &&
                "performance_schema.memory".equalsIgnoreCase(rs.getString("Name"))) {
                try {
                    mem = Long.parseLong(rs.getString("Status"));
                } catch (NumberFormatException ignored) {}
            }
        }
        rs.close();
        return mem;
    }

    static void printTopTables(Statement stmt, String serverIP) throws SQLException {
        String query = "SELECT table_schema, table_name, " +
                       "ROUND((data_length + index_length) / 1024 / 1024, 2) AS total_size, " +
                       "ROUND(data_length / 1024 / 1024, 2) AS data_size, " +
                       "ROUND(index_length / 1024 / 1024, 2) AS index_size " +
                       "FROM information_schema.tables " +
                       "WHERE table_schema NOT IN ('information_schema','mysql','performance_schema','sys') " +
                       "AND table_type = 'BASE TABLE' ORDER BY total_size DESC LIMIT 10";

        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()) {
            String desc = String.format("DB: %s | Table: %s | Total: %.2fMB | Data: %.2fMB | Index: %.2fMB",
                    rs.getString(1), rs.getString(2), rs.getDouble(3), rs.getDouble(4), rs.getDouble(5));
            log("Top_Table_Utilization", desc, serverIP, "EVT_Log");
        }
        rs.close();
    }

    static double getMemoryUsagePercent(long memoryBytes) {
        long systemMemory = Runtime.getRuntime().maxMemory(); // or a static system memory cap if known
        return (memoryBytes / (double) systemMemory) * 100;
    }
}