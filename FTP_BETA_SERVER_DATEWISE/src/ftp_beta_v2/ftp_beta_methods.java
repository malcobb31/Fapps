package ftp_beta_v2;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ftp_beta_methods {

    /*  process identity  */
    private static final String PROCESS_NAME    = "SFTP_AUTOMATION_V1";
    private static final String PROCESS_VERSION = "1.0.0";

    /*  status tags  */
    private static final String STATUS_INIT      = "init";
    private static final String STATUS_COMPLETED = "completed";
    private static final String STATUS_FAILED    = "failed";

    /*  sftp defaults  */
    private static final int DEFAULT_SFTP_PORT    = 22;
    private static final int SESSION_TIMEOUT_MS   = 30000;
    private static final int CHANNEL_TIMEOUT_MS   = 15000;
    private static final long EXECUTOR_TIMEOUT_HR = 2L;

    /*  date formatters  */
    private static final DateTimeFormatter INPUT_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DB_FMT    = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /*  log config  */
    private final DateTimeFormatter logFormat    = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final boolean           debugEnabled = Boolean.parseBoolean(
                                                      System.getProperty("debug.enabled", "false"));

    /* 
       PUBLIC ENTRY POINT
        */

    public void processFTP(String dbUrl,      String dbUser,    String dbPass,
                           String inputDate,  String threadsStr,
                           String configTable,
                           String ftpIp,      String ftpUser,
                           String ftpPass,    String ftpPortStr) {

        log("INFO", "Process STARTED | input_date=" + inputDate
            + " | config_table=" + configTable + " | ftp=" + ftpIp,
            null, "PROCESS_START");

        /* parse thread count */
        int numThreads;
        try {
            numThreads = Integer.parseInt(threadsStr.trim());
            if (numThreads <= 0) throw new NumberFormatException("must be > 0");
        } catch (NumberFormatException e) {
            log("ERROR", "Invalid no_of_threads: " + threadsStr, null, "ARG_INVALID");
            return;
        }

        /* parse sftp port */
        int ftpPort = DEFAULT_SFTP_PORT;
        try {
            int parsed = Integer.parseInt(ftpPortStr.trim());
            if (parsed > 0 && parsed <= 65535) ftpPort = parsed;
            else log("WARN", "Port out of range, using default " + DEFAULT_SFTP_PORT, null, "ARG_DEFAULT");
        } catch (NumberFormatException e) {
            log("WARN", "Invalid ftp_server_port '" + ftpPortStr + "', using default " + DEFAULT_SFTP_PORT,
                null, "ARG_DEFAULT");
        }
        /* validate input_date */
        LocalDate inputLocalDate = parseInputDate(inputDate);
        if (inputLocalDate == null) {
            log("ERROR", "Invalid input_date: '" + inputDate + "' | Expected format: yyyy-MM-dd",
                null, "ARG_INVALID");
            return;
        }
        /* fetch config rows */
        List<Map<String, Object>> configRows = new ArrayList<Map<String, Object>>();
        Connection fetchConn = null;
        try {
            fetchConn = getConnection(dbUrl, dbUser, dbPass);
            configRows = fetchConfigRows(fetchConn, configTable);
        } catch (Exception e) {
            log("ERROR", "Failed to fetch config | table=" + configTable + " | " + e.getMessage(),
                null, "DB_FATAL");
            return;
        } finally {
            closeConn(fetchConn, "main");
        }

        if (configRows.isEmpty()) {
            log("WARN", "No rows with status='init' found in [" + configTable + "]", null, "NO_RECORDS");
            return;
        }

        log("INFO", "Fetched " + configRows.size() + " row(s) to process", null, "RECORDS_READY");

        /* thread pool dispatch */
        ExecutorService executor    = Executors.newFixedThreadPool(numThreads);
        AtomicInteger   successCnt  = new AtomicInteger(0);
        AtomicInteger   failedCnt   = new AtomicInteger(0);
        AtomicInteger   skippedCnt  = new AtomicInteger(0);

        for (int i = 0; i < configRows.size(); i++) {
            final Map<String, Object> row = configRows.get(i);
            final String   iDate    = inputDate;
            final LocalDate iLocal  = inputLocalDate;
            final String   ctable   = configTable;
            final String   fIp      = ftpIp;
            final String   fUser    = ftpUser;
            final String   fPass    = ftpPass;
            final int      fPort    = ftpPort;
            final String   dUrl     = dbUrl;
            final String   dUser    = dbUser;
            final String   dPass    = dbPass;

            executor.submit(new Runnable() {
                public void run() {
                    String tName  = getStr(row, "table_name");
                    String thread = Thread.currentThread().getName();
                    Connection conn = null;

                    log("INFO", "Thread [" + thread + "] START | table=" + tName, fIp, "THREAD_START");

                    try {
                        conn = getConnection(dUrl, dUser, dPass);
                        boolean transferred = processRow(conn, row, iDate, iLocal, ctable,
                                                         fIp, fUser, fPass, fPort);
                        conn.commit();

                        if (transferred) {
                            successCnt.incrementAndGet();
                            log("INFO", "Thread [" + thread + "] SUCCESS | table=" + tName,
                                fIp, "THREAD_SUCCESS");
                        } else {
                            skippedCnt.incrementAndGet();
                            log("INFO", "Thread [" + thread + "] SKIPPED | table=" + tName,
                                fIp, "THREAD_SKIPPED");
                        }

                    } catch (Exception e) {
                        failedCnt.incrementAndGet();
                        log("ERROR", "Thread [" + thread + "] FAILED | table=" + tName
                            + " | " + e.getClass().getSimpleName() + ": " + e.getMessage(),
                            fIp, "THREAD_FAILURE");
                        safeRollback(conn, thread);
                        markFailed(conn, ctable, tName, dUrl, dUser, dPass, thread);

                    } finally {
                        closeConn(conn, thread);
                        log("INFO", "Thread [" + thread + "] END | table=" + tName, null, "THREAD_END");
                    }
                }
            });
        }

        /* orderly shutdown */
        executor.shutdown();
        try {
            if (!executor.awaitTermination(EXECUTOR_TIMEOUT_HR, TimeUnit.HOURS)) {
                log("WARN", "Executor timeout. Forcing shutdown.", null, "EXECUTOR_TIMEOUT");
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            log("ERROR", "Main thread interrupted: " + ie.getMessage(), null, "INTERRUPTED");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log("INFO", "Process DONE | Total=" + configRows.size()
            + " | Success=" + successCnt.get()
            + " | Skipped=" + skippedCnt.get()
            + " | Failed=" + failedCnt.get(),
            null, "PROCESS_END");
    }

    /* 
       CORE ROW PROCESSING
        */

    private boolean processRow(Connection conn,
                               Map<String, Object> row,
                               String inputDate,
                               LocalDate inputLocalDate,
                               String configTable,
                               String ftpIp, String ftpUser,
                               String ftpPass, int ftpPort) throws Exception {

        String tableName      = requireStr(row, "table_name");
        String sourcePath     = requireStr(row, "source_path");
        String destPath       = requireStr(row, "dest_path");
        String lastExportDate = getStr(row, "last_export_date");
        String dateToPickStr  = getStr(row, "date_to_pick");
        String prefixStatus   = getStr(row, "prefix_status");

        log("INFO", "Processing | table=" + tableName
            + " | lastExport=" + lastExportDate
            + " | dateToPick=" + dateToPickStr
            + " | prefix=" + prefixStatus,
            ftpIp, "ROW_START");

        /* parse comparison dates */
        LocalDate lastExportLocal = parseDbDate(lastExportDate, tableName);
        int       offsetDays      = parseDateOffset(dateToPickStr, tableName);
        LocalDate maxAllowedDate  = LocalDate.now().minusDays(offsetDays);

        /*  Condition 1: input_date > last_export_date  */
        if (!inputLocalDate.isAfter(lastExportLocal)) {
            log("WARN", "SKIP | COND-1 FAIL | table=" + tableName
                + " | input=" + inputLocalDate + " NOT > lastExport=" + lastExportLocal,
                ftpIp, "COND1_FAIL");
            return false;
        }
        log("INFO", "COND-1 PASS | " + inputLocalDate + " > " + lastExportLocal,
            null, "COND1_PASS");

        /*  Condition 2: input_date <= current_date - date_to_pick  */
        if (inputLocalDate.isAfter(maxAllowedDate)) {
            log("WARN", "SKIP | COND-2 FAIL | table=" + tableName
                + " | input=" + inputLocalDate + " NOT <= maxAllowed=" + maxAllowedDate
                + " [today-d-" + offsetDays + "]",
                ftpIp, "COND2_FAIL");
            return false;
        }
        log("INFO", "COND-2 PASS | " + inputLocalDate + " <= " + maxAllowedDate,
            null, "COND2_PASS");

        /*  Build file names  */
        String formattedDate = inputLocalDate.format(DB_FMT);

        String generatedFile = tableName + formattedDate + ".csv";
        String sourceFile    = normalizePath(sourcePath) + generatedFile;

        String destFileName;
        if (isValidPrefix(prefixStatus)) {
            destFileName = prefixStatus.trim() + "_" + generatedFile;
            log("INFO", "Prefix applied: " + prefixStatus.trim() + " | destFile=" + destFileName,
                ftpIp, "PREFIX_APPLIED");
        } else {
            destFileName = generatedFile;
            log("INFO", "No prefix | destFile=" + destFileName, null, "PREFIX_SKIP");
        }
        String destFilePath = normalizePath(destPath) + destFileName;

        log("INFO", "Source: " + sourceFile, ftpIp, "FILE_SRC");
        log("INFO", "Dest:   " + destFilePath, ftpIp, "FILE_DEST");

        /*  Verify source file exists  */
        log("INFO", "Remote source file validation pending", ftpIp, "REMOTE_FILE_CHECK");

        /*  SFTP transfer  */
        sftpTransfer(sourceFile, destFilePath, tableName, ftpIp, ftpUser, ftpPass, ftpPort);

        /*  Update DB on success  */
        String newExportDate  = inputLocalDate.format(DB_FMT);
        String newProcessDate = LocalDate.now().format(DB_FMT);
        updateRowSuccess(conn, configTable, tableName, newExportDate, newProcessDate);

        log("INFO", "DB updated | table=" + tableName
            + " | last_export_date=" + newExportDate
            + " | last_process_run_date=" + newProcessDate
            + " | status=" + STATUS_COMPLETED,
            ftpIp, "DB_UPDATED");

        return true;
    }

    /* 
       SFTP TRANSFER — JSch
        */

    private void sftpTransfer(String remoteSourcePath, String localDestPath,
            String tableName,
            String ftpIp, String ftpUser,
            String ftpPass, int ftpPort) throws Exception {

        JSch       jsch    = new JSch();
        Session    session = null;
        ChannelSftp channel = null;

        log("INFO", "SFTP CONNECT | host=" + ftpIp + ":" + ftpPort + " | table=" + tableName,
            ftpIp, "SFTP_CONNECT");

        try {
            session = jsch.getSession(ftpUser, ftpIp, ftpPort);
            session.setPassword(ftpPass);

            Properties cfg = new Properties();
            cfg.put("StrictHostKeyChecking", "no");
            session.setConfig(cfg);
            session.setTimeout(SESSION_TIMEOUT_MS);
            session.connect(SESSION_TIMEOUT_MS);

            log("INFO", "SFTP session open | host=" + ftpIp, ftpIp, "SFTP_SESSION_OPEN");

            channel = (ChannelSftp) session.openChannel("sftp");
            channel.connect(CHANNEL_TIMEOUT_MS);

            log("INFO", "SFTP DOWNLOAD | remote=" + remoteSourcePath + " | local=" + localDestPath,
            	    ftpIp, "SFTP_DOWNLOAD_START");

            	channel.lstat(remoteSourcePath);

            	channel.get(remoteSourcePath, localDestPath);

            	log("INFO", "SFTP DOWNLOAD SUCCESS | local=" + localDestPath + " | table=" + tableName,
            	    ftpIp, "SFTP_DOWNLOAD_OK");

        } catch (JSchException e) {
            log("ERROR", "SFTP auth/connect FAILED | host=" + ftpIp + ":" + ftpPort
                + " | " + e.getMessage(), ftpIp, "SFTP_CONN_FAIL");
            throw e;

        } catch (SftpException e) {
        	String msg = "SFTP transfer ERROR | code=" + e.id + " | " + e.getMessage()
            + " | remote=" + remoteSourcePath;
            log("ERROR", msg, ftpIp, "SFTP_XFER_FAIL");
            throw new Exception(msg, e);

        } finally {
            if (channel != null && channel.isConnected()) {
                channel.disconnect();
                log("DEBUG", "SFTP channel closed", ftpIp, "SFTP_CHAN_CLOSE");
            }
            if (session != null && session.isConnected()) {
                session.disconnect();
                log("DEBUG", "SFTP session closed | host=" + ftpIp, ftpIp, "SFTP_SESS_CLOSE");
            }
        }
    }

    /* 
       DATABASE OPERATIONS
        */

    private Connection getConnection(String dbUrl, String dbUser, String dbPass) throws SQLException {
        log("DEBUG", "DB connect | url=" + dbUrl, null, "DB_CONNECT");
        Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
        conn.setAutoCommit(false);
        return conn;
    }

    private List<Map<String, Object>> fetchConfigRows(Connection conn,
                                                       String configTable) throws SQLException {
        String sql = "SELECT table_name, source_path, dest_path, status,"
                   + " last_export_date, last_process_run_date, prefix_status, date_to_pick"
                   + " FROM " + configTable + " WHERE status = ?";

        log("INFO", "Querying [" + configTable + "] status='" + STATUS_INIT + "'",
            null, "DB_QUERY");

        List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, STATUS_INIT);
            rs = ps.executeQuery();

            ResultSetMetaData meta = rs.getMetaData();
            int colCount = meta.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<String, Object>();
                for (int i = 1; i <= colCount; i++) {
                    row.put(meta.getColumnLabel(i).toLowerCase(), rs.getObject(i));
                }
                rows.add(row);
            }
        } finally {
            if (rs != null) try { rs.close(); } catch (SQLException ignored) { }
            if (ps != null) try { ps.close(); } catch (SQLException ignored) { }
        }

        log("INFO", "Fetched " + rows.size() + " row(s)", null, "DB_FETCHED");
        return rows;
    }

    private void updateRowSuccess(Connection conn, String configTable,
                                  String tableName,
                                  String newExportDate, String newProcessDate) throws SQLException {
        String sql = "UPDATE " + configTable
                   + " SET last_export_date=?, last_process_run_date=?, status=?"
                   + " WHERE table_name=?";
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, newExportDate);
            ps.setString(2, newProcessDate);
            ps.setString(3, STATUS_COMPLETED);
            ps.setString(4, tableName);
            int n = ps.executeUpdate();
            log("DEBUG", "UPDATE rows=" + n + " | table=" + tableName, null, "DB_UPDATE");
        } finally {
            if (ps != null) try { ps.close(); } catch (SQLException ignored) { }
        }
    }

    private void updateRowStatus(Connection conn, String configTable,
                                 String tableName, String status) throws SQLException {
        String sql = "UPDATE " + configTable
                   + " SET status=?, last_process_run_date=? WHERE table_name=?";
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1, status);
            ps.setString(2, LocalDate.now().format(DB_FMT));
            ps.setString(3, tableName);
            ps.executeUpdate();
        } finally {
            if (ps != null) try { ps.close(); } catch (SQLException ignored) { }
        }
    }

    private void markFailed(Connection existingConn, String configTable, String tableName,
                             String dbUrl, String dbUser, String dbPass, String thread) {
        /* attempt with existing connection first */
        try {
            if (existingConn != null && !existingConn.isClosed()) {
                updateRowStatus(existingConn, configTable, tableName, STATUS_FAILED);
                existingConn.commit();
                log("INFO", "Row marked FAILED | table=" + tableName, null, "DB_MARK_FAILED");
                return;
            }
        } catch (Exception e) {
            log("WARN", "markFailed retry on new conn | table=" + tableName
                + " | " + e.getMessage(), null, "DB_MARK_RETRY");
        }

        /* fallback: fresh connection */
        Connection fc = null;
        try {
            fc = getConnection(dbUrl, dbUser, dbPass);
            updateRowStatus(fc, configTable, tableName, STATUS_FAILED);
            fc.commit();
            log("INFO", "Row marked FAILED (fallback) | table=" + tableName, null, "DB_MARK_FAILED");
        } catch (Exception fe) {
            log("ERROR", "CRITICAL: cannot mark FAILED | table=" + tableName
                + " | " + fe.getMessage(), null, "DB_MARK_CRITICAL");
        } finally {
            closeConn(fc, thread + "-fb");
        }
    }

    /* 
       DATE HELPERS
        */

    private LocalDate parseInputDate(String inputDate) {
        try {
            
        	return LocalDate.parse(inputDate.trim(), INPUT_FMT);
            
        } catch (Exception e) {
            return null;
        }
    }

    private LocalDate parseDbDate(String dbDate, String tableName) {
        if (dbDate == null || dbDate.trim().isEmpty()) {
            log("WARN", "last_export_date null for table=" + tableName
                + " | defaulting to 1970-01-01", null, "DATE_DEFAULT");
            return LocalDate.of(1970, 1, 1);
        }
        try {
            return LocalDate.parse(dbDate.trim(), DB_FMT);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                "Invalid DB date '" + dbDate + "' for table=" + tableName
                + " | Expected: yyyy-MM-dd");
        }
    }

    private int parseDateOffset(String dateToPickStr, String tableName) {
        if (dateToPickStr == null || dateToPickStr.trim().isEmpty()) {
            log("WARN", "date_to_pick null for table=" + tableName + " | offset=0",
                null, "DATE_OFFSET_DEFAULT");
            return 0;
        }
        String t = dateToPickStr.trim().toLowerCase();
        if (!t.startsWith("d-")) {
            throw new IllegalArgumentException(
                "Invalid date_to_pick='" + dateToPickStr + "' for table=" + tableName
                + " | Expected format: d-N (e.g. d-3)");
        }
        try {
            int n = Integer.parseInt(t.substring(2));
            if (n < 0) throw new NumberFormatException("negative");
            return n;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Invalid offset in date_to_pick='" + dateToPickStr + "' for table=" + tableName);
        }
    }

    /* 
       UTILITY HELPERS
        */

    private String normalizePath(String path) {
        if (path == null || path.trim().isEmpty()) return "";
        String p = path.trim();
        return p.endsWith("/") ? p : p + "/";
    }

    private boolean isValidPrefix(String prefix) {
        return prefix != null
            && !prefix.trim().isEmpty()
            && !"N/A".equalsIgnoreCase(prefix.trim());
    }

    private String getStr(Map<String, Object> map, String key) {
        Object v = map.get(key);
        if (v == null) return null;
        String s = v.toString().trim();
        return s.isEmpty() ? null : s;
    }

    private String requireStr(Map<String, Object> map, String key) {
        String v = getStr(map, key);
        if (v == null) throw new IllegalArgumentException("Config field '" + key + "' is null/empty");
        return v;
    }

    private void safeRollback(Connection conn, String thread) {
        if (conn == null) return;
        try {
            conn.rollback();
            log("DEBUG", "Rollback OK | thread=" + thread, null, "DB_ROLLBACK");
        } catch (SQLException e) {
            log("ERROR", "Rollback FAILED | thread=" + thread + " | " + e.getMessage(),
                null, "DB_ROLLBACK_ERR");
        }
    }

    private void closeConn(Connection conn, String tag) {
        if (conn == null) return;
        try {
            conn.close();
            log("DEBUG", "DB conn closed | tag=" + tag, null, "DB_CLOSED");
        } catch (SQLException e) {
            log("ERROR", "DB close FAILED | tag=" + tag + " | " + e.getMessage(),
                null, "DB_CLOSE_ERR");
        }
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    /* 
       CUSTOM LOG FUNCTION  (enterprise EVT_LOG format)
        */

    private void log(String level, String message, String serverIP, String evtType) {

        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled)
            return;

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp).append(" [").append(capitalize(level)).append("] ")
                  .append("Process_Name: ").append(PROCESS_NAME).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(PROCESS_VERSION).append(" ").append("Log_Level: EVT_LOG ")
                  .append(" EVT_Type: ").append(evtType != null ? evtType : "NA").append(" Description: ")
                  .append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }
}