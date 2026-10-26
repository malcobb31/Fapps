package ep_beta_v1;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class ep_beta_methods {

    private final String            processName    = "EP_BETA";
    private final String            processVersion = "1.0.0";
    private final DateTimeFormatter logFormat      = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final DateTimeFormatter dateFormat     = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final boolean           debugEnabled   = false;

    // -------------------------------------------------------------------------

    public void process(String dbUrl, String dbUser, String dbPass, String inputDate,
                        String configTable, String sshIp, String sshUser, String sshPass, int sshPort) {

        log("INFO", "Process started | input_date=" + inputDate + " config_table=" + configTable,
                null, "PROCESS_START");

        LocalDate inputLocalDate;
        try {
            inputLocalDate = LocalDate.parse(inputDate, dateFormat);
        } catch (Exception e) {
            log("ERROR", "Invalid input_date format: " + inputDate, null, "PROCESS_ERROR");
            return;
        }

        LocalDate   currentDate = LocalDate.now();
        Connection  dbConn      = null;
        Session     sshSession  = null;
        ChannelSftp sftpChannel = null;

        try {
            dbConn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            dbConn.setAutoCommit(false);
            log("INFO", "DB connected | url=" + dbUrl, null, "DB_CONNECT");

            log("INFO", "Connecting SSH | host=" + sshIp + " port=" + sshPort, sshIp, "SSH_CONNECT");
            JSch      jsch   = new JSch();
            sshSession = jsch.getSession(sshUser, sshIp, sshPort);
            sshSession.setPassword(sshPass);
            Properties sshConfig = new Properties();
            sshConfig.put("StrictHostKeyChecking", "no");
            sshSession.setConfig(sshConfig);
            sshSession.connect();

            sftpChannel = (ChannelSftp) sshSession.openChannel("sftp");
            sftpChannel.connect();
            log("INFO", "SSH/SFTP connected | host=" + sshIp, sshIp, "SSH_CONNECTED");

            String fetchSql = "SELECT table_name, source_path, last_export_date, date_to_pick"
                    + " FROM " + configTable + " WHERE status = 'completed'";
            log("INFO", "Executing query: " + fetchSql, null, "DB_QUERY");

            PreparedStatement fetchPs = dbConn.prepareStatement(fetchSql);
            ResultSet         rs      = fetchPs.executeQuery();

            while (rs.next()) {
                String tableName        = rs.getString("table_name");
                String sourcePath       = rs.getString("source_path");
                String lastExportDateStr = rs.getString("last_export_date");
                String dateToPick       = rs.getString("date_to_pick");

                log("INFO", "Row start | table=" + tableName, sshIp, "ROW_START");

                // -- Condition 1: input_date > last_export_date ---------------
                LocalDate lastExportDate;
                try {
                    lastExportDate = LocalDate.parse(lastExportDateStr, dateFormat);
                } catch (Exception e) {
                    log("WARN", "Cannot parse last_export_date='" + lastExportDateStr
                            + "' for table=" + tableName + " | skipping", null, "ROW_SKIP");
                    continue;
                }

                if (!inputLocalDate.isAfter(lastExportDate)) {
                    log("INFO", "Condition 1 failed | input_date=" + inputDate
                            + " not after last_export_date=" + lastExportDateStr
                            + " | table=" + tableName, null, "CONDITION_FAIL");
                    continue;
                }

                // -- Condition 2: input_date <= current_date - date_to_pick ---
                int       daysOffset = parseDaysOffset(dateToPick);
                LocalDate maxAllowed = currentDate.minusDays(daysOffset);

                if (inputLocalDate.isAfter(maxAllowed)) {
                    log("INFO", "Condition 2 failed | input_date=" + inputDate
                            + " > max_allowed=" + maxAllowed.format(dateFormat)
                            + " (date_to_pick=" + dateToPick + ")"
                            + " | table=" + tableName, null, "CONDITION_FAIL");
                    continue;
                }

                log("INFO", "Both conditions passed | table=" + tableName, null, "CONDITION_PASS");

                // -- Build remote file path -----------------------------------
                String formattedDate = inputLocalDate.format(dateFormat);

                String remoteFileName = tableName + formattedDate + ".csv";
                String normalizedPath = sourcePath.endsWith("/") ? sourcePath : sourcePath + "/";
                String remoteFilePath = normalizedPath + remoteFileName;

                // -- SFTP lstat to check file existence -----------------------
                boolean fileExists = false;
                try {
                    sftpChannel.lstat(remoteFilePath);
                    fileExists = true;
                    log("INFO", "Remote file found | path=" + remoteFilePath, sshIp, "REMOTE_FILE_FOUND");
                } catch (Exception e) {
                    log("INFO", "Remote file missing | path=" + remoteFilePath
                            + " | " + e.getMessage(), sshIp, "REMOTE_FILE_MISSING");
                }

                if (!fileExists) {
                    log("INFO", "Row skipped | no remote file | table=" + tableName, sshIp, "ROW_SKIP");
                    continue;
                }

                // -- Update DB: completed -> init -----------------------------
                PreparedStatement updatePs = null;
                try {
                    String updateSql = "UPDATE " + configTable
                            + " SET status = 'init', last_process_run_date = ?"
                            + " WHERE table_name = ? AND status = 'completed'";
                    updatePs = dbConn.prepareStatement(updateSql);
                    updatePs.setString(1, currentDate.format(dateFormat));
                    updatePs.setString(2, tableName);
                    int updated = updatePs.executeUpdate();
                    dbConn.commit();
                    log("INFO", "DB updated | table=" + tableName + " status=init"
                            + " last_process_run_date=" + currentDate.format(dateFormat),
                            null, "DB_UPDATED");
                    log("INFO", "DB updated | rows=" + updated + " | table=" + tableName
                            + " status=init last_process_run_date="
                            + currentDate.format(dateFormat),
                            null, "DB_UPDATED");
                } catch (SQLException e) {
                    log("ERROR", "DB update failed | table=" + tableName + " | " + e.getMessage(),
                            null, "DB_ROLLBACK");
                    try {
                        dbConn.rollback();
                        log("INFO", "Rollback successful | table=" + tableName, null, "ROLLBACK");
                    } catch (SQLException re) {
                        log("ERROR", "Rollback failed | table=" + tableName + " | " + re.getMessage(),
                                null, "ROLLBACK_FAIL");
                    }
                } finally {
                    if (updatePs != null) {
                        try { updatePs.close(); } catch (Exception ignored) {}
                    }
                }
            }

            rs.close();
            fetchPs.close();

        } catch (Exception e) {
            log("ERROR", "Unexpected process error: " + e.getMessage(), null, "PROCESS_ERROR");
        } finally {
            if (sftpChannel != null && sftpChannel.isConnected()) {
                sftpChannel.disconnect();
            }
            if (sshSession != null && sshSession.isConnected()) {
                sshSession.disconnect();
            }
            if (dbConn != null) {
                try { dbConn.close(); } catch (Exception ignored) {}
            }
        }

        log("INFO", "Process ended", null, "PROCESS_END");
    }

    // -------------------------------------------------------------------------

    private int parseDaysOffset(String dateToPick) {
        if (dateToPick == null || dateToPick.trim().isEmpty()) {
            return 0;
        }
        try {
            String normalized = dateToPick.trim().toLowerCase();
            if (normalized.startsWith("d-")) {
                return Integer.parseInt(normalized.substring(2).trim());
            }
            return Integer.parseInt(normalized);
        } catch (Exception e) {
            log("WARN", "Cannot parse date_to_pick='" + dateToPick + "', defaulting to 0",
                    null, "PARSE_WARN");
            return 0;
        }
    }

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled)
            return;
        String        timestamp  = LocalDateTime.now().format(logFormat);
        StringBuilder logBuilder = new StringBuilder(512);
        logBuilder.append("Time:").append(timestamp)
                  .append(" [").append(capitalize(level)).append("] ")
                  .append("Process_Name: ").append(processName).append(" ");
        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }
        logBuilder.append("Process_Version: ").append(processVersion).append(" ")
                  .append("Log_Level: EVT_LOG ")
                  .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                  .append(" Description: ").append(message != null ? message : "");
        System.out.println(logBuilder.toString().trim());
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }
}