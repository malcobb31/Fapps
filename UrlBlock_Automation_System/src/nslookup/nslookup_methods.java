package nslookup;

import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class nslookup_methods {

    private static String processName = "NSLOOKUP_VALIDATOR";
    private static String processVersion = "1.0.0";
    private static boolean debugEnabled = true;
    private static DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static boolean processNslookupValidation(
            String sshIp,
            String sshPort,   // NEW
            String sshUser,
            String sshPass,
            String dbUrl,
            String dbUser,
            String dbPass,
            String tableName,
            String domain) {

        Connection conn = null;
        Session session = null;

        String connectivity = "No";
        String nslookupStatus = "No";
        String nslookupValidation = "No";

        try {
            validateIdentifier(tableName);
            validateDomain(domain);

            conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);

            //  INSERT INITIAL ROW AND GET ID
            long rowId = insertInitialRow(conn, tableName, connectivity, nslookupStatus, nslookupValidation);

            log("INFO", "Initial row inserted", sshIp, "DB_INSERT");

            //  SSH CONNECTION
            JSch jsch = new JSch();

            int port = Integer.parseInt(sshPort);

            //  THIS LINE WAS MISSING
            session = jsch.getSession(sshUser, sshIp, port);

            session.setPassword(sshPass);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect(10000);

            connectivity = "Yes";
            updateColumn(conn, tableName, "Connectivity", connectivity, rowId);

            log("INFO", "SSH Connected", sshIp, "SSH_SUCCESS");

            //  NSLOOKUP CHECK
            String output1 = executeCommand(session, "which nslookup");

            if (isNslookupWorking(output1)) {

                nslookupStatus = "Yes";
                updateColumn(conn, tableName, "nslookup_status", nslookupStatus, rowId);

                log("INFO", "nslookup command exists", sshIp, "NSLOOKUP_OK");

                //  VALIDATION
                String output2 = executeCommand(session, "nslookup " + domain);

                int count = countValidAddresses(output2);

                if (count >= 2) {
                    nslookupValidation = "Yes";
                    log("INFO", "Validation Passed with count=" + count, sshIp, "NSLOOKUP_VALID");
                } else {
                    log("ERROR", "Validation Failed with count=" + count, sshIp, "NSLOOKUP_INVALID");
                }

                updateColumn(conn, tableName, "nslookup_validation", nslookupValidation, rowId);

            } else {
                log("ERROR", "nslookup command not available", sshIp, "NSLOOKUP_FAIL");
                return false;
            }

            return true;

        } catch (Exception e) {
            log("ERROR", "Exception: " + e.getMessage(), sshIp, "EXCEPTION");
            return false;

        } finally {
            try {
                if (session != null && session.isConnected()) session.disconnect();
                if (conn != null && !conn.isClosed()) conn.close();
            } catch (Exception ignored) {}
        }
    }

    //  INSERT AND RETURN ID
    private static long insertInitialRow(Connection conn, String tableName,
                                         String connectivity, String nslookupStatus,
                                         String nslookupValidation) throws SQLException {

        String query = "INSERT INTO " + tableName +
                " (`Date`, `Connectivity`, `nslookup_status`, `nslookup_validation`) VALUES (NOW(), ?, ?, ?)";

        try (PreparedStatement ps = conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

            ps.setString(1, connectivity);
            ps.setString(2, nslookupStatus);
            ps.setString(3, nslookupValidation);

            ps.executeUpdate();

            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            }
        }

        throw new SQLException("Failed to retrieve generated ID");
    }

    //  UPDATE USING ID
    private static void updateColumn(Connection conn, String tableName,
                                     String column, String value, long id) throws SQLException {

        String query = "UPDATE " + tableName + " SET " + column + " = ? WHERE id = ?";

        try (PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setString(1, value);
            ps.setLong(2, id);
            ps.executeUpdate();
        }
    }

    //  SSH COMMAND EXECUTION
    private static String executeCommand(Session session, String command) throws Exception {

        ChannelExec channel = null;

        try {
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            channel.setInputStream(null);
            channel.setErrStream(System.err);

            InputStream in = channel.getInputStream();
            channel.connect();

            StringBuilder output = new StringBuilder();
            byte[] buffer = new byte[1024];

            while (true) {
                while (in.available() > 0) {
                    int i = in.read(buffer);
                    if (i < 0) break;
                    output.append(new String(buffer, 0, i));
                }

                if (channel.isClosed()) break;

                Thread.sleep(100);
            }

            return output.toString();

        } finally {
            if (channel != null) channel.disconnect();
        }
    }

    //  COUNT VALID ADDRESS LINES
    private static int countValidAddresses(String output) {

        int count = 0;

        if (output == null || output.trim().isEmpty()) return 0;

        for (String line : output.split("\\r?\\n")) {

            line = line.trim();

            if (line.startsWith("Address:")) {

                // Ignore DNS server line
                if (line.contains("#53")) continue;

                count++;
            }
        }

        return count;
    }

    //  CHECK NSLOOKUP EXISTS
    private static boolean isNslookupWorking(String output) {

        if (output == null || output.trim().isEmpty()) {
            return false;
        }

        String lower = output.toLowerCase();

        if (lower.contains("not found") ||
            lower.contains("command not found") ||
            lower.contains("no such file") ||
            lower.contains("error")) {
            return false;
        }

        return true;
    }

    //  VALIDATION
    private static void validateIdentifier(String value) {
        if (value == null || !value.matches("[A-Za-z0-9_]+")) {
            throw new IllegalArgumentException("Invalid table name");
        }
    }

    private static void validateDomain(String domain) {
        if (domain == null || !domain.matches("[A-Za-z0-9.-]+")) {
            throw new IllegalArgumentException("Invalid domain");
        }
    }

    //  LOGGER
    private static void log(String level, String message, String serverIP, String evtType) {

        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled)
            return;

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp).append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(processName).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(processVersion)
                .append(" Log_Level: EVT_LOG EVT_Type: ")
                .append(evtType != null ? evtType : "NA")
                .append(" Description: ")
                .append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private static String capitalize(String value) {
        if (value == null || value.isEmpty()) return "";
        return value.substring(0, 1).toUpperCase() + value.substring(1).toLowerCase();
    }
}