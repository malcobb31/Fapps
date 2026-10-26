package agent1_full_v2;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class agent1_methods_v2 {

    private static String processName = "NSLOOKUP_VALIDATOR";
    private static String module = "UrlBlock_Health_Process";
    private static DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static DateTimeFormatter idFormat = DateTimeFormatter.ofPattern("ddMMyyyyHHmmssSSS");

    public static boolean processNslookupValidation(
            String sshIp,
            String sshPort,
            String sshUser,
            String sshPass,
            String dbUrl,
            String dbUser,
            String dbPass,
            String tableName,
            String domain) {

        Connection conn = null;
        Session session = null;

        try {
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);

            int port = Integer.parseInt(sshPort);

            // Fast fail if port is not reachable
            if (!isPortOpen(sshIp, port, 2000)) {
                log("ERROR", "Port not reachable", sshIp, "SSH_TIMEOUT");
                insertEventBasedOnPrevious(conn, tableName,
                        "Netsweeper_cli",
                        "SSH port not reachable for " + sshIp + ":" + sshPort);
                return false;
            }

            // SSH CONNECTION
            JSch jsch = new JSch();
            session = jsch.getSession(sshUser, sshIp, port);
            session.setPassword(sshPass);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            config.put("PreferredAuthentications", "password");
            config.put("MaxAuthTries", "1");

            session.setConfig(config);
            session.setTimeout(3000);
            session.connect(3000);

            log("INFO", "SSH Connected", sshIp, "SSH_SUCCESS");

            // Success: close latest Netsweeper_cli event if open
            closeAllOpenEvents(conn, tableName, "Netsweeper_cli");

        } catch (Exception e) {
            insertEventBasedOnPrevious(conn, tableName,
                    "Netsweeper_cli",
                    "SSH connection failed: " + e.getMessage());
            return false;
        }

        try {
            // CHECK NSLOOKUP EXISTS
            String check = executeCommand(session, "which nslookup");

            if (check == null || !check.contains("nslookup")) {
                insertEventBasedOnPrevious(conn, tableName,
                        "nsLookUp_connectivity",
                        "nslookup command not found on server");
                return false;
            }

            // Success: close latest nsLookUp_connectivity event if open
       
            closeAllOpenEvents(conn, tableName, "nsLookUp_connectivity");
            

            // RUN NSLOOKUP
            String output = executeCommand(session, "nslookup " + domain);

            int count = countValidAddresses(output);

            if (count < 2) {
                String shortOutput = output;
                if (shortOutput != null && shortOutput.length() > 120) {
                    shortOutput = shortOutput.substring(0, 120);
                }

                insertEventBasedOnPrevious(conn, tableName,
                        "nsLookUp_status",
                        "nslookup failed for domain: " + domain + " | output: " + shortOutput);
                return false;
            }

            // Success: close latest nsLookUp_status event if open
            closeAllOpenEvents(conn, tableName, "nsLookUp_status");

            log("INFO", "Validation passed", sshIp, "SUCCESS");
            return true;

        } catch (Exception e) {
            insertEventBasedOnPrevious(conn, tableName,
                    "nsLookUp_status",
                    "Error during nslookup: " + e.getMessage());
            return false;

        } finally {
            try {
                if (session != null && session.isConnected()) {
                    session.disconnect();
                }
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (Exception ignored) {
            }
        }
    }

    // Insert a new event row, using the previous status only as reference
    private static void insertEventBasedOnPrevious(Connection conn, String table, String evtName, String description) {

        PreparedStatement checkPs = null;
        PreparedStatement insertPs = null;
        ResultSet rs = null;

        try {
            if (conn == null) {
                System.out.println("DB connection is null. Skipping insert.");
                return;
            }

            if (description != null && description.length() > 255) {
                description = description.substring(0, 255);
            }

            String checkQuery = "SELECT Status FROM " + table +
                    " WHERE Evt_Name = ? ORDER BY Evt_TimeStamp DESC LIMIT 1";

            checkPs = conn.prepareStatement(checkQuery);
            checkPs.setString(1, evtName);
            rs = checkPs.executeQuery();

            String newStatus = "INIT";

            if (rs.next()) {
                String previousStatus = rs.getString("Status");
                if (previousStatus == null || previousStatus.trim().equalsIgnoreCase("CLOSED")) {
                    newStatus = "INIT";
                } else {
                    newStatus = "INIT_REPEAT";
                }
            }

            String evtId = LocalDateTime.now().format(idFormat);
            Timestamp ts = Timestamp.valueOf(LocalDateTime.now());

            String insertQuery = "INSERT INTO " + table +
                    " (Evt_id, Evt_Name, Evt_Description, Origin_Process, Module, Evt_TimeStamp, Status, Last_Update_Timestamp) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

            insertPs = conn.prepareStatement(insertQuery);
            insertPs.setString(1, evtId);
            insertPs.setString(2, evtName);
            insertPs.setString(3, description);
            insertPs.setString(4, processName);
            insertPs.setString(5, module);
            insertPs.setTimestamp(6, ts);
            insertPs.setString(7, newStatus);
            insertPs.setTimestamp(8, ts);

            insertPs.executeUpdate();

        } catch (Exception e) {
            System.out.println("DB Insert Failed: " + e.getMessage());

        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
            try { if (checkPs != null) checkPs.close(); } catch (Exception ignored) {}
            try { if (insertPs != null) insertPs.close(); } catch (Exception ignored) {}
        }
    }

    // Close the latest open row for a given event type
    private static void closeAllOpenEvents(Connection conn, String table, String evtName) {

        PreparedStatement ps = null;

        try {

            if (conn == null) {
                System.out.println("DB connection is null. Skipping close update.");
                return;
            }

            Timestamp ts = Timestamp.valueOf(LocalDateTime.now());

            String updateQuery = "UPDATE " + table +
                    " SET Status = ?, Last_Update_Timestamp = ? " +
                    " WHERE Evt_Name = ? " +
                    " AND Status IS NOT NULL " +
                    " AND UPPER(Status) NOT IN ('CLOSED', 'INIT_REPEAT')";

            ps = conn.prepareStatement(updateQuery);

            ps.setString(1, "CLOSED");
            ps.setTimestamp(2, ts);
            ps.setString(3, evtName);

            int updatedRows = ps.executeUpdate();

            if (updatedRows > 0) {
                System.out.println("Closed " + updatedRows + " open events for " + evtName);
            }

        } catch (Exception e) {
            System.out.println("Close Event Failed: " + e.getMessage());

        } finally {
            try { if (ps != null) ps.close(); } catch (Exception ignored) {}
        }
    }

    // Fast port check to prevent hangs
    private static boolean isPortOpen(String ip, int port, int timeout) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(ip, port), timeout);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // SSH EXEC
    private static String executeCommand(Session session, String command) throws Exception {

        ChannelExec channel = null;
        try {
            channel = (ChannelExec) session.openChannel("exec");
            channel.setCommand(command);
            channel.setInputStream(null);

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
            if (channel != null) {
                channel.disconnect();
            }
        }
    }

    // COUNT VALID ADDRESS
    private static int countValidAddresses(String output) {

        int count = 0;

        if (output == null) return 0;

        for (String line : output.split("\\r?\\n")) {
            line = line.trim();
            if (line.startsWith("Address:") && !line.contains("#53")) {
                count++;
            }
        }

        return count;
    }

    // LOGGER
    private static void log(String level, String message, String serverIP, String evtType) {

        String timestamp = LocalDateTime.now().format(logFormat);

        System.out.println("Time:" + timestamp +
                " [" + level + "] " +
                "Process_Name:" + processName +
                " Server_IP:" + serverIP +
                " EVT_Type:" + evtType +
                " Description:" + message);
    }
}