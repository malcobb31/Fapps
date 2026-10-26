package agent1_full;

import java.io.InputStream;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import com.jcraft.jsch.*;

public class agent1_methods {

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

            //  SSH CONNECTION
            JSch jsch = new JSch();
            session = jsch.getSession(sshUser, sshIp, Integer.parseInt(sshPort));
            session.setPassword(sshPass);

            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect(10000);

            log("INFO", "SSH Connected", sshIp, "SSH_SUCCESS");

        } catch (Exception e) {

            upsertEvent(conn, tableName,
                    "Netsweeper_cli",
                    "SSH connection failed: " + e.getMessage());

            return false;
        }

        try {

            //  CHECK NSLOOKUP
            String check = executeCommand(session, "which nslookup");

            if (check == null || !check.contains("nslookup")) {

                upsertEvent(conn, tableName,
                        "nsLookUp_connectivity",
                        "nslookup command not found on server");

                return false;
            }

            //  RUN NSLOOKUP
            String output = executeCommand(session, "nslookup " + domain);

            int count = countValidAddresses(output);

            if (count < 2) {

                upsertEvent(conn, tableName,
                        "nsLookUp_status",
                        "nslookup failed for domain: " + domain + " | output: " + output);

                return false;
            }

            //  SUCCESS  NO DB ENTRY
            log("INFO", "Validation passed", sshIp, "SUCCESS");

            return true;

        } catch (Exception e) {

            upsertEvent(conn, tableName,
                    "nsLookUp_status",
                    "Error during nslookup: " + e.getMessage());

            return false;

        } finally {
            try {
                if (session != null) session.disconnect();
                if (conn != null) conn.close();
            } catch (Exception ignored) {}
        }
    }

    //  INSERT EVENT
    private static void upsertEvent(Connection conn, String table, String evtName, String description) {
    	    PreparedStatement checkPs = null;
    	    PreparedStatement insertPs = null;
    	    ResultSet rs = null;

    	    try {

    	        if (conn == null) {
    	            System.out.println("DB connection is null. Skipping insert.");
    	            return;
    	        }

    	        //  truncate for VARCHAR(255)
    	        if (description != null && description.length() > 255) {
    	            description = description.substring(0, 255);
    	        }

    	        String checkQuery = "SELECT Status FROM " + table +
    	                " WHERE Evt_Name = ? ORDER BY Evt_TimeStamp DESC LIMIT 1";

    	        checkPs = conn.prepareStatement(checkQuery);
    	        checkPs.setString(1, evtName);

    	        rs = checkPs.executeQuery();

    	        String newStatus = "INIT"; // default

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
    //  SSH EXEC
    private static String executeCommand(Session session, String command) throws Exception {

        ChannelExec channel = (ChannelExec) session.openChannel("exec");
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

        channel.disconnect();
        return output.toString();
    }

    //  COUNT VALID ADDRESS
    private static int countValidAddresses(String output) {

        int count = 0;

        if (output == null) return 0;

        for (String line : output.split("\\r?\\n")) {

            line = line.trim();

            if (line.startsWith("Address:")) {
                count++;
            }
        }

        return count;
    }

    //  LOGGER
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