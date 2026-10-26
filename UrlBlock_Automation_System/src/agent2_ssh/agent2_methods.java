package agent2_ssh;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class agent2_methods {

	private static String processName = "SSH_CONNECTIVITY_CHECK";
	private static String module = "UrlBlock_Health_Process";

	private static DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private static DateTimeFormatter idFormat = DateTimeFormatter.ofPattern("ddMMyyyyHHmmssSSS");

	public static boolean processSSHCheck(String sshIp, String sshPort, String sshUser, String sshPass, String dbUrl,
			String dbUser, String dbPass, String tableName) {

		Connection conn = null;
		Session session = null;

		try {

			// DB CONNECT
			conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);

			int port = Integer.parseInt(sshPort);

			// STEP 1: FAST PORT CHECK (NO HANG)
			if (!isPortOpen(sshIp, port, 2000)) {

				log("ERROR", "Port not reachable", sshIp, "SSH_TIMEOUT");

				upsertEvent(conn, tableName, "Netsweeper_cli", "SSH port not reachable for " + sshIp + ":" + sshPort);

				return false;
			}

			// STEP 2: SSH CONNECT (CONTROLLED)
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

			log("INFO", "SSH connection successful", sshIp, "SSH_SUCCESS");

			return true; // success no DB entry

		} catch (Exception e) {

			log("ERROR", "SSH connection failed: " + e.getMessage(), sshIp, "SSH_FAIL");

			upsertEvent(conn, tableName, "Netsweeper_cli",
					"SSH failed for " + sshIp + ":" + sshPort + " | Reason: " + e.getMessage());

			return false;

		} finally {
			try {
				if (session != null && session.isConnected())
					session.disconnect();
				if (conn != null && !conn.isClosed())
					conn.close();
			} catch (Exception ignored) {
			}
		}
	}

	// PORT CHECK (CRITICAL - PREVENTS HANG)
	private static boolean isPortOpen(String ip, int port, int timeout) {
		try (Socket socket = new Socket()) {
			socket.connect(new InetSocketAddress(ip, port), timeout);
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	// DB INSERT (FAIL ONLY)
	private static void upsertEvent(Connection conn, String table, String evtName, String description) {
		PreparedStatement checkPs = null;
	    PreparedStatement insertPs = null;
	    ResultSet rs = null;

	    try {

	        if (conn == null) {
	            System.out.println("DB connection is null. Skipping insert.");
	            return;
	        }

	        //  trim for VARCHAR(255)
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
	// LOGGER
	private static void log(String level, String message, String serverIP, String evtType) {

		String timestamp = LocalDateTime.now().format(logFormat);

		System.out.println("Time:" + timestamp + " [" + level + "] " + "Process_Name:" + processName + " Server_IP:"
				+ serverIP + " EVT_Type:" + evtType + " Description:" + message);
	}
}