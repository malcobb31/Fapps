package rest_api_archivalV2;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import javax.net.ssl.*;
import org.json.*;

/**
 * REST API client for archiving tasks with database integration
 *
 * Fixed: idempotency and safer DB handling when the same parameters are used
 * repeatedly. - Avoids duplicate inserts into backup_process_archiving_details
 * - Reserves task_number atomically using LOCK TABLES (MySQL) - Checks for
 * existing job_id to avoid launching duplicate archive jobs - Uses defensive
 * checks and better logging
 */
public class rest_api_methods {

	// Configuration constants
	private static final String BASE_URL = "https://poc.itsimple.in/restapi";
	private static final String DB_URL = "jdbc:mysql://localhost:3306/pview?useSSL=false&serverTimezone=UTC";
	private static final String DB_USER = "root";
	private static final String DB_PASS = "";
	private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	// HTTP timeouts (in milliseconds)
	private static final int CONNECTION_TIMEOUT = 30000;
	private static final int READ_TIMEOUT = 30000;

	// Authentication credentials (consider moving to config file)
	private static final String AUTH_DB_NAME = "ADA";
	private static final String AUTH_USERNAME = "root";
	private static final String AUTH_PASSWORD = "ATempo@123";

	/**
	 * Main execution method for creating and running archiving tasks
	 */
	public void run(String enterpriseName, String username, String bucketName, String pathsInput) throws Exception {

		// Fetch Target_SMC_name automatically
		String Target_SMC_Name = getTargetSMCName(enterpriseName, username);
		if (Target_SMC_Name == null) {
			throw new Exception("Target_SMC_name not found for " + enterpriseName + " / " + username);
		}

		// Validate inputs
		validateInputs(enterpriseName, username, Target_SMC_Name, pathsInput);

		disableSSLVerification();

		String token = authenticate(BASE_URL, AUTH_DB_NAME, AUTH_USERNAME, AUTH_PASSWORD);
		logSuccess("Authentication successful");

		int taskNumber = getNextTaskNumber();
		String taskName = generateTaskName(enterpriseName, username, taskNumber);

		// USE AUTO-FETCHED Target_SMC_Name
		int taskIdFromApi = updateContainerDirectory(enterpriseName, username, Target_SMC_Name, token);

		logInfo("Received task id from container update: " + taskIdFromApi);

		if (pathsInput == null || pathsInput.equals("") || pathsInput.equals("/")) {
			pathsInput = " ";
		}

		// Insert archiving details (idempotent)
		insertArchivingDetails(enterpriseName, username, taskIdFromApi, Target_SMC_Name);
		
		// Start archive job only if not already launched for this
		// enterprise+username+SMC
		startArchiveJob(enterpriseName, username, bucketName, pathsInput, token, taskIdFromApi, Target_SMC_Name);
		
		insertTaskDetails(taskNumber, taskName);

		logSuccess("Process completed successfully");
		logInfo("========================================");
	}

	/**
	 * Validate input parameters
	 */
	private void validateInputs(String enterpriseName, String username, String Target_SMC_Name, String pathsInput)
			throws IllegalArgumentException {
		if (enterpriseName == null || enterpriseName.trim().isEmpty()) {
			throw new IllegalArgumentException("Enterprise name cannot be null or empty");
		}
		if (username == null || username.trim().isEmpty()) {
			throw new IllegalArgumentException("Username cannot be null or empty");
		}
		if (Target_SMC_Name == null || Target_SMC_Name.trim().isEmpty()) {
			throw new IllegalArgumentException("Bucket name cannot be null or empty");
		}

	}

	/**
	 * Generate task name from components
	 */
	private String generateTaskName(String enterpriseName, String username, int taskNumber) {
		return String.format("%s-%s-task%d", enterpriseName, username, taskNumber);
	}

	/**
	 * Authenticate with the REST API
	 */
	public String authenticate(String baseUrl, String dbName, String username, String password) throws Exception {
		String authUrl = baseUrl + "/auth/token/";
		logInfo("Authenticating user: " + username);
		logDebug("Authentication URL: " + authUrl);

		HttpURLConnection conn = null;
		try {
			conn = createConnection(authUrl, "POST");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");

			// Build authentication payload
			JSONObject body = new JSONObject();
			body.put("dbName", dbName);
			body.put("name", username);
			body.put("password", password);
			body.put("superUser", true);

			logDebug("Sending authentication request");

			// Send request
			writeRequestBody(conn, body.toString());

			// Read response
			int responseCode = conn.getResponseCode();
			logDebug("Authentication response code: " + responseCode);

			String responseText = readResponse(conn);
			logDebug("Authentication response received (length: " + responseText.length() + ")");

			// Handle response
			if (responseCode == 200 || responseCode == 201) {
				return parseAuthenticationToken(responseText);
			} else {
				handleAuthenticationError(responseCode, responseText);
				throw new Exception("Authentication failed with code: " + responseCode);
			}

		} catch (JSONException e) {
			logError("Failed to parse authentication response: " + e.getMessage());
			throw new Exception("Invalid JSON response from authentication endpoint", e);
		} catch (IOException e) {
			logError("Network error during authentication: " + e.getMessage());
			throw new Exception("Authentication request failed", e);
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	/**
	 * Parse authentication token from response
	 */
	private String parseAuthenticationToken(String responseText) throws Exception {
		if (!responseText.trim().startsWith("{")) {
			logError("Non-JSON authentication response received");
			throw new Exception("Server returned non-JSON response");
		}

		JSONObject jsonResponse = new JSONObject(responseText);

		if (jsonResponse.has("access")) {
			String token = jsonResponse.getString("access");
			logSuccess("Authentication token retrieved");
			return token;
		} else {
			logError("Response does not contain access token");
			throw new Exception("Access token not found in response");
		}
	}

	/**
	 * Handle authentication errors
	 */
	private void handleAuthenticationError(int responseCode, String responseText) {
		switch (responseCode) {
		case 401:
			logError("Authentication failed: Invalid credentials");
			break;
		case 403:
			logError("Authentication failed: Access forbidden");
			break;
		case 500:
			logError("Authentication failed: Server error");
			break;
		default:
			logError("Authentication failed with code " + responseCode + ": " + responseText);
		}
	}

	/**
	 * PUT to /infrastructure/storage-managers/9/containers/{id}/ Builds
	 * directory path and saves returned id/name/directory into DB table
	 * backup_process_archiving_details Returns the task_id (id from response)
	 * or throws on failure
	 */
	private int updateContainerDirectory(String enterpriseName, String username, String Target_SMC_Name, String token)
			throws Exception {

		// ---- Fetch the bucket_id ( = containerId ) from DB ----
		Integer containerId = getTargetSMCIdByName(Target_SMC_Name);
		if (containerId == null) {
			throw new RuntimeException("Target_SMC_id not found for Target_SMC_name: " + Target_SMC_Name);
		}

		// ---- Build Directory Path ----
		String directoryPath = String.format("C:\\DemoDirectory\\%s\\%s", enterpriseName, username);
		logInfo("Updating container directory for bucket: " + Target_SMC_Name + ", containerId: " + containerId);
		logDebug("Directory to set: " + directoryPath);

		HttpURLConnection conn = null;

		try {
			// ---- FIX: storageManagerPk = ALWAYS 9 ----
			String urlStr = BASE_URL + "/infrastructure/storage-managers/9/containers/" + containerId + "/";

			logDebug("Container Update URL: " + urlStr);

			conn = createConnection(urlStr, "PUT");
			conn.setRequestProperty("Authorization", "Bearer " + token);
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");
			conn.setDoOutput(true);

			// ---- Payload ----
			JSONObject payload = new JSONObject();
			payload.put("directory", directoryPath);

			logDebug("Container update payload: " + payload.toString(2));

			writeRequestBody(conn, payload.toString());

			int responseCode = conn.getResponseCode();
			String response = readResponse(conn);

			logDebug("Container PUT response code: " + responseCode);
			logDebug("Container PUT response: " + response);

			if (responseCode == 200) {
				JSONObject json = new JSONObject(response);

				int taskId = json.getInt("id");
				String taskName = json.getString("name");
				String taskDirectory = json.getString("directory");

				logSuccess("Container directory updated successfully. Task ID: " + taskId);

				// ---- Save to DB ----
				// Use idempotent insert (only insert if no row exists with same
				// task_id)
				insertArchivingDetailsFromApi(taskId, taskName, taskDirectory);

				return taskId;
			} else {
				throw new RuntimeException("Container update failed: " + response);
			}

		} finally {
			if (conn != null)
				conn.disconnect();
		}
	}

	private void insertArchivingDetailsFromApi(int taskId, String taskName, String taskDirectory) {
		String queryUpdate = "UPDATE backup_process_archiving_details SET task_name = ?, task_directory = ? WHERE task_id = ?";

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
				PreparedStatement ps = conn.prepareStatement(queryUpdate)) {

			ps.setString(1, taskName);
			ps.setString(2, taskDirectory);
			ps.setInt(3, taskId);

			int rows = ps.executeUpdate();
			if (rows > 0) {
				logDebug("Updated archiving details from API for task_id:" + taskId);
			} else {
				logWarn("No row found for task_id " + taskId + " to update."
						+ " This means insertArchivingDetails() was not called.");
			}

		} catch (SQLException e) {
			logError("Failed to update task_name/directory: " + e.getMessage());
		}
	}

	private void startArchiveJob(String enterpriseName, String username, String bucketName, String pathsInput,
			String token, int taskId, String targetSmcName) throws Exception {

		String urlStr = BASE_URL + "/files/archive/";
		logInfo("Starting archive job for task_id: " + taskId);
		logDebug("Archive API URL: " + urlStr);

		// --- Idempotency check
		if (isArchiveJobAlreadyLaunched(enterpriseName, username, targetSmcName)) {
			logInfo("An archive job was already launched for enterprise=" + enterpriseName + ", username=" + username
					+ ", targetSmc=" + targetSmcName + ". Skipping new launch.");
			return;
		}

		HttpURLConnection conn = null;

		// These will be passed to updateJobId()
		String lastSource = null;
		String lastDestination = null;

		try {
			conn = createConnection(urlStr, "POST");
			conn.setRequestProperty("Authorization", "Bearer " + token);
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");

			// Normalize paths
			List<String> paths = Arrays.stream((pathsInput == null ? "" : pathsInput).split(",")).map(String::trim)
					.filter(p -> !p.isEmpty()).map(p -> {
						while (p.startsWith("/"))
							p = p.substring(1);
						return p;
					}).collect(Collectors.toList());

			if (paths.isEmpty())
				paths.add("");

			// Build pathInfo
			JSONArray pathInfoArr = new JSONArray();

			for (String path : paths) {
				JSONObject pi = new JSONObject();

				String source;
				if (path.isEmpty()) {
					source = String.format("s3@%s-%s-%s-SourceSMCPN:/", enterpriseName, username, bucketName);
				} else {
					source = String.format("s3@%s-%s-%s-SourceSMCPN:/%s", enterpriseName, username, bucketName, path);
				}

				String destination = String.format("archive@%s:/%s", enterpriseName, username);

				pi.put("source", source);
				pi.put("destination", destination);

				pathInfoArr.put(pi);
				logDebug("Added pathInfo entry: source=" + source + ", destination=" + destination);

				// Store last values for DB insert
				lastSource = source;
				lastDestination = destination;
			}

			// Build final payload
			JSONObject payload = new JSONObject();
			payload.put("pathInfo", pathInfoArr);
			payload.put("osType", "Windows");
			payload.put("createFolder", true);
			payload.put("metadataList", new JSONArray());

			JSONArray settings = new JSONArray();
			settings.put(new JSONObject().put("archiveSymLinks", true));
			payload.put("settings", settings);

			logDebug("Archive job payload: " + payload.toString(2));

			writeRequestBody(conn, payload.toString());

			int responseCode = conn.getResponseCode();
			String response = readResponse(conn);

			logDebug("Archive POST response code: " + responseCode);
			logDebug("Archive POST response: " + response);

			if (responseCode == 200 || responseCode == 201) {
				JSONObject json = new JSONObject(response);
				

				if (json.has("jobId")) {
					int jobId = json.getInt("jobId");
					
					logSuccess("Archiving job launched. jobId: " + jobId);

					// *** FIXED: lastSource and lastDestination are now valid
					// ***
					updateJobId(taskId, jobId, lastSource, lastDestination);
					

				} else {
					logWarn("Archive API responded OK but missing jobId");
				}
			} else if (responseCode == 401) {
				logError("Unauthorized: Token may be invalid for archive POST");
				throw new RuntimeException("Authorization failed during archive POST");
			} else {
				logError("Archive POST failed. Code: " + responseCode);
				throw new RuntimeException("Archive POST failed: " + response);
			}

		} catch (IOException | JSONException e) {
			logError("Exception during archive POST: " + e.getMessage());
			throw e;
		} finally {
			if (conn != null)
				conn.disconnect();
		}
	}

	/**
	 * Check DB whether an archive job has already been launched for the same
	 * enterprise+username+targetSmc
	 */
	private boolean isArchiveJobAlreadyLaunched(String enterpriseName, String username, String targetSmcName) {
		String q = "SELECT job_id FROM backup_process_archiving_details WHERE enterprise_name = ? AND username = ? AND Target_SMC_name = ? AND job_id IS NOT NULL LIMIT 1";
		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
				PreparedStatement ps = conn.prepareStatement(q)) {
			ps.setString(1, enterpriseName);
			ps.setString(2, username);
			ps.setString(3, targetSmcName);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					int jobId = rs.getInt(1);
					logDebug("Found existing job_id " + jobId + " for enterprise=" + enterpriseName + ", username="
							+ username);
					return true;
				}
			}
		} catch (SQLException e) {
			logError("DB error checking existing archive job: " + e.getMessage());
		}
		return false;
	}

	// ============= DATABASE METHODS =============

	/**
	 * Get the next task number from database
	 * <p>
	 * This implementation uses MySQL table lock to ensure only one process
	 * reserves the next number at a time.
	 */
	private int getNextTaskNumber() {
		String selectQuery = "SELECT COALESCE(MAX(task_number), 0) FROM backup_process_task_details";
		String insertQuery = "INSERT INTO backup_process_task_details (task_number) VALUES (?)";

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
			conn.setAutoCommit(false);

			try (Statement lockStmt = conn.createStatement()) {
				// Lock the table for write (MySQL). This prevents race
				// conditions across connections.
				lockStmt.execute("LOCK TABLES backup_process_task_details WRITE");
			}

			int nextNumber = 1;
			try (PreparedStatement ps = conn.prepareStatement(selectQuery); ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					nextNumber = rs.getInt(1) + 1;
				}
			}

			try (PreparedStatement ps2 = conn.prepareStatement(insertQuery)) {
				ps2.setInt(1, nextNumber);
				ps2.executeUpdate();
			}

			try (Statement unlock = conn.createStatement()) {
				unlock.execute("UNLOCK TABLES");
			}

			conn.commit();
			logDebug("Next task_number reserved: " + nextNumber);
			return nextNumber;

		} catch (SQLException e) {
			logWarn("task_number reservation failed: " + e.getMessage());
			return 1;
		}
	}

	/**
	 * Insert archiving details into database (idempotent)
	 */
	private void insertArchivingDetails(String enterpriseName, String username, int taskId, String targetSMCName) {

		if (targetSMCName == null || targetSMCName.trim().isEmpty()) {
			logError("Unable to insert archiving details — Target_SMC_name not found!");
			return;
		}

		String queryExists = "SELECT 1 FROM backup_process_archiving_details WHERE enterprise_name = ? AND username = ? AND Target_SMC_name = ? AND task_id = ? LIMIT 1";
		String queryInsert = "INSERT INTO backup_process_archiving_details (enterprise_name, username, Target_SMC_name, task_id) VALUES (?, ?, ?, ?)";

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
			// Check existing
			try (PreparedStatement ps = conn.prepareStatement(queryExists)) {
				ps.setString(1, enterpriseName);
				ps.setString(2, username);
				ps.setString(3, targetSMCName);
				ps.setInt(4, taskId);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						logDebug("Archiving details already present for task_id: " + taskId + ", skipping insert.");
						return;
					}
				}
			}

			try (PreparedStatement ps2 = conn.prepareStatement(queryInsert)) {
				ps2.setString(1, enterpriseName);
				ps2.setString(2, username);
				ps2.setString(3, targetSMCName);
				ps2.setInt(4, taskId);

				ps2.executeUpdate();
				logDebug("Inserted archiving details for task_id: " + taskId);
			}

		} catch (SQLException e) {
			logError("Error inserting archiving details: " + e.getMessage());
		}
	}

	private Integer getTargetSMCIdByName(String targetSmcName) {
		String query = "SELECT Target_SMC_id FROM backup_process_enterprise_user_detail "
				+ "WHERE Target_SMC_name = ? LIMIT 1";

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
				PreparedStatement ps = conn.prepareStatement(query)) {

			ps.setString(1, targetSmcName);

			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					int id = rs.getInt("Target_SMC_id");
					logDebug("Found Target_SMC_id " + id + " for Target_SMC_name: " + targetSmcName);
					return id;
				} else {
					logWarn("No Target_SMC_id found for Target_SMC_name: " + targetSmcName);
					return null;
				}
			}

		} catch (SQLException e) {
			logError("DB error getting Target_SMC_id for " + targetSmcName + ": " + e.getMessage());
			return null;
		}
	}
	 public void dbupdate(int jobId)
	    {
	    	String sql = "UPDATE backup_process_archiving_details SET process_name = ? WHERE job_id="+ jobId;
	    	
	    	try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
	                PreparedStatement ps = conn.prepareStatement(sql)) {

	               ps.setString(1, "Archive");
	               int rows = ps.executeUpdate();
	               logInfo("DB Update completed Rows affected: " + rows);

	           } catch (Exception e) {
	               logError("DB Error (saveProcessName): " + e.getMessage());
	           }
	       }
	    
    
	/**
	 * Update job ID for a task
	 */
	private void updateJobId(int taskId, int jobId, String source, String destination) {
		String query = "UPDATE backup_process_archiving_details SET job_id = ?,source_path = ?,destination_path = ? WHERE task_id = ?";

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
				PreparedStatement ps = conn.prepareStatement(query)) {

			ps.setInt(1, jobId);
			ps.setString(2, source);
			ps.setString(3, destination);
			ps.setInt(4, taskId);
			
			int rowsAffected = ps.executeUpdate();

			if (rowsAffected > 0) {
				logDebug("Job ID updated for task ID: " + taskId);
				dbupdate(jobId);
			} else {
				logWarn("No rows updated for task ID: " + taskId);
			}

		} catch (SQLException e) {
			logError("Failed to update job ID: " + e.getMessage());
		}
	}

	private String getTargetSMCName(String enterpriseName, String username) {
		String query = "SELECT Target_SMC_name FROM backup_process_enterprise_user_detail "
				+ "WHERE enterprise_name = ? AND user_name = ? LIMIT 1";

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
				PreparedStatement ps = conn.prepareStatement(query)) {

			ps.setString(1, enterpriseName);
			ps.setString(2, username);

			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getString("Target_SMC_name");
				} else {
					logError("No Target_SMC_name found for enterprise=" + enterpriseName + ", username=" + username);
					return null;
				}
			}

		} catch (SQLException e) {
			logError("DB error while fetching Target_SMC_name: " + e.getMessage());
			return null;
		}
	}

	/**
	 * Insert task details into database (idempotent on name)
	 */
	private void insertTaskDetails(int taskNumber, String taskName) {
		String queryExists = "SELECT 1 FROM backup_process_task_details WHERE task_number = ? OR name = ? LIMIT 1";
		String queryInsert = "INSERT INTO backup_process_task_details (task_number, name) VALUES (?, ?)";

		try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
			try (PreparedStatement ps = conn.prepareStatement(queryExists)) {
				ps.setInt(1, taskNumber);
				ps.setString(2, taskName);
				try (ResultSet rs = ps.executeQuery()) {
					System.out.println("ResultSet : " + rs.next());
					if (rs.next()) {
						logDebug("Task already exists (by number or name): " + taskName);
						return;
					}
				}
			}

			try (PreparedStatement ps2 = conn.prepareStatement(queryInsert)) {
				ps2.setInt(1, taskNumber);
				ps2.setString(2, taskName);

				int rowsAffected = ps2.executeUpdate();

				if (rowsAffected > 0) {
					logDebug("Task details inserted: " + taskName);
				} else {
					logWarn("No rows inserted into task_details");
				}
			}

		} catch (SQLException e) {
			logError("Failed to insert task details: " + e.getMessage());
		}
	}

	// ============= HTTP UTILITIES =============

	/**
	 * Create and configure HTTP connection
	 */
	private HttpURLConnection createConnection(String urlStr, String method) throws IOException {
		URL url = new URL(urlStr);
		URLConnection uc = url.openConnection();
		HttpURLConnection conn;

		if (uc instanceof HttpsURLConnection) {
			conn = (HttpsURLConnection) uc;
		} else {
			conn = (HttpURLConnection) uc;
		}

		conn.setRequestMethod(method);
		conn.setConnectTimeout(CONNECTION_TIMEOUT);
		conn.setReadTimeout(READ_TIMEOUT);
		conn.setDoOutput(true);
		conn.setDoInput(true);
		return conn;
	}

	/**
	 * Write request body to connection
	 */
	private void writeRequestBody(HttpURLConnection conn, String body) throws IOException {
		try (OutputStream os = conn.getOutputStream();
				OutputStreamWriter writer = new OutputStreamWriter(os, StandardCharsets.UTF_8)) {
			writer.write(body);
			writer.flush();
		}
	}

	/**
	 * Read response from HTTP connection
	 */
	private String readResponse(HttpURLConnection conn) throws IOException {
		int responseCode = conn.getResponseCode();

		// Choose appropriate stream based on response code
		InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream() : conn.getErrorStream();

		if (is == null) {
			return "";
		}

		return readStream(is);
	}

	/**
	 * Read input stream to string
	 */
	private String readStream(InputStream is) throws IOException {
		StringBuilder sb = new StringBuilder();

		try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line).append("\n");
			}
		}

		return sb.toString().trim();
	}

	// ============= SSL CONFIGURATION =============

	/**
	 * Disable SSL verification (WARNING: Use only for development/testing)
	 */
	private void disableSSLVerification() {
		logWarn("Disabling SSL certificate verification - NOT RECOMMENDED FOR PRODUCTION");

		try {
			// Create trust manager that accepts all certificates
			TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
				public java.security.cert.X509Certificate[] getAcceptedIssuers() {
					return null;
				}

				public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
					// Trust all clients
				}

				public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
					// Trust all servers
				}
			} };

			// Install the trust manager
			SSLContext sc = SSLContext.getInstance("TLS");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

			// Disable hostname verification
			HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);

		} catch (Exception e) {
			logError("Failed to disable SSL verification: " + e.getMessage());
		}
	}

	// ============= LOGGING METHODS =============

	private void logInfo(String msg) {
		System.out.println(formatLogMessage("INFO", msg));
	}

	private void logDebug(String msg) {
		System.out.println(formatLogMessage("DEBUG", msg));
	}

	private void logSuccess(String msg) {
		System.out.println(formatLogMessage("SUCCESS", msg));
	}

	private void logError(String msg) {
		System.err.println(formatLogMessage("ERROR", msg));
	}

	private void logWarn(String msg) {
		System.out.println(formatLogMessage("WARN", msg));
	}

	private String formatLogMessage(String level, String msg) {
		return String.format("[%s] [%s] %s", LocalDateTime.now().format(TIME_FORMATTER), level, msg);
	}

	// ============= MAIN METHOD FOR TESTING =============

	public static void main(String[] args) {

	}
}
