package rest_api_archival;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import javax.net.ssl.*;
import org.json.*;

public class rest_api_methods_final {

    private static final String BASE_URL = "https://poc.itsimple.in/restapi";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/pview";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void run(String enterpriseName, String username, String bucketName, String pathsInput) throws Exception {
        disableSSL();
        logInfo("========================================");
        logInfo("Starting Task Process");
        logInfo("Enterprise: " + enterpriseName);
        logInfo("Username: " + username);
        logInfo("Bucket: " + bucketName);
        logInfo("Paths: " + pathsInput);
        logInfo("========================================");

        String token = authenticate(BASE_URL, "ADA", "root", "ATempo@123");
        logSuccess("Authentication successful.");

        int taskNumber = getNextTaskNumber();
        String taskName = enterpriseName + "-" + username + "-task" + taskNumber;

        JSONArray datasetArray = new JSONArray();
        for (String p : pathsInput.split(",")) {
            JSONObject pathObj = new JSONObject();
            pathObj.put("path", "s3@" + bucketName + ":/" + p.trim());
            datasetArray.put(pathObj);
        }

        JSONObject payload = new JSONObject();
        payload.put("waitIfMaxSimultaneousRunsReached", false);
        payload.put("name", taskName);
        payload.put("enabled", true);

        payload.put("type", new JSONObject().put("name", "auto_archiving"));
        payload.put("source", new JSONObject().put("name", bucketName));
        payload.put("dataset", datasetArray);

        JSONObject target = new JSONObject();
        target.put("repository", enterpriseName);
        target.put("repositoryPath", "/" + enterpriseName + "/" + enterpriseName + "-" + username);
        payload.put("target", target);

        JSONObject options = new JSONObject();
        options.put("archiveSameFileAgain", true);
        options.put("compareUsingDigest", false);
        options.put("archiveEmptyFolders", true);
        payload.put("options", options);

        payload.put("archiveProject", new JSONObject().put("name", enterpriseName + "-Project"));

        logDebug("Task payload: " + payload.toString(2));

        int taskId = createTask(payload, token);
        insertArchivingDetails(enterpriseName, username, bucketName, taskId);

        int jobId = runTask(taskId, token);
        updateJobId(taskId, jobId);

        insertTaskDetails(taskNumber, taskName);
        logSuccess("Task executed successfully with Job ID: " + jobId);
    }

    // ============= AUTHENTICATE =============
    public String authenticate(String baseUrl, String dbName, String username, String password) throws Exception {
		String authUrl = baseUrl + "/auth/token/";
		logInfo("Authenticating user: " + username);
		logDebug("Authentication URL: " + authUrl);
		logDebug("Database: " + dbName);

		HttpURLConnection conn = null;
		try {
			URL url = new URL(authUrl);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");

			// Build JSON payload
			JSONObject body = new JSONObject();
			body.put("dbName", dbName);
			body.put("name", username);
			body.put("password", password);
			body.put("superUser", true);

			logDebug("Request payload: " + body.toString(2));

			// Send request
			try (OutputStream os = conn.getOutputStream()) {
				os.write(body.toString().getBytes(StandardCharsets.UTF_8));
			}

			int responseCode = conn.getResponseCode();
			logDebug("Authentication response code: " + responseCode);

			InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
					: conn.getErrorStream();

			String responseText = readStream(is);
			logDebug("Authentication response: " + responseText);

			// Validate response
			if (responseCode == 200 || responseCode == 201) {
				if (!responseText.trim().startsWith("{")) {
					logError("Non-JSON authentication response received");
					throw new Exception("Server returned non-JSON response");
				}

				JSONObject jsonResponse = new JSONObject(responseText);
				if (jsonResponse.has("access")) {
					String token = jsonResponse.getString("access");
					logSuccess("Authentication successful");
					return token;
				} else {
					logError("Response does not contain access token");
					throw new Exception("Access token not found in response");
				}
			} else if (responseCode == 401) {
				logError("Authentication failed: Invalid credentials");
				throw new Exception("Invalid username or password");
			} else if (responseCode == 403) {
				logError("Authentication failed: Access forbidden");
				throw new Exception("User does not have required permissions");
			} else {
				logError("Authentication failed with code: " + responseCode);
				throw new Exception("Authentication failed: " + responseText);
			}

		} catch (Exception e) {
			logError("Exception during authentication: " + e.getMessage());
			throw e;
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

    // ============= CREATE TASK =============
    private int createTask(JSONObject payload, String token) throws Exception {
        try {
            String urlStr = BASE_URL + "/activity/tasks/";
            URL url = new URL(urlStr);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);
            System.out.println("Payload After: "+ payload.toString(2));
            try (OutputStream os = conn.getOutputStream()) {
            	
                os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
					: conn.getErrorStream();
            String response = readStream(is);
            logDebug("Response Code: " + responseCode);
            logDebug("Create Task API Response: " + response);

            if (responseCode != 200 && responseCode != 201) {
                logError("Create Task failed with code " + responseCode);
                throw new IOException("Invalid response code " + responseCode);
            }

            JSONObject json = new JSONObject(response);
            return json.getInt("id");

        } catch (IOException e) {
            logError("Create Task API failed: " + e.getMessage());
            throw e;
        }
    }


    // ============= RUN TASK =============
    private int runTask(int taskId, String token) throws Exception {
        String urlStr = BASE_URL + "/activity/tasks/" + taskId + "/run/";
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        JSONObject body = new JSONObject().put("testMode", false);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.toString().getBytes(StandardCharsets.UTF_8));
        }

        String response = readResponse(conn);
        JSONObject json = new JSONObject(response);
        return json.getInt("jobId");
    }

    // ============= DATABASE =============
    private int getNextTaskNumber() {
        String query = "SELECT MAX(task_number) FROM task_details";
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(query);
             ResultSet rs = ps.executeQuery()) {
            if (rs.next()) return rs.getInt(1) + 1;
        } catch (SQLException e) {
            logWarn("task_details check failed: " + e.getMessage());
        }
        return 1;
    }

    private void insertArchivingDetails(String enterprise, String user, String bucket, int taskId) {
        String query = "INSERT INTO archiving_details (enterprise_name, username, bucket_name, task_id, created_at) " +
                       "VALUES (?, ?, ?, ?, NOW())";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(query)) {

            ps.setString(1, enterprise);
            ps.setString(2, user);
            ps.setString(3, bucket);
            ps.setInt(4, taskId);

            int rowsAffected = ps.executeUpdate();

            if (rowsAffected > 0) {
                logDebug("Archiving details inserted for task ID: " + taskId);
            } else {
                logWarn("No rows inserted into archiving_details");
            }

        } catch (SQLException e) {
            logError("Failed to insert archiving details: " + e.getMessage());
        }
    }


    private void updateJobId(int taskId, int jobId) {
        String query = "UPDATE archiving_details SET job_id = ? WHERE task_id = ?";
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setInt(1, jobId);
            ps.setInt(2, taskId);
            ps.executeUpdate();
        } catch (SQLException e) {
            logError("Failed updating job_id: " + e.getMessage());
        }
    }

    private void insertTaskDetails(int taskNumber, String taskName) {
        String query = "INSERT INTO task_details (task_number, name) VALUES (?, ?)";
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(query)) {
            ps.setInt(1, taskNumber);
            ps.setString(2, taskName);
            ps.executeUpdate();
        } catch (SQLException e) {
            logWarn("Failed inserting into task_details: " + e.getMessage());
        }
    }

    // ============= SSL & UTILS =============
    private void disableSSL() throws Exception {
        TrustManager[] trustAll = new TrustManager[]{new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() { return null; }
            public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
            public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
        }};
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAll, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        HttpsURLConnection.setDefaultHostnameVerifier((h, s) -> true);
    }

    private String readResponse(HttpURLConnection conn) throws IOException {
        InputStream is = conn.getResponseCode() >= 400 ? conn.getErrorStream() : conn.getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) sb.append(line);
        br.close();
        return sb.toString();
    }
    private static String readStream(InputStream is) throws IOException {
		if (is == null) {
			return "";
		}

		StringBuilder sb = new StringBuilder();
		try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		}

		return sb.toString();
	}

    // ============= LOGGING =============
    private void logInfo(String msg) { System.out.println(getTime() + " [INFO] " + msg); }
    private void logDebug(String msg) { System.out.println(getTime() + " [DEBUG] " + msg); }
    private void logSuccess(String msg) { System.out.println(getTime() + " [SUCCESS] " + msg); }
    private void logError(String msg) { System.err.println(getTime() + " [ERROR] " + msg); }
    private void logWarn(String msg) { System.out.println(getTime() + " [WARN] " + msg); }
    private String getTime() { return "[" + LocalDateTime.now().format(TIME_FORMATTER) + "]"; }
}
