package rest_api_v3;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import javax.net.ssl.*;
import org.json.*;

public class rest_api_methods {

    // =========================== CONFIG ===========================
    private static final String BASE_URL = "https://poc.itsimple.in/restapi";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/pview";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final String LOG_PREFIX_INFO = "[INFO]";
	private static final String LOG_PREFIX_DEBUG = "[DEBUG]";
	private static final String LOG_PREFIX_SUCCESS = "[SUCCESS]";
	private static final String LOG_PREFIX_ERROR = "[ERROR]";
	private static final String LOG_PREFIX_WARN = "[WARN]";
	public String  rootPath;
	public JSONObject folderResponse;
	private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    // ===============================================================

	public void run(String id, String path) throws Exception {
	    disableSSL();

//	    System.out.println("========================================");
//	    System.out.println("Starting REST API Process (v3)");
//	    System.out.println("Bucket ID: " + id);
//	    System.out.println("Path: " + path);
//	    System.out.println("========================================");

	    // Step 1: Authenticate
	    String token = authenticate(BASE_URL, "ADA", "root", "ATempo@123");
//	    System.out.println("Access Token: " + token);
//	    System.out.println("----------------------------------------");

	    // Step 2: Perform GET
	    JSONObject browserResponse = getRepositoryBrowser(id, token);
	    
	    // Extract browserHandle safely
	    int browserHandle;
	    if (browserResponse.has("browserHandle")) {
	        browserHandle = browserResponse.getInt("browserHandle");
	        rootPath= browserResponse.getString("root");
	    } else if (browserResponse.has("object") && browserResponse.getJSONObject("object").has("browserHandle")) {
	        browserHandle = browserResponse.getJSONObject("object").getInt("browserHandle");
	        rootPath= browserResponse.getString("root");
	    } else {
	        throw new JSONException("browserHandle not found in response: " + browserResponse.toString());
	    }

//	    logInfo("----------------------------------------");
//	    logInfo("Browser Handle: " + browserHandle);

	    // Step 3: Update in DB
	    updateBrowserHandleInDB(id, browserHandle);
//	    System.out.println("Path: ["+path+"] Root: ["+rootPath+"]");
	    // Step 4: Browse folder
	    if(path == null || path.isEmpty())
	    {
	    	path=rootPath;
	    	JSONObject folderResponse = postBrowseFolder(id, browserHandle, path, token);
	    	System.out.println(folderResponse.toString(2));
	    }
	    else
	    { folderResponse = postBrowseFolder(id, browserHandle, path, token);
	    System.out.println(folderResponse.toString(2));
	    }

//	    logSuccess("Folder browsing completed successfully for bucket_id=" + id);
//	    System.out.println("========================================");
	}


    // =================== AUTHENTICATION =====================
    public String authenticate(String baseUrl, String dbName, String username, String password) throws Exception {
		String authUrl = baseUrl + "/auth/token/";
//		logInfo("Authenticating user: " + username);
//		logDebug("Authentication URL: " + authUrl);
//		logDebug("Database: " + dbName);

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

//			logDebug("Request payload: " + body.toString(2));

			// Send request
			try (OutputStream os = conn.getOutputStream()) {
				os.write(body.toString().getBytes(StandardCharsets.UTF_8));
			}

			int responseCode = conn.getResponseCode();
//			logDebug("Authentication response code: " + responseCode);

			InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
					: conn.getErrorStream();

			String responseText = readStream(is);
//			logDebug("Authentication response: " + responseText);

			// Validate response
			if (responseCode == 200 || responseCode == 201) {
				if (!responseText.trim().startsWith("{")) {
//					logError("Non-JSON authentication response received");
					throw new Exception("Server returned non-JSON response");
				}

				JSONObject jsonResponse = new JSONObject(responseText);
				if (jsonResponse.has("access")) {
					String token = jsonResponse.getString("access");
//					logSuccess("Authentication successful");
					return token;
				} else {
//					logError("Response does not contain access token");
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

    // =================== GET BROWSER =====================
    private JSONObject getRepositoryBrowser(String id, String token) throws Exception {
        String urlStr = BASE_URL + "/infrastructure/object-storages/4/buckets/" + id + "/browser/";
        URL url = new URL(urlStr);
//        System.out.println("Payload "+urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Bearer " + token);

        String response = readResponse(conn);
//        System.out.println("GET Response " +response);
        return new JSONObject(response);
        
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

    // =================== POST BROWSE =====================
	private JSONObject postBrowseFolder(String id, int browserHandle, String path, String token) throws Exception {
	    String urlStr = BASE_URL + "/infrastructure/object-storages/4/buckets/" + id + "/browse/";
	    URL url = new URL(urlStr);
	    
	    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	    conn.setRequestMethod("POST");
	    conn.setRequestProperty("Authorization", "Bearer " + token);
	    conn.setRequestProperty("Content-Type", "application/json");
	    conn.setDoOutput(true);
//	    logDebug("Request Url: " + urlStr);
	    JSONObject payload = new JSONObject();
	    payload.put("browserHandle", browserHandle);
	    payload.put("path", path);
	    
//	    logDebug("Request payload: " + payload.toString(2));

	    try (OutputStream os = conn.getOutputStream()) {
	        os.write(payload.toString().getBytes());
	    }

	    String response = readResponse(conn);
	    return new JSONObject(response);
	}


    // =================== DATABASE INSERT =====================
	private void updateBrowserHandleInDB(String id, int browserHandle) {
	    String query = "UPDATE backup_process_enterprise_user_detail SET browserHandle = ? WHERE bucket_id = ?";

	    try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
	         PreparedStatement ps = conn.prepareStatement(query)) {

	        ps.setInt(1, browserHandle);
	        ps.setString(2, id);

	        int rows = ps.executeUpdate();
//	        if (rows > 0)
//	            System.out.println("Database Updated (bucket_id=" + id + ", browserHandle=" + browserHandle + ")");
//	        else
//	            System.out.println(" No matching bucket_id found: " + id);

	    } catch (SQLException e) {
	        System.out.println(" Database Update Failed: " + e.getMessage());
	    }
	}

    // =================== SSL DISABLE =====================
    private void disableSSL() throws Exception {
        TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() { return null; }
            public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
            public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
        }};
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
    }

    // =================== READ RESPONSE =====================
    private String readResponse(HttpURLConnection conn) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(
                conn.getResponseCode() >= 400 ? conn.getErrorStream() : conn.getInputStream()));
        String inputLine;
        StringBuilder response = new StringBuilder();

        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();
        return response.toString();
    }
    

	private void logInfo(String message) {
		System.out.println(getTimestamp() + " " + LOG_PREFIX_INFO + " " + message);
	}

	/**
	 * Logs debug messages (detailed technical information)
	 */
	private void logDebug(String message) {
		System.out.println(getTimestamp() + " " + LOG_PREFIX_DEBUG + " " + message);
	}

	/**
	 * Logs success messages
	 */
	private void logSuccess(String message) {
		System.out.println(getTimestamp() + " " + LOG_PREFIX_SUCCESS + " " + message);
	}

	/**
	 * Logs error messages
	 */
	private void logError(String message) {
		System.err.println(getTimestamp() + " " + LOG_PREFIX_ERROR + " " + message);
	}

	/**
	 * Logs warning messages
	 */
	private void logWarn(String message) {
		System.out.println(getTimestamp() + " " + LOG_PREFIX_WARN + " " + message);
	}

	/**
	 * Gets current timestamp for logging
	 */
	private String getTimestamp() {
		return "[" + LocalDateTime.now().format(TIME_FORMATTER) + "]";
		}
	
}
