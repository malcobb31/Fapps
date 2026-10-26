package rest_api_v4;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import javax.net.ssl.*;
import org.json.*;

/**
 * REST API Methods for Enterprise and User Management
 * Handles authentication, repository creation, folder setup, bucket configuration,
 * storage containers, and policy management.
 * 
 * @author NinjaTech AI
 * @version 4.0
 */
public class rest_api_methods {
	
	// ==================== CONSTANTS ====================
	private static final String BASE_URL = "https://poc.itsimple.in/restapi";
	private static final String LOG_PREFIX_INFO = "[INFO]";
	private static final String LOG_PREFIX_DEBUG = "[DEBUG]";
	private static final String LOG_PREFIX_SUCCESS = "[SUCCESS]";
	private static final String LOG_PREFIX_ERROR = "[ERROR]";
	private static final String LOG_PREFIX_WARN = "[WARN]";
	private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	
	// API Configuration
	private static final int AGENT_ID = 2;
	private static final int STORAGE_MANAGER_ID = 9;
	private static final int OBJECT_STORAGE_ID = 4;
	private static final String BASE_DIRECTORY = "/C:/DemoDirectory/";
	private static final String DEFAULT_DB_NAME = "ADA";
	private static final String DEFAULT_SUPER_USER = "root";
	private static final String DEFAULT_SUPER_PASSWORD = "ATempo@123";
	private static final String DEFAULT_ORGANIZATION = "_Archiving";
	private static final long DEFAULT_VOLUME_QUOTA = 10737418240L; // 10GB in bytes
	
	// HTTP Response Codes
	private static final int HTTP_OK = 200;
	private static final int HTTP_CREATED = 201;
	private static final int HTTP_BAD_REQUEST = 400;
	private static final int HTTP_UNAUTHORIZED = 401;
	private static final int HTTP_FORBIDDEN = 403;
	private static final int HTTP_NOT_FOUND = 404;
	private static final int HTTP_CONFLICT = 409;
	private static final int HTTP_PRECONDITION_FAILED = 412;
	
	// ==================== MAIN ORCHESTRATION METHOD ====================
	
	/**
	 * Main orchestration method - executes all setup steps in sequence
	 * Handles three cases:
	 * 1. New Enterprise + New User (Full Setup)
	 * 2. Existing Enterprise + New User (User Setup)
	 * 3. Existing Enterprise + Existing User (No Action)
	 * 
	 * @param enterpriseName The name of the enterprise
	 * @param username The username to create/check
	 * @param password User password
	 * @param confirmPassword Password confirmation
	 * @param accessKey S3 access key
	 * @param secretKey S3 secret key
	 * @param bucketName Comma-separated bucket names
	 * @param userQuota User quota in bytes
	 * @throws Exception if any step fails
	 */
	public void run(String enterpriseName, String username, String password, String confirmPassword, 
			String accessKey, String secretKey, String bucketName, String userQuota) throws Exception {

		logInfo("========================================");
		logInfo("Starting REST API Setup Process");
		logInfo("Enterprise: " + enterpriseName);
		logInfo("Username: " + username);
		logInfo("Timestamp: " + LocalDateTime.now().format(TIME_FORMATTER));
		logInfo("========================================");

		// Input validation
//		validateInputs(enterpriseName, username, password, confirmPassword, accessKey, secretKey, bucketName, userQuota);

		try {
			// Step 0: Disable SSL verification for development
			logInfo("Step 0: Disabling SSL verification...");
			disableSSL();
			logSuccess("SSL verification disabled successfully");

			// Step 1: Authenticate with super user credentials
			logInfo("Step 1: Authenticating super user...");
			String token = authenticate(BASE_URL, DEFAULT_DB_NAME, DEFAULT_SUPER_USER, DEFAULT_SUPER_PASSWORD);
			
			if (token == null || token.trim().isEmpty()) {
				throw new Exception("Authentication failed - no token received, Code: 5");
			}
			
			logSuccess("Authentication successful. Token obtained.");
			logDebug("Token length: " + token.length());

			// Step 1.5: Check Enterprise and User existence in database
			logInfo("Step 1.5: Checking enterprise and user existence in database...");
			boolean enterpriseExists = rest_api_DBConnection.enterpriseExists(enterpriseName);
			boolean userExists = rest_api_DBConnection.userExists(enterpriseName, username);
			
			logDebug("Enterprise exists in DB: " + enterpriseExists);
			logDebug("User exists in DB: " + userExists);

			// ========================== CASE HANDLING ==========================

			if (!enterpriseExists && !userExists) {
				// CASE 1: New Enterprise + New User - Full Setup
				handleCase1_FullSetup(enterpriseName, username, password, bucketName, 
						accessKey, secretKey, userQuota, token);

			} else if (enterpriseExists && !userExists) {
				// CASE 2: Existing Enterprise + New User - User Setup Only
				handleCase2_UserSetup(enterpriseName, username, password, bucketName, 
						accessKey, secretKey, userQuota, token);

			} else if (enterpriseExists && userExists) {
				// CASE 3: Both Exist - No Action Needed
				handleCase3_BothExist(enterpriseName, username);
			}

			// ===================================================================

		} catch (Exception e) {
			logError("Setup process failed: " + e.getMessage());
			logError("Failure, Code: 4");
			throw e;
		}
	}

	// ==================== CASE HANDLERS ====================
	
	/**
	 * CASE 1: Enterprise = N, User = N -> Full setup sequence
	 */
	private void handleCase1_FullSetup(String enterpriseName, String username, String password, 
			String bucketName, String accessKey, String secretKey, String userQuota, String token) 
			throws Exception {
		
		logInfo("[CASE 1] Enterprise = N, User = N -> Running full setup sequence...");
		String Date = LocalDateTime.now().format(TIME_FORMATTER);
		// Step 2: Create Enterprise Repository
		logInfo("Step 2: Creating enterprise repository...");
		int enterpriseId = handleEnterprise(enterpriseName, username, token);
		logSuccess("Enterprise ID obtained: " + enterpriseId);
		rest_api_DBConnection.UserVolumeQuota(enterpriseName,enterpriseId,username,userQuota,Date);
		// Step 3: Create Enterprise Folder
		logInfo("Step 3: Creating enterprise folder structure...");
		createEnterpriseFolder(enterpriseId, enterpriseName, username, token);
		logSuccess("Enterprise folder created successfully");

		// Step 4: Create Archival Project
		logInfo("Step 4: Creating archival project...");
		createProject(enterpriseName, token);
		logSuccess("Archival project created successfully");

		// Step 5: Create Agent Folder (Enterprise Level)
		logInfo("Step 5: Creating enterprise agent folder...");
		createAgentFolder(enterpriseName, token);
		logSuccess("Enterprise agent folder created successfully");

		// Step 6: Create User Folder
		logInfo("Step 6: Creating user folder...");
		createUserFolder(enterpriseName, username, token);
		logSuccess("User folder created successfully");

		// Step 7: Handle Bucket(s) and SMC Configuration
		logInfo("Step 7: Handling bucket creation and SMC configuration...");
		handleBucket(enterpriseName, username, password, bucketName, accessKey, secretKey, userQuota, token);
		logSuccess("Bucket handling completed successfully");

		// Step 8: Create Storage Container
		logInfo("Step 8: Creating storage container...");
		createContainer(enterpriseName, username, token);
		logSuccess("Storage container created successfully");

		// Step 9: Create Policy
		logInfo("Step 9: Creating archival policy...");
		createPolicy(enterpriseName,enterpriseId, token);
		logSuccess("Policy created successfully");

		// Step 10: Update user status in database
		rest_api_DBConnection.user_status();
		
		logInfo("========================================");
		logSuccess("CASE 1 COMPLETE: Enterprise & User Created Successfully, Code: 1");
		logInfo("========================================");
	}

	/**
	 * CASE 2: Enterprise = Y, User = N -> User setup sequence
	 */
	private void handleCase2_UserSetup(String enterpriseName, String username, String password, 
			String bucketName, String accessKey, String secretKey, String userQuota, String token) 
			throws Exception {
		
		logInfo("[CASE 2] Enterprise = Y, User = N -> Running user setup sequence...");

		// Get existing enterprise info
		logInfo("Fetching existing enterprise repository info...");
		int enterpriseId = rest_api_DBConnection.getEnterpriseId(enterpriseName);
		logDebug("Existing enterprise ID: " + enterpriseId);
		String Date = LocalDateTime.now().format(TIME_FORMATTER);
		rest_api_DBConnection.UserVolumeQuota(enterpriseName,enterpriseId,username,userQuota,Date);
		// Step 2: Create User Folder in Repository
		logInfo("Step 2: Creating user folder in repository...");
		createEnterpriseFolder(enterpriseId, enterpriseName, username, token);
		logSuccess("User folder in repository created successfully");

		// Step 3: Create User Folder on Disk
		logInfo("Step 3: Creating user folder on disk...");
		createUserFolder(enterpriseName, username, token);
		logSuccess("User folder on disk created successfully");

		// Step 4: Create SMCs for User
		logInfo("Step 4: Creating SMCs for user...");
		handleBucket(enterpriseName, username, password, bucketName, accessKey, secretKey, userQuota, token);
		logSuccess("User SMCs created successfully");

		// Step 5: Create Storage Container
		logInfo("Step 5: Creating storage container...");
		assignExistingTargetSMCToUser(enterpriseName, username);
		logSuccess("Storage container created successfully");

		// Step 6: Update user status in database
		rest_api_DBConnection.user_status();
		
		logInfo("========================================");
		logSuccess("CASE 2 COMPLETE: User Created Successfully, Code: 2");
		logInfo("========================================");
	}

	/**
	 * CASE 3: Enterprise = Y, User = Y -> No action needed
	 */
	private void handleCase3_BothExist(String enterpriseName, String username) throws Exception {
		logInfo("[CASE 3] Enterprise = Y, User = Y -> No creation needed");
		
		rest_api_DBConnection.user_status();
		
		logInfo("========================================");
		logSuccess("CASE 3 COMPLETE: Both enterprise and user already exist, Code: 3");
		logInfo("========================================");
	}

	// ==================== INPUT VALIDATION ====================
	
	/**
	 * Validates all input parameters
	 */

	// ==================== SSL CONFIGURATION ====================
	
	/**
	 * Disables SSL certificate verification (for development/testing only)
	 * WARNING: This should NOT be used in production environments
	 */
	private void disableSSL() throws Exception {
		try {
			logDebug("Creating trust manager that accepts all certificates...");
			
			TrustManager[] trustAllCerts = new TrustManager[] { 
				new X509TrustManager() {
					public java.security.cert.X509Certificate[] getAcceptedIssuers() {
						return null;
					}

					public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
						// Trust all clients
					}

					public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
						// Trust all servers
					}
				} 
			};

			SSLContext sc = SSLContext.getInstance("SSL");
			sc.init(null, trustAllCerts, new java.security.SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
			HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);

			logDebug("SSL verification disabled successfully");
			
		} catch (Exception e) {
			logError("Failed to disable SSL verification: " + e.getMessage());
			throw new Exception("SSL configuration failed", e);
		}
	}

	// ==================== AUTHENTICATION ====================
	
	/**
	 * Authenticates user and retrieves JWT access token
	 * 
	 * @param baseUrl Base URL of the REST API
	 * @param dbName Database name
	 * @param username Username for authentication
	 * @param password Password for authentication
	 * @return JWT access token
	 * @throws Exception if authentication fails
	 */
	public String authenticate(String baseUrl, String dbName, String username, String password) throws Exception {
		String authUrl = baseUrl + "/auth/token/";
		
		logInfo("Authenticating user: " + username);
		logDebug("Authentication URL: " + authUrl);
		logDebug("Database: " + dbName);

		HttpURLConnection conn = null;
		
		try {
			// Setup connection
			URL url = new URL(authUrl);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setDoOutput(true);
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");
			conn.setConnectTimeout(10000); // 10 seconds
			conn.setReadTimeout(10000);

			// Build JSON payload
			JSONObject body = new JSONObject();
			body.put("dbName", dbName);
			body.put("name", username);
			body.put("password", password);
			body.put("superUser", true);

			logDebug("Request payload prepared (password hidden)");

			// Send request
			try (OutputStream os = conn.getOutputStream()) {
				byte[] input = body.toString().getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
				os.flush();
			}

			int responseCode = conn.getResponseCode();
			logDebug("Authentication response code: " + responseCode);

			// Read response
			InputStream is = (responseCode >= HTTP_OK && responseCode < 300) ? 
					conn.getInputStream() : conn.getErrorStream();

			String responseText = readStream(is);
			logDebug("Authentication response received (length: " + responseText.length() + " chars)");

			// Validate response
			switch (responseCode) {
				case HTTP_OK:
				case HTTP_CREATED:
					if (!responseText.trim().startsWith("{")) {
						logError("Non-JSON authentication response received");
						throw new Exception("Server returned non-JSON response");
					}

					JSONObject jsonResponse = new JSONObject(responseText);
					
					if (!jsonResponse.has("access")) {
						logError("Response does not contain access token");
						throw new Exception("Access token not found in response");
					}
					
					String token = jsonResponse.getString("access");
					logSuccess("Authentication successful");
					logDebug("Token received (first 20 chars): " + token.substring(0, Math.min(20, token.length())) + "...");
					
					return token;

				case HTTP_UNAUTHORIZED:
					logError("Authentication failed: Invalid credentials");
					throw new Exception("Invalid username or password");

				case HTTP_FORBIDDEN:
					logError("Authentication failed: Access forbidden");
					throw new Exception("User does not have required permissions");

				case HTTP_BAD_REQUEST:
					logError("Bad request: " + responseText);
					throw new Exception("Invalid authentication request: " + responseText);

				default:
					logError("Authentication failed with code: " + responseCode);
					throw new Exception("Authentication failed (HTTP " + responseCode + "): " + responseText);
			}

		} catch (SocketTimeoutException e) {
			logError("Authentication timeout: " + e.getMessage());
			throw new Exception("Authentication request timed out", e);
			
		} catch (IOException e) {
			logError("Network error during authentication: " + e.getMessage());
			throw new Exception("Network error during authentication", e);
			
		} catch (JSONException e) {
			logError("JSON parsing error: " + e.getMessage());
			throw new Exception("Invalid response format from server", e);
			
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	// ==================== ENTERPRISE MANAGEMENT ====================
	
	/**
	 * Creates or retrieves existing enterprise repository
	 * 
	 * @param enterpriseName Name of the enterprise
	 * @param username Owner username
	 * @param token Authentication token
	 * @return Enterprise repository ID
	 * @throws Exception if creation fails
	 */
	public int handleEnterprise(String enterpriseName, String username, String token) throws Exception {
		logInfo("Handling enterprise repository: " + enterpriseName);

		// Check if enterprise already exists in database
		if (rest_api_DBConnection.enterpriseExists(enterpriseName)) {
			int existingId = rest_api_DBConnection.getEnterpriseId(enterpriseName);
			logWarn("Enterprise already exists in database with ID: " + existingId);
			logInfo("Returning existing enterprise ID");
			return existingId;
		}

		logInfo("Creating new enterprise repository via API...");
		HttpsURLConnection conn = null;

		try {
			// Setup connection
			URL url = new URL(BASE_URL + "/datamanagement/repositories/");
			conn = (HttpsURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Authorization", "Bearer " + token);
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");
			conn.setDoOutput(true);
			conn.setConnectTimeout(15000);
			conn.setReadTimeout(15000);

			// Prepare JSON payload
			JSONObject payload = new JSONObject();
			payload.put("name", enterpriseName);
			payload.put("mode", "archiving");
			payload.put("comment", "Repository for " + enterpriseName);
			payload.put("volumeQuota", DEFAULT_VOLUME_QUOTA);

			JSONObject owner = new JSONObject();
			owner.put("name", DEFAULT_SUPER_USER);
			payload.put("owner", owner);

			JSONObject organization = new JSONObject();
			organization.put("name", DEFAULT_ORGANIZATION);
			payload.put("organization", organization);

			logDebug("Enterprise creation payload prepared");

			// Send request
			try (OutputStream os = conn.getOutputStream()) {
				byte[] input = payload.toString().getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
				os.flush();
			}

			int responseCode = conn.getResponseCode();
			String response = readResponse(conn);

			logDebug("Enterprise creation response code: " + responseCode);

			// Handle response
			switch (responseCode) {
				case HTTP_CREATED:
					JSONObject json = new JSONObject(response);
					
					if (!json.has("id")) {
						throw new RuntimeException("Enterprise ID not found in response");
					}
					
					int enterpriseId = json.getInt("id");

					// Save to database
					rest_api_DBConnection.insertEnterprise(enterpriseName, enterpriseId, "", "");
					logSuccess("Enterprise created successfully with ID: " + enterpriseId);
					
					if (json.has("name")) {
						logInfo("Enterprise name: " + json.getString("name"));
					}

					return enterpriseId;

				case HTTP_CONFLICT:
					logWarn("Enterprise already exists on server (conflict)");
					throw new RuntimeException("Enterprise name already exists: " + enterpriseName);

				case HTTP_UNAUTHORIZED:
					logError("Unauthorized: Invalid or expired token");
					throw new RuntimeException("Authorization failed - please check token");

				case HTTP_BAD_REQUEST:
					logError("Bad request: " + response);
					throw new RuntimeException("Invalid enterprise configuration: " + response);

				default:
					logError("Failed to create enterprise. Response code: " + responseCode);
					throw new RuntimeException("Enterprise creation failed (HTTP " + responseCode + "): " + response);
			}

		} catch (JSONException e) {
			logError("JSON error while handling enterprise: " + e.getMessage());
			throw new Exception("Invalid response format", e);
			
		} catch (IOException e) {
			logError("Network error while handling enterprise: " + e.getMessage());
			throw new Exception("Network error during enterprise creation", e);
			
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	// ==================== FOLDER MANAGEMENT ====================
	
	/**
	 * Creates a folder within the enterprise repository
	 * This creates the logical folder in the repository management system
	 * 
	 * @param repositoryId Repository ID
	 * @param enterpriseName Enterprise name
	 * @param username Username for folder
	 * @param token Authentication token
	 * @throws Exception if folder creation fails
	 */
	public void createEnterpriseFolder(int repositoryId, String enterpriseName, String username, String token)
			throws Exception {

		String folderName = username;
		logInfo("Creating enterprise folder: " + folderName);
		logDebug("Repository ID: " + repositoryId);

		HttpsURLConnection conn = null;
		int browserHandle = -1;

		try {
			// Step 1: Get browser handle
			logDebug("Retrieving browser handle for repository browsing...");
			String browserUrl = BASE_URL + "/datamanagement/repositories/" + repositoryId + "/browser/?format=json";
			URL url = new URL(browserUrl);
			conn = (HttpsURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Authorization", "Bearer " + token);
			conn.setRequestProperty("Accept", "application/json");
			conn.setConnectTimeout(10000);
			conn.setReadTimeout(10000);

			String browserResponse = readResponse(conn);
			int responseCode = conn.getResponseCode();

			logDebug("Browser handle response code: " + responseCode);

			if (responseCode != HTTP_OK) {
				logError("Failed to get browser handle");
				throw new RuntimeException(
						"Failed to retrieve browser handle (HTTP " + responseCode + "): " + browserResponse);
			}

			if (!browserResponse.trim().startsWith("{")) {
				logError("Invalid JSON response from browser endpoint");
				throw new RuntimeException("Non-JSON response from browser endpoint");
			}

			JSONObject browserJson = new JSONObject(browserResponse);
			
			if (!browserJson.has("browserHandle")) {
				throw new RuntimeException("Browser handle not found in response");
			}
			
			browserHandle = browserJson.getInt("browserHandle");
			logSuccess("Browser handle obtained: " + browserHandle);

			conn.disconnect();

			// Step 2: Create folder
			logDebug("Creating folder with path: /" + folderName);
			String folderUrl = BASE_URL + "/datamanagement/repositories/" + repositoryId + "/browse/folder/";
			url = new URL(folderUrl);
			conn = (HttpsURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Authorization", "Bearer " + token);
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setDoOutput(true);
			conn.setConnectTimeout(10000);
			conn.setReadTimeout(10000);

			JSONObject payload = new JSONObject();
			payload.put("browserHandle", browserHandle);
			payload.put("path", "/" + folderName);

			logDebug("Folder creation payload prepared");

			try (OutputStream os = conn.getOutputStream()) {
				byte[] input = payload.toString().getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
				os.flush();
			}

			String postResponse = readResponse(conn);
			responseCode = conn.getResponseCode();

			logDebug("Folder creation response code: " + responseCode);

			switch (responseCode) {
				case HTTP_OK:
				case HTTP_CREATED:
					logSuccess("Folder created successfully: /" + folderName);
					break;

				case HTTP_CONFLICT:
					logWarn("Folder already exists: /" + folderName);
					break;

				case HTTP_UNAUTHORIZED:
					logError("Unauthorized: Token may be invalid");
					throw new RuntimeException("Authorization failed during folder creation");

				case HTTP_BAD_REQUEST:
					logError("Bad request: " + postResponse);
					throw new RuntimeException("Invalid folder creation request: " + postResponse);

				default:
					logError("Folder creation failed with code: " + responseCode);
					throw new RuntimeException("Failed to create folder (HTTP " + responseCode + "): " + postResponse);
			}

		} catch (JSONException e) {
			logError("JSON error during folder creation: " + e.getMessage());
			throw new Exception("Invalid response format", e);
			
		} catch (IOException e) {
			logError("Network error during folder creation: " + e.getMessage());
			throw new Exception("Network error during folder creation", e);
			
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}
	
	/**
	 * Creates a physical user folder on the agent's file system
	 * This creates the actual directory structure for storing user data
	 * 
	 * @param enterpriseName Enterprise name
	 * @param username Username
	 * @param token Authentication token
	 * @throws Exception if folder creation fails
	 */
	public void createUserFolder(String enterpriseName, String username, String token) throws Exception {

	    String path = BASE_DIRECTORY + enterpriseName + "/" + username;

	    logInfo("Creating user folder for Enterprise: " + enterpriseName + ", Username: " + username);
	    logDebug("Resolved folder path: " + path);

	    HttpsURLConnection conn = null;
	    int browserHandle = -1;

	    try {
	        // Step 1: Get Browser Handle
	        logDebug("Retrieving agent browser handle...");
	        String browserUrl = BASE_URL + "/infrastructure/agents/" + AGENT_ID + "/browser/?format=json";
	        URL url = new URL(browserUrl);
	        conn = (HttpsURLConnection) url.openConnection();
	        conn.setRequestMethod("GET");
	        conn.setRequestProperty("Authorization", "Bearer " + token);
	        conn.setRequestProperty("Accept", "application/json");
	        conn.setConnectTimeout(10000);
	        conn.setReadTimeout(10000);

	        String browserResponse = readResponse(conn);
	        int responseCode = conn.getResponseCode();

	        if (responseCode != HTTP_OK) {
	        	logError("Failed to get agent browser handle");
	            throw new RuntimeException("Browser handle retrieval failed (HTTP " + responseCode + "): " + browserResponse);
	        }

	        JSONObject browserJson = new JSONObject(browserResponse);
	        
	        if (!browserJson.has("browserHandle")) {
	        	throw new RuntimeException("Browser handle not found in response");
	        }
	        
	        browserHandle = browserJson.getInt("browserHandle");
	        logSuccess("Browser handle obtained: " + browserHandle);

	        conn.disconnect();

	        // Step 2: Create User Folder
	        logDebug("Creating user folder at path: " + path);
	        String folderUrl = BASE_URL + "/infrastructure/agents/" + AGENT_ID + "/browse/folder/";
	        url = new URL(folderUrl);
	        conn = (HttpsURLConnection) url.openConnection();
	        conn.setRequestMethod("POST");
	        conn.setRequestProperty("Authorization", "Bearer " + token);
	        conn.setRequestProperty("Accept", "application/json");
	        conn.setRequestProperty("Content-Type", "application/json");
	        conn.setDoOutput(true);
	        conn.setConnectTimeout(10000);
	        conn.setReadTimeout(10000);

	        JSONObject payload = new JSONObject();
	        payload.put("browserHandle", browserHandle);
	        payload.put("path", path);

	        logDebug("User folder creation payload prepared");

	        try (OutputStream os = conn.getOutputStream()) {
	            byte[] input = payload.toString().getBytes(StandardCharsets.UTF_8);
	            os.write(input, 0, input.length);
	            os.flush();
	        }

	        String postResponse = readResponse(conn);
	        responseCode = conn.getResponseCode();

	        logDebug("User folder creation response code: " + responseCode);

	        switch (responseCode) {
	        	case HTTP_OK:
	        	case HTTP_CREATED:
	        		logSuccess("User folder created successfully: " + path);
	        		break;
	        		
	        	case HTTP_CONFLICT:
	        		logWarn("User folder already exists: " + path);
	        		break;
	        		
	        	case HTTP_UNAUTHORIZED:
	        		logError("Unauthorized: Token may be invalid");
	        		throw new RuntimeException("Authorization failed during user folder creation");
	        		
	        	default:
	        		logError("User folder creation failed with code: " + responseCode);
	        		throw new RuntimeException("Failed to create user folder (HTTP " + responseCode + "): " + postResponse);
	        }

	    } catch (JSONException e) {
	        logError("JSON error during user folder creation: " + e.getMessage());
	        throw new Exception("Invalid response format", e);
	        
	    } catch (IOException e) {
	        logError("Network error during user folder creation: " + e.getMessage());
	        throw new Exception("Network error during user folder creation", e);
	        
	    } finally {
	        // Step 3: Always close browser session
	        if (browserHandle != -1) {
	            closeBrowserSession(AGENT_ID, browserHandle, token);
	        }

	        if (conn != null) {
	        	conn.disconnect();
	        }
	    }
	}

	/**
	 * Closes an agent browser session to free up server resources
	 * 
	 * @param agentId Agent ID
	 * @param browserHandle Browser handle to close
	 * @param token Authentication token
	 */
	public void closeBrowserSession(int agentId, int browserHandle, String token
			) {
	    try {
	        logDebug("Closing browser session with handle: " + browserHandle);
	        
	        String url = BASE_URL + "/infrastructure/agents/" + agentId + "/browse/close/";
	        HttpsURLConnection conn = (HttpsURLConnection) new URL(url).openConnection();
	        conn.setRequestMethod("POST");
	        conn.setRequestProperty("Authorization", "Bearer " + token);
	        conn.setRequestProperty("Content-Type", "application/json");
	        conn.setDoOutput(true);
	        conn.setConnectTimeout(5000);
	        conn.setReadTimeout(5000);

	        JSONObject payload = new JSONObject();
	        payload.put("browserHandle", browserHandle);

	        try (OutputStream os = conn.getOutputStream()) {
	            byte[] input = payload.toString().getBytes(StandardCharsets.UTF_8);
	            os.write(input, 0, input.length);
	            os.flush();
	        }

	        int responseCode = conn.getResponseCode();
	        String response = readResponse(conn);
	        
	        logDebug("Browser close response code: " + responseCode);
	        
	        if (responseCode == HTTP_OK || responseCode == HTTP_CREATED) {
	            logSuccess("Browser session closed successfully");
	        } else {
	            logWarn("Browser close returned code: " + responseCode);
	        }
	        
	        conn.disconnect();

	    } catch (Exception e) {
	        logError("Failed to close browser session: " + e.getMessage());
	        // Don't throw - this is cleanup, shouldn't break the main flow
	    }
	}

	// ==================== PROJECT MANAGEMENT ====================
	
	/**
	 * Creates archival project for the enterprise
	 * Projects are organizational units for managing archival operations
	 * 
	 * @param enterpriseName Enterprise name
	 * @param token Authentication token
	 * @throws Exception if project creation fails
	 */
	public void createProject(String enterpriseName, String token) throws Exception {
		String projectName = enterpriseName + "-Project";
		logInfo("Creating archival project: " + projectName);

		HttpURLConnection conn = null;

		try {
			String url = BASE_URL + "/organizations/archiving-projects/";
			conn = (HttpURLConnection) new URL(url).openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Authorization", "Bearer " + token);
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");
			conn.setDoOutput(true);
			conn.setConnectTimeout(15000);
			conn.setReadTimeout(15000);

			JSONObject payload = new JSONObject();
			payload.put("name", projectName);
			payload.put("comment", "Archival project for " + enterpriseName);

			logDebug("Project creation payload prepared");

			try (OutputStream os = conn.getOutputStream()) {
				byte[] input = payload.toString().getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
				os.flush();
			}

			int responseCode = conn.getResponseCode();
			logDebug("Project creation response code: " + responseCode);

			InputStream is = (responseCode >= HTTP_OK && responseCode < 300) ? 
					conn.getInputStream() : conn.getErrorStream();

			String response = readStream(is);

			switch (responseCode) {
				case HTTP_OK:
				case HTTP_CREATED:
					if (response.trim().startsWith("{")) {
						JSONObject jsonResponse = new JSONObject(response);
						logSuccess("Archival project created successfully");
						logInfo("Project ID: " + jsonResponse.getInt("id"));
						logInfo("Project Name: " + jsonResponse.getString("name"));
						
						if (jsonResponse.has("parent")) {
							logInfo("Parent Organization: " + jsonResponse.getJSONObject("parent").getString("name"));
						}
					} else {
						logWarn("Project may have been created but response is non-JSON");
					}
					break;

				case HTTP_CONFLICT:
					logWarn("Project already exists: " + projectName);
					break;

				case HTTP_UNAUTHORIZED:
					logError("Unauthorized: Invalid token");
					throw new Exception("Authorization failed during project creation");

				case HTTP_BAD_REQUEST:
					logError("Bad request: " + response);
					throw new Exception("Invalid project configuration: " + response);

				default:
					logError("Failed to create project with code: " + responseCode);
					throw new Exception("Project creation failed (HTTP " + responseCode + "): " + response);
			}

		} catch (JSONException e) {
			logError("JSON error during project creation: " + e.getMessage());
			throw new Exception("Invalid response format", e);
			
		} catch (IOException e) {
			logError("Network error during project creation: " + e.getMessage());
			throw new Exception("Network error during project creation", e);
			
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	// ==================== BUCKET MANAGEMENT ====================
	
	/**
	 * Creates or retrieves S3 bucket(s) with SMC (Storage Media Component) configuration
	 * Handles multiple buckets specified in comma-separated format
	 * 
	 * @param enterpriseName Enterprise name
	 * @param username Username
	 * @param password User password
	 * @param bucketNames Comma-separated bucket names
	 * @param accessKey S3 access key
	 * @param secretKey S3 secret key
	 * @param userQuota User quota
	 * @param token Authentication token
	 * @throws Exception if bucket handling fails
	 */
	public void handleBucket(String enterpriseName, String username, String password, String bucketNames,
			String accessKey, String secretKey, String userQuota, String token) throws Exception {

		logInfo("Handling bucket creation for enterprise: " + enterpriseName);
		String[] buckets = bucketNames.split(",");
		logInfo("Number of buckets to process: " + buckets.length);

		int successCount = 0;
		int failureCount = 0;

		for (int i = 0; i < buckets.length; i++) {
			String bucket = buckets[i].trim();

			if (bucket.isEmpty()) {
				logWarn("Skipping empty bucket name at position " + (i + 1));
				continue;
			}

			logInfo("========================================");
			logInfo("Processing bucket " + (i + 1) + " of " + buckets.length + ": " + bucket);
			logInfo("========================================");

			try {
				// Construct SMC displayName and alias
				String displayName = enterpriseName + "-" + username + "-" + bucket + "-SourceSMC";
				String alias = enterpriseName + "-" + username + "-" + bucket + "-SourceSMCPN";
				String s3Path = "s3://" + bucket;
				String localPath = "C:/DemoDirectory/" + bucket;

				logDebug("Display Name: " + displayName);
				logDebug("Alias: " + alias);
				logDebug("S3 Path: " + s3Path);
				logDebug("Local Path: " + localPath);

				HttpsURLConnection conn = null;

				try {
					// Step 1: Check if bucket already exists
					logDebug("Checking if bucket already exists...");
					URL getUrl = new URL(BASE_URL + "/infrastructure/object-storages/" + OBJECT_STORAGE_ID + "/buckets/");
					conn = (HttpsURLConnection) getUrl.openConnection();
					conn.setRequestMethod("GET");
					conn.setRequestProperty("Authorization", "Bearer " + token);
					conn.setRequestProperty("Accept", "application/json");
					conn.setConnectTimeout(10000);
					conn.setReadTimeout(10000);

					int getResponseCode = conn.getResponseCode();
					String getResponse = readResponse(conn);

					logDebug("Get buckets response code: " + getResponseCode);

					if (getResponseCode != HTTP_OK) {
						logError("Failed to retrieve existing buckets");
						throw new RuntimeException("Unable to check existing buckets (HTTP " + getResponseCode + ")");
					}

					JSONObject json = new JSONObject(getResponse);
					JSONArray results = json.getJSONArray("results");
					logDebug("Found " + results.length() + " existing buckets");

					boolean exists = false;
					for (int j = 0; j < results.length(); j++) {
						JSONObject obj = results.getJSONObject(j);
						String existingDisplayName = obj.getString("displayName");

						if (existingDisplayName.equals(displayName)) {
							exists = true;
							logWarn("Bucket already exists: " + displayName);
							
							int bucketId = obj.getInt("id");
							logInfo("Bucket ID: " + bucketId);

							// Insert into database
							rest_api_DBConnection.insertEnterpriseUserDetail(
									enterpriseName, 
									username, 
									password, 
									bucket,
									String.valueOf(bucketId), 
									userQuota, 
									existingDisplayName,
									obj.getJSONObject("useAsSource").getString("alias"), 
									s3Path,
									localPath
							);
							
							logSuccess("Bucket info saved to database");
							break;
						}
					}

					conn.disconnect();

					if (!exists) {
						// Step 2: Create new bucket
						logInfo("Creating new bucket: " + bucket);

						URL postUrl = new URL(BASE_URL + "/infrastructure/object-storages/" + OBJECT_STORAGE_ID + "/buckets/");
						conn = (HttpsURLConnection) postUrl.openConnection();
						conn.setRequestMethod("POST");
						conn.setRequestProperty("Authorization", "Bearer " + token);
						conn.setRequestProperty("Content-Type", "application/json");
						conn.setRequestProperty("Accept", "application/json");
						conn.setDoOutput(true);
						conn.setConnectTimeout(15000);
						conn.setReadTimeout(15000);

						// Build payload
						JSONObject payload = new JSONObject();
						payload.put("name", bucket);
						payload.put("nbThreads", 2);
						payload.put("sendMetadata", true);
						payload.put("displayName", displayName);
						payload.put("accessKey", accessKey);
						payload.put("secretKey", secretKey);

						JSONObject useAsSource = new JSONObject();
						useAsSource.put("enabled", true);
						useAsSource.put("alias", alias);
						payload.put("useAsSource", useAsSource);

						logDebug("Bucket creation payload prepared");

						try (OutputStream os = conn.getOutputStream()) {
							byte[] input = payload.toString().getBytes(StandardCharsets.UTF_8);
							os.write(input, 0, input.length);
							os.flush();
						}

						int postResponseCode = conn.getResponseCode();
						String postResponse = readResponse(conn);

						logDebug("Bucket creation response code: " + postResponseCode);

						switch (postResponseCode) {
							case HTTP_OK:
							case HTTP_CREATED:
								JSONObject respJson = new JSONObject(postResponse);
								logSuccess("Bucket created successfully: " + respJson.getString("displayName"));
								
								int newBucketId = respJson.getInt("id");
								logInfo("Bucket ID: " + newBucketId);

								// Insert into database
								rest_api_DBConnection.insertEnterpriseUserDetail(
										enterpriseName, 
										username, 
										password,
										respJson.getString("name"), 
										String.valueOf(newBucketId), 
										userQuota,
										respJson.getString("displayName"),
										respJson.getJSONObject("useAsSource").getString("alias"),
										s3Path, 
										localPath
								);
								
								logSuccess("Bucket info saved to database");
								break;

							case HTTP_CONFLICT:
								logWarn("Bucket already exists on server: " + bucket);
								break;

							case HTTP_UNAUTHORIZED:
								logError("Unauthorized: Token may be invalid");
								throw new RuntimeException("Authorization failed during bucket creation");

							case HTTP_BAD_REQUEST:
								logError("Bad request: " + postResponse);
								throw new RuntimeException("Invalid bucket configuration: " + postResponse);

							case HTTP_FORBIDDEN:
								logError("Forbidden: Insufficient permissions");
								throw new RuntimeException("Insufficient permissions to create bucket");

							default:
								logError("Failed to create bucket with code: " + postResponseCode);
								throw new RuntimeException("Bucket creation failed (HTTP " + postResponseCode + "): " + postResponse);
						}

						conn.disconnect();
					}

					successCount++;
					logSuccess("Bucket '" + bucket + "' handled successfully");

				} catch (Exception e) {
					failureCount++;
					logError("Exception while handling bucket '" + bucket + "': " + e.getMessage());
					
					if (conn != null) {
						conn.disconnect();
					}
					
					// Decide whether to continue or fail completely
					// Currently continuing with next bucket
					logWarn("Continuing with next bucket...");
				}

			} catch (Exception e) {
				failureCount++;
				logError("Unexpected error processing bucket '" + bucket + "': " + e.getMessage());
			}
		}

		logInfo("========================================");
		logInfo("Bucket handling summary:");
		logInfo("Total buckets: " + buckets.length);
		logInfo("Successful: " + successCount);
		logInfo("Failed: " + failureCount);
		logInfo("========================================");

		if (successCount > 0) {
			logSuccess("Bucket handling completed with " + successCount + " successful bucket(s)");
		}
		
		if (failureCount > 0) {
			logWarn("Some buckets failed to process: " + failureCount);
		}
	}

	// ==================== AGENT FOLDER MANAGEMENT ====================
	
	/**
	 * Creates agent folder for file system access at enterprise level
	 * This creates the main enterprise directory on the agent's file system
	 * 
	 * @param enterpriseName Enterprise name
	 * @param token Authentication token
	 * @throws Exception if folder creation fails
	 */
	public void createAgentFolder(String enterpriseName, String token) throws Exception {
	    logInfo("Creating agent folder for enterprise: " + enterpriseName);

	    HttpsURLConnection conn = null;
	    int browserHandle = -1;

	    try {
	        // Step 1: Get browser handle
	        logDebug("Retrieving agent browser handle...");
	        browserHandle = getAgentBrowserHandle(token);
	        logSuccess("Agent browser handle obtained: " + browserHandle);

	        // Step 2: Create folder
	        String folderPath = BASE_DIRECTORY + enterpriseName + "/";
	        createAgentFolderInternal(browserHandle, folderPath, token);
	        
	        logSuccess("Agent folder created successfully for enterprise: " + enterpriseName);

	    } catch (Exception e) {
	        logError("Exception during agent folder creation: " + e.getMessage());
	        throw e;
	        
	    } finally {
	        // Always close browser session
	        if (browserHandle != -1) {
	            closeBrowserSession(AGENT_ID, browserHandle, token);
	        }
	    }
	}

	/**
	 * Retrieves agent browser handle for file system operations
	 * 
	 * @param token Authentication token
	 * @return Browser handle ID
	 * @throws Exception if retrieval fails
	 */
	private int getAgentBrowserHandle(String token) throws Exception {
	    HttpsURLConnection conn = null;
	    
	    try {
	        URL url = new URL(BASE_URL + "/infrastructure/agents/" + AGENT_ID + "/browser/");
	        conn = (HttpsURLConnection) url.openConnection();
	        conn.setRequestMethod("GET");
	        conn.setRequestProperty("Authorization", "Bearer " + token);
	        conn.setRequestProperty("Accept", "application/json");
	        conn.setConnectTimeout(10000);
	        conn.setReadTimeout(10000);

	        int responseCode = conn.getResponseCode();
	        logDebug("Agent browser response code: " + responseCode);

	        if (responseCode != HTTP_OK) {
	            String errorResponse = readResponse(conn);
	            logError("Failed to get agent browser handle: " + errorResponse);
	            throw new Exception("Unable to retrieve agent browser handle (HTTP " + responseCode + "): " + errorResponse);
	        }

	        String response = readResponse(conn);
	        JSONObject json = new JSONObject(response);
	        
	        logDebug("Agent browser response: " + response);
	        
	        if (!json.has("browserHandle")) {
	            throw new Exception("Response does not contain browserHandle field");
	        }
	        
	        int handle = json.getInt("browserHandle");
	        logDebug("Browser handle extracted: " + handle);
	        
	        return handle;

	    } catch (JSONException e) {
	        logError("JSON error while getting browser handle: " + e.getMessage());
	        throw new Exception("Invalid response format", e);
	        
	    } catch (IOException e) {
	        logError("Network error while getting browser handle: " + e.getMessage());
	        throw new Exception("Network error during browser handle retrieval", e);
	        
	    } finally {
	        if (conn != null) {
	            conn.disconnect();
	        }
	    }
	}

	/**
	 * Internal method to create folder using browser handle
	 * 
	 * @param browserHandle Browser handle
	 * @param folderPath Full folder path
	 * @param token Authentication token
	 * @throws Exception if folder creation fails
	 */
	private void createAgentFolderInternal(int browserHandle, String folderPath, String token) throws Exception {
	    HttpsURLConnection conn = null;
	    
	    try {
	        logDebug("Creating agent folder at path: " + folderPath);

	        URL url = new URL(BASE_URL + "/infrastructure/agents/" + AGENT_ID + "/browse/folder/");
	        conn = (HttpsURLConnection) url.openConnection();
	        conn.setRequestMethod("POST");
	        conn.setRequestProperty("Authorization", "Bearer " + token);
	        conn.setRequestProperty("Content-Type", "application/json");
	        conn.setRequestProperty("Accept", "application/json");
	        conn.setDoOutput(true);
	        conn.setConnectTimeout(10000);
	        conn.setReadTimeout(10000);

	        JSONObject payload = new JSONObject();
	        payload.put("browserHandle", browserHandle);
	        payload.put("path", folderPath);

	        logDebug("Agent folder creation payload prepared");

	        try (OutputStream os = conn.getOutputStream()) {
	            byte[] input = payload.toString().getBytes(StandardCharsets.UTF_8);
	            os.write(input, 0, input.length);
	            os.flush();
	        }

	        int responseCode = conn.getResponseCode();
	        String response = readResponse(conn);

	        logDebug("Agent folder creation response code: " + responseCode);

	        switch (responseCode) {
	            case HTTP_OK:
	            case HTTP_CREATED:
	                logSuccess("Agent folder created successfully: " + folderPath);
	                break;
	                
	            case HTTP_CONFLICT:
	                logWarn("Agent folder already exists: " + folderPath);
	                break;
	                
	            case HTTP_UNAUTHORIZED:
	                logError("Unauthorized: Invalid token");
	                throw new Exception("Authorization failed during agent folder creation");
	                
	            case HTTP_BAD_REQUEST:
	                logError("Bad request: " + response);
	                throw new Exception("Invalid folder path or request: " + response);
	                
	            default:
	                logError("Failed to create agent folder with code: " + responseCode);
	                throw new Exception("Agent folder creation failed (HTTP " + responseCode + "): " + response);
	        }

	    } catch (JSONException e) {
	        logError("JSON error during agent folder creation: " + e.getMessage());
	        throw new Exception("Invalid response format", e);
	        
	    } catch (IOException e) {
	        logError("Network error during agent folder creation: " + e.getMessage());
	        throw new Exception("Network error during agent folder creation", e);
	        
	    } finally {
	        if (conn != null) {
	            conn.disconnect();
	        }
	    }
	}

	// ==================== STORAGE CONTAINER MANAGEMENT ====================
	
	/**
	 * Creates storage container for the enterprise
	 * Storage containers define where archived data is physically stored
	 * 
	 * @param enterpriseName Enterprise name
	 * @param userName Username
	 * @param token Authentication token
	 * @throws Exception if container creation fails
	 */
	public void createContainer(String enterpriseName, String userName, String token) throws Exception {
	    logInfo("Creating storage container for enterprise: " + enterpriseName);

	    token = token.trim();
	    HttpsURLConnection conn = null;

	    try {
	        disableSSL();

	        // ----------------------------------------------------------
	        // 1. CHECK IF TARGET SMC ALREADY EXISTS IN DB
	        // ----------------------------------------------------------
	        JSONObject existing = rest_api_DBConnection.getExistingTargetSMC(enterpriseName, userName);

	        if (existing != null) {
	            String id  = existing.optString("Target_SMC_id", null);
	            String name = existing.optString("Target_SMC_name", null);

	            if (id != null && name != null) {
	                logInfo("Target SMC already saved in DB  Using existing values");
	                logInfo("SMC ID = " + id + "   | Name = " + name);
	                return; // DONE — NO CREATION REQUIRED
	            }
	        }

	        // ----------------------------------------------------------
	        // 2. CREATE NEW CONTAINER (SINCE DB DOES NOT HAVE IT)
	        // ----------------------------------------------------------
	        String url = BASE_URL + "/infrastructure/storage-managers/" + STORAGE_MANAGER_ID + "/containers/";
	        String containerName = enterpriseName + "-TargetSMC";
	        String containerPath = BASE_DIRECTORY + enterpriseName;

	        JSONObject payload = new JSONObject();
	        payload.put("name", containerName);
	        payload.put("directory", containerPath);
	        payload.put("compressionType", "none");
	        payload.put("nbThreads", 2);
	        payload.put("digestType", "none");

	        JSONObject prefix = new JSONObject();
	        prefix.put("type", "none");
	        payload.put("archiveFolderPrefix", prefix);

	        conn = (HttpsURLConnection) new URL(url).openConnection();
	        conn.setRequestMethod("POST");
	        conn.setRequestProperty("Authorization", "Bearer " + token);
	        conn.setRequestProperty("Content-Type", "application/json");
	        conn.setRequestProperty("Accept", "application/json");
	        conn.setDoOutput(true);

	        try (OutputStream os = conn.getOutputStream()) {
	            os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
	        }

	        int responseCode = conn.getResponseCode();
	        String response = readResponse(conn);

	        if (responseCode == HTTP_OK || responseCode == HTTP_CREATED) {
	            JSONObject json = new JSONObject(response);
	            String smcId   = json.get("id").toString();
	            String smcName = json.getString("name");

	            updateTargetSMCInDB(enterpriseName, userName, smcId, smcName);
	            return;
	        }

	        throw new Exception("Container creation failed: HTTP " + responseCode + "  " + response);

	    } finally {
	        if (conn != null) conn.disconnect();
	    }
	}

	private void updateTargetSMCInDB(String enterprise, String user, String smcId, String smcName) throws Exception {
	    boolean ok = rest_api_DBConnection.updateTargetSMC(enterprise, user, smcId, smcName);

	    if (!ok)
	        throw new Exception("DB update failed for Target SMC");

	    logSuccess("Target SMC saved  ID=" + smcId + "  Name=" + smcName);
	}

	
	public void assignExistingTargetSMCToUser(String enterpriseName, String userName) throws Exception {
	    logInfo("Assigning existing Target SMC to new user for enterprise: " + enterpriseName);

	    // 1. Fetch existing SMC for enterprise (from ANY existing user)
	    JSONObject existing = rest_api_DBConnection.getTargetSMCByEnterprise(enterpriseName);

	    if (existing == null) {
	        logWarn("No Target SMC found in DB for enterprise=" + enterpriseName 
	                + ". Cannot assign to user=" + userName);
	        return;  // Nothing to assign
	    }

	    String smcId = existing.optString("Target_SMC_id", null);
	    String smcName = existing.optString("Target_SMC_name", null);

	    if (smcId == null || smcName == null) {
	        logWarn("Target SMC row exists but values are NULL. Enterprise=" 
	                + enterpriseName);
	        return;
	    }

	    // 2. Assign Target SMC to this user
	    boolean ok = rest_api_DBConnection.updateTargetSMC(enterpriseName, userName, smcId, smcName);

	    if (!ok) {
	        throw new Exception("Failed to assign Target SMC to user=" + userName);
	    }

	    logSuccess("Existing Target SMC assigned to user. " +
	               "User=" + userName + " | SMC ID=" + smcId + " | Name=" + smcName);
	}

	

	// ==================== POLICY MANAGEMENT ====================
	
	/**
	 * Creates archival policy for the enterprise
	 * Policies define retention rules and storage destinations for archived data
	 * 
	 * @param enterpriseName Enterprise name
	 * @param token Authentication token
	 * @throws Exception if policy creation fails
	 */
	public void createPolicy(String enterpriseName,int enterpriseId,  String token) throws Exception {
	    String policyName = enterpriseName + "-Policy";
	    String containerName = enterpriseName + "-TargetSMC";

	    logInfo("Creating archival policy: " + policyName);
	    logDebug("Target container: " + containerName);

	    HttpsURLConnection conn = null;

	    try {
	        URL url = new URL(BASE_URL + "/datamanagement/policies/");
	        conn = (HttpsURLConnection) url.openConnection();
	        conn.setRequestMethod("POST");
	        conn.setRequestProperty("Authorization", "Bearer " + token);
	        conn.setRequestProperty("Content-Type", "application/json");
	        conn.setRequestProperty("Accept", "application/json");
	        conn.setDoOutput(true);
	        conn.setConnectTimeout(15000);
	        conn.setReadTimeout(15000);

	        // Build payload
	        JSONObject payload = new JSONObject();
	        payload.put("name", policyName);
	        payload.put("comment", "Archival policy for " + enterpriseName);

	        JSONObject retention = new JSONObject();
	        retention.put("name", "Permanent Retention");
	        payload.put("retention", retention);

	        payload.put("reduceImpactOnSource", false);

	        // Storage container list
	        JSONArray storageContainerList = new JSONArray();
	        JSONObject containerObj = new JSONObject();

	        JSONObject storageContainer = new JSONObject();
	        storageContainer.put("name", containerName);

	        JSONObject innerRetention = new JSONObject();
	        innerRetention.put("name", "Permanent Retention");

	        containerObj.put("storageContainer", storageContainer);
	        containerObj.put("retention", innerRetention);

	        storageContainerList.put(containerObj);
	        payload.put("storageContainerList", storageContainerList);

	        logDebug("Policy creation payload prepared");

	        // Send request
	        try (OutputStream os = conn.getOutputStream()) {
	            byte[] input = payload.toString().getBytes(StandardCharsets.UTF_8);
	            os.write(input, 0, input.length);
	            os.flush();
	        }

	        int responseCode = conn.getResponseCode();
	        String response = readResponse(conn);

	        logDebug("Policy creation response code: " + responseCode);

	        JSONObject respJson = null;

	        // Handle response codes
	        switch (responseCode) {
	            case HTTP_OK:
	            case HTTP_CREATED:
	                logSuccess("Policy created successfully: " + policyName);

	                if (response.trim().startsWith("{")) {
	                    respJson = new JSONObject(response);

	                    if (respJson.has("id")) {
	                        logInfo("Policy ID: " + respJson.getInt("id"));
	                    }
	                    if (respJson.has("name")) {
	                        logInfo("Policy Name: " + respJson.getString("name"));
	                    }
	                } else {
	                    logWarn("Policy created but response is non-JSON");
	                }
	                break;

	            case HTTP_CONFLICT:
	                logWarn("Policy already exists: " + policyName);
	                // Don't proceed to update if policy already exists
	                return;

	            case HTTP_UNAUTHORIZED:
	                logError("Unauthorized: Token is invalid or expired");
	                throw new Exception("Authorization failed during policy creation");

	            case HTTP_BAD_REQUEST:
	                logError("Bad Request: Invalid policy configuration");
	                logError("Response: " + response);
	                throw new Exception("Invalid policy configuration: " + response);

	            case HTTP_NOT_FOUND:
	                logError("Not Found: Storage container may not exist");
	                throw new Exception("Storage container not found: " + containerName);

	            case HTTP_FORBIDDEN:
	                logError("Forbidden: Insufficient permissions");
	                throw new Exception("Insufficient permissions to create policy");

	            default:
	                logError("Policy creation failed with code: " + responseCode);
	                logError("Response body: " + response);
	                throw new Exception("Failed to create policy (HTTP " + responseCode + "): " + response);
	        }

	        // AUTO-INTEGRATED STEP: UPDATE archivePolicyUnix
	        if (respJson != null && respJson.has("id") && respJson.has("name")) {
	            logInfo("Automatically updating archivePolicyUnix for newly created policy...");
	            updateArchivePolicyUnix(respJson,enterpriseId, token);
	        } else {
	            logWarn("Cannot update archivePolicyUnix - missing required fields in policy response");
	        }

	    } catch (JSONException e) {
	        logError("JSON error during policy creation: " + e.getMessage());
	        throw new Exception("Invalid response format", e);
	        
	    } catch (IOException e) {
	        logError("Network error during policy creation: " + e.getMessage());
	        throw new Exception("Network error during policy creation", e);
	        
	    } finally {
	        if (conn != null) {
	            conn.disconnect();
	        }
	    }
	}

	/**
	 * Updates archivePolicyUnix setting for the repository
	 * This links the newly created policy to the repository
	 * 
	 * @param createPolicyResponse Response from policy creation
	 * @param token Authentication token
	 * @throws Exception if update fails
	 */
	public void updateArchivePolicyUnix(JSONObject createPolicyResponse,int enterpriseId, String token) throws Exception {

	    String setting = "archivePolicyUnix";
	    HttpsURLConnection conn = null;

	    try {
	        // ---------------------------------------------------
	        // Step 1: Extract required values from createPolicy()
	        // ---------------------------------------------------
	        if (!createPolicyResponse.has("id") || !createPolicyResponse.has("name")) {
	            logError("Invalid policy response: Missing required fields (id/name)");
	            throw new RuntimeException("createPolicy() did not return required fields");
	        }

	        String policyName = createPolicyResponse.getString("name");

	        logInfo("Updating archivePolicyUnix for Repository ID: " + enterpriseId);
	        logDebug("Setting: " + setting);
	        logDebug("Policy Name to apply: " + policyName);


	        // ---------------------------------------------------
	        // Step 2: Prepare PUT request
	        // ---------------------------------------------------
	        String urlStr = BASE_URL + "/datamanagement/repositories/" + enterpriseId + "/settings/" + setting + "/";
	        URL url = new URL(urlStr);

	        conn = (HttpsURLConnection) url.openConnection();
	        conn.setRequestMethod("PUT");
	        conn.setRequestProperty("Authorization", "Bearer " + token);
	        conn.setRequestProperty("Accept", "application/json");
	        conn.setRequestProperty("Content-Type", "application/json");
	        conn.setDoOutput(true);

	        JSONObject payload = new JSONObject();
	        payload.put("value", policyName);

	        logDebug("archivePolicyUnix update payload:\n" + payload.toString(2));

	        try (OutputStream os = conn.getOutputStream()) {
	            os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
	        }


	        // ---------------------------------------------------
	        // Step 3: Read Response
	        // ---------------------------------------------------
	        String putResponse = readResponse(conn);
	        int responseCode = conn.getResponseCode();

	        logDebug("archivePolicyUnix PUT response code: " + responseCode);
	        logDebug("archivePolicyUnix PUT response: " + putResponse);

	        if (!putResponse.trim().startsWith("{")) {
	            logError("Non-JSON response received");
	            throw new RuntimeException("Invalid response: " + putResponse);
	        }

	        JSONObject json = new JSONObject(putResponse);


	        // ---------------------------------------------------
	        // Step 4: Success / Failure Evaluation
	        // ---------------------------------------------------
	        if (responseCode == 200) {
	            if (json.has(setting) && json.getString(setting).equals(policyName)) {

	                logSuccess("archivePolicyUnix updated successfully to: " + json.getString(setting));
	            } else {
	                logWarn("PUT succeeded but unexpected response body");
	            }

	        } else if (responseCode == 401) {
	            logError("Unauthorized: Token may be invalid");
	            throw new RuntimeException("Authorization failed for archivePolicyUnix update");

	        } else {
	            logError("archivePolicyUnix update failed. Code: " + responseCode);
	            throw new RuntimeException("archivePolicyUnix update failed: " + putResponse);
	        }

	    } catch (Exception e) {
	        logError("Exception during archivePolicyUnix update: " + e.getMessage());
	        throw e;

	    } finally {
	        if (conn != null) conn.disconnect();
	    }
	}


	/**
	 * Reads response from HttpsURLConnection (handles both success and error
	 * streams)
	 */
	private String readResponse(HttpsURLConnection conn) throws IOException {
		int code = conn.getResponseCode();
		InputStream is;

		if (code >= 400) {
			is = conn.getErrorStream();
			if (is == null) {
				is = conn.getInputStream();
			}
		} else {
			is = conn.getInputStream();
		}

		return readStream(is);
	}

	/**
	 * Reads input stream and converts to string
	 */
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

	// ==================== LOGGING METHODS ====================

	/**
	 * Logs informational messages
	 */
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
