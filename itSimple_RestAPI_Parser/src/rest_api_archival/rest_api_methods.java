package rest_api_archival;

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
        // Validate inputs
        validateInputs(enterpriseName, username, bucketName, pathsInput);
        
        disableSSLVerification();
        
        logInfo("========================================");
        logInfo("Starting Task Process");
        logInfo("Enterprise: " + enterpriseName);
        logInfo("Username: " + username);
        logInfo("Bucket: " + bucketName);
        logInfo("Paths: " + pathsInput);
        logInfo("========================================");

        String token = null;
        try {
            // Authenticate
            token = authenticate(BASE_URL, AUTH_DB_NAME, AUTH_USERNAME, AUTH_PASSWORD);
            logSuccess("Authentication successful");

            // Get next task number
            int taskNumber = getNextTaskNumber();
            String taskName = generateTaskName(enterpriseName, username, taskNumber);

            // Build task payload
            JSONObject payload = buildTaskPayload(enterpriseName, username, bucketName, pathsInput, taskName);
            logDebug("Task payload: " + payload.toString(2));

            // Create task
            int taskId = createTask(payload, token);
            logSuccess("Task created with ID: " + taskId);

            // Insert archiving details
            insertArchivingDetails(enterpriseName, username, bucketName, taskId);

            // Run task
            int jobId = runTask(taskId, token);
            logSuccess("Task executed with Job ID: " + jobId);

            // Update job ID
            updateJobId(taskId, jobId);

            // Insert task details
            insertTaskDetails(taskNumber, taskName);
            
            logSuccess("Process completed successfully");
            logInfo("========================================");

        } catch (Exception e) {
            logError("Task process failed: " + e.getMessage());
            throw e;
        }
    }

    /**
     * Validate input parameters
     */
    private void validateInputs(String enterpriseName, String username, String bucketName, String pathsInput) 
            throws IllegalArgumentException {
        if (enterpriseName == null || enterpriseName.trim().isEmpty()) {
            throw new IllegalArgumentException("Enterprise name cannot be null or empty");
        }
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("Username cannot be null or empty");
        }
        if (bucketName == null || bucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("Bucket name cannot be null or empty");
        }
        if (pathsInput == null || pathsInput.trim().isEmpty()) {
            throw new IllegalArgumentException("Paths input cannot be null or empty");
        }
    }

    /**
     * Generate task name from components
     */
    private String generateTaskName(String enterpriseName, String username, int taskNumber) {
        return String.format("%s-%s-task%d", enterpriseName, username, taskNumber);
    }

    /**
     * Build the complete task payload
     */
    private JSONObject buildTaskPayload(String enterpriseName, String username, String bucketName, 
                                        String pathsInput, String taskName) {
        JSONObject payload = new JSONObject();
        
        // Basic task configuration
        payload.put("waitIfMaxSimultaneousRunsReached", false);
        payload.put("name", taskName);
        payload.put("enabled", true);

        // Task type
        payload.put("type", new JSONObject().put("name", "auto_archiving"));
        
        // Source bucket
        String bucketNewname = enterpriseName+"-"+username+"-"+bucketName+"-SourceSMCPN";
        payload.put("source", new JSONObject().put("name",bucketNewname));
        
        // Dataset (paths)
        JSONArray datasetArray = buildDatasetArray(bucketNewname, pathsInput);
        payload.put("dataset", datasetArray);

        // Target configuration
        JSONObject target = new JSONObject();
        target.put("repository", enterpriseName);
        target.put("repositoryPath", String.format("/%s-%s",enterpriseName, username));
        payload.put("target", target);

        // Archiving options
        JSONObject options = new JSONObject();
        options.put("archiveSameFileAgain", true);
        options.put("compareUsingDigest", false);
        options.put("archiveEmptyFolders", true);
        payload.put("options", options);

        // Archive project
        payload.put("archiveProject", new JSONObject().put("name", enterpriseName + "-Project"));

        return payload;
    }

    /**
     * Build dataset array from comma-separated paths
     */
    private JSONArray buildDatasetArray(String bucketName, String pathsInput) {
        JSONArray datasetArray = new JSONArray();
        
        List<String> paths = Arrays.stream(pathsInput.split(","))
                .map(String::trim)
                .filter(p -> !p.isEmpty())
                .collect(Collectors.toList());
        
        for (String path : paths) {
            JSONObject pathObj = new JSONObject();
            pathObj.put("path", String.format("s3@%s:/%s", bucketName, path));
            datasetArray.put(pathObj);
        }
        
        return datasetArray;
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
     * Create a new archiving task
     */
    private int createTask(JSONObject payload, String token) throws Exception {
        String urlStr = BASE_URL + "/activity/tasks/";
        logInfo("Creating task");
        logDebug("Create Task URL: " + urlStr);

        HttpURLConnection conn = null;
        try {
            conn = createConnection(urlStr, "POST");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");

            // Send request
            writeRequestBody(conn, payload.toString());

            // Read response
            int responseCode = conn.getResponseCode();
            String response = readResponse(conn);
            
            logDebug("Create Task Response Code: " + responseCode);
            logDebug("Create Task Response: " + response);

            if (responseCode == 200 || responseCode == 201) {
                JSONObject json = new JSONObject(response);
                if (!json.has("id")) {
                    throw new Exception("Task ID not found in response");
                }
                int taskId = json.getInt("id");
                logSuccess("Task created with ID: " + taskId);
                return taskId;
            } else {
                logError("Create Task failed with code " + responseCode);
                throw new IOException("Create Task failed: " + response);
            }

        } catch (JSONException e) {
            logError("Failed to parse create task response: " + e.getMessage());
            throw new Exception("Invalid JSON response from create task endpoint", e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    /**
     * Run an existing task
     */
    private int runTask(int taskId, String token) throws Exception {
        String urlStr = BASE_URL + "/activity/tasks/" + taskId + "/run/";
        logInfo("Running task ID: " + taskId);
        logDebug("Run Task URL: " + urlStr);

        HttpURLConnection conn = null;
        try {
            conn = createConnection(urlStr, "POST");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");

            // Build run payload
            JSONObject body = new JSONObject();
            body.put("testMode", false);

            // Send request
            writeRequestBody(conn, body.toString());

            // Read response
            int responseCode = conn.getResponseCode();
            String response = readResponse(conn);
            
            logDebug("Run Task Response Code: " + responseCode);
            logDebug("Run Task Response: " + response);

            if (responseCode == 200 || responseCode == 201) {
                JSONObject json = new JSONObject(response);
                if (!json.has("jobId")) {
                    throw new Exception("Job ID not found in response");
                }
                int jobId = json.getInt("jobId");
                logSuccess("Task started with Job ID: " + jobId);
                return jobId;
            } else {
                logError("Run Task failed with code " + responseCode);
                throw new IOException("Run Task failed: " + response);
            }

        } catch (JSONException e) {
            logError("Failed to parse run task response: " + e.getMessage());
            throw new Exception("Invalid JSON response from run task endpoint", e);
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    // ============= DATABASE METHODS =============

    /**
     * Get the next task number from database
     */
    private int getNextTaskNumber() {
        String selectQuery = "SELECT COALESCE(MAX(task_number), 0) FROM task_details";
        String insertQuery = "INSERT INTO task_details (task_number) VALUES (?)";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {
            conn.setAutoCommit(false); // Start transaction

            int nextNumber = 1;

            // Step 1: Fetch current max task_number
            try (PreparedStatement ps = conn.prepareStatement(selectQuery);
                 ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    nextNumber = rs.getInt(1) + 1;
                }
            }

            // Step 2: Insert new row with next task number
            try (PreparedStatement ps = conn.prepareStatement(insertQuery)) {
                ps.setInt(1, nextNumber);
                ps.executeUpdate();
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
     * Insert archiving details into database
     */
    private void insertArchivingDetails(String enterprise, String user, String bucket, int taskId) {
        String query = "INSERT INTO backup_process_archiving_details (enterprise_name, username, bucket_name, task_id) " +
                       "VALUES (?, ?, ?, ?)";

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


    /**
     * Update job ID for a task
     */
    private void updateJobId(int taskId, int jobId) {
    String query = "UPDATE backup_process_archiving_details SET job_id = ? WHERE task_id = ?";

    try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
         PreparedStatement ps = conn.prepareStatement(query)) {

        ps.setInt(1, jobId);
        ps.setInt(2, taskId);

        int rowsAffected = ps.executeUpdate();

        if (rowsAffected > 0) {
            logDebug("Job ID updated for task ID: " + taskId);
        } else {
            logWarn("No rows updated for task ID: " + taskId);
        }

    } catch (SQLException e) {
        logError("Failed to update job ID: " + e.getMessage());
    }
}



    /**
     * Insert task details into database
     */
    private void insertTaskDetails(int taskNumber, String taskName) {
        String query = "INSERT INTO backup_process_task_details (task_number, name) VALUES (?, ?)";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(query)) {

            ps.setInt(1, taskNumber);
            ps.setString(2, taskName);

            int rowsAffected = ps.executeUpdate();

            if (rowsAffected > 0) {
                logDebug("Task details inserted: " + taskName);
            } else {
                logWarn("No rows inserted into task_details");
            }

        } catch (SQLException e) {
            logError("Failed to insert task details: " + e.getMessage());
        }
    }


    /**
     * Get database connection
     */
    
    // ============= HTTP UTILITIES =============

    /**
     * Create and configure HTTP connection
     */
    private HttpURLConnection createConnection(String urlStr, String method) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
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
        InputStream is = (responseCode >= 200 && responseCode < 300) 
                ? conn.getInputStream() 
                : conn.getErrorStream();
        
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
            TrustManager[] trustAllCerts = new TrustManager[]{
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
        return String.format("[%s] [%s] %s", 
                LocalDateTime.now().format(TIME_FORMATTER), 
                level, 
                msg);
    }

    // ============= MAIN METHOD FOR TESTING =============

    public static void main(String[] args) {
        rest_api_methods api = new rest_api_methods();
        
        try {
            // Example usage
            String enterprise = "TestEnterprise";
            String username = "testuser";
            String bucket = "my-bucket";
            String paths = "/path1,/path2,/path3";
            
            api.run(enterprise, username, bucket, paths);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
