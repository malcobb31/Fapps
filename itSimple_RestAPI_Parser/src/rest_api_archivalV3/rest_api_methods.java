package rest_api_archivalV3;

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

        // NOTE: Removed the old insertArchivingDetails(...) call here to avoid creating rows without job_id.
        // The task_name & task_directory are saved by updateContainerDirectory(...) via insertArchivingDetailsFromApi(...).
        // Start archive jobs (one per bucket)
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
            throw new IllegalArgumentException("Target SMC name cannot be null or empty");
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
                String taskName = json.optString("name", null);
                String taskDirectory = json.optString("directory", null);

                logSuccess("Container directory updated successfully. Task ID: " + taskId);

                // ---- Save to DB ----
                // Upsert the task_name/task_directory for this task_id.
                // If the row doesn't exist yet (because we removed pre-insert),
                // insert a row with task_id + task_name + task_directory + enterprise/username.
                insertArchivingDetailsFromApi(enterpriseName, username, taskId, taskName, taskDirectory);

                return taskId;
            } else {
                throw new RuntimeException("Container update failed: " + response);
            }

        } finally {
            if (conn != null)
                conn.disconnect();
        }
    }

    /**
     * Insert or update task_name/task_directory for a task coming from the container PUT response.
     * This is an upsert: update if task_id exists, otherwise insert a new row (with enterprise/username).
     */
    private void insertArchivingDetailsFromApi(String enterpriseName, String username, int taskId, String taskName,
            String taskDirectory) {

        String queryUpdate = "UPDATE backup_process_archiving_details SET task_name = ?, task_directory = ? WHERE task_id = ?";
        String queryInsert = "INSERT INTO backup_process_archiving_details (enterprise_name, username, Target_SMC_name, task_id, task_name, task_directory) VALUES (?, ?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS)) {

            // Try update first
            try (PreparedStatement ps = conn.prepareStatement(queryUpdate)) {
                ps.setString(1, taskName);
                ps.setString(2, taskDirectory);
                ps.setInt(3, taskId);

                int rows = ps.executeUpdate();
                if (rows > 0) {
                    logDebug("Updated archiving details from API for task_id:" + taskId);
                    return;
                }
            }

            // If update affected 0 rows, insert a fresh row (so we do not leave task_name null)
            // We attempt to figure out Target_SMC_name for insert by querying backup_process_enterprise_user_detail.
            String targetSmc = null;
            try (PreparedStatement psFind = conn.prepareStatement(
                    "SELECT Target_SMC_name FROM backup_process_enterprise_user_detail WHERE enterprise_name = ? AND user_name = ? LIMIT 1")) {
                psFind.setString(1, enterpriseName);
                psFind.setString(2, username);
                try (ResultSet rs = psFind.executeQuery()) {
                    if (rs.next()) {
                        targetSmc = rs.getString("Target_SMC_name");
                    }
                }
            } catch (SQLException ex) {
                logWarn("Failed to look up Target_SMC_name while inserting archiving details: " + ex.getMessage());
            }

            try (PreparedStatement psi = conn.prepareStatement(queryInsert)) {
                psi.setString(1, enterpriseName);
                psi.setString(2, username);
                psi.setString(3, targetSmc);
                psi.setInt(4, taskId);
                psi.setString(5, taskName);
                psi.setString(6, taskDirectory);

                int rows = psi.executeUpdate();
                if (rows > 0) {
                    logDebug("Inserted archiving details (from API) for task_id: " + taskId);
                } else {
                    logWarn("Insert archiving details (from API) returned 0 rows for task_id: " + taskId);
                }
            }

        } catch (SQLException e) {
            logError("Failed to update/insert task_name/directory: " + e.getMessage());
        }
    }

    private void startArchiveJob(String enterpriseName, String username, String bucketName, String pathsInput,
            String token, int taskId, String targetSmcName) throws Exception {

        if (bucketName == null || bucketName.trim().isEmpty()) {
            logError("No bucketName provided, skipping archive launch.");
            return;
        }

        // Split buckets and paths
        List<String> buckets = Arrays.stream(bucketName.split(",")).map(String::trim).filter(b -> !b.isEmpty())
                .collect(Collectors.toList());
        List<String> paths = Arrays.stream((pathsInput == null ? "" : pathsInput).split(","))
                .map(String::trim).filter(p -> !p.isEmpty())
                .map(p -> p.startsWith("/") ? p.substring(1) : p)
                .collect(Collectors.toList());

        if (buckets.size() != paths.size()) {
            throw new IllegalArgumentException(
                    "Bucket count and path count must match! Buckets: " + buckets.size() + ", Paths: " + paths.size());
        }

        String baseUrl = BASE_URL + "/files/archive/";
        logInfo("Starting archive job(s) for task_id: " + taskId);
        logDebug("Archive API base URL: " + baseUrl);

        for (int i = 0; i < buckets.size(); i++) {
            String bucket = buckets.get(i);
            String path = paths.get(i);

            logInfo("Processing bucket: " + bucket + " with path: " + path);

            // Idempotency check
            try {
                if (archivingJobExistsForBucket(enterpriseName, username, targetSmcName, bucket)) {
                    logInfo("An archive job already exists for bucket '" + bucket + "' - skipping.");
                    continue;
                }
            } catch (SQLException e) {
                logWarn("DB check for existing job failed for bucket " + bucket + ": " + e.getMessage()
                        + " — continuing to attempt job creation.");
            }

            HttpURLConnection conn = null;
            String lastSource = null;
            String lastDestination = null;

            try {
                conn = createConnection(baseUrl, "POST");
                conn.setRequestProperty("Authorization", "Bearer " + token);
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setRequestProperty("Accept", "application/json");

                // Build pathInfo for this bucket-path pair
                JSONArray pathInfoArr = new JSONArray();
                JSONObject pi = new JSONObject();

                lastSource = String.format("s3@%s-%s-%s-SourceSMCPN:/%s", enterpriseName, username, bucket, path);
                lastDestination = String.format("archive@%s:/%s-%s", enterpriseName,enterpriseName, username);

                pi.put("source", lastSource);
                pi.put("destination", lastDestination);
                pathInfoArr.put(pi);

                logDebug("Added pathInfo entry: source=" + lastSource + ", destination=" + lastDestination);

                // Build payload
                JSONObject payload = new JSONObject();
                payload.put("pathInfo", pathInfoArr);
                payload.put("osType", "Windows");
                payload.put("createFolder", true);
                payload.put("metadataList", new JSONArray());

                JSONArray settings = new JSONArray();
                settings.put(new JSONObject().put("archiveSymLinks", true));
                payload.put("settings", settings);

                logDebug("Archive job payload for bucket '" + bucket + "': " + payload.toString(2));

                writeRequestBody(conn, payload.toString());

                int responseCode = conn.getResponseCode();
                String response = readResponse(conn);

                logDebug("Archive POST response code: " + responseCode);
                logDebug("Archive POST response: " + response);

                if (responseCode == 200 || responseCode == 201) {
                    JSONObject json = new JSONObject(response);
                    if (json.has("jobId")) {
                        int jobId = json.getInt("jobId");
                        logSuccess("Archiving job launched for bucket '" + bucket + "'. jobId: " + jobId);

                        // Ensure task_name & task_directory
                        String[] taskInfo = getTaskInfoForTaskId(taskId); // [0]=task_name, [1]=task_directory
                        String taskName = (taskInfo[0] == null || taskInfo[0].trim().isEmpty()) ? ("task-" + taskId)
                                : taskInfo[0];
                        String taskDirectory = (taskInfo[1] == null || taskInfo[1].trim().isEmpty())
                                ? String.format("C:\\DemoDirectory\\%s\\%s", enterpriseName, username) : taskInfo[1];

                        // Insert into DB
                        insertArchivingDetailsWithJob(enterpriseName, username, targetSmcName, taskId, jobId, bucket,
                                lastSource, lastDestination, taskName, taskDirectory);

                        try {
                            dbupdate(jobId);
                        } catch (Exception e) {
                            logWarn("dbupdate failed for jobId " + jobId + ": " + e.getMessage());
                        }

                    } else {
                        logWarn("Archive API responded OK but missing jobId for bucket '" + bucket + "'");
                    }
                } else {
                    logError("Archive POST failed for bucket '" + bucket + "'. Code: " + responseCode);
                    throw new RuntimeException("Archive POST failed: " + response);
                }

            } catch (IOException | JSONException e) {
                logError("Exception during archive POST for bucket '" + bucket + "': " + e.getMessage());
                throw e;
            } finally {
                if (conn != null)
                    conn.disconnect();
            }
        }

        logInfo("All bucket processing complete for task_id: " + taskId);
    }

    /**
     * Returns true if a non-null job_id already exists for this enterprise/username/targetSmcName+bucket.
     * Uses source_path LIKE pattern to detect a previously launched job for the same bucket.
     */
    private boolean archivingJobExistsForBucket(String enterpriseName, String username, String targetSmcName,
            String bucket) throws SQLException {

        String q = "SELECT job_id FROM backup_process_archiving_details WHERE enterprise_name = ? AND username = ? "
                + "AND Target_SMC_name = ? AND job_id IS NOT NULL AND source_path LIKE ? LIMIT 1";

        // New like pattern matches corrected source format:
        //  ...-SourceSMCPN:/<bucket>...
        String likePattern = "%-SourceSMCPN:/" + bucket + "%";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                PreparedStatement ps = conn.prepareStatement(q)) {
            ps.setString(1, enterpriseName);
            ps.setString(2, username);
            ps.setString(3, targetSmcName);
            ps.setString(4, likePattern);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    int existingJobId = rs.getInt(1);
                    logDebug("Found existing job_id " + existingJobId + " for bucket " + bucket);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Insert a row into backup_process_archiving_details that includes the returned job_id and paths.
     * This ensures we do NOT create DB entries without job_id.
     */
    private void insertArchivingDetailsWithJob(String enterpriseName, String username, String targetSmcName,
            int taskId, int jobId, String bucketName, String sourcePath, String destinationPath, String taskName,
            String taskDirectory) {

        if (jobId <= 0) {
            logError("Refusing to insert archiving details: invalid jobId=" + jobId);
            return;
        }

        String queryInsert = "INSERT INTO backup_process_archiving_details "
                + "(enterprise_name, username, Target_SMC_name, bucket_name, task_id, job_id, source_path, destination_path, task_name, task_directory) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                PreparedStatement ps = conn.prepareStatement(queryInsert)) {

            ps.setString(1, enterpriseName);
            ps.setString(2, username);
            ps.setString(3, targetSmcName);
            ps.setString(4, bucketName);
            ps.setInt(5, taskId);
            ps.setInt(6, jobId);
            ps.setString(7, sourcePath);
            ps.setString(8, destinationPath);
            ps.setString(9, taskName);
            ps.setString(10, taskDirectory);

            int rows = ps.executeUpdate();
            if (rows > 0) {
                logDebug("Inserted archiving detail row with job_id " + jobId + " for task_id " + taskId);
            } else {
                logWarn("Insert archiving detail returned 0 rows for job_id " + jobId);
            }

        } catch (SQLException e) {
            logError("Failed to insert archiving details for job_id " + jobId + ": " + e.getMessage());
        }
    }

    /**
     * Fetch task_name and task_directory (if present) for a given taskId.
     * Returns a String array: [task_name, task_directory] (elements may be null).
     */
    private String[] getTaskInfoForTaskId(int taskId) {
        String q = "SELECT task_name, task_directory FROM backup_process_archiving_details WHERE task_id = ? LIMIT 1";
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                PreparedStatement ps = conn.prepareStatement(q)) {
            ps.setInt(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String tn = rs.getString("task_name");
                    String td = rs.getString("task_directory");
                    logDebug("Fetched task info for task_id " + taskId + ": name=" + tn + ", dir=" + td);
                    return new String[] { tn, td };
                }
            }
        } catch (SQLException e) {
            logWarn("Failed to fetch task info for task_id " + taskId + ": " + e.getMessage());
        }
        return new String[] { null, null };
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
     *
     * NOTE: This method is preserved but no longer called by run().
     * We avoid creating rows with NULL job_id. All job rows are created after job_id is returned.
     */
    @SuppressWarnings("unused")
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

    public void dbupdate(int jobId) {
        // guard: don't run for invalid job ids
        if (jobId <= 0) {
            logWarn("dbupdate called with invalid jobId: " + jobId + " — skipping update.");
            return;
        }

        String sql = "UPDATE backup_process_archiving_details SET process_name = ? WHERE job_id = " + jobId;

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, "Archive");
            int rows = ps.executeUpdate();
            logInfo("DB Update completed Rows affected: " + rows);

        } catch (Exception e) {
            logError("DB Error (saveProcessName): " + e.getMessage());
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
        InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream()
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
