package rest_api_retrive;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class rest_api_methods_final {

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

    // =====================================================
    // PUBLIC ENTRY POINT (USED BY YOUR MAIN)
    // =====================================================
    public void run(String enterpriseName, String username, String bucketName, String path) throws Exception {
        disableSSL();

        String token = authenticate(BASE_URL, AUTH_DB_NAME, AUTH_USERNAME, AUTH_PASSWORD);

        // --- Start archive job ---
        startArchiveJob(enterpriseName, username, bucketName, path, token);
    }

    // =====================================================
    // AUTHENTICATION (reference style)
    // =====================================================
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
            logDebug("Authentication response text: " + responseText);


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

    // Extract token from JSON
    private String parseAuthenticationToken(String jsonText) {
        JSONObject json = new JSONObject(jsonText);
        return json.getString("access");
    }

    // =====================================================
    // ARCHIVE JOB (Your required version)
    // =====================================================
    private void startArchiveJob(String enterprise, String username, String bucket, String path, String token) throws Exception {

        String urlStr = BASE_URL + "/files/retrieve/";
        logInfo("Calling /files/archive API");

        HttpURLConnection conn = createConnection(urlStr, "POST");
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");

        // ---- Build source ----
        String source = "archive@" + enterprise + ":/" + username + "/" + path;

        // ---- Build destination ----
        String destination = "s3@" + enterprise + "-" + username + "-" + bucket + "-SourceSMCPN:/archiveretrieval";


        // ---- Build pathInfo array ----
        JSONArray pathInfoArr = new JSONArray();
        JSONObject pi = new JSONObject();
        pi.put("source", source);
        pi.put("destination", destination);
        pathInfoArr.put(pi);

        // ---- Final payload ----
        JSONObject payload = new JSONObject();
        payload.put("pathInfo", pathInfoArr);
        payload.put("osType", "Windows");
        payload.put("createDirectory", true);

        logDebug("FINAL PAYLOAD SENT TO SERVER:\n" + payload.toString(2));

        writeRequestBody(conn, payload.toString());

        int responseCode = conn.getResponseCode();
        String response = readResponse(conn);

        System.out.println("===== SERVER RESPONSE =====");
        System.out.println(response);
        System.out.println("===========================");

        if (responseCode != 200 && responseCode != 201) {
            throw new RuntimeException("Archive API failed. Code: " + responseCode + " Response: " + response);
        }
    }

    private String extractParentFolder(String path) {
        if (path == null || !path.contains("/")) {
            return ""; // file in root
        }
        String[] split = path.split("/");
        if (split.length <= 1) return "";
        return split[0]; // only top-level folder
    }

    // =====================================================
    // UTILITY HELPERS (same style as reference)
    // =====================================================

    private HttpURLConnection createConnection(String url, String method) throws Exception {
        URL u = new URL(url);
        HttpURLConnection conn = (HttpURLConnection) u.openConnection();
        conn.setRequestMethod(method);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        return conn;
    }

    private void writeRequestBody(HttpURLConnection conn, String body) throws Exception {
        OutputStream os = conn.getOutputStream();
        os.write(body.getBytes(StandardCharsets.UTF_8));
        os.close();
    }

    private String readResponse(HttpURLConnection conn) throws Exception {
        InputStream is;
        if (conn.getResponseCode() >= 200 && conn.getResponseCode() < 300) {
            is = conn.getInputStream();
        } else {
            is = conn.getErrorStream();
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();

        String line;
        while ((line = br.readLine()) != null)
            sb.append(line);

        br.close();
        return sb.toString();
    }

    // =====================================================
    // DISABLE SSL (your typical pattern)
    // =====================================================
    private void disableSSL() {
        try {
            TrustManager[] trustAll = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() { return null; }
                    public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                    public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                }
            };

            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAll, new java.security.SecureRandom());

            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier((a, b) -> true);

        } catch (Exception ignored) {}
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
}
