package rest_api_retrive;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;    //  Added for DB operations
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class rest_api_methods {

    private static final String BASE_URL = "https://poc.itsimple.in/restapi";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/pview?useSSL=false&serverTimezone=UTC";
    private static final String DB_USER = "root";
    private static final String DB_PASS = "";
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    private static final int CONNECTION_TIMEOUT = 30000;
    private static final int READ_TIMEOUT = 30000;
    
    private static final String AUTH_DB_NAME = "ADA";
    private static final String AUTH_USERNAME = "root";
    private static final String AUTH_PASSWORD = "ATempo@123";

    // =====================================================
    // MAIN ENTRY
    // =====================================================
    public void run(String enterpriseName, String username, String bucketName, String path) throws Exception {
        disableSSL();
        String token = authenticate(BASE_URL, AUTH_DB_NAME, AUTH_USERNAME, AUTH_PASSWORD);
        
        startArchiveJob(enterpriseName, username, bucketName, path, token);
    }

    // =====================================================
    // AUTHENTICATION
    // =====================================================
    public String authenticate(String baseUrl, String dbName, String username, String password) throws Exception {
        String authUrl = baseUrl + "/auth/token/";
        logInfo("Authenticating user: " + username);
        logDebug("Authentication URL: " + authUrl);

        HttpURLConnection conn = createConnection(authUrl, "POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");

        JSONObject body = new JSONObject();
        body.put("dbName", dbName);
        body.put("name", username);
        body.put("password", password);
        body.put("superUser", true);

        writeRequestBody(conn, body.toString());

        int responseCode = conn.getResponseCode();
        String response = readResponse(conn);

        if (responseCode == 200 || responseCode == 201) {
            JSONObject json = new JSONObject(response);
            return json.getString("access");
        }

        throw new Exception("Authentication failed: " + response);
    }

    // =====================================================
    // START RETRIEVE JOB + STORE jobId IN DB
    // =====================================================
    private void startArchiveJob(String enterprise, String username, String bucket, String path, String token) throws Exception {

        String urlStr = BASE_URL + "/files/retrieve/";
        logInfo("Calling /files/retrieve API");

        HttpURLConnection conn = createConnection(urlStr, "POST");
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");

        // Build source/destination
        
     // Fix the path: if it starts with "/", remove the first "/"
//        if (path.startsWith("/")) {
//            path = path.substring(1);
//        }

        // Then build source and destination
        String source = "archive@" + enterprise + ":" + path;
        String destination = "s3@" + "Airtel-Retrieval-SMCPN:/archiveretrieval";


        JSONArray pathInfoArr = new JSONArray();
        JSONObject pi = new JSONObject();
        pi.put("source", source);
        pi.put("destination", destination);
        pathInfoArr.put(pi);

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
            throw new RuntimeException("Retrieve API failed. Code: " + responseCode);
        }

        // --------------------------------------
        //  Extract jobId and save to DB
        // --------------------------------------
        try {
            JSONObject resp = new JSONObject(response);

            if (resp.has("jobId")) {
                int jobId = resp.getInt("jobId");
                logInfo("RETRIEVAL jobId received: " + jobId);

                saveRetrieveId(jobId, enterprise, username, source, destination);  //  CALL DB UPDATE
                dbupdate(jobId);
                logSuccess("retrieve_id saved successfully into DB.");
                
            } else {
                logWarn("jobId NOT found in response!");
            }

        } catch (JSONException e) {
            logError("Failed to parse jobId from response: " + e.getMessage());
        }
    }

    // =====================================================
    //  DB UPDATE METHOD
    // =====================================================
    
    public void dbupdate(int jobId)
    {
    	String sql = "UPDATE backup_process_archiving_details SET process_name = ? WHERE job_id="+ jobId;
    	
    	try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                PreparedStatement ps = conn.prepareStatement(sql)) {

               ps.setString(1, "Retrieve");
               int rows = ps.executeUpdate();
               logInfo("DB Update completed Rows affected: " + rows);

           } catch (Exception e) {
               logError("DB Error (saveProcessName): " + e.getMessage());
           }
       }
    
    private void saveRetrieveId(int retrieveId, String enterprise, String username, String source, String destination) {

    	 String sql = "INSERT INTO backup_process_archiving_details "
                 + "(job_id, source_path, destination_path, enterprise_name, username) "
                 + "VALUES (?, ?, ?, ?, ?)";
        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setInt(1, retrieveId);
            ps.setString(2, source);
            ps.setString(3, destination);
            ps.setString(4, enterprise);
            ps.setString(5, username);

            int rows = ps.executeUpdate();
            logInfo("DB Update completed. Rows affected: " + rows);

        } catch (Exception e) {
            logError("DB Error (saveRetrieveId): " + e.getMessage());
        }
    }


    // =====================================================
    // UTIL
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
        InputStream is = (conn.getResponseCode() >= 200 && conn.getResponseCode() < 300)
                ? conn.getInputStream()
                : conn.getErrorStream();

        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sb = new StringBuilder();

        String line;
        while ((line = br.readLine()) != null)
            sb.append(line);

        br.close();
        return sb.toString();
    }

    // SSL DISABLE (as before)
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

    // Logging
    private void logInfo(String msg) { System.out.println(formatLogMessage("INFO", msg)); }
    private void logDebug(String msg) { System.out.println(formatLogMessage("DEBUG", msg)); }
    private void logSuccess(String msg) { System.out.println(formatLogMessage("SUCCESS", msg)); }
    private void logError(String msg) { System.err.println(formatLogMessage("ERROR", msg)); }
    private void logWarn(String msg) { System.out.println(formatLogMessage("WARN", msg)); }

    private String formatLogMessage(String level, String msg) {
        return String.format("[%s] [%s] %s",
                LocalDateTime.now().format(TIME_FORMATTER),
                level,
                msg);
    }
}
