package rest_api_restoration;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class rest_api_methods {

    private static final String BASE_URL = "https://poc.itsimple.in/restapi";

    /* ===========================================================
       LOGGING UTILITIES
     =========================================================== */
    private void logInfo(String msg) {
//        System.out.println("[" + timestamp() + "] [INFO] " + msg);
    }

    private void logDebug(String msg) {
//        System.out.println("[" + timestamp() + "] [DEBUG] " + msg);
    }

    private void logError(String msg) {
//        System.err.println("[" + timestamp() + "] [ERROR] " + msg);
    }

    private String timestamp() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }

    /* ===========================================================
       AUTHENTICATION METHOD (DO NOT CHANGE)
     =========================================================== */
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

            JSONObject body = new JSONObject();
            body.put("dbName", dbName);
            body.put("name", username);
            body.put("password", password);
            body.put("superUser", true);

            logDebug("Request payload: " + body.toString(2));

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.toString().getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = conn.getResponseCode();
            logDebug("Authentication response code: " + responseCode);

            InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream() : conn.getErrorStream();
            String responseText = readStream(is);
            logDebug("Authentication response: " + responseText);

            if (responseCode == 200 || responseCode == 201) {
                JSONObject jsonResponse = new JSONObject(responseText);
                if (jsonResponse.has("access")) {
                    String token = jsonResponse.getString("access");
                    logInfo("Authentication successful");
                    return token;
                } else {
                    logError("Access token not found in response");
                    throw new Exception("Access token not found");
                }
            } else {
                throw new Exception("Authentication failed: " + responseText);
            }
        } catch (Exception e) {
            logError("Exception during authentication: " + e.getMessage());
            throw e;
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    private static String readStream(InputStream is) throws IOException {
        if (is == null) return "";
        StringBuilder sb = new StringBuilder();
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
        }
        return sb.toString();
    }

    /* ===========================================================
       CORE METHOD: BROWSE REPOSITORY FOLDER
     =========================================================== */
    public void run(String enterpriseName, String username, String folderPath) throws Exception {
        disableSSL();

        logInfo("========================================");
        logInfo("Starting Repository Browse Process");
        logInfo("Enterprise: " + enterpriseName);
        logInfo("Username: " + username);
        logInfo("Folder Path: " + folderPath);
        logInfo("========================================");

        HttpURLConnection conn = null;

        try {
            // STEP 1: AUTHENTICATE
            String token = authenticate(BASE_URL, "ADA", "root", "ATempo@123");
            logInfo("Authentication successful. Token obtained.");

            // STEP 2: GET repositories
            String repoUrl = BASE_URL + "/datamanagement/repositories/?limit=1000";
            logDebug("Fetching repository list: " + repoUrl);

            conn = createConnection(repoUrl, "GET");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("Accept", "application/json");

            int repoResponseCode = conn.getResponseCode();
            String repoResponse = readResponse(conn);
            logDebug("Repository response code: " + repoResponseCode);
            logDebug("Repository raw response: " + repoResponse);

            if (repoResponseCode != 200) {
                logError("Failed to fetch repositories. Code: " + repoResponseCode);
                return;
            }

            // FIXED: Parse as JSONObject, not JSONArray
            JSONObject repoObj = new JSONObject(repoResponse);
            JSONArray repoArray = null;

            if (repoObj.has("results")) {
                repoArray = repoObj.getJSONArray("results");
            } else if (repoObj.has("items")) {
                repoArray = repoObj.getJSONArray("items");
            } else {
                logError("Unexpected response structure: " + repoObj.toString(2));
                return;
            }

            int repoId = -1;
            for (int i = 0; i < repoArray.length(); i++) {
                JSONObject repo = repoArray.getJSONObject(i);
                if (repo.has("name") && repo.getString("name").equalsIgnoreCase(enterpriseName)) {
                    repoId = repo.getInt("id");
                    logInfo("Repository found: " + enterpriseName + " (ID: " + repoId + ")");
                    break;
                }
            }

            if (repoId == -1) {
                logError("Repository not found for enterprise: " + enterpriseName);
                return;
            }

            // STEP 3: GET browser handle
            String browserUrl = BASE_URL + "/datamanagement/repositories/" + repoId + "/browser/";
            logDebug("Fetching browser handle from: " + browserUrl);

            conn = createConnection(browserUrl, "GET");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("Accept", "application/json");

            int browserResponseCode = conn.getResponseCode();
            String browserResponse = readResponse(conn);
            logDebug("Browser response code: " + browserResponseCode);
            logDebug("Browser response: " + browserResponse);
            

            if (browserResponseCode != 200) {
                logError("Failed to fetch browser handle. Response: " + browserResponse);
                return;
            }

            JSONObject browserObj = new JSONObject(browserResponse);
            int browserHandle = browserObj.getInt("browserHandle");
            logInfo("Browser Handle received: " + browserHandle);

            // STEP 4: POST browse
            String browseUrl = BASE_URL + "/datamanagement/repositories/" + repoId + "/browse/";
            logDebug("Posting browse request to: " + browseUrl);

            conn = createConnection(browseUrl, "POST");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Accept", "application/json");

            String fullPath ="";
            JSONObject browsePayload = new JSONObject();
            browsePayload.put("browserHandle", browserHandle);
            if(folderPath == null || folderPath.isEmpty())
            {
            	fullPath = "/" + username + "/" ;
            }
            else
            {
            	fullPath = folderPath;
            }
            browsePayload.put("path", fullPath);

            logDebug("Browse payload: " + browsePayload.toString());
            writeRequestBody(conn, browsePayload.toString());

            int browseResponseCode = conn.getResponseCode();
            String browseResponse = readResponse(conn);
//            System.out.println(browserResponse);
            logDebug("Browse API response code: " + browseResponseCode);

            if (browseResponseCode == 200 || browseResponseCode == 201) {
                logInfo("Browse request successful!");
                logDebug("Browse Response: " + browseResponse);
                System.out.println(browseResponse);
            } else {
                logError("Browse request failed with code: " + browseResponseCode);
                logDebug("Response: " + browseResponse);
            }

        } catch (Exception e) {
            logError("Exception during repository browsing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (conn != null) conn.disconnect();
        }

        logInfo("========================================");
        logInfo("Repository Browse Process Completed");
        logInfo("========================================");
    }

    /* ===========================================================
       HELPER METHODS
     =========================================================== */

    private void disableSSL() throws Exception {
        try {
            TrustManager[] trustAllCerts = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers() { return null; }
                    public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                    public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {}
                }
            };

            SSLContext sc = SSLContext.getInstance("SSL");
            sc.init(null, trustAllCerts, new java.security.SecureRandom());
            HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
            HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);

            logDebug("SSL verification disabled");
        } catch (Exception e) {
            logError("Failed to disable SSL: " + e.getMessage());
            throw e;
        }
    }

    private HttpURLConnection createConnection(String urlStr, String method) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod(method);
        conn.setDoInput(true);
        conn.setDoOutput(method.equals("POST") || method.equals("PUT"));
        conn.setConnectTimeout(15000);
        conn.setReadTimeout(15000);
        return conn;
    }

    private void writeRequestBody(HttpURLConnection conn, String body) throws IOException {
        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = body.getBytes("utf-8");
            os.write(input, 0, input.length);
        }
    }

    private String readResponse(HttpURLConnection conn) throws IOException {
        InputStream is = conn.getResponseCode() >= 400 ? conn.getErrorStream() : conn.getInputStream();
        if (is == null) return "";
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) response.append(line);
        return response.toString();
    }

    /* ===========================================================
       MAIN (FOR TESTING)
     =========================================================== */
    public static void main(String[] args) throws Exception {
    }
}
