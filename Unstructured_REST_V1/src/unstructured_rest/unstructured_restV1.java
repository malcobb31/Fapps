package unstructured_rest;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.*;
import org.json.JSONObject;

public  class unstructured_restV1  {

    private static final String LOG_INFO = "[INFO]";
    private static final String LOG_DEBUG = "[DEBUG]";
    private static final String LOG_SUCCESS = "[SUCCESS]";
    private static final String LOG_ERROR = "[ERROR]";
    private static final String LOG_WARN = "[WARN]";
    private static final DateTimeFormatter TF =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // =========================================================
    // MAIN FLOW
    // =========================================================

    public void run(String csvPath) throws Exception {

        // ================= HARD-CODED CONFIG =================
        final String baseUrl = "https://172.20.161.214/restapi";
        final int objectStoragePk = 5;
        final int bucketId = 14;

        final String dbName = "ADA";
        final String user   = "root";
        final String pass   = "ATempo@123";
        // =====================================================

        final int THREAD_COUNT = 1;
        final int BATCH_SIZE = 500;

        logInfo("========================================");
        logInfo("MIRIA BUCKET FOLDER CREATION STARTED");
        logInfo("CSV Path: " + csvPath);
        logInfo("Threads: " + THREAD_COUNT);
        logInfo("Batch Size: " + BATCH_SIZE);
        logInfo("========================================");

        disableSSL();

        final String token = authenticate(baseUrl, dbName, user, pass);

        List<String> folders = readCsv(csvPath);
        logInfo("Total folders to create: " + folders.size());

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

        List<List<String>> batches = splitIntoBatches(folders, BATCH_SIZE);

        for (List<String> batch : batches) {
            executor.submit(new FolderWorker(
                    baseUrl, token,
                    objectStoragePk, bucketId,
                    batch
            ));
        }

        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.HOURS);

        logSuccess("Folder creation process completed");
    }

    class FolderWorker implements Runnable {

        private final String baseUrl;
        private final String token;
        private final int objectStoragePk;
        private final int bucketId;
        private final List<String> folders;

        FolderWorker(String baseUrl, String token,
                     int objectStoragePk, int bucketId,
                     List<String> folders) {

            this.baseUrl = baseUrl;
            this.token = token;
            this.objectStoragePk = objectStoragePk;
            this.bucketId = bucketId;
            this.folders = folders;
        }

        @Override
        public void run() {

            int browserHandle = -1;

            try {
                browserHandle = getBucketBrowserHandle(
                        baseUrl, token, objectStoragePk, bucketId
                );

                for (String folder : folders) {
                    try {
                        createFolder(
                            baseUrl, token,
                            objectStoragePk, bucketId,
                            browserHandle, folder
                        );
                    } catch (Exception e) {
                        logWarn("Skipped folder [" + folder + "] : " + e.getMessage());
                    }
                }

            } catch (Exception e) {
                logError("Worker failed: " + e.getMessage());

            } finally {
                if (browserHandle != -1) {
                    closeBrowser(
                        baseUrl, token,
                        objectStoragePk, bucketId,
                        browserHandle
                    );
                }
            }
        }
    }

    private List<List<String>> splitIntoBatches(List<String> list, int size) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            batches.add(list.subList(i, Math.min(i + size, list.size())));
        }
        return batches;
    }



    private void disableSSL() throws Exception {
        TrustManager[] trustAll = new TrustManager[]{
            new X509TrustManager() {
                public java.security.cert.X509Certificate[] getAcceptedIssuers(){ return null; }
                public void checkClientTrusted(java.security.cert.X509Certificate[] c, String a){}
                public void checkServerTrusted(java.security.cert.X509Certificate[] c, String a){}
            }
        };

        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAll, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
        HttpsURLConnection.setDefaultHostnameVerifier((h, s) -> true);

        logDebug("SSL verification disabled");
    }

    // =========================================================
    // AUTH (UNCHANGED STYLE)
    // =========================================================

    public String authenticate(String baseUrl, String dbName,
                               String username, String password) throws Exception {

        String authUrl = baseUrl + "/auth/token/";
        logInfo("Authenticating user: " + username);

        HttpsURLConnection conn = (HttpsURLConnection) new URL(authUrl).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setDoOutput(true);

        JSONObject body = new JSONObject();
        body.put("dbName", dbName);
        body.put("name", username);
        body.put("password", password);
        body.put("superUser", true);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.toString().getBytes(StandardCharsets.UTF_8));
        }

        String response = readResponse(conn);
        int code = conn.getResponseCode();

        if (code == 200 || code == 201) {
            JSONObject json = new JSONObject(response);
            logSuccess("Authentication successful");
            return json.getString("access");
        }

        throw new Exception("Authentication failed: " + response);
    }

    // =========================================================
    // BROWSER HANDLE (BUCKET)
    // =========================================================

    private int getBucketBrowserHandle(String baseUrl, String token,
                                       int objectStoragePk, int bucketId) throws Exception {

        String url = baseUrl + "/infrastructure/object-storages/"
                + objectStoragePk + "/buckets/" + bucketId + "/browser/";

        HttpsURLConnection conn = (HttpsURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        conn.setRequestProperty("Authorization", "Bearer " + token);

        String response = readResponse(conn);

        JSONObject json = new JSONObject(response);
        int handle = json.getInt("browserHandle");

        logSuccess("Bucket browser handle obtained: " + handle);
        return handle;
    }

    // =========================================================
    // CREATE FOLDER
    // =========================================================

    private void createFolder(String baseUrl, String token,
                              int objectStoragePk, int bucketId,
                              int browserHandle, String folder) throws Exception {

        String url = baseUrl + "/infrastructure/object-storages/"
                + objectStoragePk + "/buckets/" + bucketId + "/browse/folder/";

        HttpsURLConnection conn = (HttpsURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + token);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        JSONObject payload = new JSONObject();
        payload.put("browserHandle", browserHandle);
        payload.put("path", "/" + folder);

        try (OutputStream os = conn.getOutputStream()) {
            os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
        }

        String response = readResponse(conn);
        int code = conn.getResponseCode();

        if (code == 200 || code == 201) {
            logSuccess("Folder created: /" + folder);
        } else if (code == 409) {
            logWarn("Folder already exists: /" + folder);
        } else {
            throw new Exception("Folder creation failed: " + response);
        }
    }

    // =========================================================
    // CLOSE BROWSER
    // =========================================================

    private void closeBrowser(String baseUrl, String token,
                              int objectStoragePk, int bucketId,
                              int browserHandle) {

        try {
            String url = baseUrl + "/infrastructure/object-storages/"
                    + objectStoragePk + "/buckets/" + bucketId + "/browse/close/";

            HttpsURLConnection conn = (HttpsURLConnection) new URL(url).openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Bearer " + token);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            JSONObject payload = new JSONObject();
            payload.put("browserHandle", browserHandle);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
            }

            readResponse(conn);
            logDebug("Browser session closed");

        } catch (Exception e) {
            logWarn("Failed to close browser session: " + e.getMessage());
        }
    }

    // =========================================================
    // CSV
    // =========================================================

    private List<String> readCsv(String path) throws Exception {
        List<String> list = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(path));
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            if (!line.isEmpty()) list.add(line);
        }
        br.close();
        return list;
    }

    // =========================================================
    // IO + LOGGING
    // =========================================================

    private String readResponse(HttpsURLConnection conn) throws Exception {
        InputStream is = conn.getResponseCode() >= 400
                ? conn.getErrorStream()
                : conn.getInputStream();
        return readStream(is);
    }

    private String readStream(InputStream is) throws Exception {
        if (is == null) return "";
        BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) sb.append(line);
        return sb.toString();
    }

    private void logInfo(String m){ System.out.println(ts()+" "+LOG_INFO+" "+m); }
    private void logDebug(String m){ System.out.println(ts()+" "+LOG_DEBUG+" "+m); }
    private void logSuccess(String m){ System.out.println(ts()+" "+LOG_SUCCESS+" "+m); }
    private void logError(String m){ System.err.println(ts()+" "+LOG_ERROR+" "+m); }
    private void logWarn(String m){ System.out.println(ts()+" "+LOG_WARN+" "+m); }

    private String ts() {
        return "[" + LocalDateTime.now().format(TF) + "]";
    }
}
