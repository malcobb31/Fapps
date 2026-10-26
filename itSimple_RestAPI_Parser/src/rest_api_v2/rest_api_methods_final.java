
package rest_api_v2;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

import javax.net.ssl.*;
import org.json.*;

public class rest_api_methods_final {
	private static final String BASE = "https://poc.itsimple.in/restapi";

	public void run(String enterpriseName, String username, String password, String confirmPassword, String accessKey,
			String secretKey, String bucketName, String userQuota) throws Exception {

		disableSSL();

		// Step 1: Authenticate
		String token = authenticate(BASE, "ADA", "root", "ATempo@123");

		System.out.println("Access Token: " + token);

		// Step 2: Enterprise repository
		int enterpriseId = handleEnterprise(enterpriseName, username, token);

		// Step 3: Enterprise Folder Creation
		createEnterpriseFolder(enterpriseId, enterpriseName, username, token);

		// Step 4: Archival Project
		createProject(enterpriseName, token);

		// Step 5: Bucket
		handleBucket(enterpriseName, username, password, bucketName, accessKey, secretKey, userQuota, token);

		// Step 6: Agent Folder
		createAgentFolder(enterpriseName, token);
		
		

		// Step 7: Storage Container
		createContainer(enterpriseName, token);

		// Step 8: Policy
		createPolicy(enterpriseName, token);
	}

	private void disableSSL() throws Exception {
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
			}

			public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
			}
		} };
		SSLContext sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, new java.security.SecureRandom());
		HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
	}

	public String authenticate(String baseUrl, String dbName, String username, String password) throws Exception {
		String authUrl = baseUrl + "/auth/token/";
		System.out.println("[INFO] Authenticating via: " + authUrl);

		URL url = new URL(authUrl);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);

		// Set proper JSON headers
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("Accept", "application/json");

		// Build JSON body
		JSONObject body = new JSONObject();
		body.put("dbName", dbName);
		body.put("name", username);
		body.put("password", password);
		body.put("superUser", true);

		// Send JSON body
		try (OutputStream os = conn.getOutputStream()) {
			os.write(body.toString().getBytes(StandardCharsets.UTF_8));
		}

		int responseCode = conn.getResponseCode();
		System.out.println("[INFO] Auth Response Code: " + responseCode);

		InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream() : conn.getErrorStream();

		String responseText = readStream(is);
		System.out.println("[DEBUG] Auth Response: " + responseText);

		if (!responseText.trim().startsWith("{")) {
			System.err.println("[ERROR] Non-JSON auth response: " + responseText);
			throw new Exception("Server did not return JSON");
		}

		JSONObject jsonResponse = new JSONObject(responseText);
		if (jsonResponse.has("access")) {
			return jsonResponse.getString("access");
		} else {
			System.err.println("[ERROR] Auth failed: " + responseText);
			return null; // or throw Exception
		}

	}

	private String readResponse(HttpsURLConnection conn) throws IOException {
		InputStream is;
		int code = conn.getResponseCode();
		System.out.println("Response code:" + code);
		if (code >= 400) {
			is = conn.getErrorStream(); // read server error message
		} else {
			is = conn.getInputStream();
		}

		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null)
			sb.append(line);
		br.close();
		return sb.toString();
	}

	public int handleEnterprise(String enterpriseName, String username, String token) throws Exception {
		if (rest_api_DBConnection.enterpriseExists(enterpriseName)) {
			System.out.println("Enterprise already exists: " + enterpriseName);

			return rest_api_DBConnection.getEnterpriseId(enterpriseName);
		}

		URL url = new URL("https://poc.itsimple.in/restapi/datamanagement/repositories/");
		HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Authorization", "Bearer " + token);
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("Accept", "application/json");
		conn.setDoOutput(true);
		long Vol = 10737418240L;
		// Prepare JSON payload
		JSONObject payload = new JSONObject();
		payload.put("name", enterpriseName);
		payload.put("mode", "archiving");
		payload.put("comment", enterpriseName);
		payload.put("volumeQuota", Vol); // 10GB in bytes

		JSONObject owner = new JSONObject();
		owner.put("name", "root");
		payload.put("owner", owner);

		JSONObject organization = new JSONObject();
		organization.put("name", "_Archiving");
		payload.put("organization", organization);

		// Send request
		try (OutputStream os = conn.getOutputStream()) {
			os.write(payload.toString().getBytes("UTF-8"));
			os.flush();
		}

		int responseCode = conn.getResponseCode();
		String response = readResponse(conn); // Ensure readResponse handles
												// error stream if code != 200
		System.out.println("[DEBUG] Response (" + responseCode + "): " + response);

		if (responseCode != 201) {
			throw new RuntimeException(
					"Failed to create enterprise. Response code: " + responseCode + ", Body: " + response);
		}

		// Parse JSON safely
		JSONObject json = new JSONObject(response);
		int enterpriseId = json.has("id") ? json.getInt("id") : -1;

		rest_api_DBConnection.insertEnterprise(enterpriseName, enterpriseId, "", "");
		System.out.println("Created enterprise: " + enterpriseName + " with ID: " + enterpriseId);

		return enterpriseId;
	}

	public void createEnterpriseFolder(int repositoryId, String enterpriseName, String username, String token)
			throws Exception {
		System.out.println("Creating repository folder for enterprise: " + enterpriseName + " (Repository ID: "
				+ repositoryId + ")");

		// Step 1: GET browser handle
		String browserUrl = BASE + "/datamanagement/repositories/" + repositoryId + "/browser/?format=json";
		URL url = new URL(browserUrl);
		HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Authorization", "Bearer " + token);
		conn.setRequestProperty("Accept", "application/json");

		String browserResponse = readResponse(conn);
		int responseCode = conn.getResponseCode();

		if (responseCode != 200) {
			System.out.println("=== GET browser failed ===");
			System.out.println(browserResponse);
			throw new RuntimeException("Failed to get repository browser handle. Code: " + responseCode);
		}

		if (!browserResponse.trim().startsWith("{")) {
			throw new RuntimeException("Non-JSON response from repository browser: "
					+ browserResponse.substring(0, Math.min(browserResponse.length(), 200)));
		}

		JSONObject browserJson = new JSONObject(browserResponse);
		int browserHandle = browserJson.getInt("browserHandle");
		System.out.println("[INFO] Retrieved browserHandle: " + browserHandle);

		// Step 2: POST to create folder
		String folderUrl = BASE + "/datamanagement/repositories/" + repositoryId + "/browse/folder/";
		url = new URL(folderUrl);
		conn = (HttpsURLConnection) url.openConnection();
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Authorization", "Bearer " + token);
		conn.setRequestProperty("Accept", "application/json");
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setDoOutput(true);

		JSONObject payload = new JSONObject();
		payload.put("browserHandle", browserHandle);
		payload.put("path", "/" + enterpriseName + "-" + username);

		try (OutputStream os = conn.getOutputStream()) {
			os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
		}

		String postResponse = readResponse(conn);
		responseCode = conn.getResponseCode();

		System.out.println("[DEBUG] Folder Creation Response (" + responseCode + "): " + postResponse);

		if (responseCode != 200 && responseCode != 201) {
			throw new RuntimeException(
					"Failed to create folder. Code: " + responseCode + ", Response: " + postResponse);
		}

		System.out.println(
				"[SUCCESS] Folder created successfully for enterprise: " + enterpriseName + " (" + username + ")");
	}

	public void handleBucket(String enterpriseName, String username, String password, String bucketNames,
			String accessKey, String secretKey, String userQuota, String token) throws Exception {

		String[] buckets = bucketNames.split(",");
		for (String bucket : buckets) {
			bucket = bucket.trim();

			// Construct SMC displayName and alias dynamically
			String displayName = enterpriseName + "-" + username + "-" + bucket + "-SourceSMC";
			String alias = enterpriseName + "-" + username + "-" + bucket + "-SourceSMCPN";
			

			System.out.println("[INFO] Creating bucket: " + bucket);

			// --- Step 1: GET existing buckets ---
			URL getUrl = new URL(BASE + "/infrastructure/object-storages/4/buckets/");
			HttpsURLConnection conn = (HttpsURLConnection) getUrl.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Authorization", "Bearer " + token);
			conn.setRequestProperty("Accept", "application/json");

			int getResponseCode = conn.getResponseCode();
			String getResponse;
			if (getResponseCode >= 200 && getResponseCode < 300) {
				getResponse = readStream(conn.getInputStream());
			} else {
				getResponse = readStream(conn.getErrorStream());
				System.err.println("[ERROR] GET Buckets failed with code: " + getResponseCode);
				System.err.println("[ERROR] Response: " + getResponse);
				conn.disconnect();
				continue;
			}
			conn.disconnect();

			JSONObject json = new JSONObject(getResponse);
			JSONArray results = json.getJSONArray("results");

			boolean exists = false;
			for (int i = 0; i < results.length(); i++) {
				JSONObject obj = results.getJSONObject(i);
				String existingDisplayName = obj.getString("displayName");
				if (existingDisplayName.equals(displayName)) {
					exists = true;
					System.out.println("[INFO] Bucket already exists: " + displayName);

					// Insert into DB anyway
					rest_api_DBConnection.insertEnterpriseUserDetail(enterpriseName, username, password, bucket,
							String.valueOf(obj.getInt("id")), userQuota, existingDisplayName,
							obj.getJSONObject("useAsSource").getString("alias"), "s3://" + bucket,
							"C:/DemoDirectory/" + bucket);
					break;
				}
			}

			if (!exists) {
				// --- Step 2: POST create bucket ---
				URL postUrl = new URL(BASE + "/infrastructure/object-storages/4/buckets/");
				conn = (HttpsURLConnection) postUrl.openConnection();
				conn.setRequestMethod("POST");
				conn.setRequestProperty("Authorization", "Bearer " + token);
				conn.setRequestProperty("Content-Type", "application/json");
				conn.setDoOutput(true);

				// --- Build payload exactly as confirmed ---
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

				System.out.println("[DEBUG] Payload being sent:\n" + payload.toString(4));

				try (OutputStream os = conn.getOutputStream()) {
					os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
				}

				int postResponseCode = conn.getResponseCode();
				String postResponse;
				if (postResponseCode >= 200 && postResponseCode < 300) {
					postResponse = readStream(conn.getInputStream());
					JSONObject respJson = new JSONObject(postResponse);
					System.out.println("[SUCCESS] Created bucket: " + respJson.getString("displayName"));

					// Insert into DB
					rest_api_DBConnection.insertEnterpriseUserDetail(enterpriseName, username, password,
							respJson.getString("name"), String.valueOf(respJson.getInt("id")), userQuota,
							respJson.getString("displayName"), respJson.getJSONObject("useAsSource").getString("alias"),
							"s3://" + respJson.getString("name"), "C:/DemoDirectory/" + bucket);
				} else {
					postResponse = readStream(conn.getErrorStream());
					System.err.println("[ERROR] Failed to create bucket. Code: " + postResponseCode + ", Response: "
							+ postResponse);
				}
				conn.disconnect();
			}
		}
	}

	

	public void createProject(String enterpriseName, String token) throws Exception {
		String url = "https://poc.itsimple.in/restapi/organizations/archiving-projects/";

		JSONObject payload = new JSONObject();
		payload.put("name", enterpriseName + "-Project");
		payload.put("comment", enterpriseName + "-Project");

		System.out.println("[INFO] Creating Archiving Project: " + enterpriseName + "-Project");
		System.out.println("[DEBUG] Payload: " + payload.toString(2));

		HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Authorization", "Bearer " + token);
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setRequestProperty("Accept", "application/json");
		conn.setDoOutput(true);

		OutputStream os = conn.getOutputStream();
		os.write(payload.toString().getBytes("UTF-8"));
		os.close();

		int responseCode = conn.getResponseCode();
		System.out.println("[INFO] Response Code: " + responseCode);

		InputStream is = (responseCode >= 200 && responseCode < 300) ? conn.getInputStream() : conn.getErrorStream();

		StringBuilder responseBuilder = new StringBuilder();
		if (is != null) {
			BufferedReader in = new BufferedReader(new InputStreamReader(is));
			String inputLine;
			while ((inputLine = in.readLine()) != null) {
				responseBuilder.append(inputLine);
			}
			in.close();
		}
		String response = responseBuilder.toString();

		System.out.println("[DEBUG] Response Body: " + response);

		if (response.trim().startsWith("{")) {
			JSONObject jsonResponse = new JSONObject(response);
			System.out.println("[SUCCESS] Archiving Project created successfully:");
			System.out.println("ID: " + jsonResponse.getInt("id"));
			System.out.println("Name: " + jsonResponse.getString("name"));
			System.out.println("Parent Org: " + jsonResponse.getJSONObject("parent").getString("name"));
		} else {
			System.out.println("[WARN] Non-JSON response:\n" + response);
		}

		conn.disconnect();
	}

	public void createAgentFolder(String enterpriseName, String token) throws Exception {
		System.out.println("Creating agent folder for " + enterpriseName);
		URL url = new URL("https://poc.itsimple.in/restapi/infrastructure/agents/2/browser/");
		HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty("Authorization", "Bearer " + token);

		String response = readResponse(conn);
		JSONObject json = new JSONObject(response);
		int browserHandle = json.getInt("browserHandle");

		// POST create folder
		url = new URL("https://poc.itsimple.in/restapi/infrastructure/agents/2/browse/folder/");
		conn = (HttpsURLConnection) url.openConnection();
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Authorization", "Bearer " + token);
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setDoOutput(true);

		JSONObject payload = new JSONObject();
		payload.put("browserHandle", browserHandle);
		payload.put("path", "\\C:\\DemoDirectory\\" + enterpriseName);

		try (OutputStream os = conn.getOutputStream()) {
			os.write(payload.toString().getBytes("UTF-8"));
		}

		
		response = readResponse(conn);
        int responseCode = conn.getResponseCode();

        System.out.println("Agent folder creation response code: " + responseCode);
        System.out.println("Agent folder creation response: " + response);
		System.out.println("Agent folder created for " + enterpriseName);
	}

	public void createContainer(String enterpriseName, String token) throws Exception {
	    System.out.println("[INFO] Creating storage container for: " + enterpriseName);

	    //  Step 1: Re-authenticate just before this call to ensure token is fresh
	    token = token.trim(); // remove accidental spaces or newline chars

	    //  Step 2: Disable SSL verification
	    disableSSL();

	    String url = "https://poc.itsimple.in/restapi/infrastructure/storage-managers/9/containers/";

	    //  Step 3: Build JSON payload
	    JSONObject payload = new JSONObject();
	    payload.put("name", enterpriseName);
	    payload.put("directory", "C:\\\\DemoDirectory\\\\" + enterpriseName);

	    System.out.println("[DEBUG] Payload being sent: " + payload.toString());

	    //  Step 4: Open connection
	    HttpsURLConnection conn = (HttpsURLConnection) new URL(url).openConnection();
	    conn.setRequestMethod("POST");
	    conn.setRequestProperty("Authorization", "Bearer " + token); // critical line
	    conn.setRequestProperty("Content-Type", "application/json");
	    conn.setRequestProperty("Accept", "application/json");
	    conn.setDoOutput(true);

	    // Step 5: Send payload
	    try (OutputStream os = conn.getOutputStream()) {
	        os.write(payload.toString().getBytes(StandardCharsets.UTF_8));
	    }

	    //  Step 6: Read response properly
	    int responseCode = conn.getResponseCode();
	    String response = readResponse(conn);

	    System.out.println("[INFO] Response Code (createContainer): " + responseCode);
	    System.out.println("[DEBUG] Response Body: " + response);

	    //  Step 7: Handle results
	    
	    if (responseCode == 200 || responseCode == 201) {
	        System.out.println("[SUCCESS] Container created successfully for: " + enterpriseName);
	    } else if (responseCode == 412) {
	        System.out.println("[INFO] Container already exists for: " + enterpriseName);
	    } else if (responseCode == 401) {
	        System.err.println("[ERROR] Unauthorized (401). Token may be invalid or expired.");
	        System.err.println("[HINT] Try regenerating token right before this call.");
	    } else {
	        System.err.println("[ERROR] Failed to create container for " + enterpriseName + ". Code: " + responseCode);
	        System.err.println("[ERROR] Body: " + response);
	    }

	    conn.disconnect();
	}



	public void createPolicy(String enterpriseName, String token) throws Exception {
		System.out.println("Creating policy for " + enterpriseName);
		URL url = new URL("https://poc.itsimple.in/restapi/datamanagement/policies/");
		HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
		conn.setRequestMethod("POST");
		conn.setRequestProperty("Authorization", "Bearer " + token);
		conn.setRequestProperty("Content-Type", "application/json");
		conn.setDoOutput(true);

		JSONObject payload = new JSONObject();

        String policyName = enterpriseName + "-Policy";
        String containerName = enterpriseName + "-TargetSMC";

        payload.put("name", policyName);
        payload.put("comment", policyName);

        JSONObject retention = new JSONObject();
        retention.put("name", "Permanent Retention");
        payload.put("retention", retention);

        payload.put("reduceImpactOnSource", false);

        // storageContainerList array
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

		try (OutputStream os = conn.getOutputStream()) {
			os.write(payload.toString().getBytes("UTF-8"));
		}

		readResponse(conn);
		System.out.println("Policy created: " + policyName);
	}

	private static String readStream(InputStream is) throws IOException {
		if (is == null)
			return "";
		BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = br.readLine()) != null)
			sb.append(line);
		br.close();
		return sb.toString();
	}
}
