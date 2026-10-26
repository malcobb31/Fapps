
package restoration_parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.json.JSONObject;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import api_parser.PrintUtilMsg;

public class restore_methods {
	static PrintUtilMsg pu = new PrintUtilMsg();

	public static String currentDate() {
		String current_date = "";
		try {
			SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd");
			Calendar c1 = Calendar.getInstance();
			Date date = new Date();
			c1.setTime(date);
			current_date = sdf_date.format(c1.getTime());
		} catch (Exception var6) {
//			System.out.println("currentTime:: Exception while computing current date");
//			var6.printStackTrace();
		}
		return current_date;
	}

	private static String formatDateForMySQL(String rawDate) {
		if (rawDate == null || rawDate.trim().isEmpty()) {
			return null;
		}
		try {
			OffsetDateTime odt = OffsetDateTime.parse(rawDate);
			return odt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
		} catch (Exception e) {
//			pu.sop("Datetime parse failed for: " + rawDate, e.toString());
			return null;
		}
	}

	// Main method to process Veeam session data
	public static boolean processVeeamSessionData(String baseUrl, String username, String password, String eventTable,
			String db_URL, String db_User, String db_Pass, String db_Name) {
		boolean status = false;
		try {
//			pu.sop("processVeeamSessionData", "Starting Veeam session data processing");

			// Step 1: Authenticate and get token
			String token = authenticateAndGetToken(baseUrl, username, password);

			if (token != null) {
//				pu.sop("Token Received: ", token);

				// Step 2: Check if table exists
				if (checkTableExists(eventTable, db_URL, db_User, db_Pass)) {
					// Step 3: Get session data with FileLevelRestore filter
					getVeeamSessionData(baseUrl, token, eventTable, db_URL, db_User, db_Pass, db_Name);
					status = true;
					System.out.println("Success");
				} else {
//					pu.sop("Table Check", "Table " + eventTable + " does not exist. No table available.");
//					System.out.println("No table available - Table " + eventTable + " does not exist");
					System.out.println("Fail");
				}
			} else {
//				pu.sop("Authentication", "Failed to get token");
				System.out.println("Fail");
			}

		} catch (Exception e) {
//			pu.sop("Exception", "Exception in processVeeamSessionData: " + e.toString());
//			e.printStackTrace();
			System.out.println("Fail");
		}
		
		return status;
		
	}

	// Authentication method for OAuth2 token
	// Authentication method for Veeam v1.x Session Manager
	private static String authenticateAndGetToken(String baseUrl, String username, String password) {
		try {
//			pu.sop("authenticateAndGetToken", "Starting authentication");

			// Correct endpoint for OAuth2 authentication
			String apiUrl = baseUrl + "/api/oauth2/token";
//			pu.sop("apiUrl", apiUrl);

			disableSSLCertification();

			URL url = new URL(apiUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("accept", "application/json");
			conn.setRequestProperty("x-api-version", "1.2-rev0");
			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			conn.setDoOutput(true);

			// Build form params just like curl
			String urlParams = "grant_type=password" + "&username=" + URLEncoder.encode(username, "UTF-8")
					+ "&password=" + URLEncoder.encode(password, "UTF-8")
					+ "&refresh_token=&code=&use_short_term_refresh=&vbr_token=";

			try (OutputStream os = conn.getOutputStream()) {
				byte[] input = urlParams.getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
			}

			int responseCode = conn.getResponseCode();
//			pu.sop("Response Code", String.valueOf(responseCode));

			// Read response (success or error)
			try (BufferedReader br = new BufferedReader(new InputStreamReader(
					responseCode >= 200 && responseCode < 300 ? conn.getInputStream() : conn.getErrorStream(),
					StandardCharsets.UTF_8))) {

				StringBuilder response = new StringBuilder();
				String line;
				while ((line = br.readLine()) != null) {
					response.append(line.trim());
				}

				if (responseCode == 200) {
//					pu.sop("Authentication Response Body", response.toString());

					// Extract access_token from JSON
					com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
					com.fasterxml.jackson.databind.JsonNode jsonNode = mapper.readTree(response.toString());
					String accessToken = jsonNode.has("access_token") ? jsonNode.get("access_token").asText() : null;

					if (accessToken != null && !accessToken.isEmpty()) {
//						pu.sop("Access Token", accessToken);
						return accessToken;
					}
				} else {
//					pu.sop("Authentication failed. Response Body", response.toString());
				}
			}

			conn.disconnect();

		} catch (Exception e) {
//			e.printStackTrace();
//			pu.sop("Error in authentication", e.getMessage());
		}
		return null;
	}

	// Check if table exists
	private static boolean checkTableExists(String tableName, String db_URL, String db_User, String db_Pass) {
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		boolean tableExists = false;

		try {
			String query = "SHOW TABLES LIKE '" + tableName + "'";
//			pu.sop("Table Check Query", query);

			conn = new api_parser.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);

			if (rs.next()) {
				tableExists = true;
//				pu.sop("Table Status", "Table " + tableName + " exists");
			} else {
//				pu.sop("Table Status", "Table " + tableName + " does not exist");
			}

		} catch (Exception e) {
//			pu.sop("Exception in checkTableExists", e.toString());
//			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (Exception e) {
//				e.printStackTrace();
			}
		}
		return tableExists;
	}

	// Get Veeam session data with FileLevelRestore filter
	private static void getVeeamSessionData(String baseUrl, String token, String eventTable, String db_URL,
			String db_User, String db_Pass, String db_Name) {
		try {
			String apiUrl = baseUrl + "/api/v1/sessions?typeFilter=FileLevelRestore";

//			pu.sop("Veeam Session API URL", apiUrl);

			HttpURLConnection conn = (HttpURLConnection) new URL(apiUrl).openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("accept", "application/json");
			conn.setRequestProperty("x-api-version", "1.2-rev0");
			conn.setRequestProperty("Authorization", "Bearer " + token);

			int responseCode = conn.getResponseCode();
//			pu.sop("Veeam Session Response Code", String.valueOf(responseCode));

			if (responseCode == 200) {
				BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				StringBuilder response = new StringBuilder();
				String line;
				while ((line = in.readLine()) != null) {
					response.append(line);
				}
				in.close();

//				pu.sop("Veeam Session Response", response.toString());

				// Process and insert the session data
				processAndInsertSessionData(response.toString(), eventTable, db_URL, db_User, db_Pass, db_Name);

			} else {
//				pu.sop("Failed to retrieve session data. Response Code", String.valueOf(responseCode));

				// Read error response
				BufferedReader errorReader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
				StringBuilder errorResponse = new StringBuilder();
				String errorLine;
				while ((errorLine = errorReader.readLine()) != null) {
					errorResponse.append(errorLine);
				}
				errorReader.close();
//				pu.sop("Error Response", errorResponse.toString());
			}
			conn.disconnect();

		} catch (Exception e) {
//			e.printStackTrace();
//			pu.sop("Error in getVeeamSessionData", e.getMessage());
		}
	}

	// Process and insert session data
	// Process and insert session data with clear and refresh logic
	public static void processAndInsertSessionData(String jsonString, String eventTable, String db_URL, String db_User,
			String db_Pass, String db_Name) throws IOException {
//		pu.sop("processAndInsertSessionData", "Processing Veeam session data with refresh logic");

		Connection conn = null;

		try {
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode rootNode = objectMapper.readTree(jsonString);

			JsonNode sessionsArray = rootNode.get("data");
			if (sessionsArray == null) {
				sessionsArray = rootNode; // In case response is directly in an array
			}

			conn = new api_parser.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
			conn.setAutoCommit(false);

		
			// Step 1: Truncate table (clear all data)
			try (Statement stmt = conn.createStatement()) {
			    stmt.executeUpdate("TRUNCATE TABLE " + eventTable);
			
//				pu.sop("Data Cleanup", "Deleted " + deletedRows + " existing records for date: " + currentDate);
			}

			// Step 2: Insert fresh data
			String insertQuery = "INSERT INTO " + eventTable
					+ " (platform_name, session_name, creation_time, end_time, "
					+ "result_status, processed_at, update_date,enterprizeId,rto"
					+ ") VALUES (?,?,?,?,?,?,?,?,?)";

			int recordCount = 0;
			try (PreparedStatement psInsert = conn.prepareStatement(insertQuery)) {
			    if (sessionsArray != null && sessionsArray.isArray()) {
			        for (JsonNode session : sessionsArray) {
			            try {
			                String platformName = getJsonValue(session, "platformName");
			                String sessionName = getJsonValue(session, "name");
			                String creationTime = getJsonValue(session, "creationTime");
			                String endTime = getJsonValue(session, "endTime");
			                String enterprizeid = "20";
			                // Handle nested result JSON
			                String resultStatus = "Unknown";
			                JsonNode resultNode = session.get("result");
			                if (resultNode != null && resultNode.has("result")) {
			                    resultStatus = resultNode.get("result").asText();
			                }

			                
			                String timePeriod = "0";
			                
			                try {
			                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			                    Date start = sdf.parse(formatDateForMySQL(creationTime));
			                    Date end = sdf.parse(formatDateForMySQL(endTime));

			                    long diffInSeconds = (end.getTime() - start.getTime()) / 1000;

			                    long hours = diffInSeconds / 3600;
			                    long minutes = (diffInSeconds % 3600) / 60;
			                    long seconds = diffInSeconds % 60;

			                    StringBuilder sb = new StringBuilder();
			                    if (hours > 0) sb.append(hours).append(" Hrs ");
			                    if (minutes > 0) sb.append(minutes).append(" Min ");
			                    sb.append(String.format("%02d Sec", seconds));

			                    timePeriod = sb.toString().trim();
			                } catch (Exception pe) {
			                    timePeriod = "0 Sec";
			                }
			               


			                // Defaults
			                platformName = platformName.isEmpty() ? "N/A" : platformName;
			                sessionName = sessionName.isEmpty() ? "N/A" : sessionName;
			                resultStatus = resultStatus.isEmpty() ? "Unknown" : resultStatus;

			                // Current timestamp
			                String currentTimestamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
			                        .format(new java.util.Date());

			                // Insert values
			             // Insert values
			                psInsert.setString(1, platformName);
			                psInsert.setString(2, sessionName);
			                psInsert.setString(3, formatDateForMySQL(creationTime));
			                psInsert.setString(4, formatDateForMySQL(endTime));
			                psInsert.setString(5, resultStatus);
			                psInsert.setString(6, currentTimestamp);
			                psInsert.setString(7, currentTimestamp);
			                psInsert.setString(8, enterprizeid);

			                // Set timePeriod only for Success, else keep empty
			                if ("Success".equalsIgnoreCase(resultStatus)) {
			                    psInsert.setString(9, timePeriod);
			                } else {
			                    psInsert.setString(9, "");
			                }

			                
			                psInsert.addBatch();
			                recordCount++;

			            } catch (Exception e) {
			                continue;
			            }
			        }

			        if (recordCount > 0) {
			            psInsert.executeBatch();
			            conn.commit();
			        }
			    }
			}

			// Step 3: Generate JSON file
			generateJsonFile(jsonString, recordCount);

		} catch (Exception e) {
			try {
				if (conn != null)
					conn.rollback();
			} catch (Exception rollbackEx) {
//				pu.sop("Rollback Exception", rollbackEx.toString());
			}
//			pu.sop("EXCEPTION: processAndInsertSessionData", e.toString());
//			e.printStackTrace();
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (Exception e) {
//				e.printStackTrace();
			}
		}
	}

	// Generate JSON file with processed data
	private static void generateJsonFile(String originalJson, int recordCount) {
	    try {
	        ObjectMapper objectMapper = new ObjectMapper();
	        JsonNode rootNode = objectMapper.readTree(originalJson);

	        // Create output JSON structure using Jackson
	        ObjectNode outputJson = objectMapper.createObjectNode();
	        outputJson.put("timestamp", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date()));
	        outputJson.put("totalRecords", recordCount);
	        outputJson.set("processedData", rootNode); // embed JSON object, not string

	        // Save to file
	        String fileName = "veeam_session_data_"
	                + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date()) + ".json";

	        objectMapper.writerWithDefaultPrettyPrinter().writeValue(new File(fileName), outputJson);

//	        pu.sop("JSON File Generated", "File saved as: " + fileName);

	    } catch (Exception e) {
//	        pu.sop("Error generating JSON file", e.toString());
	    }
	}

	// Helper method to safely get JSON values
	private static String getJsonValue(JsonNode node, String fieldName) {
		try {
			JsonNode fieldNode = node.get(fieldName);
			if (fieldNode != null && !fieldNode.isNull()) {
				return fieldNode.asText();
			}
		} catch (Exception e) {
//			pu.sop("Error getting field: " + fieldName, e.toString());
		}
		return ""; // Return empty string if field is null or doesn't exist
	}

	// SSL Certificate disabling method
	private static void disableSSLCertification() {
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
			public X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
			}

			public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
			}
		} };

		try {
			SSLContext sc = SSLContext.getInstance("TLS");
			sc.init(null, trustAllCerts, new SecureRandom());
			HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		} catch (Exception e) {
			// Ignore
		}

		HostnameVerifier allHostsValid = new HostnameVerifier() {
			public boolean verify(String hostname, SSLSession session) {
				return true;
			}
		};

		HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
	}

	// Usage example/main method
	public static void main(String[] args) {

	}
}
