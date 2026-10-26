package api_parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
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
//import java.util.ArrayList;
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
import org.omg.CORBA.portable.InputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import api_parser.PrintUtilMsg;

//import api_parser.SNORangeFinder;
//import api_parser.PrintUtilMsg;

public class api_methods {
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
			System.out.println("currentTime:: Exception while computing current date");
			var6.printStackTrace();
		}
		return current_date;
	}

	private static String formatDateForMySQL(String rawDate) {
		if (rawDate == null || rawDate.trim().isEmpty()) {
			return null;
		}
		try {
			OffsetDateTime odt = OffsetDateTime.parse(rawDate);
			// MySQL DATETIME(6) format
			return odt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"));
		} catch (Exception e) {
			pu.sop("Datetime parse failed for: " + rawDate, e.toString());
			return null;
		}
	}

	// Main method to process backup server jobs
	public static boolean processBackupServerJobs(String baseUrl, String username, String password, String eventTable,
			String db_URL, String db_User, String db_Pass, String db_Name) {
		boolean status = false;
		try {
			pu.sop("processBackupServerJobs", "Starting backup server jobs processing");

			// Step 1: Authenticate and get token
			String token = authenticateAndGetToken(baseUrl, username, password);

			if (token != null) {
				pu.sop("Token Received: ", token);

				// Step 2: Check if table exists
				if (checkTableExists(eventTable, db_URL, db_User, db_Pass)) {
					// Step 3: Get backup server jobs data
					getBackupServerJobsData(baseUrl, token, eventTable, db_URL, db_User, db_Pass, db_Name);
					status = true;
				} else {
					pu.sop("Table Check", "Table " + eventTable + " does not exist. No table available.");
					System.out.println("No table available - Table " + eventTable + " does not exist");
				}
			} else {
				pu.sop("Authentication", "Failed to get token");
			}

		} catch (Exception e) {
			pu.sop("Exception", "Exception in processBackupServerJobs: " + e.toString());
			e.printStackTrace();
		}
		return status;
	}

	// Authentication method
	private static String authenticateAndGetToken(String baseUrl, String username, String password) {
		try {
			pu.sop("authenticateAndGetToken", "Starting authentication");

			// Correct endpoint
			String apiUrl = baseUrl + "/api/v3/token";
			pu.sop("apiUrl", apiUrl);

			// Build form-urlencoded request body
			String formParams = "grant_type=password&username=" + username + "&password=" + password;
			pu.sop("formParams", formParams);

			disableSSLCertification();

			URL url = new URL(apiUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
			conn.setDoOutput(true);

			// Send form parameters
			try (OutputStream os = conn.getOutputStream()) {
				byte[] input = formParams.getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
			}

			// Read response
			int responseCode = conn.getResponseCode();
			pu.sop("Response Code", String.valueOf(responseCode));

			if (responseCode == 200) {
				BufferedReader in = new BufferedReader(
						new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8));
				StringBuilder response = new StringBuilder();
				String line;
				while ((line = in.readLine()) != null) {
					response.append(line);
				}
				in.close();
				pu.sop("Authentication Response", response.toString());

				// Parse JSON response to extract access_token
				JSONObject jsonResponse = new JSONObject(response.toString());
				return jsonResponse.getString("access_token");
			} else {
				pu.sop("Authentication failed. Response Code: ", String.valueOf(responseCode));

				java.io.InputStream errorStream = conn.getErrorStream();
				if (errorStream != null) {
					BufferedReader errorReader = new BufferedReader(
							new InputStreamReader(errorStream, StandardCharsets.UTF_8));
					StringBuilder errorResponse = new StringBuilder();
					String errorLine;
					while ((errorLine = errorReader.readLine()) != null) {
						errorResponse.append(errorLine);
					}
					errorReader.close();
					pu.sop("Error Response", errorResponse.toString());
				} else {
					pu.sop("Error Response", "No error stream available.");
				}
			}
			conn.disconnect();

		} catch (Exception e) {
			e.printStackTrace();
			pu.sop("Error in authentication", e.getMessage());
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
			pu.sop("Table Check Query", query);

			conn = new api_parser.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);

			if (rs.next()) {
				tableExists = true;
				pu.sop("Table Status", "Table " + tableName + " exists");
			} else {
				pu.sop("Table Status", "Table " + tableName + " does not exist");
			}

		} catch (Exception e) {
			pu.sop("Exception in checkTableExists", e.toString());
			e.printStackTrace();
		} finally {
			try {
				if (rs != null)
					rs.close();
				if (stmt != null)
					stmt.close();
				if (conn != null)
					conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return tableExists;
	}

	// Get backup server jobs data
	private static void getBackupServerJobsData(String baseUrl, String token, String eventTable, String db_URL,
			String db_User, String db_Pass, String db_Name) {
		try {
			String apiUrl = baseUrl + "/api/v3/infrastructure/backupServers/jobs";
			pu.sop("Backup Jobs API URL", apiUrl);

			disableSSLCertification();

			URL url = new URL(apiUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Bearer " + token);

			int responseCode = conn.getResponseCode();
			pu.sop("Backup Jobs Response Code", String.valueOf(responseCode));

			if (responseCode == 200) {
				BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				StringBuilder response = new StringBuilder();
				String line;
				while ((line = in.readLine()) != null) {
					response.append(line);
				}
				in.close();

				pu.sop("Backup Jobs Response", response.toString());

				// Process and insert the backup jobs data
				processAndInsertBackupJobsData(response.toString(), eventTable, db_URL, db_User, db_Pass, db_Name);

			} else {
				pu.sop("Failed to retrieve backup jobs data. Response Code", String.valueOf(responseCode));

				// Read error response
				BufferedReader errorReader = new BufferedReader(new InputStreamReader(conn.getErrorStream()));
				StringBuilder errorResponse = new StringBuilder();
				String errorLine;
				while ((errorLine = errorReader.readLine()) != null) {
					errorResponse.append(errorLine);
				}
				errorReader.close();
				pu.sop("Error Response", errorResponse.toString());
			}
			conn.disconnect();

		} catch (Exception e) {
			e.printStackTrace();
			pu.sop("Error in getBackupServerJobsData", e.getMessage());
		}
	}

	// Process and insert backup jobs data
	// Process and insert backup jobs data
	// Process and insert/update backup jobs data
	public static void processAndInsertBackupJobsData(String jsonString, String eventTable, String db_URL,
	        String db_User, String db_Pass, String db_Name) throws IOException {
	    pu.sop("processAndInsertBackupJobsData", "Processing backup jobs data (UPSERT: update or insert)");

	    Connection conn = null;
	    PreparedStatement psUpdate = null;
	    PreparedStatement psInsert = null;

	    try {
	        ObjectMapper objectMapper = new ObjectMapper();
	        JsonNode rootNode = objectMapper.readTree(jsonString);

	        JsonNode jobsArray = rootNode.get("data");

	        // Update query
	        String updateQuery = "UPDATE " + eventTable + " SET "
	                + "time=?, update_date=?, size=?, rpo=?, retention=?, criticality=?, "
	                + "advised_data_to_be_restored=?, enterprise_id=? "
	                + "WHERE job_name=?";

	        // Insert query
	        String insertQuery = "INSERT INTO " + eventTable + " (time, update_date, job_name, size, rpo, retention, "
	                + "criticality, advised_data_to_be_restored, final_data_to_be_restored, enterprise_id) "
	                + "VALUES (?,?,?,?,?,?,?,?,?,?)";

	        conn = new api_parser.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
	        conn.setAutoCommit(false);

	        psUpdate = conn.prepareStatement(updateQuery);
	        psInsert = conn.prepareStatement(insertQuery);

	        int updatedCount = 0;
	        int insertedCount = 0;

	        if (jobsArray != null && jobsArray.isArray()) {
	            for (JsonNode job : jobsArray) {
	                try {
	                    String jobName = getJsonValue(job, "name");
	                    String lastRun = getJsonValue(job, "lastRun");
	                    String transferredData = getJsonValue(job, "transferredData");
	                    String scheduleType = getJsonValue(job, "scheduleType");
	                    String retentionLimit = getJsonValue(job, "retentionLimit");
	                    String retentionLimitType = getJsonValue(job, "retentionLimitType");
	                    String rpo = getJsonValue(job, "scheduleType");
	                    String organizationUid = "20"; // enterprise_id
	                    

	                    // Defaults
	                    jobName = jobName.isEmpty() ? "N/A" : jobName;
	                    scheduleType = scheduleType.isEmpty() ? "N/A" : scheduleType;

	                    // Retention merged
	                    String retention = "N/A";
	                    if (!retentionLimit.isEmpty() && !retentionLimitType.isEmpty()) {
	                        retention = retentionLimit + " " + retentionLimitType;
	                    }

	                    // Convert transferredData to GB (size column)
	                    String size = "0 GB";
	                    if (!transferredData.isEmpty()) {
	                        try {
	                            double transferredDataGB = Double.parseDouble(transferredData) / (1024.0 * 1024 * 1024);
	                            size = String.format("%.3f GB", transferredDataGB);
	                        } catch (NumberFormatException nfe) {
	                            size = "0 GB";
	                        }
	                    }

	                    // Criticality
	                    int retentionCriticality = getRetentionCriticality(retention);
	                    int scheduleCriticality = getScheduleCriticality(scheduleType);
	                    int criticality = Math.min(retentionCriticality, scheduleCriticality);

	                    // Advised restore %
	                    double advisedDataRestore = getAdvisedRestoreValue(criticality, 1);

	                    // Current run time (update_date)
	                    String updateDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date());

	                    // ---------- Try UPDATE first ----------
	                    psUpdate.setString(1, formatDateForMySQL(lastRun)); // time
	                    psUpdate.setString(2, updateDate); // update_date
	                    psUpdate.setString(3, size); // size
	                    psUpdate.setString(4, rpo); // rpo
	                    psUpdate.setString(5, retention); // retention
	                    psUpdate.setInt(6, criticality); // criticality
	                    psUpdate.setDouble(7, advisedDataRestore); // advised_data_to_be_restored
//	                    psUpdate.setDouble(8, advisedDataRestore); // final_data_to_be_restored
//	                    psUpdate.setString(9, ""); // rto
	                    psUpdate.setInt(8, Integer.parseInt(organizationUid)); // enterprise_id
	                    psUpdate.setString(9, jobName); // WHERE job_name=?
//	                    + "time=?, update_date=?, size=?, rpo=?, retention=?, criticality=?, "
//		                + "advised_data_to_be_restored=?, enterprise_id=? "
//		                + "WHERE job_name=?";

	                    int rows = psUpdate.executeUpdate();

	                    if (rows == 0) {
	                        // ---------- If no update, do INSERT ----------
	                        psInsert.setString(1, formatDateForMySQL(lastRun)); // time
	                        psInsert.setString(2, updateDate); // update_date
	                        psInsert.setString(3, jobName); // job_name
	                        psInsert.setString(4, size); // size
	                        psInsert.setString(5, rpo); // rpo
	                        psInsert.setString(6, retention); // retention
	                        psInsert.setInt(7, criticality); // criticality
	                        psInsert.setDouble(8, advisedDataRestore); // advised_data_to_be_restored
	                        psInsert.setDouble(9, advisedDataRestore); // final_data_to_be_restored
//	                        psInsert.setString(10, ""); // rto
	                        psInsert.setInt(10, Integer.parseInt(organizationUid)); // enterprise_id
	                        
//	                        "INSERT INTO " + eventTable + " (time, update_date, job_name, size, rpo, retention, "
//	    	                + "criticality, advised_data_to_be_restored, final_data_to_be_restored, enterprise_id) "
//	    	                + "VALUES (?,?,?,?,?,?,?,?,?,?)";

	                        psInsert.executeUpdate();
	                        insertedCount++;
	                        pu.sop("Inserted Record", "Job: " + jobName);
	                    } else {
	                        updatedCount++;
	                        pu.sop("Updated Record", "Job: " + jobName);
	                    }

	                } catch (Exception e) {
	                    pu.sop("Error processing record", e.toString());
	                    continue;
	                }
	            }

	            conn.commit();
	            pu.sop("Data Upsert", "Updated: " + updatedCount + ", Inserted: " + insertedCount);

	        } else {
	            pu.sop("JSON Format", "No 'data' array found in response");
	        }

	    } catch (Exception e) {
	        try {
	            if (conn != null)
	                conn.rollback();
	        } catch (Exception rollbackEx) {
	            pu.sop("Rollback Exception", rollbackEx.toString());
	        }
	        System.out.println("EXCEPTION JAR: processAndInsertBackupJobsData: " + e.toString());
	        e.printStackTrace();
	    } finally {
	        try {
	            if (psUpdate != null)
	                psUpdate.close();
	            if (psInsert != null)
	                psInsert.close();
	            if (conn != null)
	                conn.close();
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
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
			pu.sop("Error getting field: " + fieldName, e.toString());
		}
		return ""; // Return empty string if field is null or doesn't exist
	}

	// Retention mapping
	private static int getRetentionCriticality(String retention) {
		if (retention == null || retention.equals("N/A"))
			return 4;

		retention = retention.toLowerCase();
		if (retention.contains("year")) {
			try {
				int years = Integer.parseInt(retention.replaceAll("[^0-9]", ""));
				if (years >= 5 && years <= 7)
					return 1;
				else if (years >= 3 && years < 5)
					return 2;
				else if (years >= 1 && years < 3)
					return 3;
				else
					return 4;
			} catch (Exception e) {
				return 4;
			}
		}
		return 4; // Default
	}

	// ScheduleType mapping
	private static int getScheduleCriticality(String schedule) {
		if (schedule == null)
			return 4;
		schedule = schedule.toLowerCase();

		if (schedule.contains("daily") || schedule.contains("continuously"))
			return 1;
		if (schedule.contains("periodic"))
			return 2;
		if (schedule.contains("month"))
			return 3;
		if (schedule.contains("notscheduled") || schedule.contains("unknown"))
			return 4;
		return 4;
	}

	// Advised restore % mapping
	private static double getAdvisedRestoreValue(int criticality, int ageing) {
		switch (criticality) {
		case 1:
			return (ageing == 1) ? 5.0 : 4.5;
		case 2:
			return (ageing == 1) ? 4.0 : 3.5;
		case 3:
			return (ageing == 1) ? 3.0 : 2.5;
		case 4:
			return (ageing == 1) ? 2.0 : 1.0;
		default:
			return 1.0;
		}
	}

	// SSL Certificate disabling method (same as your original code)
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
