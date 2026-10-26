package com.fapps.api;

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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.json.JSONObject;


import com.fapps.db.SNORangeFinder;
import com.fapps.util.PrintUtilMsg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Source {
	PrintUtilMsg pu = new PrintUtilMsg();
	
	protected String[] binarySearch(String binarySearch, String date, String sourceTable, String db_url, String db_name, String db_pass ) {
		String[] snoRangeReturn = new String[4];
		try {
			pu.sop("getAPILogData", "Is Binary Search? ["+binarySearch+"]");
		if(binarySearch.equalsIgnoreCase("yes")) {
			String current_month = "";
			String next_month    = "";
			String year          = "";
			String from_date     = "";
			String to_date       = "";
			String[] dates       =  new String[2];
			long[] snoRange  = null;
			long[] snoRangeFinal = new long[2];
			//int[] snoRange  = null;
			//int[] snoRangeFinal = new int[2];
			 
			boolean hashExist    = false; 
		
			String repId="120";
			String[] pull_data_details=null;
			String from_date_final="";
			String to_date_final="";
			boolean intrval_and_month = false;
			pull_data_details = date.split("~~"); 
			String[] dates_details = pull_data_details[1].split("#");
			
			if(dates_details.length > 1) {
				hashExist = true;
			}	
			pu.sop("Status Date Interval", "Interval Status is "+hashExist);
			
			// ==============Getting Current Month if trendType is monthly
			
			if(pull_data_details[2].toLowerCase().equals("monthly") || (pull_data_details[2].toLowerCase().equals("daily") && hashExist==true )) {
				intrval_and_month = true;
				
				if(hashExist==false) { //===only monthly data
					
					pu.sop("Trend Type", "Monthly only "+hashExist);
					
					current_month = pull_data_details[1].substring(5, 7); 
					pu.sop( "Monthly", "Month is "+current_month);
				
					if(Integer.parseInt(current_month)==12) {
						next_month = "01";
						pu.sop( "Monthly", "Next Month is "+next_month);
						
					}else {
									
						next_month = ""+(Integer.parseInt(current_month)+1);
						pu.sop( "Monthly", "Next Month is "+next_month);
					}
					if(next_month.length()==1) {
						next_month = "0"+next_month;
					}
					pu.sop( "Monthly", "Final Next Month is "+next_month);
					
					from_date = pull_data_details[1].substring(0, 8)+"01";
					
					pu.sop( "Monthly", "From data is "+from_date);
					
					from_date_final = from_date;
					
					if(Integer.parseInt(current_month)==12) {
						 year = ""+((Integer.parseInt(pull_data_details[1].substring(0, 4)))+1);
						to_date = year+"-"+next_month+"-"+"01";
						
					}else {
						 to_date = from_date.replace(current_month, next_month);
						
					}
					pu.sop( "Monthly", "To data is "+to_date);
					
					//to_date_final = to_date;   ////changing for monthly to-date as input date
					
					to_date_final = pull_data_details[1];
				}else if(hashExist==true && pull_data_details[2].toLowerCase().equals("monthly")){
					
					pu.sop("Trend Type", "Monthly Interval "+hashExist);
					
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					//String shared_date = sdf.format(dates[0]);
					
					Date shared_date = sdf.parse(dates_details[0]);
					
					Calendar c = Calendar.getInstance(); 
					c.setTime(shared_date); 
					c.add(Calendar.MONTH, -Integer.parseInt(dates_details[1]));
					pu.sop("Getting Require month and year", "Year is "+c.getTime());
					
					Date from_month = c.getTime();
				    
				    String from_year_month = sdf.format(from_month);
					 
					/*Date to_date1 = new SimpleDateFormat("yyyy-MM-dd").parse(""+c.getTime());
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					String to_date_final1 = sdf.format(to_date);*/
				
				    pu.sop("Getting Require month and year", "Year is "+from_year_month);
					
					
					from_date_final = from_year_month.substring(0, 8)+"01";
					
					pu.sop("Final From Date", "From Date "+from_date_final);
					
					
					to_date_final = dates_details[0].substring(0, 8)+"01";
					
					pu.sop("Final To Date", "To Date "+to_date_final);
					
				}else {
					
					pu.sop("Trend Type", "Date Interval "+hashExist);
					
					int numberOfDays =0;
					
					Date shared_date = new SimpleDateFormat("yyyy-MM-dd").parse(dates_details[0]);
					
					numberOfDays = (Integer.parseInt(dates_details[1]))-1;
					pu.sop("Number of days","Days are: "+numberOfDays);
					
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
					to_date_final = sdf.format(shared_date);
						
					System.out.println("To date is "+to_date_final);
						
					Calendar c = Calendar.getInstance();
				    c.setTime(shared_date);	
						
				    c.add(Calendar.DATE, -numberOfDays);
				        
				     Date from_fetched_date = c.getTime();
				    
				    from_date_final = sdf.format(from_fetched_date);
				    
				    System.out.println("From date is "+from_date_final);
				}
				dates[0] = from_date_final;
				dates[1] = to_date_final;
				//pull_data_details[2] = "daily";
				snoRange = new SNORangeFinder().getStartEndIdOfTable( sourceTable,
						  db_url,
						  db_name,
						  db_pass );
				pu.sop( "TABLE MIN MAX VALUE", "MIN ["+snoRange[0]+"], MAX ["+snoRange[1]+"]");
				
				long [] snoRangeToFromDate  = null;
				//int [] snoRangeToFromDate  = null;
				
				for(int i=0; i< dates.length; i++) {
				
					snoRangeToFromDate = new SNORangeFinder().getStartAndEndId(dates[i], /* date */ 
											   			 snoRange[0], 
					   									 snoRange[1], 
					   									 sourceTable, 
					   									 "DATE_FORMAT("+pull_data_details[0]+",'%Y-%m-%d')",
					   									"daily", /*   daily//weekly//monthly ==Trend Type    */
					   									 repId, 
					   									 false, 
					   									 "sno", 
					   									 db_url,
					   									 db_name, 
					   									 db_pass,
					   									 //"YYYY-MM-DD");
					   									"%Y-%m-%d");
					pu.sop("getAPILogData", "Table Name ["+sourceTable+"]");
					pu.sop("getAPILogData", "FOR DATE ["+dates[i]+"], MinSno["+snoRangeToFromDate[0]+"], MaxSno["+snoRangeToFromDate[1]+"]");
					pu.sop("getAPILogData", "Date ["+date+"]");
					if(i==0) {
						snoRangeFinal[0] = snoRangeToFromDate[0];
					}else {
						snoRangeFinal[1] = snoRangeToFromDate[1];
					}
				}
				snoRangeReturn[0] =""+snoRangeFinal[0];
				snoRangeReturn[1] =""+snoRangeFinal[1];
				snoRangeReturn[2] =from_date_final;
				snoRangeReturn[3] =to_date_final;
			// ========closing trend type monthly	
			}else {
				snoRange = new SNORangeFinder().getStartEndIdOfTable( sourceTable,
						  db_url,
						  db_name,
						  db_pass );
				pu.sop( "TABLE MIN MAX VALUE", "MIN ["+snoRange[0]+"], MAX ["+snoRange[1]+"]");
				snoRange = new SNORangeFinder().getStartAndEndId(pull_data_details[1], /* date */ 
			   			 snoRange[0], 
							 snoRange[1], 
							 sourceTable, 
							 "DATE_FORMAT("+pull_data_details[0]+",'%Y-%m-%d')",
							pull_data_details[2].toLowerCase(), /*   daily//weekly//monthly ==Trend Type    */
							 repId, 
							 false, 
							 "sno", 
							 db_url,
							 db_name, 
							 db_pass,
							 //"YYYY-MM-DD");
							 "%Y-%m-%d");
				pu.sop("getAPILogData", "Table Name ["+sourceTable+"]");
				pu.sop("getAPILogData", "MinSno["+snoRange[0]+"], MaxSno["+snoRange[1]+"]");
				pu.sop("getAPILogData", "Date ["+date+"]");
				
				snoRangeReturn[0] =""+snoRange[0];
				snoRangeReturn[1] =""+snoRange[1];
				snoRangeReturn[2] ="NA";
				snoRangeReturn[3] ="NA";
			}
		}	
		
		}catch(Exception ex) {
			System.out.println("EXCEPTION JAR: Join_Left: "+ex.toString());
			
			//ex.printStackTrace();
			
		}
		
		return snoRangeReturn;
	}	
	
	public String currentDate() {
	      String current_date = "";
	      try {
	         SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd");
	         Calendar c1 = Calendar.getInstance();
	         Date date = new Date();
	         c1.setTime(date);
	         //Date nextDate = c1.getTime();
	         current_date = sdf_date.format(c1.getTime());
	      } catch (Exception var6) {
	         System.out.println("currentTime::   Exception while computing current date");
	         var6.printStackTrace();
	      }
	      return current_date;
	   }
	
	public ArrayList<String[]> getDataFromSourceTable( String inputDate, String db_url, String db_User, String db_pass, String serverTable, String siteTable){
		ArrayList<String[]> data = null;
		Connection conn = null;
		Statement stmt  = null;
		ResultSet rs    = null;
		String query    = null;
		
		int rowCount = 0;
		try{
			query = "select A.siteName, A.userName, A.password, A.url, A.port, A.catalogName, A.lang, B.name, B.Category from "
					+ "" + serverTable + " B, "+siteTable+" A where A.siteName=B.siteName";
			this.pu.sop("Query API Log:  getDataFromScheduleTable: ", query);
	        conn = new com.fapps.db.MySQLConnection().getConnection(db_url, db_User, db_pass);
	        stmt = conn.createStatement();
	        rs = stmt.executeQuery(query);
	        rs.last();
	        rowCount = rs.getRow();
	        rs.beforeFirst();
	        this.pu.sop("Row Count", "Total rows:[" + rowCount + "]");
	        if (rowCount > 0) {
	        	data = new ArrayList<String[]>();
	            int colNumber = 0;
	            String[] tableData = null;
	            while(rs.next()) {
	            	   colNumber = 0;
	                   tableData = new String[9];
	                   for(colNumber = 0; colNumber < 9; ++colNumber) {
	                      tableData[colNumber] = rs.getString(colNumber + 1);
	                   }
	                   data.add(tableData);
	            }
	        }
		}catch( Exception ex ){
			System.out.println("EXCEPTION JAR: ::  getDataFromScheduleTable: "+ex.toString());
			data = null;
			ex.printStackTrace();
		}finally{
			try{
				if( conn != null ){
					if( rs != null )
						rs.close();
					if( stmt != null )
						stmt.close();
					conn.close();
				}
			}catch( Exception e ){}
		}
		return data;
	}
	
	public boolean processSourceTransactions(String scheduleTable, ArrayList<String[]> getDataFromSourceTable, String  inputdate, 
			String  eventTable, String category, String  db_URL, String  db_User, String db_Pass, String db_Name) {
		boolean status = false;
		//String updateSourceTableStatus = "true"; 
		try {
			pu.sop(" in processSourceTransactions", "");
			String[] AL_Values = null;
			for(int i=0; i< getDataFromSourceTable.size(); i++) {
				AL_Values = getDataFromSourceTable.get(i);
				
				callAuthenticateAndHitGetReq(AL_Values[0], AL_Values[1],AL_Values[2], AL_Values[3], AL_Values[4], 
						AL_Values[5], AL_Values[6], AL_Values[7], AL_Values[8], eventTable, db_URL, db_User, db_Pass, db_Name);
			}
			status = true;
		}catch(Exception e) {
			pu.sop("Exception:::: ", "Exception while processingScheduledTransactions:::: "+e.toString());
			//e.printStackTrace();
		}
		return status;
	}
	private void callAuthenticateAndHitGetReq(String server, String user, String password, String baseUrl, 
			String port, String catalog, String lang, String serverName, String category, String eventTable, String db_URL, 
			String db_User, String db_Pass, String db_Name) {

		String token = authenticateAndGetToken(catalog, server, user, password, lang, baseUrl, port);
		if (token != null) {
			pu.sop("Token Received: ", token);
			hitGetRequestWithToken(baseUrl, port, token, server, serverName, category, eventTable, db_URL, db_User, db_Pass, db_Name);
		} else {
			pu.sop("Response from authenticateAndGetToken","Null");
			// what to be done when its null (have to ask Gagan sir)
		}
	}
	private String authenticateAndGetToken(String catalog, String server, String user, String password, String lang,
			String baseUrl, String port) {
		try {
			pu.sop("In authenticateAndGetToken ", "");
			pu.sop("baseUrl ", baseUrl);
			String apiUrl = baseUrl +":"+ port + "/tina/api/users/authenticate";
			pu.sop("apiUrl ", apiUrl);
			String jsonInputString = String.format(
					"{ \"catalog\": \"%s\", \"server\": \"%s\", \"user\": \"%s\", \"password\": \"%s\", \"lang\": \"%s\" }",
					catalog, server, user, password, lang);
			pu.sop("jsonInputString ", jsonInputString);
			disableSSLCertification();
			URL url = new URL(apiUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("PUT");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setDoOutput(true);

			// Send request body
			try (OutputStream os = conn.getOutputStream()) {
				byte[] input = jsonInputString.getBytes(StandardCharsets.UTF_8);
				os.write(input, 0, input.length);
			}

			// Read response
			int responseCode = conn.getResponseCode();
			if (responseCode == 200) { 
				BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				StringBuilder response = new StringBuilder();
				String line;
				while ((line = in.readLine()) != null) {
					response.append(line);
				}
				in.close();
				pu.sop("Response: ", response.toString());
				// Parse JSON response to extract token
				JSONObject jsonResponse = new JSONObject(response.toString());
				return jsonResponse.getString("token"); // Extract the token
			} else {
				pu.sop("Authentication failed. Response Code: ", String.valueOf(responseCode));
			}

			conn.disconnect();

		} catch (Exception e) {
			e.printStackTrace();
			pu.sop("Error ", e.getMessage());
		}
		return null;
	}

	private void hitGetRequestWithToken(String baseUrl, String port, String token, String server, String serverName, 
			String category, String eventTable, String db_URL, String db_User, String db_Pass, String db_Name) {
		try {
			String ext = "";
			pu.sop("category ", category);
			pu.sop("serverName ", serverName);
			if(category.equalsIgnoreCase("host")) {
				ext = "host";
			}else {
				ext = "appl";
			}
			//String apiUrl = baseUrl +":" + port + "/tina/api/jobs/last-n-historic-jobs?folderName="+category+"."+serverName+"&typeList=backup&content=FULL";
			String apiUrl = baseUrl +":" + port + "/tina/api/jobs/last-n-historic-jobs?folderName="+ext+"."+serverName+"&typeList=backup&content=FULL";
			pu.sop("apiUrl ", apiUrl);
			disableSSLCertification();
			URL url = new URL(apiUrl);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Bearer " + token);

			// Read response
			int responseCode = conn.getResponseCode();
			pu.sop("Response Code: ", String.valueOf(responseCode));

			if (responseCode == 200) { 
				BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
				StringBuilder response = new StringBuilder();
				String line;
				while ((line = in.readLine()) != null) {
					response.append(line);
				}
				in.close();
				pu.sop("Response: " , response.toString());
				getHostHeaderValues(response.toString(), server, category, eventTable, db_URL, db_User, db_Pass, db_Name);
			} else {
				pu.sop("Failed to retrieve data. Response Code: " , String.valueOf(responseCode));
			}
			conn.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
			pu.sop("Error " , e.getMessage());
		}
	}
	
	public void getHostHeaderValues(String jsonString, String server, String category, String eventTable, String db_URL, String db_User, 
			String db_Pass, String db_Name) throws IOException {
		pu.sop("In getHostHeaderValues " , "");
		//ArrayList<String[]> data=null;
		Connection conn = null;
		PreparedStatement ps = null;
		String query    = null;
		try {
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonArray = objectMapper.readTree(jsonString);
						
			//for (JsonNode obj : jsonArray) {
			for(int i = jsonArray.size() -1;i >= 0; i--) {
				JsonNode obj = jsonArray.get(i);
				String backupDate = obj.get("backupDate").asText();
			    String highestAlarm = obj.get("highestAlarm").asText();
			    String hostName = obj.get("hostName").asText();
			    String isActive = obj.get("isActive").asText();
			    String isFull = obj.get("isFull").asText();;
			    String isIncr = obj.get("isIncr").asText();;
			    String modificationDate = obj.get("modificationDate").asText();;
			    String presentationId = obj.get("presentationId").asText();;
			    String priority = obj.get("priority").asText();
			    String processedObjects = obj.get("processedObjects").asText();
			    String processedVolume = obj.get("processedVolume").asText();
			    String status = obj.get("status").asText();
			    String stratName = obj.get("stratName").asText();
			    String submittedDate = obj.get("submittedDate").asText();
			    String type = obj.get("type").asText();
			    String agentName = obj.get("agentName").asText();
			    String applicationType = "";
			    JsonNode medias = obj.get("medias");
                if (medias != null) {
                	applicationType = medias.asText();
                    System.out.println("Application Type: " + applicationType);
                }

			    
			    query = "insert into "+eventTable+" (date,backupTime,insert_date,siteName,priority,hostName,severity,"
			    		+ "highest_Alarm,isFull,fileSize,isActive,agent_Name,executed_Date,isIncr,Medias,Presentation_ID,"
			    		+ "Process_Volume,Processed_Object,Strategy_Type,status) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			    conn = new com.fapps.db.MySQLConnection().getConnection(db_URL,db_User,db_Pass);
				conn.setAutoCommit(false);
				ps = conn.prepareStatement(query);
				ps.setString(1, currentDate());
				ps.setString(2, backupDate);       
				ps.setString(3, "");
				ps.setString(4, server);
				ps.setString(5, priority);
				ps.setString(6, hostName);
				ps.setString(7, type);
				ps.setString(8, highestAlarm);
				ps.setString(9, isFull);
				ps.setString(10, processedVolume);
				ps.setString(11, isActive);
				ps.setString(12, agentName);
				ps.setString(13, submittedDate);
				ps.setString(14, isIncr);
				ps.setString(15, applicationType);
				ps.setString(16, presentationId);
				ps.setString(17, processedVolume);
				ps.setString(18, processedObjects);
				ps.setString(19, stratName);
				ps.setString(20, status);
				
				ps.addBatch();
				ps.executeBatch();
				conn.commit();
			    
			}
			        
		}catch(Exception e) {
			System.out.println("EXCEPTION JAR: getHeaderValues: "+e.toString());
			e.printStackTrace();
		}finally{
			try{
				if( conn != null ){
					if( ps != null )
						ps.close();

					conn.close();
				}
			}catch( Exception e ){}
		}
		//return "";
	}
		  private static void disableSSLCertification() {
		// TODO Auto-generated method stub
		// Create a trust manager that does not validate certificate chains
		  TrustManager[] trustAllCerts = new TrustManager[]{new X509TrustManager(){
		      public X509Certificate[] getAcceptedIssuers(){return null;}
		     
			public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				// TODO Auto-generated method stub
				
			}
			public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				// TODO Auto-generated method stub
				
			}
		  }};
		  
		  
		  

		  // Install the all-trusting trust manager
		  try {
		      SSLContext sc = SSLContext.getInstance("TLS");
		      sc.init(null, trustAllCerts, new SecureRandom());
		      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
		  } catch (Exception e) {
		      ;
		  }
		  
		  // Create all-trusting host name verifier
	        HostnameVerifier allHostsValid = new HostnameVerifier() {
	            public boolean verify(String hostname, SSLSession session) {
	                return true;
	            }

				 };
	       
	        // Install the all-trusting host verifier
	        HttpsURLConnection.setDefaultHostnameVerifier(allHostsValid);
		  
		  
		
	}
}