package api_parser;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;



public class SNORangeFinder {

	api_parser.PrintUtilMsg pu = new api_parser.PrintUtilMsg();
	api_parser.MySQLConnection msc = new api_parser.MySQLConnection();
	
	
	
	final String TAG = "SNO query";
	String date_format = "%Y-%m-%d";
	long min_left = 0;
	long min_right = 0;
	long max_value = 0;
	long count = 0;
	
	
	
	public long[] getStartEndIdOfTable(String table, String db_url,
			String db_name, String db_pass) {
		long[] startEndId = null;
		String query = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		//System.out.println("--------------------------------------");
		min_right = 0;
		min_left = 0;
		max_value = 0;
		count = 0;
		try {
			query = "select min(sno) min,max(sno) max from " + table;
			conn = new api_parser.MySQLConnection().getConnection(db_url,
					db_name, db_pass);
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				startEndId = new long[]{rs.getLong("min"), rs.getLong("max")};
				
			}
			
			
		} catch (Exception ex) {
			startEndId = null;
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (rs != null) {
						rs.close();
					}
					if (stmt != null) {
						stmt.close();
					}
					conn.close();
				}
			} catch (Exception e) {
			}
		}
		return startEndId;
	}

	
	
	/*getStartAndEndId(date, 
  			 snoRange[0], 
				 snoRange[1], 
				 sourceTable, 
				 "DATE_FORMAT(date,'%Y-%m-%d')",#datefield
				 "daily", #trendType
				 repId, 
				 false, #false
				 "sno", 
				 db_url,
				 db_name, 
				 db_pass);*/
	public long[] getStartAndEndId(String date, long startId, long endId,
			String tableName, String dateField, String trendType, String repId,
			boolean trendHaveSameLevalReport, String idColumn, String db_url,
			String db_name, String db_pass, String dateFormat ) {
		// pu.sop( "getStartAndEndId", " date " +date+" dateField "
		// +dateField+"");
	
		this.date_format=dateFormat;
		int numOfAttr = 0;
		long leftId = 0;
		long rightId = 0;
		long midID = 0;
		long[] finalRange = null;
		String tempDate = null;
		Date midDate = null;
		Date sDate = null;
		String day = null;
		String month = null;
		String startDay = null;
		String monthday = null;
		String query = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			midID = (startId + endId) / 2;
			if(count==0) {
				this.max_value = endId;
			}
			this.count++;
			System.out.println("Value of max "+this.max_value);
			if (date != null) {
				// day = date.substring( date.lastIndexOf("-"), date.indexOf("
				// ") );
				// day = date.substring( date.lastIndexOf("-"));//COMMENTED BY
				// TANUJ ON 30-SEP-2018
				day = date.substring(date.lastIndexOf("-") + 1);// ADDED BY
																// TANUJ ON
																// 30-SEP-2018
				month = date.substring(5, date.lastIndexOf("-"));
				// pu.sop( "getStartAndEndId called ", " day " +day+" month "
				// +month+"");
			}
			if (date != null) {
				// monthday = date.substring( 4, date.indexOf(" ") );
				// monthday = date.substring( 4 );//COMMENTED BY TANUJ ON
				// 30-SEP-2018
				monthday = date.substring(5);// ADDED BY TANUJ ON 30-SEP-2018
				// pu.sop( "getStartAndEndId next called ", " monthday "
				// +monthday+"");
			}
			if (trendHaveSameLevalReport) {
				if (trendType.equalsIgnoreCase("weekly")) {
					startDay = this.getDayName(date);
					// pu.sop("dayname","Date of ["+date+"] and Day name is
					// ["+startDay+"]");
					if (dateField == null) {
						dateField = "week";
					}
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("monthly")) {
					if (dateField == null) {
						dateField = "month";
					}
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("yearly")) {
					if (dateField == null) {
						dateField = "year";
					}
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				} else {
					// pu.sop( "inside if else block called", "query ");
					// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
					// %H:%i:%s') temp from "+tableName+" where
					// "+idColumn+"="+midID;
					query = "select DATE_FORMAT(" + dateField
							+ ",'%Y-%m-%d') temp from " + tableName + " where "
							+ idColumn + "=" + midID;
					// query = "select DATE_FORMAT(DATE(fb_ldate),'%Y-%m-%d
					// %H:%i:%s') ftemp, DATE_FORMAT(DATE(twit_ldate),'%Y-%m-%d
					// %H:%i:%s') ttemp,DATE_FORMAT(DATE(go_ldate),'%Y-%m-%d
					// %H:%i:%s') gtemp from social_profile_update where
					// "+idColumn+"="+midID;
				}
			} else {
				if (tableName.toLowerCase().contains("_weekly")
						&& dateField.equalsIgnoreCase("week")) {
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (tableName.toLowerCase().contains("_monthly")
						&& dateField.equalsIgnoreCase("month")) {
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("monthly")) {

					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT("+dateField+",'%Y-%m'),"
							+"-'"+ day + "'),'%Y-%m-%d') temp from " + tableName
							+ " where " + idColumn + " = " + midID;

				} else if (tableName.toLowerCase().contains("_yearly")
						&& dateField.equalsIgnoreCase("year")) {
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				} else {
					if (dateField.indexOf(" or ") != -1) {
						String[] dfieldTemp = dateField.split(" or ");
						String dateFieldTemp = "";
						numOfAttr = dfieldTemp.length;
						for (int i = 0; i < dfieldTemp.length; i++) {
							// dateFieldTemp = dateFieldTemp + "
							// DATE_FORMAT("+dfieldTemp[i]+", '%Y-%m-%d
							// %H:%i:%s') temp"+i+",";
							dateFieldTemp = dateFieldTemp + " DATE_FORMAT("
									+ dfieldTemp[i] + ", '%Y-%m-%d') temp" + i
									+ ",";
						}
						if (dateFieldTemp.indexOf(",") != -1) {
							dateFieldTemp = dateFieldTemp.substring(0,
									dateFieldTemp.length() - 1);
						}
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select " + dateFieldTemp + " from " + tableName
								+ " where " + idColumn + "=" + midID;

					} else {
						// pu.sop( "inside else block called:::", "query ");
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select DATE_FORMAT(" + dateField
								+ ",'"+date_format+"') temp from " + tableName
								+ " where " + idColumn + "=" + midID;
					}
					// query = "select DATE_FORMAT(DATE(fb_ldate),'%Y-%m-%d
					// %H:%i:%s') ftemp, DATE_FORMAT(DATE(twit_ldate),'%Y-%m-%d
					// %H:%i:%s') ttemp,DATE_FORMAT(DATE(go_ldate),'%Y-%m-%d
					// %H:%i:%s') gtemp from social_profile_update where
					// "+idColumn+"="+midID;
				}
			}
			pu.sop("StartId:["+startId+"],EndId:["+endId+"]","Query for getting mid id date ["+midID+"] ["+query+"]");
			conn = msc.getConnection(db_url, db_name, db_pass);
			//// pu.sop( "SNO Test", "Connection created.");
			// pu.sop( "getStartAndEndId function called", "query "+query+"");

			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			while (rs.next()) {
				if (numOfAttr > 0) {
					for (int i = 0; i < numOfAttr; i++) {
						if (rs.getString("temp" + i) != null
								&& !rs.getString("temp" + i)
										.equalsIgnoreCase("null")) {
							pu.sop("TempDate Check", "Date from DB is ["
									+ rs.getString("temp" + i) + "]");
							try {
								tempDate = rs.getString("temp" + i).trim();
							} catch (Exception ein) {
								pu.sop("EXCEPTION",
										"Exception in getting date from DB ["
												+ ein.getMessage() + "]");
							}
						}
						if (tempDate != null) {
							break;
						}
					}
				} else {
					try {
						tempDate = rs.getString("temp").trim();
					} catch (Exception e) {
						
						tempDate = null;
					}
				}
			}
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
				pu.sop("query executed ", "exception ex " + ex + "");
			}

			pu.sop( "getStartAndEndId function ::", "tempDate "+tempDate+" date :: "+date+"");
			
			if ((date != null && !date.equalsIgnoreCase("null"))
					&& (tempDate != null
							&& !tempDate.equalsIgnoreCase("null"))) {
				sDate = this.getFormatedDate(date);
				midDate = this.getFormatedDate(tempDate);
				 //======pu.sop( TAG,
						 //======"date:["+date+"],SDate:["+sDate+"],"
								 //======+ "tempDate:["+tempDate+"],midDate:["+midDate+"] "
								 //======+ "and comparision value:["+sDate.compareTo(midDate)+"]");
				
				if(sDate.compareTo(midDate) == 0) {
					min_left=midID;
					min_right =midID;
					System.out.println("min_left "+min_left);
					System.out.println("min_right "+min_right);
					
				}
				
				//inout date is smaller and have more than 1 id to search, go left
				if (sDate.compareTo(midDate) < 0 && (endId - startId) > 1) {
					tempDate = null;
					midDate = null;
					sDate = null;
					day = null;
					month = null;
					startDay = null;
					monthday = null;
					query = null;
					
					
					finalRange = this.getStartAndEndId(date, startId, midID,
							tableName, dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass,date_format);
				} else if (sDate.compareTo(midDate) > 0	&& (endId - startId) > 1) {   //input date is greater, go right
					tempDate = null;
					midDate = null;
					sDate = null;
					day = null;
					month = null;
					startDay = null;
					monthday = null;
					query = null;
					finalRange = this.getStartAndEndId(date, midID, endId,
							tableName, dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass,date_format);
				} else {   //both dates are equal and will go both left and right OR Date not found yet
					tempDate = null;
					midDate = null;
					sDate = null;
					day = null;
					month = null;
					startDay = null;
					monthday = null;
					query = null;
					//min_left = 0;
					//min_right = 0;
					
					
					
					 pu.sop( "getStartAndEndId", "Going to calculate left ID.Start ID:["+startId+"] and Mid Id:["+midID+"]");
					leftId = this.getLeftSNO(date, startId, midID, tableName,
							dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass);
					
					
					 pu.sop( "getStartAndEndId", "Going to calculate right ID.Mid Id:["+midID+"] and End Id:["+endId+"]");
					 
					 //min_left = 0;
					//min_right = 0;
					rightId = this.getRightSNO(date, midID, endId, tableName,
							dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass);
					
					System.out.println("Final Left Id is "+leftId);
					System.out.println("Final Right Id is "+rightId);
					
					if(leftId==0 && rightId>0) {
						leftId=rightId;
					}else if(rightId==0 && leftId>0) {
						rightId = leftId;
					}
					
					finalRange = new long[]{leftId, rightId};
				}
			} else if (tempDate == null) {
				pu.sop("temp date is null ", "temp date " + tempDate + "");
				//finalRange = new int[]{startId, endId};
				finalRange = new long[]{0, 0};
				return finalRange;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
			}
			tempDate = null;
			midDate = null;
			sDate = null;
			day = null;
			month = null;
			startDay = null;
			monthday = null;
			query = null;
		}
		if (tempDate == null) {
			return finalRange;
		}
		return finalRange;
	}

	public long getLeftSNO(String date, long startId, long endId, String tableName,
			String dateField, String trendType, String repId,
			boolean trendHaveSameLevalReport, String idColumn, String db_url,
			String db_name, String db_pass) {
		long leftSNO = 0;
		long midID = 0;
		int numOfAttr = 0;
		String tempDate = null;
		
		
		
		Date midDate = null;
		Date sDate = null;
		String day = null;
		String month = null;
		String startDay = null;
		String monthday = null;
		String query = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// pu.sop( "getLeftSNO", "Going to calculate left ID.Start
			// ID:["+startId+"] and End Id:["+endId+"]");
			midID = (startId + endId) / 2;
			if (date != null) {
				// day = date.substring( date.lastIndexOf("-"), date.indexOf("
				// ") );
				// day = date.substring( date.lastIndexOf("-"));//COMMENTED BY
				// TANUJ ON 30-SEP-2018
				day = date.substring(date.lastIndexOf("-") + 1);// ADDED BY
																// TANUJ ON
																// 30-SEP-2018
				month = date.substring(5, date.lastIndexOf("-"));
			}
			if (date != null) {
				// monthday = date.substring( 4, date.indexOf(" ") );
				// monthday = date.substring( 4 );//COMMENTED BY TANUJ ON
				// 30-SEP-2018
				monthday = date.substring(5);// ADDED BY TANUJ ON 30-SEP-2018
			}
			if (trendHaveSameLevalReport) {
				if (trendType.equalsIgnoreCase("weekly")) {
					startDay = this.getDayName(date);
					// pu.sop("dayname","Date of ["+date+"] and Day name is
					// ["+startDay+"]");
					if (dateField == null) {
						dateField = "week";
					}
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("monthly")) {
					if (dateField == null) {
						dateField = "month";
					}
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("yearly")) {
					if (dateField == null) {
						dateField = "year";
					}
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				} else {
					// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
					// %H:%i:%s') temp from "+tableName+" where
					// "+idColumn+"="+midID;
					query = "select DATE_FORMAT(" + dateField
							+ ",'%Y-%m-%d ') temp from " + tableName + " where "
							+ idColumn + "=" + midID;
				}
			} else {
				if (tableName.toLowerCase().contains("_weekly")
						&& dateField.equalsIgnoreCase("week")) {
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (tableName.toLowerCase().contains("_monthly")
						&& dateField.equalsIgnoreCase("month")) {
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				}  else if (trendType.equalsIgnoreCase("monthly")) {

					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT("+dateField+",'%Y-%m'),"
							+"-'"+ day + "'),'%Y-%m-%d') temp from " + tableName
							+ " where " + idColumn + " = " + midID;

				}  else if (tableName.toLowerCase().contains("_yearly")
						&& dateField.equalsIgnoreCase("year")) {
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				}
				/*
				 * else{ query = "select DATE_FORMAT("
				 * +dateField+",'%Y-%m-%d %H:%i:%s') temp from "
				 * +tableName+" where "+idColumn+"="+midID; } }
				 */

				else {
					if (dateField.indexOf(" or ") != -1) {
						String[] dfieldTemp = dateField.split(" or ");
						String dateFieldTemp = "";
						numOfAttr = dfieldTemp.length;
						for (int i = 0; i < dfieldTemp.length; i++) {
							// dateFieldTemp = dateFieldTemp + "
							// DATE_FORMAT("+dfieldTemp[i]+", '%Y-%m-%d
							// %H:%i:%s') temp"+i+",";
							dateFieldTemp = dateFieldTemp + " DATE_FORMAT("
									+ dfieldTemp[i] + ", '%Y-%m-%d') temp" + i
									+ ",";
						}
						if (dateFieldTemp.indexOf(",") != -1) {
							dateFieldTemp = dateFieldTemp.substring(0,
									dateFieldTemp.length() - 1);
						}
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select " + dateFieldTemp + " from " + tableName
								+ " where " + idColumn + "=" + midID;

					} else {
						// pu.sop( "inside else block calleds", "query ");
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select DATE_FORMAT(" + dateField
								+ ",'"+date_format+"') temp from " + tableName
								+ " where " + idColumn + "=" + midID;
					}
					// query = "select DATE_FORMAT(DATE(fb_ldate),'%Y-%m-%d
					// %H:%i:%s') ftemp, DATE_FORMAT(DATE(twit_ldate),'%Y-%m-%d
					// %H:%i:%s') ttemp,DATE_FORMAT(DATE(go_ldate),'%Y-%m-%d
					// %H:%i:%s') gtemp from social_profile_update where
					// "+idColumn+"="+midID;
				}
			}

			pu.sop("Left Sno Query","Query for getting mid id : ["+midID+"] ["+query+"]");
			conn = msc.getConnection(db_url, db_name, db_pass);
			// pu.sop( "SNO Test", "Connection created.");
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			/*
			 * while( rs.next() ){ tempDate = rs.getString("temp");
			 * 
			 * }
			 */

			while (rs.next()) {
				if (numOfAttr > 0) {
					for (int i = 0; i < numOfAttr; i++) {
						if (rs.getString("temp" + i) != null
								&& !rs.getString("temp" + i)
										.equalsIgnoreCase("null")) {
							pu.sop("TempDate Check", "Date from DB is ["
									+ rs.getString("temp" + i) + "]");
							try {
								tempDate = rs.getString("temp" + i).trim();
							} catch (Exception ein) {
								pu.sop("EXCEPTION",
										"Exception in getting date from DB ["
												+ ein.getMessage() + "]");
							}
						}
						if (tempDate != null) {
							break;
						}
					}
				} else {
					try {
						tempDate = rs.getString("temp").trim();
					} catch (Exception e) {
						
						tempDate = null;
					}
				}
			}
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
			}
			// if( date != null ){
			if (date != null 
					&& !date.equalsIgnoreCase("null")
					&& tempDate != null
					&& !tempDate.equalsIgnoreCase("null")) {

				sDate = this.getFormatedDate(date);
				midDate = this.getFormatedDate(tempDate);
				 pu.sop( "Left SNo COmparison",
						 "date:["+date+"],SDate:["+sDate+"],"
								 + "tempDate:["+tempDate+"],midDate:["+midDate+"] "
								 + "and comparision value:["+sDate.compareTo(midDate)+"]");
				 
				 if (sDate.compareTo(midDate) == 0){
					 min_left = midID;
				 }
				if (sDate.compareTo(midDate) == 0 && (midID - startId) > 1) {
					tempDate = null;
					midDate = null;
					sDate = null;
					day = null;
					month = null;
					startDay = null;
					monthday = null;
					query = null;
					
					min_left = midID;
					leftSNO = this.getLeftSNO(date, startId, midID, tableName,
							dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass);
				} else if (sDate.compareTo(midDate) > 0
						&& (endId - midID) > 1) {      //input date is greater, go right
					tempDate = null;
					midDate = null;
					sDate = null;
					day = null;
					month = null;
					startDay = null;
					monthday = null;
					query = null;
					leftSNO = this.getLeftSNO(date, midID, endId, tableName,
							dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass);
				} else if (sDate.compareTo(midDate) < 0
						&& (endId - midID) > 1) {
					tempDate = null;
					midDate = null;
					sDate = null;
					day = null;
					month = null;
					startDay = null;
					monthday = null;
					query = null;
					leftSNO = this.getRightSNO(date, midID, endId, tableName,
							dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass);
				} else {
					System.out.println("======Left In else:::min_left::::"+min_left+" ,sDate is "+sDate+" and midDate is "+midDate+" start is is "+startId+" ,mid id is "+midID + " ,End Id is "+endId);
					if(min_left!=0) {   //if we have found inout date earlier
						System.out.println("======input date found earlier, found. going for min_left-1 "+(min_left-1));
						if(min_left-1>=1) {
							
							Date foundDate = getLeftSNOFinalLoop(date, min_left-1, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
							sDate = this.getFormatedDate(date);
							if(sDate.compareTo(foundDate)==0) {
								System.out.println("======min_left-1, found. going for min_left-2 "+(min_left-2));
								leftSNO = min_left-1;
								if(min_left-2>=1) {
									Date foundDate1 = getLeftSNOFinalLoop(date, min_left-2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
									sDate = this.getFormatedDate(date);
									if(sDate.compareTo(foundDate1)==0) {
										System.out.println("======min_left-2 found, ****END "+(min_left-2));
										leftSNO = min_left-2;
									}
								}
							}else {   //date is greater, last search for min-left-1, if not equals then min_left will be the final
								System.out.println("======min_left-1, NOT found. going for min_left-2 "+(min_left-2));
								if(min_left-2>=1) {
									Date foundDate1 = getLeftSNOFinalLoop(date, min_left-2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
									sDate = this.getFormatedDate(date);
									if(sDate.compareTo(foundDate1)==0) {
										System.out.println("======min_left-2 found, ****END "+(min_left-2));
										leftSNO = min_left-2;
									}else {
										System.out.println("======min_left-2 NOT found, ****END "+min_left);
										leftSNO = min_left;
									}
								}else {
									System.out.println("======min_left-2 is less than one, ****END "+min_left);
									leftSNO = min_left;
								}
							}
						}else {
							System.out.println("======min_left-1 less than one, ****END "+min_left);
							leftSNO = min_left; 
						}
					}else {     //if never found
						System.out.println("======input date never found earlier, found. going for midID-1 "+(midID-1));
						if(min_left-1>=1) {
							Date foundDate = getLeftSNOFinalLoop(date, midID-1, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
							sDate = this.getFormatedDate(date);
							if(sDate.compareTo(foundDate)==0) {
								System.out.println("======midID-1, found. going for midId-2 "+(midID-2));
								leftSNO = midID-1;
								if(min_left-2>=1) {
									Date foundDate1 = getLeftSNOFinalLoop(date, midID-2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
									sDate = this.getFormatedDate(date);
									if(sDate.compareTo(foundDate1)==0) {
										System.out.println("======midId-2, found. ****END "+(midID-2));
										leftSNO = midID-2;
									}
								}
							
							}else {   //date is greater, last search for midID-1, if not equals then min_left will be the final
								System.out.println("======midId-1, NOT found. going for midId-2 "+(midID-2));
									if(min_left-2>=1) {
										Date foundDate1 = getLeftSNOFinalLoop(date, midID-2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
										sDate = this.getFormatedDate(date);
										if(sDate.compareTo(foundDate1)==0) {
											System.out.println("======midId-2, found. ****END "+(midID-2));
											leftSNO = midID-2;
										}else {
											System.out.println("======midId-2, NOT found. ****END "+0);
											leftSNO = 0;
										}
									}else {
										System.out.println("======midId-2 is less than 1, ****END "+0);
										leftSNO = 0;
									}
							}
						}else {
							System.out.println("======min_left-1, less than one "+0);
							leftSNO = 0;
						}
						
					}
					
					
					//Save sno as midID, every time we find date for left 
					
					/*//21/22/23
					System.out.println("Left In else::: sDate is "+sDate+" and midDate is "+midDate+" start is is "+startId+" ,mid id is "+midID + " ,End Id is "+endId);
					if(sDate.compareTo(midDate) == 0 && ((midID - startId) == 1)) {
						int endIdFinal = 0;
						endIdFinal = endId;
						System.out.println("Calling final Loop "+(endIdFinal-1));
						Date foundDate = getLeftSNOFinalLoop(date, startId, endIdFinal-1, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
						sDate = this.getFormatedDate(date);
						if(sDate.compareTo(foundDate)==0) {
							if((endIdFinal-1)-startId==0) {
								leftSNO = endIdFinal-1;
							}else {
								//Still one search left
								System.out.println("Previous Id matched, now Calling final Loop "+(endIdFinal-2));
								foundDate = getLeftSNOFinalLoop(date, startId, endIdFinal-2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
								sDate = this.getFormatedDate(date);
								if(sDate.compareTo(foundDate)==0) {
									leftSNO = endIdFinal-2;
								}else {
									leftSNO = endIdFinal-1;
								}
							}
						}else {  //not found
							leftSNO = endIdFinal;
						}
						
						
						
					}else {
						System.out.print("Left: Both are not equal, End Id is "+endId+" and midId is "+midID);
						if(endId-midID==0) {
							leftSNO = 0;
						}else {
							System.out.print("Left: Unplanned:::  Both are not equal, End Id is "+endId+" and midId is "+midID);
						//leftSNO = startId+1;
						//leftSNO = leftSNO +1;
						//leftSNO = midID;
							int endIdFinal = 0;
							endIdFinal = endId;
							System.out.println("Calling final Loop "+(endIdFinal-1));
							Date foundDate = getLeftSNOFinalLoop(date, startId, endIdFinal-1, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
							sDate = this.getFormatedDate(date);
							if(sDate.compareTo(foundDate)==0) {
								if((endIdFinal-1)-startId==0) {
									leftSNO = endIdFinal-1;
								}else {
									//Still one search left
									System.out.println("Previous Id matched, now Calling final Loop "+(endIdFinal-2));
									foundDate = getLeftSNOFinalLoop(date, startId, endIdFinal-2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
									sDate = this.getFormatedDate(date);
									if(sDate.compareTo(foundDate)==0) {
										leftSNO = endIdFinal-2;
									}else {
										leftSNO = 0;
									}
								}
							}else {  //not found
								leftSNO = 0;
							}
							
						}
					}*/
				}
			} else if (tempDate == null) {
				pu.sop("temp date is null ", "temp date " + tempDate + "");
				//leftSNO = startId;
				leftSNO =0;
				return leftSNO;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
			}
			tempDate = null;
			midDate = null;
			sDate = null;
			day = null;
			month = null;
			startDay = null;
			monthday = null;
			query = null;
		}
		return leftSNO;
	}

	public long getRightSNO(String date, long startId, long endId,
			String tableName, String dateField, String trendType, String repId,
			boolean trendHaveSameLevalReport, String idColumn, String db_url,
			String db_name, String db_pass) {
		long rightSNO = 0;
		long midID = 0;
		int numOfAttr = 0;
		String tempDate = null;
		Date midDate = null;
		Date sDate = null;
		String day = null;
		String month = null;
		String startDay = null;
		String monthday = null;
		String query = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// pu.sop( "getRightSNO", "Going to calculate left ID.Start
			// ID:["+startId+"] and End Id:["+endId+"]");
			midID = (startId + endId) / 2;
			if (date != null) {
				// day = date.substring( date.lastIndexOf("-"), date.indexOf("
				// ") );
				// day = date.substring( date.lastIndexOf("-"));//COMMENTED BY
				// TANUJ ON 30-SEP-2018
				day = date.substring(date.lastIndexOf("-") + 1);// ADDED BY
																// TANUJ ON
																// 30-SEP-2018
				month = date.substring(5, date.lastIndexOf("-"));
			}
			if (date != null) {
				// monthday = date.substring( 4, date.indexOf(" ") );
				// monthday = date.substring( 4);//COMMENTED BY TANUJ ON
				// 30-SEP-2018
				monthday = date.substring(5);// ADDED BY TANUJ ON 30-SEP-2018
			}
			if (trendHaveSameLevalReport) {
				if (trendType.equalsIgnoreCase("weekly")) {
					startDay = this.getDayName(date);
					// pu.sop("dayname","Date of ["+date+"] and Day name is
					// ["+startDay+"]");
					if (dateField == null) {
						dateField = "week";
					}
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("monthly")) {
					if (dateField == null) {
						dateField = "month";
					}
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("yearly")) {
					if (dateField == null) {
						dateField = "year";
					}
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				} else {
					// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
					// %H:%i:%s') temp from "+tableName+" where
					// "+idColumn+"="+midID;
					query = "select DATE_FORMAT(" + dateField
							+ ",'%Y-%m-%d') temp from " + tableName + " where "
							+ idColumn + "=" + midID;
				}
			} else {
				if (tableName.toLowerCase().contains("_weekly")
						&& dateField.equalsIgnoreCase("week")) {
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (tableName.toLowerCase().contains("_monthly")
						&& dateField.equalsIgnoreCase("month")) {
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("monthly")) {

					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT("+dateField+",'%Y-%m'),"
							+"-'"+day+ "'),'%Y-%m-%d') temp from " + tableName
							+ " where " + idColumn + " = " + midID;
				
				} else if (tableName.toLowerCase().contains("_yearly")
						&& dateField.equalsIgnoreCase("year")) {
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				}
				/*
				 * else{ query = "select DATE_FORMAT("
				 * +dateField+",'%Y-%m-%d %H:%i:%s') temp from "
				 * +tableName+" where "+idColumn+"="+midID; } }
				 */
				else {
					if (dateField.indexOf(" or ") != -1) {
						String[] dfieldTemp = dateField.split(" or ");
						String dateFieldTemp = "";
						numOfAttr = dfieldTemp.length;
						for (int i = 0; i < dfieldTemp.length; i++) {
							// dateFieldTemp = dateFieldTemp + "
							// DATE_FORMAT("+dfieldTemp[i]+", '%Y-%m-%d
							// %H:%i:%s') temp"+i+",";
							dateFieldTemp = dateFieldTemp + " DATE_FORMAT("
									+ dfieldTemp[i] + ", '%Y-%m-%d') temp" + i
									+ ",";
						}
						if (dateFieldTemp.indexOf(",") != -1) {
							dateFieldTemp = dateFieldTemp.substring(0,
									dateFieldTemp.length() - 1);
						}
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select " + dateFieldTemp + " from " + tableName
								+ " where " + idColumn + "=" + midID;

					} else {
						// pu.sop( "inside else block called", "query ");
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select DATE_FORMAT(" + dateField
								+ ",'"+date_format+"') temp from " + tableName
								+ " where " + idColumn + "=" + midID;
					}
					// query = "select DATE_FORMAT(DATE(fb_ldate),'%Y-%m-%d
					// %H:%i:%s') ftemp, DATE_FORMAT(DATE(twit_ldate),'%Y-%m-%d
					// %H:%i:%s') ttemp,DATE_FORMAT(DATE(go_ldate),'%Y-%m-%d
					// %H:%i:%s') gtemp from social_profile_update where
					// "+idColumn+"="+midID;
				}
			}

			pu.sop("Right Sno Query","Query for getting mid id ["+midID+"] ["+query+"]");
			conn = msc.getConnection(db_url, db_name, db_pass);
			// pu.sop( "SNO Test", "Connection created.");
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			/*
			 * while( rs.next() ){ tempDate = rs.getString("temp"); }
			 */

			while (rs.next()) {
				if (numOfAttr > 0) {
					for (int i = 0; i < numOfAttr; i++) {
						if (rs.getString("temp" + i) != null
								&& !rs.getString("temp" + i)
										.equalsIgnoreCase("null")) {
							pu.sop("TempDate Check", "Date from DB is ["
									+ rs.getString("temp" + i) + "]");
							try {
								tempDate = rs.getString("temp" + i).trim();
							} catch (Exception ein) {
								pu.sop("EXCEPTION",
										"Exception in getting date from DB ["
												+ ein.getMessage() + "]");
							}
						}
						if (tempDate != null) {
							break;
						}
					}
				} else {
					try {
						tempDate = rs.getString("temp").trim();
					} catch (Exception e) {
						
						tempDate = null;
					}
				}
			}
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
			}
			// if( date != null ){
			if ((date != null && !date.equalsIgnoreCase("null"))
				&& (tempDate != null && !tempDate.equalsIgnoreCase("null")) ) {

				sDate = this.getFormatedDate(date);
				midDate = this.getFormatedDate(tempDate);
				 pu.sop( "Right SNo comparison",
						 "date:["+date+"],SDate:["+sDate+"],"
								 + "tempDate:["+tempDate+"],midDate:["+midDate+"] "
								 + "and comparision value:["+sDate.compareTo(midDate)+"]");
				
				 //this.max_value=37;
				 if (sDate.compareTo(midDate) == 0){
					 min_right = midID;
				 }
				if (sDate.compareTo(midDate) == 0 && (endId - midID) > 1) {
					tempDate = null;
					midDate = null;
					sDate = null;
					day = null;
					month = null;
					startDay = null;
					monthday = null;
					query = null;
					min_right = midID;
					rightSNO = this.getRightSNO(date, midID, endId, tableName,
							dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass);
				} else if (sDate.compareTo(midDate) < 0
						&& (midID - startId) > 1) {
					tempDate = null;
					midDate = null;
					sDate = null;
					day = null;
					month = null;
					startDay = null;
					monthday = null;
					query = null;
					rightSNO = this.getRightSNO(date, startId, midID, tableName,
							dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass);
				} else if (sDate.compareTo(midDate) > 0
						&& (midID - startId) > 1) {
					tempDate = null;
					midDate = null;
					sDate = null;
					day = null;
					month = null;
					startDay = null;
					monthday = null;
					query = null;
					rightSNO = this.getLeftSNO(date, startId, midID, tableName,
							dateField, trendType, repId,
							trendHaveSameLevalReport, idColumn, db_url, db_name,
							db_pass);
				} else {
					
					System.out.println("======Right In else:::min_right is :::"+min_right+" ,sDate is "+sDate+" and midDate is "+midDate+" start is is "+startId+" ,mid id is "+midID + " ,End Id is "+endId);
					
					if(min_right!=0) {   //if we have found input date earlier
						System.out.println(this.max_value+"======input date found earlier, found. going for min_right+1 "+(min_right+1));
						if((min_right+1)>=1 && (min_right+1)<=this.max_value) {
							
							Date foundDate = getRightSNOFinalLoop(date, min_right+1, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
							sDate = this.getFormatedDate(date);
							if(sDate.compareTo(foundDate)==0) {
								System.out.println("======min_right+1, found. going for min_right+2 "+(min_right+2));
								rightSNO = min_right+1;
								if((min_right+2)>=1 && (min_right+2)<=this.max_value) {
									Date foundDate1 = getRightSNOFinalLoop(date, min_right+2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
									sDate = this.getFormatedDate(date);
									if(sDate.compareTo(foundDate1)==0) {
										System.out.println("======min_right+2, found. ****END "+(min_right+2));
										rightSNO = min_right+2;
									}
								}	
							}else {   //date is greater, last search for min-left+2, if not equals then min_right will be the final
								System.out.println("======min_right+2, found. going for min_right+2 "+(min_right+2));
								if((min_right+2)>=1 && (min_right+2)<=this.max_value) {
									Date foundDate1 = getRightSNOFinalLoop(date, min_right+2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
									sDate = this.getFormatedDate(date);
									if(sDate.compareTo(foundDate1)==0) {
										System.out.println("======min_right+2, found. ****END "+(min_right+2));
										rightSNO = min_right+2;
									}else {
										System.out.println("======min_right+2, NOT found. ****END "+min_right);
										rightSNO = min_right;
									}
								}else {
									System.out.println("======min_right+2, is less than 1. ****END "+min_right);
									rightSNO = min_right;
								}
							}
						}else {
							rightSNO = min_right;
						}
					}else {     //if never found
						System.out.println("======It never found till now, last stages to go. going for midID+1 "+(midID+1));
						if((midID+1)>=1 && (midID+1)<=this.max_value) {
							Date foundDate = getRightSNOFinalLoop(date, midID+1, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
							sDate = this.getFormatedDate(date);
							if(sDate.compareTo(foundDate)==0) {
								System.out.println("======midID+1, found. going for midID+2 "+(midID+2) );
								rightSNO = midID+1;
								if((midID+2)>=1 && (midID+2)<=this.max_value) {
									Date foundDate1 = getRightSNOFinalLoop(date, midID+2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
									sDate = this.getFormatedDate(date);
									if(sDate.compareTo(foundDate1)==0) {
										System.out.println("======midID+2, found. ****END "+(midID+2));
										rightSNO = midID+2;
									}
								}
							}else {   //date is greater, last search for midID-1, if not equals then min_left will be the final
								System.out.println("======midID+1, NOT found. going for midID+2 "+(midID+2));
								if((midID+2)>=1 &&  (midID+2)<=this.max_value) {
									Date foundDate1 = getRightSNOFinalLoop(date, midID+2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
									sDate = this.getFormatedDate(date);
									if(sDate.compareTo(foundDate1)==0) {
										System.out.println("======midID+2, found. ****END "+(midID+2));
										rightSNO = midID+2;
									}else {
										System.out.println("======midID+2, NOT Found,  ****END "+(midID+2));
										rightSNO = 0;
									}
								}else {
									System.out.println("======midID+2 is less than 1, never found. ****END");
									rightSNO = 0;
								}
							}
						}else {
							System.out.println("======midID is less than 1, never found. ****END");	
							rightSNO = 0;
						}
						
					}
					
					
					/*System.out.print("Right In else::: sDate is "+sDate+" and midDate is "+midDate+" start is is "+startId+" ,mid id is "+midID +" ,End Id is "+endId);
					if(sDate.compareTo(midDate) == 0 && ((endId - midID) == 1)) {
						int startIdFinal = 0;
						startIdFinal = midID;
						System.out.println("Calling final Loop "+(startIdFinal+1));
						Date foundDate = getRightSNOFinalLoop(date, startId, startIdFinal+1, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
						sDate = this.getFormatedDate(date);
						if(sDate.compareTo(foundDate)==0) {
							if(endId-(startIdFinal+1)==0) {
								rightSNO = startIdFinal+1;
							}else {
								//Still one search left
								System.out.println("Previous ID matched, now Calling final Loop "+(startIdFinal+2));
								foundDate = getLeftSNOFinalLoop(date, startId, startIdFinal+2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
								sDate = this.getFormatedDate(date);
								if(sDate.compareTo(foundDate)==0) {
									rightSNO = startIdFinal+2;
								}else {
									rightSNO = startIdFinal+1;
								}
							}
						}else {  //not found
							rightSNO = startIdFinal;
						}
						
						
						
					}else {
						
						System.out.print("Right: Both are not equal, End Id is "+endId+" and midId is "+midID);
						if(endId-midID==0) {
							rightSNO = 0;
						}else {
							System.out.print("Right: Unplanned::: Both are not equal, End Id is "+endId+" and midId is "+midID);
						//leftSNO = startId+1;
						//leftSNO = leftSNO +1;
						//rightSNO = 0;
							int startIdFinal = 0;
							startIdFinal = midID;
							System.out.println("Calling final Loop "+(startIdFinal+1));
							Date foundDate = getRightSNOFinalLoop(date, startId, startIdFinal+1, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
							sDate = this.getFormatedDate(date);
							if(sDate.compareTo(foundDate)==0) {
								if(endId-(startIdFinal+1)==0) {
									rightSNO = startIdFinal+1;
								}else {
									//Still one search left
									System.out.println("Previous ID matched, now Calling final Loop "+(startIdFinal+2));
									foundDate = getLeftSNOFinalLoop(date, startId, startIdFinal+2, tableName, dateField, trendType, repId, trendHaveSameLevalReport, idColumn, db_url, db_name, db_pass);
									sDate = this.getFormatedDate(date);
									if(sDate.compareTo(foundDate)==0) {
										rightSNO = startIdFinal+2;
									}else {
										rightSNO = 0;
									}
								}
							}else {  //not found
								rightSNO = 0;
							}
							
							
						}
						
						
					}*/
				}
			} else if (tempDate == null) {
				pu.sop("temp date is null ", "temp date " + tempDate + "");
				rightSNO = endId;
				return rightSNO;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
			}
			tempDate = null;
			midDate = null;
			sDate = null;
			day = null;
			month = null;
			startDay = null;
			monthday = null;
			query = null;
		}
		return rightSNO;
	}

	
	public Date getLeftSNOFinalLoop(String date, long findID, String tableName,
			String dateField, String trendType, String repId,
			boolean trendHaveSameLevalReport, String idColumn, String db_url,
			String db_name, String db_pass) {
		
		
		//String[] final_dates = null;
		long leftSNO = 0;
		long midID = 0;
		int numOfAttr = 0;
		String tempDate = null;
		Date midDate = null;
		Date sDate = null;
		String day = null;
		String month = null;
		String startDay = null;
		String monthday = null;
		String query = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// pu.sop( "getLeftSNO", "Going to calculate left ID.Start
			// ID:["+startId+"] and End Id:["+endId+"]");
			//Gaurav midID = (startId + endId) / 2;
			midID = findID;
			
			if (date != null) {
				// day = date.substring( date.lastIndexOf("-"), date.indexOf("
				// ") );
				// day = date.substring( date.lastIndexOf("-"));//COMMENTED BY
				// TANUJ ON 30-SEP-2018
				day = date.substring(date.lastIndexOf("-") + 1);// ADDED BY
																// TANUJ ON
																// 30-SEP-2018
				month = date.substring(5, date.lastIndexOf("-"));
			}
			if (date != null) {
				// monthday = date.substring( 4, date.indexOf(" ") );
				// monthday = date.substring( 4 );//COMMENTED BY TANUJ ON
				// 30-SEP-2018
				monthday = date.substring(5);// ADDED BY TANUJ ON 30-SEP-2018
			}
			if (trendHaveSameLevalReport) {
				if (trendType.equalsIgnoreCase("weekly")) {
					startDay = this.getDayName(date);
					// pu.sop("dayname","Date of ["+date+"] and Day name is
					// ["+startDay+"]");
					if (dateField == null) {
						dateField = "week";
					}
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("monthly")) {
					if (dateField == null) {
						dateField = "month";
					}
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("yearly")) {
					if (dateField == null) {
						dateField = "year";
					}
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				} else {
					// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
					// %H:%i:%s') temp from "+tableName+" where
					// "+idColumn+"="+midID;
					query = "select DATE_FORMAT(" + dateField
							+ ",'%Y-%m-%d ') temp from " + tableName + " where "
							+ idColumn + "=" + midID;
				}
			} else {
				if (tableName.toLowerCase().contains("_weekly")
						&& dateField.equalsIgnoreCase("week")) {
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (tableName.toLowerCase().contains("_monthly")
						&& dateField.equalsIgnoreCase("month")) {
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				}  else if (trendType.equalsIgnoreCase("monthly")) {

					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT("+dateField+",'%Y-%m'),"
							+"-'"+ day + "'),'%Y-%m-%d') temp from " + tableName
							+ " where " + idColumn + " = " + midID;

				}  else if (tableName.toLowerCase().contains("_yearly")
						&& dateField.equalsIgnoreCase("year")) {
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				}
				/*
				 * else{ query = "select DATE_FORMAT("
				 * +dateField+",'%Y-%m-%d %H:%i:%s') temp from "
				 * +tableName+" where "+idColumn+"="+midID; } }
				 */

				else {
					if (dateField.indexOf(" or ") != -1) {
						String[] dfieldTemp = dateField.split(" or ");
						String dateFieldTemp = "";
						numOfAttr = dfieldTemp.length;
						for (int i = 0; i < dfieldTemp.length; i++) {
							// dateFieldTemp = dateFieldTemp + "
							// DATE_FORMAT("+dfieldTemp[i]+", '%Y-%m-%d
							// %H:%i:%s') temp"+i+",";
							dateFieldTemp = dateFieldTemp + " DATE_FORMAT("
									+ dfieldTemp[i] + ", '%Y-%m-%d') temp" + i
									+ ",";
						}
						if (dateFieldTemp.indexOf(",") != -1) {
							dateFieldTemp = dateFieldTemp.substring(0,
									dateFieldTemp.length() - 1);
						}
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select " + dateFieldTemp + " from " + tableName
								+ " where " + idColumn + "=" + midID;

					} else {
						// pu.sop( "inside else block calleds", "query ");
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select DATE_FORMAT(" + dateField
								+ ",'"+date_format+"') temp from " + tableName
								+ " where " + idColumn + "=" + midID;
					}
					// query = "select DATE_FORMAT(DATE(fb_ldate),'%Y-%m-%d
					// %H:%i:%s') ftemp, DATE_FORMAT(DATE(twit_ldate),'%Y-%m-%d
					// %H:%i:%s') ttemp,DATE_FORMAT(DATE(go_ldate),'%Y-%m-%d
					// %H:%i:%s') gtemp from social_profile_update where
					// "+idColumn+"="+midID;
				}
			}

			pu.sop("Left Sno Query","Query for getting mid id : ["+midID+"] ["+query+"]");
			conn = msc.getConnection(db_url, db_name, db_pass);
			// pu.sop( "SNO Test", "Connection created.");
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			/*
			 * while( rs.next() ){ tempDate = rs.getString("temp");
			 * 
			 * }
			 */

			while (rs.next()) {
				if (numOfAttr > 0) {
					for (int i = 0; i < numOfAttr; i++) {
						if (rs.getString("temp" + i) != null
								&& !rs.getString("temp" + i)
										.equalsIgnoreCase("null")) {
							pu.sop("TempDate Check", "Date from DB is ["
									+ rs.getString("temp" + i) + "]");
							try {
								tempDate = rs.getString("temp" + i).trim();
							} catch (Exception ein) {
								pu.sop("EXCEPTION",
										"Exception in getting date from DB ["
												+ ein.getMessage() + "]");
							}
						}
						if (tempDate != null) {
							break;
						}
					}
				} else {
					try {
						tempDate = rs.getString("temp").trim();
					} catch (Exception e) {
						
						tempDate = null;
					}
				}
			}
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
			}
			// if( date != null ){
			if (date != null 
					&& !date.equalsIgnoreCase("null")
					&& tempDate != null
					&& !tempDate.equalsIgnoreCase("null")) {

				sDate = this.getFormatedDate(date);
				midDate = this.getFormatedDate(tempDate);
				 pu.sop( "Left SNo COmparison",
						 "date:["+date+"],SDate:["+sDate+"],"
								 + "tempDate:["+tempDate+"],midDate:["+midDate+"] "
								 + "and comparision value:["+sDate.compareTo(midDate)+"]");
				 
				 
				 
			
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
			}
			tempDate = null;
			//midDate = null;
			sDate = null;
			day = null;
			month = null;
			startDay = null;
			monthday = null;
			query = null;
		}
		return midDate;
	}
	
	
	
	public Date getRightSNOFinalLoop(String date, long findID, 
			String tableName, String dateField, String trendType, String repId,
			boolean trendHaveSameLevalReport, String idColumn, String db_url,
			String db_name, String db_pass) {
		long rightSNO = 0;
		long midID = 0;
		int numOfAttr = 0;
		String tempDate = null;
		Date midDate = null;
		Date sDate = null;
		String day = null;
		String month = null;
		String startDay = null;
		String monthday = null;
		String query = null;
		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			// pu.sop( "getRightSNO", "Going to calculate left ID.Start
			// ID:["+startId+"] and End Id:["+endId+"]");
			//midID = (startId + endId) / 2;
			midID = findID;
			
			if (date != null) {
				// day = date.substring( date.lastIndexOf("-"), date.indexOf("
				// ") );
				// day = date.substring( date.lastIndexOf("-"));//COMMENTED BY
				// TANUJ ON 30-SEP-2018
				day = date.substring(date.lastIndexOf("-") + 1);// ADDED BY
																// TANUJ ON
																// 30-SEP-2018
				month = date.substring(5, date.lastIndexOf("-"));
			}
			if (date != null) {
				// monthday = date.substring( 4, date.indexOf(" ") );
				// monthday = date.substring( 4);//COMMENTED BY TANUJ ON
				// 30-SEP-2018
				monthday = date.substring(5);// ADDED BY TANUJ ON 30-SEP-2018
			}
			if (trendHaveSameLevalReport) {
				if (trendType.equalsIgnoreCase("weekly")) {
					startDay = this.getDayName(date);
					// pu.sop("dayname","Date of ["+date+"] and Day name is
					// ["+startDay+"]");
					if (dateField == null) {
						dateField = "week";
					}
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("monthly")) {
					if (dateField == null) {
						dateField = "month";
					}
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("yearly")) {
					if (dateField == null) {
						dateField = "year";
					}
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				} else {
					// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
					// %H:%i:%s') temp from "+tableName+" where
					// "+idColumn+"="+midID;
					query = "select DATE_FORMAT(" + dateField
							+ ",'%Y-%m-%d') temp from " + tableName + " where "
							+ idColumn + "=" + midID;
				}
			} else {
				if (tableName.toLowerCase().contains("_weekly")
						&& dateField.equalsIgnoreCase("week")) {
					query = "select DATE_FORMAT(STR_TO_DATE(concat(substr("
							+ dateField + ",2,length(" + dateField
							+ ")), ' ', '" + startDay
							+ "'),'%V%X %W'),'%Y-%m-%d %H:%i:%s') temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (tableName.toLowerCase().contains("_monthly")
						&& dateField.equalsIgnoreCase("month")) {
					query = "select if(substring(str_to_date(month,'%M'),6,2) = '"
							+ month
							+ "', DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'), DATE_ADD(DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date(month,'%M %Y'),'%Y-%m'),'"
							+ day
							+ "'),'%Y-%m-%d %H:%i:%s'),INTERVAL 1 MONTH)) temp from "
							+ tableName + " where " + idColumn + "=" + midID;
				} else if (trendType.equalsIgnoreCase("monthly")) {

					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT("+dateField+",'%Y-%m'),"
							+"-'"+day+ "'),'%Y-%m-%d') temp from " + tableName
							+ " where " + idColumn + " = " + midID;
				
				} else if (tableName.toLowerCase().contains("_yearly")
						&& dateField.equalsIgnoreCase("year")) {
					query = "select DATE_FORMAT(CONCAT(DATE_FORMAT(str_to_date("
							+ dateField + ",'%Y'),'%Y'),'" + monthday
							+ "'),'%Y-%m-%d %H:%i:%s') temp from " + tableName
							+ " where " + idColumn + "=" + midID;
				}
				/*
				 * else{ query = "select DATE_FORMAT("
				 * +dateField+",'%Y-%m-%d %H:%i:%s') temp from "
				 * +tableName+" where "+idColumn+"="+midID; } }
				 */
				else {
					if (dateField.indexOf(" or ") != -1) {
						String[] dfieldTemp = dateField.split(" or ");
						String dateFieldTemp = "";
						numOfAttr = dfieldTemp.length;
						for (int i = 0; i < dfieldTemp.length; i++) {
							// dateFieldTemp = dateFieldTemp + "
							// DATE_FORMAT("+dfieldTemp[i]+", '%Y-%m-%d
							// %H:%i:%s') temp"+i+",";
							dateFieldTemp = dateFieldTemp + " DATE_FORMAT("
									+ dfieldTemp[i] + ", '%Y-%m-%d') temp" + i
									+ ",";
						}
						if (dateFieldTemp.indexOf(",") != -1) {
							dateFieldTemp = dateFieldTemp.substring(0,
									dateFieldTemp.length() - 1);
						}
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select " + dateFieldTemp + " from " + tableName
								+ " where " + idColumn + "=" + midID;

					} else {
						// pu.sop( "inside else block called", "query ");
						// query = "select DATE_FORMAT("+dateField+",'%Y-%m-%d
						// %H:%i:%s') temp from "+tableName+" where
						// "+idColumn+"="+midID;
						query = "select DATE_FORMAT(" + dateField
								+ ",'"+date_format+"') temp from " + tableName
								+ " where " + idColumn + "=" + midID;
					}
					// query = "select DATE_FORMAT(DATE(fb_ldate),'%Y-%m-%d
					// %H:%i:%s') ftemp, DATE_FORMAT(DATE(twit_ldate),'%Y-%m-%d
					// %H:%i:%s') ttemp,DATE_FORMAT(DATE(go_ldate),'%Y-%m-%d
					// %H:%i:%s') gtemp from social_profile_update where
					// "+idColumn+"="+midID;
				}
			}

			pu.sop("Right Sno Query","Query for getting mid id ["+midID+"] ["+query+"]");
			conn = msc.getConnection(db_url, db_name, db_pass);
			// pu.sop( "SNO Test", "Connection created.");
			stmt = conn.createStatement();
			rs = stmt.executeQuery(query);
			/*
			 * while( rs.next() ){ tempDate = rs.getString("temp"); }
			 */

			while (rs.next()) {
				if (numOfAttr > 0) {
					for (int i = 0; i < numOfAttr; i++) {
						if (rs.getString("temp" + i) != null
								&& !rs.getString("temp" + i)
										.equalsIgnoreCase("null")) {
							pu.sop("TempDate Check", "Date from DB is ["
									+ rs.getString("temp" + i) + "]");
							try {
								tempDate = rs.getString("temp" + i).trim();
							} catch (Exception ein) {
								pu.sop("EXCEPTION",
										"Exception in getting date from DB ["
												+ ein.getMessage() + "]");
							}
						}
						if (tempDate != null) {
							break;
						}
					}
				} else {
					try {
						tempDate = rs.getString("temp").trim();
					} catch (Exception e) {
						
						tempDate = null;
					}
				}
			}
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
			}
			// if( date != null ){
			if ((date != null && !date.equalsIgnoreCase("null"))
				&& (tempDate != null && !tempDate.equalsIgnoreCase("null")) ) {

				sDate = this.getFormatedDate(date);
				midDate = this.getFormatedDate(tempDate);
				 pu.sop( "Right SNo comparison",
						 "date:["+date+"],SDate:["+sDate+"],"
								 + "tempDate:["+tempDate+"],midDate:["+midDate+"] "
								 + "and comparision value:["+sDate.compareTo(midDate)+"]");
				 
			} 
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			try {
				if (conn != null) {
					if (rs != null)
						rs.close();
					if (stmt != null)
						stmt.close();
					conn.close();
				}
			} catch (Exception ex) {
			}
			tempDate = null;
			//midDate = null;
			sDate = null;
			day = null;
			month = null;
			startDay = null;
			monthday = null;
			query = null;
		}
		return midDate;
	}
	
	
	
	public Date getFormatedDate(String date) {
		Date result = null;
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		SimpleDateFormat sdf3 = new SimpleDateFormat("yyyy-MM-dd HH");
		SimpleDateFormat sdf4 = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat sdf5 = new SimpleDateFormat("yyyy/MM/dd");
		try {
			result = new Date();
			if (date.matches("([0-9]{4})/([0-9]{2})/([0-9]{2})")) {
				// pu.sop( "Date Format of date", "Date is ["+date+"] and format
				// is :[yyyy/MM/dd]");
				result = sdf5.parse(date);
			} else if (date.matches("([0-9]{4})-([0-9]{2})-([0-9]{2})")) {
				// pu.sop( "Date Format of date is", "Date is ["+date+"] and
				// format is :[yyyy-MM-dd]");
				result = sdf4.parse(date);
			} else if (date
					.matches("([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2})")) {
				// pu.sop( "Date Format of date", "Date is ["+date+"] and format
				// is :[yyyy-MM-dd HH]");
				result = sdf3.parse(date);
			} else if (date.matches(
					"([0-9]{4})/([0-9]{2})/([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})")) {
				// pu.sop( "Date Format of date", "Date is ["+date+"] and format
				// is :[yyyy/MM/dd HH:mm:ss]");
				result = sdf2.parse(date);
			} else if (date.matches(
					"([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})")) {
				result = sdf1.parse(date);
				// result = sdf1.parse(date , new ParsePosition(0));
				// pu.sop( "Date Format of date", "Date is ["+date+"] and format
				// is :[yyyy-MM-dd HH:mm:ss] and converted date:["+result+"]");
			}
		} catch (Exception ex) {
			result = null;
			pu.sop("getFormatedDate", "No date format found :");
			ex.printStackTrace();
		}
		return result;
	}

	public String getDayName(String dateInString) {
		String dayname = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date date = sdf.parse(dateInString);
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(date);
			int day = calendar.get(Calendar.DAY_OF_WEEK);
			switch (day) {
				case 1 :
					dayname = "Sunday";
					break;
				case 2 :
					dayname = "Monday";
					break;
				case 3 :
					dayname = "Tuesday";
					break;
				case 4 :
					dayname = "Wednesday";
					break;
				case 5 :
					dayname = "Thursday";
					break;
				case 6 :
					dayname = "Friday";
					break;
				case 7 :
					dayname = "Saturday";
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return dayname;
	}
}
