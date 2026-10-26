package com.fapps.api;

import com.fapps.util.PrintUtilMsg;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.Callable;

public class ThreadProcessCSV implements Callable<String> {
	Events ev = new Events();
	PrintUtilMsg pu = new PrintUtilMsg();
	private final String sno;
	private final String inputDate;
	private final String tableNameTransaction;
	private final String type;
	private final String module;
	private final String circle;
	private final String tech;
	private final String db_URL;
	private final String db_User;
	private final String db_Pass;
	private final String threadName;
	private final String filePath;
	private final String tableNameSchedule;
	private final String db_Name;
	private final String host;
	private final String eventTable;
	private final String maxSno;
	private final String dateColumn;
	private final String dateColumnExits;
	private final int rawLimit;
	private final String logServerIP;
	private final String version;
	private final String serverInfoTable;
	private final String SourceColumn;
	private final String DestColumn;
	private final String queryBase;
	private final String querySuffix;
	private String dbUrl;
	private String dbUser;
	private String dbPass;
	private String host2;
	private String port;
	private String dbname;

	public ThreadProcessCSV(String inputDate, String sno, String tableNameTransaction, String type, String module, 
			String circle, String tech, String db_URL, String db_User, String db_Pass, int threadName, 
			String filePath, String tableNameSchedule, String db_Name, String host, String eventTable, 
			String maxSno, String dateColumn, String dateColumnExits, int rawLimit, String logServerIP, String version, String serverInfoTable, String SourceColumn, String DestColumn, String queryBase, String querySuffix) {
		this.sno = sno;
		this.inputDate = inputDate;
		this.tableNameTransaction = tableNameTransaction;
		this.type = type;
		this.module = module;
		this.circle = circle;
		this.tech = tech;
		this.db_URL = db_URL;
		this.db_User = db_User;
		this.db_Pass= db_Pass;
		this.threadName = "T"+threadName;
		this.filePath  = filePath;
		this.tableNameSchedule = tableNameSchedule;
		this.db_Name = db_Name;
		this.host = host;
		this.eventTable = eventTable;
		this.maxSno = maxSno;
		this.dateColumn = dateColumn;
		this.dateColumnExits = dateColumnExits;
		this.rawLimit = rawLimit;
		this.logServerIP = logServerIP;
		this.version = version;
		this.serverInfoTable = serverInfoTable;
		this.SourceColumn = SourceColumn;
		this.DestColumn = DestColumn;
		this.queryBase = queryBase;
		this.querySuffix = querySuffix;
    
	}
	
	@Override
	public String call() throws Exception {
		String final_data = "";
		String response = "error";
		System.out.println("ThreadProcessCSV:::: Starting "+threadName);
		System.out.println("Time:"+currentTime()+" [Debug] Process_Name: EP Server_IP: "+this.logServerIP+" Process_Version: "+this.version+" Log_Level: EVT_LOG  EVT_Type: Thread_Start Description: tableName:"+this.tableNameTransaction+"#threadName:"+threadName);
		String start_time  = currentTime();
		String status = createCSVViaLoadCommand();
		System.out.println("["+threadName+"] final status is "+status);
		if(status.split("EPBreak")[0].equals("true")) {
			response = "success";
		}else if(status.split("EPBreak")[0].equals("true_0")){
			response = "success_0";
		}else {
			response="failed";
		}
		System.out.println("Time:"+currentTime()+" [Debug] Process_Name: EP Server_IP: "+this.logServerIP+" Process_Version: "+this.version+" Log_Level: EVT_LOG  EVT_Type: Thread_Status Description: tableName:"+this.tableNameTransaction+"#threadName:"+threadName+"#response:"+response);
		final_data = threadName+","+response+","+this.tableNameTransaction+","+this.sno+","+this.type+","+this.module+","+this.circle+","+this.tech+","+start_time+","+status.split("EPBreak")[1];
		//return threadName+","+response+","+this.tableNameTransaction+","+this.sno+","+this.type+","+this.module+","+this.circle+","+this.tech+","+start_time+","+status.split("EPBreak")[1];
		updateStatusAndEvent(final_data);
		System.out.println("Time:"+currentTime()+" [Debug] Process_Name: EP Server_IP: "+this.logServerIP+" Process_Version: "+this.version+" Log_Level: EVT_LOG  EVT_Type: Thread_End Description: tableName:"+this.tableNameTransaction+"#threadName:"+threadName);
		return final_data;
	}
	
	public String[] getServerInfo(String serverInfoTable, String db_URL, String db_User, String db_Pass) {
	    String[] serverInfo = new String[6]; // url, username, password,
	    Connection conn = null;
	    PreparedStatement ps = null;
	    ResultSet rs = null;
	    try {
	        String query = "select dburl, dbuser, dbpass, dbhost, dbport, dbname from " + serverInfoTable + 
	                      " where server_status = 'A'";
	        conn = new com.fapps.db.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
	        ps = conn.prepareStatement(query);
	        // ps.setString(1, sno);
	        rs = ps.executeQuery();
		
	        if(rs.next()) {
	            serverInfo[0] = rs.getString("dburl");
	            serverInfo[1] = rs.getString("dbuser");
	            serverInfo[2] = rs.getString("dbpass");
				serverInfo[3] = rs.getString("dbhost");
				serverInfo[4] = rs.getString("dbport");
				serverInfo[5] = rs.getString("dbname");
	        }
	    } catch(Exception ex) {
	        ex.printStackTrace();
	        return null;
	    } finally {
	        try {
	            if(conn != null) {
	                if(rs != null) rs.close();
	                if(ps != null) ps.close();
	                conn.close();
	            }
	        } catch(Exception e) {}
	    }
	    return serverInfo;
	}

	private String createCSVViaLoadCommand() {
		String status = "false";
		String file_records = "0";
		//long tableMaxSno = 0;
		try {
			String query = "";
			String path  = "";
			String path1 = "";
			String typeValue = "";
			//String start_time  = currentTime();
			String[] serverinfo = getServerInfo(this.serverInfoTable, this.db_URL, this.db_User, this.db_Pass);
			if(serverinfo!=null) {
				this.dbUrl = serverinfo[0];
				this.dbUser = serverinfo[1];
				this.dbPass = serverinfo[2];
				this.host2  = serverinfo[3];
				this.port   = serverinfo[4];
				this.dbname = serverinfo[5];
			}else {
				System.out.println("["+threadName+"] ThreadProcessCSV:::: createCSVViaLoadCommand:::: Server info not found for server id "+this.host2);
			}
			System.out.println(serverinfo[0]+"--"+serverinfo[1]+"--"+serverinfo[2]+"--"+serverinfo[3]+"--"+serverinfo[4]+"--"+serverinfo[5]);

			String input_date = this.inputDate.replaceAll("-", "_");
			System.out.println("["+threadName+"] ThreadProcessCSV:::: In createCSVViaLoadCommand::: sno["+this.sno+"], Table_name["+this.tableNameTransaction+"], type["+this.type+"], module["+this.module+"], circle["+this.circle+"], tech["+this.tech+"]");
			ev.insertEpEvents(this.inputDate, "NA", this.eventTable, this.threadName, this.type, this.module, this.circle, this.tech, this.tableNameTransaction, this.sno, currentTime(), "NA",
					"event_start",this.db_URL, this.db_User, this.db_Pass);
			//updateScheduleTableTransactionInProcess(tableNameSchedule, tableNameTransaction,  threadName, sno, db_URL, db_User, db_Pass );
			// String[] columnNames = getTableColumnName(this.db_URL, this.db_User, this.db_Pass, 
			// 		this.tableNameTransaction, this.threadName, this.dateColumn, this.dateColumnExits);
			// String[] columnNames = getTableColumnName(this.dbUrl, this.dbUser, this.dbPass, 
			// 		this.tableNameTransaction, this.threadName, this.dateColumn, this.dateColumnExits);

			// bringing the source and destination column names in same order
			String[] SourceColumnNames = this.SourceColumn.split(",");
			String[] DestColumnNames = this.DestColumn.split(",");
			String[] columnNames = new String[SourceColumnNames.length];
			for(int i=0; i<SourceColumnNames.length; i++) {
				for(int j=0; j<DestColumnNames.length; j++) {
					if(SourceColumnNames[i].equalsIgnoreCase(DestColumnNames[j])) {
						columnNames[i] = DestColumnNames[j];
						break;
					}
				}
			}
			
			// String columnNamesHeader = SourceColumnNames[0];
			// need the columnNamesHeader to have dest column names
			// String columnNamesHeader = DestColumnNames[0];
			String columnNamesHeader = String.join(",", DestColumnNames);
			//System.out.println("["+threadName+"] ThreadProcessCSV:::: columnNamesHeader ["+columnNamesHeader+"]");
			columnNamesHeader = "'"+columnNamesHeader.replaceAll(",", "','")+"'";
			if(this.type.equalsIgnoreCase("r")) {
				typeValue = "raw";
			}else {
				typeValue = "process";
			}
			//path = String.valueOf(String.valueOf(path)) + "/" + tableName + ".csv";
			path = filePath + "/"+typeValue+"/"+typeValue+"_"+this.module+"_"+this.circle+"_"+this.tech+"_"+this.sno+"_"+input_date+"_"+time()+".csv";
			path1 = filePath + "/"+typeValue+"/"+typeValue+"_"+this.module+"_"+this.circle+"_"+this.tech+"_"+this.sno+"_"+input_date+"*";
			System.out.println("["+threadName+"] ThreadProcessCSV:::: In createCSVViaLoadCommand::: "+path);
			System.out.println("Time:"+currentTime()+" [Debug] Process_Name: EP Server_IP: "+this.logServerIP+" Process_Version: "+this.version+" Log_Level: EVT_LOG  EVT_Type: fileName Description: tableName:"+this.tableNameTransaction+"#threadName:"+threadName+"#fileName:"+path);
			System.out.println("["+threadName+"] ThreadProcessCSV:::: In createCSVViaLoadCommand::: "+path1);
			Process p2=null;
			try {
				//String[] rmcmd = new String[]{"rm", "-f", path1};
        		//p2 = Runtime.getRuntime().exec("rm "+path1);
				p2 = Runtime.getRuntime().exec(new String[]{"sh","remove_file.sh",path1});
        		//p2 = Runtime.getRuntime().exec(rmcmd);
        		int exitCode = p2.waitFor();
        		System.out.println("["+threadName+"] ThreadProcessCSV:::: In createCSVViaLoadCommand::: exitCode"+exitCode);
        		//p2.waitFor();
        	}catch(Exception ee) {
        	}finally {
            	try {
            		if(p2!=null) {
    					p2.destroy();
    				}
            	}catch(Exception e) {
            		e.printStackTrace();
            	}
        	}
			//Thread.sleep(1000);
			//String host = "127.0.0.1";
			/*query = "mysql -u" + username + " --password=\"" + passwd + "\" -h" + host + " " + dbname
                    + " -s -N -e  'select \"" + cname + "\" campaign_name, \"" + cid + "\" cid,\""
                    + cstartdate + "\" start_datetime,\"" + rulename + "\" rule_name,\"" + rulequery
                    + "\" filter_query,\"" + cenddate + "\" end_datetime,mobile,delivery_status,delivery_time,submit_time,\""
                    + campaign_date + "\" campaign_end_date, MRD_NO "+additionalColumns+" from " + tableName.toLowerCase()
                    + "' | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
			*/
			/*query = "mysql -u" + this.db_User + " --password=\"" + this.db_Pass + "\" -h" + host + " " + db_Name
                    + " -s -N -e  'select "+columnNamesHeader+" union all select " + columnNames +""
                    + " from " + this.tableNameTransaction+" where sno>"+snoRange[0]+" and sno<"+snoRange[1]
                    + "' | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";

			*/
			/*query = "mysql -u" + this.db_User + " --password=\"" + this.db_Pass + "\" -h" + host + " " + db_Name
                    + " -s -N -e  \"select "+columnNamesHeader+" union all select " + columnNames[1] +""
                    + " from " + this.tableNameTransaction+" where sno>="+snoRange[0]+" and sno<="+snoRange[1]
                    + " and  DATE_FORMAT("+this.dateColumnTransactionTable+",'%Y-%m-%d')=DATE_FORMAT('"+this.inputDate+"','%Y-%m-%d')\" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
			*/
			// long currentMaxSno = getTableMaxSno(this.dbUrl, this.dbUser, this.dbPass, this.tableNameTransaction, 
			long currentMaxSno = getTableMaxSno(this.dbUrl, this.dbUser, this.dbPass, this.tableNameTransaction, 
					this.threadName, this.dateColumn, this.dateColumnExits);
			long newMaxSno = currentMaxSno;
			long updateMaxSno = Long.parseLong(this.maxSno);
			long newRowLimit = this.rawLimit;
			if(currentMaxSno > 0) {
				long diff = currentMaxSno - Long.parseLong(this.maxSno);
				if(diff>this.rawLimit) {
					
					newMaxSno = Long.parseLong(this.maxSno) + this.rawLimit; 
				}else if (diff<this.rawLimit) {
					newRowLimit = Math.max(0, diff);
				}
			}
			
			// String[] serverinfo = getServerInfo(this.serverInfoTable, this.db_URL, this.db_User, this.db_Pass);
			// if(serverinfo!=null) {
			// 	this.dbUrl = serverinfo[0];
			// 	this.dbUser = serverinfo[1];
			// 	this.dbPass = serverinfo[2];
			// 	this.host2   = serverinfo[3];
			// 	this.port   = serverinfo[4];
			// 	this.dbname = serverinfo[5];
			// }else {
			// 	System.out.println("["+threadName+"] ThreadProcessCSV:::: createCSVViaLoadCommand:::: Server info not found for server id "+this.host2);
			// }

			// if(this.dateColumnExits.equalsIgnoreCase("Y")) {
			// 	query = "mysql -u" + this.db_User + " --password=\"" + this.db_Pass + "\" -h" + host + " " + db_Name + " -s -N -e  \"select "+columnNamesHeader+" union all select " + columnNames[1] +""
            //         + " from " + this.tableNameTransaction+" where "+this.dateColumn+" >"+this.maxSno+" and "+this.dateColumn+" <="+newMaxSno+" \" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
			// }else {
			// 	query = "mysql -u" + this.db_User + " --password=\"" + this.db_Pass + "\" -h" + host + " " + db_Name + " -s -N -e  \"select "+columnNamesHeader+" union all (select " + columnNames[1] +""
	        //         + " from " + this.tableNameTransaction+" limit "+updateMaxSno+","+this.rawLimit+") \" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
			// }
			// if(this.dateColumnExits.equalsIgnoreCase("Y")) {
			// 	query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all select " + columnNames[1] +""
            //         + " from " + this.tableNameTransaction+" where "+this.dateColumn+" >"+this.maxSno+" and "+this.dateColumn+" <="+newMaxSno+" \" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
			// }else {
			// 	query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all (select " + columnNames[1] +""
	        //         + " from " + this.tableNameTransaction+" limit "+updateMaxSno+","+this.rawLimit+") \" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
			// }

			// String sourceColumnSql = String.join(",", SourceColumnNames);
			// if(this.dateColumnExits.equalsIgnoreCase("Y")) {
			// 	query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all select " + sourceColumnSql +""
            //         + " from " + this.tableNameTransaction+" where "+this.dateColumn+" >"+this.maxSno+" and "+this.dateColumn+" <="+newMaxSno+" \" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
			// }else {
			// 	query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all (select " + sourceColumnSql +""
	        //         + " from " + this.tableNameTransaction+" limit "+updateMaxSno+","+this.rawLimit+") \" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
			// }
//			String sourceColumnSql = String.join(",", SourceColumnNames);
//			if(this.dateColumnExits.equalsIgnoreCase("Y")) {
//				// query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all select " + sourceColumnSql +""
//                //      + " from " + this.tableNameTransaction+" where "+this.dateColumn+" >"+this.maxSno+" and "+this.dateColumn+" <="+newMaxSno+" \" | sed 's/"/\\"/g;s/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' " +"| tr -d '\\r' >> '" +path + "'";
//				query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P " + serverinfo[4] + " " + serverinfo[5] + " -s -N -e \"select " + columnNamesHeader + " union all select " + sourceColumnSql + ""
//					+ " from " + this.tableNameTransaction + " where " + this.dateColumn + " >" + this.maxSno + " and " + this.dateColumn + " <=" + newMaxSno + " \" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' | tr -d '\\r' >> '" + path + "'"; 
//			}else {
//				// query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all (select " + sourceColumnSql +""
//	            //      + " from " + this.tableNameTransaction+" limit "+updateMaxSno+","+this.rawLimit+") \" | sed 's/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' "+"| tr -d '\\r' >> '" + path + "'";
//				// query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all (select " + sourceColumnSql +""
//	            //      + " from " + this.tableNameTransaction+" limit "+updateMaxSno+","+newRowLimit+") \" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' "+"| tr -d '\\r' >> '" + path + "'";
//				query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all (select " + sourceColumnSql +""
//	                + " from " + this.tableNameTransaction+" limit "+this.maxSno+","+this.rawLimit+") \" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' "+"| tr -d '\\r' >> '" + path + "'"; // changed this.maxsno and this.rawLimiton 26th feb 26
//			}
			// CHANGE START: FIXED AT / VT / LT LOGIC

			String flag = this.dateColumnExits;
			String sourceColumnSql = String.join(",", SourceColumnNames);

			if(flag.equalsIgnoreCase("AT")) {

			    // Integer PK incremental (SAFE)
				 query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] +
						 " -P " + serverinfo[4] + " " + serverinfo[5] + " -s -N -e \"select " + columnNamesHeader + " union all select " + sourceColumnSql + ""
					        + " from " + this.tableNameTransaction + 
					        " where " + this.dateColumn + " >" + this.maxSno + " and " +
					        this.dateColumn + " <=" + newMaxSno +
					        " \" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' | tr -d '\\r' >> '" +
					        path + "'"; 

			}
			else if(flag.equalsIgnoreCase("VT")) {

			    // Varchar PK incremental (SAFE)
				 query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] +
						 "\" -h" + serverinfo[3] + " -P " + serverinfo[4] + " " + serverinfo[5] +
						 " -s -N -e \"select " + columnNamesHeader + " union all select " + sourceColumnSql + ""
					        + " from " + this.tableNameTransaction + " where " + this.dateColumn + " > '" + this.maxSno +
					        "' and " + this.dateColumn + " <='" + newMaxSno + "' \" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' | tr -d '\\r' >> '" + path + "'"; 

			}
			else if(flag.equalsIgnoreCase("LT")) {

			    // LIMIT BASED (UNCHANGED)
				query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all (select " + sourceColumnSql +"" 
				        + " from " + this.tableNameTransaction+" order by "+ this.dateColumn +" limit "+this.maxSno+","+this.rawLimit+") \" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' "+"| tr -d '\\r' >> '" + path + "'";
				
			}
//			else if(flag.equalsIgnoreCase("DT")) {
//
//			    // DATE-WISE SINGLE DAY (NEW)
//				String suffix = (this.querySuffix == null || this.querySuffix.trim().isEmpty() || this.querySuffix.equalsIgnoreCase("null")) 
//		                ? "" : " " + this.querySuffix;
//
//		query = "mysql -u" + serverinfo[1] +
//		        " --password=\"" + serverinfo[2] + "\"" +
//		        " -h" + serverinfo[3] +
//		        " -P " + serverinfo[4] +
//		        " " + serverinfo[5] +
//		        " -s -N -e \"select " + columnNamesHeader +
//		        " union all (select " + sourceColumnSql +
//		        " from " + this.tableNameTransaction +
//		        " where " + this.dateColumn + " >= '" + this.inputDate + " 00:00:00'" +
//		        " and " + this.dateColumn + " <= '" + this.inputDate + " 23:59:59'" +
//		        suffix +
//		        ")\" | sed 's/\"/\\\\\"/g;s/\t/\"|\"/g;s/\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\N/g;' | tr -d '\r' >> '" + path + "'";
//			}
//			else if(flag.equalsIgnoreCase("JT")) {
//
//			    // JOIN-BASED SINGLE DAY (NEW)
//			    String baseFrom = (this.queryBase == null || this.queryBase.trim().isEmpty() || this.queryBase.equalsIgnoreCase("null")) ? this.tableNameTransaction : this.queryBase;
//			    String suffix = (this.querySuffix == null || this.querySuffix.trim().isEmpty() || this.querySuffix.equalsIgnoreCase("null")) ? "" : " " + this.querySuffix;
//
//			    query = "mysql -u" + serverinfo[1] +
//			            " --password=\"" + serverinfo[2] + "\"" +
//			            " -h" + serverinfo[3] +
//			            " -P " + serverinfo[4] +
//			            " " + serverinfo[5] +
//			            " -s -N -e \"select " + columnNamesHeader +
//			            " union all (select " + sourceColumnSql +
//			            " from " + baseFrom +
//			            " where " + this.dateColumn + " >= '" + this.inputDate + " 00:00:00'" +
//			            " and " + this.dateColumn + " <= '" + this.inputDate + " 23:59:59'" +
//			            suffix +
//			            ")\" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' | tr -d '\\r' >> '" + path + "'";
//
//			}
			else if(flag.equalsIgnoreCase("Y")) {

			    // OLD DATE LOGIC (UNCHANGED)
			    query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P " + serverinfo[4] + " " + serverinfo[5] + " -s -N -e \"select " + columnNamesHeader + " union all select " + sourceColumnSql + ""
			        + " from " + this.tableNameTransaction + " where " + this.dateColumn + " >" + this.maxSno + " and " + this.dateColumn + " <=" + newMaxSno + " \" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' | tr -d '\\r' >> '" + path + "'"; 

			}
			else {

			    // OLD N LOGIC (UNCHANGED)
			    query = "mysql -u" + serverinfo[1] + " --password=\"" + serverinfo[2] + "\" -h" + serverinfo[3] + " -P "+ serverinfo[4]+" " + serverinfo[5] + " -s -N -e  \"select "+columnNamesHeader+" union all (select " + sourceColumnSql +"" 
			        + " from " + this.tableNameTransaction+" limit "+this.maxSno+","+this.rawLimit+") \" | sed 's/\"/\\\\\"/g;s/\\t/\"|\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;s/\"NULL\"/\\\\N/g;' "+"| tr -d '\\r' >> '" + path + "'";
			}

			// CHANGE END
			//mysqldump --set-gtid-purged=OFF --no-tablespaces --single-transaction -h 172.23.11.42 -P 3308 -u Protean -pCkyc@2026 ckyc cust_images --skip-add-drop-table --no-create-info --insert-ignore --where="columnname > 0 and columnname < 100" > cust_images_10Cr_1.sql
			//mysqldump --set-gtid-purged=OFF --no-tablespaces --single-transaction -h 172.23.11.42 -P 3308 -u Protean -pCkyc@2026 ckyc cust_images --skip-add-drop-table --no-create-info --insert-ignore --where="1 LIMIT 0, 100000000" > cust_images_10Cr_1.sql
			Process p = null;
			System.out.println("query to create file " + query);
            String[] commands = new String[]{"bash", "-c", query};
            try {
                p = Runtime.getRuntime().exec(commands);
                p.waitFor();
                System.out.println("["+threadName+"] Success: file written ");
                System.out.println("Time:"+currentTime()+" [Debug] Process_Name: EP Server_IP: "+this.logServerIP+" Process_Version: "+this.version+" Log_Level: EVT_LOG  EVT_Type: FileCreate Description: tableName:"+this.tableNameTransaction+"#threadName:"+threadName+"#Success: file written");
                status = "true";
                
            }catch (Exception ein) {
            	status = "false";
            	System.out.println("Time:"+currentTime()+" [Debug] Process_Name: EP Server_IP: "+this.logServerIP+" Process_Version: "+this.version+" Log_Level: Error  EVT_Type: FileCreate Description: tableName:"+this.tableNameTransaction+"#threadName:"+threadName+"#Fail: file written");
                ein.printStackTrace();
            }finally {
            	try {
            		if(p!=null) {
                    	p.destroy();
                    }
            	}catch(Exception ee) {
            		ee.printStackTrace();
            	}
            }
            String data;
    		status = "true_0";
    		file_records = "0";
            Process p1=null;
            
            BufferedReader br = null;
            try {
                p1 = Runtime.getRuntime().exec("wc -l "+path);
				p1.waitFor();
                br = new BufferedReader(
                    new InputStreamReader(p1.getInputStream()));
                while ((data = br.readLine()) != null) {
                    System.out.println("line: " + data);
                	if(Integer.parseInt(data.split(" ")[0]) > 1) {
                		file_records = data.split(" ")[0];
                		System.out.println("File Count is: "+file_records);
                		System.out.println("Time:"+currentTime()+" [Debug] Process_Name: EP Server_IP: "+this.logServerIP+" Process_Version: "+this.version+" Log_Level: EVT_LOG  EVT_Type: rawCount Description: tableName:"+this.tableNameTransaction+"#threadName:"+threadName+"#rawCount:"+file_records);
                		status = "true";
//						if(this.dateColumnExits.equalsIgnoreCase("Y")){//updated on 26th feb 26
//                		
//                		updateMaxSno(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction, 
//                				this.threadName, this.tableNameSchedule, newMaxSno);
//						}else{
//							long updatedsno = Long.parseLong(this.maxSno) + Long.parseLong(file_records) - 1;
//							updateMaxSno(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction, 
//                				this.threadName, this.tableNameSchedule, updatedsno);
//							
//						}//till here
                		// CHANGE START: FIX maxSno update for new flags

                		if(flag.equalsIgnoreCase("AT") || flag.equalsIgnoreCase("VT")) { //09-04-2026

                		    long updatedsno = Long.parseLong(this.maxSno) + Long.parseLong(file_records) - 1;

                		    updateMaxSno(this.db_URL, this.db_User, this.db_Pass,
                		            this.tableNameTransaction,
                		            this.threadName,
                		            this.tableNameSchedule,
                		            updatedsno);

                		}
                		else if(flag.equalsIgnoreCase("LT")) {

                		    long newOffset = Long.parseLong(this.maxSno) + this.rawLimit;

                		    updateMaxSno(this.db_URL, this.db_User, this.db_Pass,
                		            this.tableNameTransaction,
                		            this.threadName,
                		            this.tableNameSchedule,
                		            newOffset);

                		}
                		else if(flag.equalsIgnoreCase("Y")) {

                		    updateMaxSno(this.db_URL, this.db_User, this.db_Pass,
                		            this.tableNameTransaction,
                		            this.threadName,
                		            this.tableNameSchedule,
                		            newMaxSno);

                		}
                		else if(flag.equalsIgnoreCase("DT") || flag.equalsIgnoreCase("JT")) {
                		    // NO-OP: DT and JT do not require maxSno updates (not incremental)
                		}
                		else {

                		    long updatedsno = Long.parseLong(this.maxSno) + Long.parseLong(file_records) - 1;

                		    updateMaxSno(this.db_URL, this.db_User, this.db_Pass,
                		            this.tableNameTransaction,
                		            this.threadName,
                		            this.tableNameSchedule,
                		            updatedsno);
                		}

                		// CHANGE END
                	}else {
                		System.out.println("File Count is: "+data.split(" ")[0]);
                		System.out.println("Time:"+currentTime()+" [Debug] Process_Name: EP Server_IP: "+this.logServerIP+" Process_Version: "+this.version+" Log_Level: EVT_LOG  EVT_Type: rawCount Description: tableName:"+this.tableNameTransaction+"#threadName:"+threadName+"#rawCount:0");
                		status = "true_0";
                	}
                }
                // p1.waitFor();
                System.out.println ("exit: " + p1.exitValue());
            } catch (Exception e) {
            	file_records = "0";
            	status = "true_0";
				System.out.println("Exception while reading a CSV file to check line count "+e.toString());
				System.out.println("Time:"+currentTime()+" [Debug] Process_Name: EP Server_IP: "+this.logServerIP+" Process_Version: "+this.version+" Log_Level: Error  EVT_Type: rawCount Description: tableName:"+this.tableNameTransaction+"#threadName:"+threadName);
				e.printStackTrace();
            }finally {
            	try {
            		if(br!=null) {
    					br.close();
    				}
    				if(p1!=null) {
    					p1.destroy();
    				}
            	}catch(Exception e) {
            		e.printStackTrace();
            	}
            }
		}catch(Exception e) {
			status = "false";
			System.out.print("ThreadProcessCSV:::: createCSVViaLoadCommand:::: Exception while processing CSV "+e.toString());
			e.printStackTrace();
		}
		return status+"EPBreak"+file_records;
	}
	
	private void updateMaxSno(String db_URL, String db_User, String db_Pass, String tableNameTransaction, 
			String threadName, String tableNameSchedule, long tableMaxSno) {
		Connection conn = null;
		PreparedStatement ps = null;
		String query    = null;
		//long tableMaxSno = getTableMaxSno(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction, this.threadName);
		try {
			query = "update "+tableNameSchedule+ " set maxSno=? where table_name=?";
			conn = new com.fapps.db.MySQLConnection().getConnection(db_URL,db_User,db_Pass);
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(query);
					ps.setString(1, Long.toString(tableMaxSno));
					ps.setString(2, tableNameTransaction);
					ps.addBatch();
					pu.sop("["+threadName+"] update query after batch addition",	ps.toString());
					ps.executeBatch();
					conn.commit();
		}catch(Exception ex) {
			System.out.println("["+threadName+"] EXCEPTION JAR: updateMaxSno: ["+threadName+"]" +ex.toString());
			ex.printStackTrace();
		}finally{
			try{
				if( conn != null ){
					if( ps != null ) {
						ps.close();
					}
					conn.close();
				}
			}catch( Exception e ){}
		}
	}

	private long getTableMaxSno(String db_url, String db_User, String db_pass, String table, String threadName,
			String dateColumn, String dateColumnExits) {
		long response = 0;
		Connection conn = null;
		Statement stmt  = null;
		ResultSet rs    = null;
		String query    = null;
		int rowCount = 0;
		try{
			if(dateColumnExits.equalsIgnoreCase("Y")||dateColumnExits.equalsIgnoreCase("AT")||dateColumnExits.equalsIgnoreCase("VT")||dateColumnExits.equalsIgnoreCase("LT")) {
				query="select max("+dateColumn+") as sno from "+table;
			}else {
				// query="select count(*) as sno from "+table;
				return 0; //updtaed on 26th
			}
			System.out.println("["+threadName+"] Query API Log:  getTableMaxSno: " +query);
			conn = new com.fapps.db.MySQLConnection().getConnection(db_url,db_User,db_pass);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rs   = stmt.executeQuery( query );
			rs.last();
			rowCount = rs.getRow();
			rs.beforeFirst();
			//pu.sop("Row Count", "Total rows:["+rowCount+"]");
			if( rowCount > 0 ){
				while( rs.next() ){
					response = rs.getLong("sno");
				}	
			}
			System.out.println("["+threadName+"] MaxSno: "+response);
			}catch( Exception ex ){
				System.out.println("["+threadName+"] EXCEPTION JAR: ::  getTableMaxSno: "+ex.toString());
				response = 0;
				ex.printStackTrace();
			}finally{
				try{
					if( conn != null ){
						if( rs != null ) {
							rs.close();
						}
						if( stmt != null ) {
							stmt.close();
						}
						conn.close();
					}
				}catch( Exception e ){}
			}
		return response;
	}
	
	private String[] getTableColumnName(String db_url, String db_User, String db_pass, String table, 
			String threadName, String columnName, String dateColumnExits) {
		String[] response = null;
		Connection conn = null;
		Statement stmt  = null;
		ResultSet rs    = null;
		String query    = null;
		try{
			query="select * from "+table+" limit 1";
			System.out.println("["+threadName+"] Query API Log:  getTableColumnName: " +query);
			System.out.println("["+threadName+"] Query API Log:  getTableColumnName: " +db_url+","+db_User+","+db_pass);
			conn = new com.fapps.db.MySQLConnection().getConnection(db_url,db_User,db_pass);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
			rs   = stmt.executeQuery( query );
			if (rs != null) {
				response = new String[2];
		        ResultSetMetaData columns = rs.getMetaData();
		        int i = 0;
		        while (i < columns.getColumnCount()) {
		        	i++;
		        	System.out.print(columns.getColumnName(i) + "\t");
		        	//columnNames.add(columns.getColumnName(i));
		        	if(this.dateColumnExits.equalsIgnoreCase("Y")) {
		        		if(!columns.getColumnName(i).equals(columnName)) {
		        			response[0] = response[0]+","+columns.getColumnName(i);
		        			response[1] = response[1]+","+"replace("+columns.getColumnName(i)+",'\\\"',\\\"'\\\")";
		        		}
		        	}else {
		        		response[0] = response[0]+","+columns.getColumnName(i);
		        		response[1] = response[1]+","+"replace("+columns.getColumnName(i)+",'\\\"',\\\"'\\\")";
		        	}
		        }
		        System.out.println("["+threadName+"] i: "+i);
			}
			
			response[0]  = response[0].replace("null", "");
			response[1]  = response[1].replace("null", "");
			response[0]  = response[0].substring(1);
			response[1]  = response[1].substring(1);
			System.out.println("["+threadName+"] Columns are: "+response[0]);
			System.out.println("["+threadName+"] Columns are: "+response[1]);
		}catch( Exception ex ){
			System.out.println("["+threadName+"] EXCEPTION JAR: ::  getTableColumnName: "+ex.toString());
			response = null;
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
		return response;
	}
	
	/*private void updateScheduleTableTransactionInProcess(String scheduleTableName, String transactionTableName, String threadName, String sno, String db_URL, String db_User, String db_Pass ) {
		Connection conn = null;
		PreparedStatement ps = null;
		String query    = null;
		try {
			String sync_status = "InProcess_EP";
			query = "update "+scheduleTableName+ " set sync_status=? where sno=? and table_name=?";
			conn = new com.fapps.db.MySQLConnection().getConnection(db_URL,db_User,db_Pass);
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(query);
			ps.setString(1,sync_status);
			ps.setString(2, sno);       
			ps.setString(3, transactionTableName);
			ps.addBatch();
			pu.sop("["+threadName+"] Insert query after batch addition",	ps.toString());
			ps.executeBatch();
			conn.commit();
		}catch(Exception ex) {
			System.out.println("["+threadName+"] EXCEPTION JAR: updateScheduleTableTransactionInProcess: ["+threadName+"]" +ex.toString());
			//status = false;
			ex.printStackTrace();
		}finally{
			try{
				if( conn != null ){
					if( ps != null )
						ps.close();
					conn.close();
				}
			}catch( Exception e ){}
		}
	}*/
	
	/*public String[] getSnoFromTransactionTable(String transactionTableName, String date, String date_column, String db_url, String db_User, String db_pass){
		String[] snoRange  = null;
		String binaryDate = "";
		try{
			Source sbs = new Source();
			binaryDate = date_column+"~~"+date+"~~daily";
			snoRange = sbs.binarySearch("yes", binaryDate, transactionTableName, db_url, db_User, db_pass);
		}catch( Exception ex ){
			System.out.println("EXCEPTION JAR: getDataFromSourceTable: "+ex.toString());
			snoRange = null;
			ex.printStackTrace();
		}
		return snoRange;
	}*/
	
	private String currentTime() {
		String current_time = "";
		try {
			SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Calendar c1 = Calendar.getInstance();
			Date date = new Date();
			c1.setTime(date);
			//Date nextDate = c1.getTime();
			current_time =sdf_date.format(c1.getTime());
		}catch(Exception E) {
			System.out.println("currentTime::   Exception while computing current date");
			E.printStackTrace();
		}
		return current_time;
	}
	
	private String time() {
		String current_time = "";
		try {
			SimpleDateFormat sdf_date = new SimpleDateFormat("HH_mm_ss");
			Calendar c1 = Calendar.getInstance();
			Date date = new Date();
			c1.setTime(date);
			//Date nextDate = c1.getTime();
			current_time =sdf_date.format(c1.getTime());
		}catch(Exception E) {
			System.out.println("currentTime::   Exception while computing current date");
			E.printStackTrace();
		}
		return current_time;
	}
	private void updateStatusAndEvent(String final_data) {
        try {
        	String updateScheduleTableStatus = "true";
        	updateScheduleTableStatus = "true";
            System.out.println("["+final_data.split(",")[0]+"] processScheduledTransactions:::: Response from CSV Thread::::: "+new Date()+ " :::: "+final_data);
            if(final_data.split(",")[1].equals("failed")) {
            	updateScheduleTableStatus = "false";
            }else if(final_data.split(",")[1].equals("success_0")) {
            	updateScheduleTableStatus = "true_0";
            }else {
            	updateScheduleTableStatus = "true";
            }
            try {
            	updateScheduleTableTransaction( updateScheduleTableStatus, this.tableNameSchedule,final_data.split(",")[2] ,final_data.split(",")[0],  
            		final_data.split(",")[3], 
            	this.db_URL, this.db_User, this.db_Pass, this.inputDate);
            }catch(Exception e) {
            	e.printStackTrace();
            }
           	ev.insertEpEvents(this.inputDate,updateScheduleTableStatus, this.eventTable, final_data.split(",")[0], 
         			final_data.split(",")[4], final_data.split(",")[5], final_data.split(",")[6], final_data.split(",")[7],  final_data.split(",")[2], final_data.split(",")[3], 
           			final_data.split(",")[8], final_data.split(",")[9], "event_end",db_URL, db_User, db_Pass);
           	/*insertEpEvents(this.inputDate,updateScheduleTableStatus, this.eventTable, final_data.split(",")[0], 
           			final_data.split(",")[4], final_data.split(",")[5], final_data.split(",")[6], final_data.split(",")[7],  final_data.split(",")[2], final_data.split(",")[3], 
           			final_data.split(",")[8], final_data.split(",")[9], db_URL, db_User, db_Pass);*/
        }catch (Exception eee) {
        	System.out.println("Exception while processing in updateStatusAndEvent function");
            eee.printStackTrace();
        }
	}
	
	private void updateScheduleTableTransaction(String status, String scheduleTableName, String transactionTableName, String threadName, String sno, 
			String db_URL, String db_User, String db_Pass, String inputDate ) {
		Connection conn = null;
		PreparedStatement ps = null;
		String query    = null;
		String last_export_time= "";
		try {
			String sync_status = "";
			if(status.equalsIgnoreCase("true")) {
				// sync_status = "Ready_for_Export_Fre";
				sync_status = "Ready_for_Export_Fre";//updated on 26th feb 26
			}else if(status.equalsIgnoreCase("true_0")){
				sync_status = "Ready_for_Export_Fre_0";//	updated on 26th feb 26
			}else{
				sync_status = "Error_EP_Fre";//	updated on 26th feb 26
			}
			query = "update "+scheduleTableName+ " set last_export_timestamp=?, sync_status=? where sno=? and table_name=?";
			conn = new com.fapps.db.MySQLConnection().getConnection(db_URL,db_User,db_Pass);
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(query);
			last_export_time = inputDate+" "+currentTime().split(" ")[1];
			ps.setString(1, last_export_time);     
			ps.setString(2, sync_status);
			ps.setString(3, sno);       
			ps.setString(4, transactionTableName);
			ps.addBatch();
			pu.sop("["+threadName+"] Insert query after batch addition",	ps.toString());
			ps.executeBatch();
			conn.commit();
		}catch(Exception ex) {
			System.out.println("["+threadName+"] EXCEPTION JAR: updateTable: ["+threadName+"]" +ex.toString());
			//status = false;
			ex.printStackTrace();
		}finally{
			try{
				if( conn != null ){
					if( ps != null )
						ps.close();
					conn.close();
				}
			}catch( Exception e ){}
		}
	}
	
	/*private void insertEpEvents(String inputDate,String insertEpEventStatus, String epEventTable, String threadName, 
			String type, String module, String circle, String tech, String tableNameTransaction, String sno, String start_time, String file_records,
			String db_URL, String db_User, String db_Pass ) {
		Connection conn = null;
		PreparedStatement ps = null;
		String query    = null;
		try {
			String sync_status = "Ep_Success";
			String input_date = inputDate.replaceAll("-", "_");
			String typeF = "";
			if(type.equalsIgnoreCase("r")) {
				typeF = "raw";
			}else {
				typeF = "process";
			}
			query="insert into "+epEventTable+"("
					+ "Export_Timetamp,"    //1
					+ "Module,"    //2
					+ "Circle,"         //3
					+ "Tech,"             //4
					+ "Table_Name,"           //5
					+ "File_Name,"            //6
					+ "Export_Status,"           //7
					+ "start_time,"   //8
					+ "size,"    //9
					+ "event_name)"   //10
					+ " Values(?,?,?,?,?,?,?,?,?,?)";
			conn = new com.fapps.db.MySQLConnection().getConnection(db_URL,db_User,db_Pass);
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(query);
			ps.setString(1,currentTime());
			ps.setString(2, module);       
			ps.setString(3, circle);
			ps.setString(4, tech);
			ps.setString(5, tableNameTransaction);
			ps.setString(6, typeF+"_"+module+"_"+circle+"_"+tech+"_"+sno+"_"+input_date+".csv");
			ps.setString(7, insertEpEventStatus);
			ps.setString(8, start_time);
			ps.setString(9,file_records);
			ps.setString(10, "event_end");
			ps.addBatch();
			pu.sop("["+threadName+"] Source:::: insertEpEvents:::: Insert query: ",	ps.toString());
			ps.executeBatch();
			conn.commit();
		}catch(Exception ex) {
			System.out.println("["+threadName+"] EXCEPTION JAR: Source:::: insertEpEvents: ["+threadName+"]" +ex.toString());
			//status = false;
			ex.printStackTrace();
		}finally{
			try{
				if( conn != null ){
					if( ps != null )
						ps.close();
					conn.close();
				}
			}catch( Exception e ){}
		}
	}*/
}