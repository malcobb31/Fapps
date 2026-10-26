package com.fapps.api;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.Callable;

import com.fapps.util.PrintUtilMsg;



public class ThreadProcessCSV_bkup_15Dec2023 implements Callable<String> {

	PrintUtilMsg pu = new PrintUtilMsg();
	
	private final String sno;
	private final String inputDate;
	private final String tableNameTransaction;
	private final String type;
	private final String module;
	private final String circle;
	private final String tech;
	private final String dateColumnTransactionTable;
	private final String db_URL;
	private final String db_User;
	private final String db_Pass;
	private final String threadName;
	private final String filePath;
	private final String tableNameSchedule;
	private final String db_Name;
	private final String host;
	
	public ThreadProcessCSV_bkup_15Dec2023(String inputDate, String sno, String tableNameTransaction, String type, String module, String circle, String tech, String dateColumnTransactionTable,
			String db_URL, String db_User, String db_Pass, int threadName, String filePath, String tableNameSchedule, String db_Name, String host) {
		this.sno = sno;
		this.inputDate = inputDate;
		this.tableNameTransaction = tableNameTransaction;
		this.type = type;
		this.module = module;
		this.circle = circle;
		this.tech = tech;
		this.dateColumnTransactionTable = dateColumnTransactionTable;
		this.db_URL = db_URL;
		this.db_User = db_User;
		this.db_Pass= db_Pass;
		this.threadName = "T"+threadName;
		this.filePath  = filePath;
		this.tableNameSchedule = tableNameSchedule;
		this.db_Name = db_Name;
		this.host = host;
	}
	
	
	@Override
	public String call() throws Exception {
		String response = "error";
		System.out.println("ThreadProcessCSV:::: Starting "+threadName);
		String status = createCSVViaLoadCommand();
			
		if(status=="true") {
			response = "success";
		}else if(status=="true_0"){
			response = "success_0";
		}else {
			response="failed";
		}
		
		
		return threadName+","+response+","+this.tableNameTransaction+","+this.sno+","+this.type+","+this.module+","+this.circle+","+this.tech;
	}
	
	
	private String createCSVViaLoadCommand() {
		String status = "false";
		try {
			String query = "";
			String path  = "";
			String typeValue = "";
			
			String input_date = this.inputDate.replaceAll("-", "_");
			
			System.out.println("["+threadName+"] ThreadProcessCSV:::: In createCSVViaLoadCommand::: sno["+this.sno+"], Table_name["+this.tableNameTransaction+"], type["+this.type+"], module["+this.module+"], circle["+this.circle+"], tech["+this.tech+"]");
			
			
			//updateScheduleTableTransactionInProcess(tableNameSchedule, tableNameTransaction,  threadName, sno, db_URL, db_User, db_Pass );
			
			String columnNames = getTableColumnName(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction, this.threadName);
			
			String[] snoRange = getSnoFromTransactionTable(this.tableNameTransaction, this.inputDate, this.dateColumnTransactionTable, this.db_URL, this.db_User,this.db_Pass);
			
			String columnNamesHeader = columnNames;
			
			columnNamesHeader = "'"+columnNamesHeader.replaceAll(",", "','")+"'";
			
			
			if(this.type.equalsIgnoreCase("r")) {
				typeValue = "raw";
			}else {
				typeValue = "process";
			}
			
			//path = String.valueOf(String.valueOf(path)) + "/" + tableName + ".csv";
			path = filePath + "/"+typeValue+"/"+typeValue+"_"+this.module+"_"+this.circle+"_"+this.tech+"_"+input_date  + ".csv";
			//path  = this.filePath;
			System.out.println("["+threadName+"] ThreadProcessCSV:::: In createCSVViaLoadCommand::: "+path);
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
			
			query = "mysql -u" + this.db_User + " --password=\"" + this.db_Pass + "\" -h" + host + " " + db_Name
                    + " -s -N -e  \"select "+columnNamesHeader+" union all select " + columnNames +""
                    + " from " + this.tableNameTransaction+" where sno>="+snoRange[0]+" and sno<="+snoRange[1]
                    + " and  DATE_FORMAT("+this.dateColumnTransactionTable+",'%Y-%m-%d')=DATE_FORMAT('"+this.inputDate+"','%Y-%m-%d')\" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
			
			
			
			System.out.println("query to create file " + query);
            String[] commands = new String[]{"bash", "-c", query};
            try {
                Process p = Runtime.getRuntime().exec(commands);
                p.waitFor();
                System.out.println("["+threadName+"] Success: file written ");
                status = "true";
                
            }catch (Exception ein) {
            	status = "false";
                ein.printStackTrace();
            }
			
            
            
            String row = "";
            int count_row = 0;
            BufferedReader csvReader = null;
            
            try {
	            csvReader = new BufferedReader(new FileReader(path));
	            
	            while ((row = csvReader.readLine()) != null) {
	            	
	            	count_row++;
	            	if(count_row>1) {
	            		status = "true";
	            		break;
	            	}
	            	
	            }
	            
	            if(count_row>1) {
	            	status = "true";
	            }else {
	            	status = "true_0";
	            }
	            
	            
	            if(csvReader!=null) {
					csvReader.close();
				}
	            
            }catch(Exception e) {
				status = "true_0";
				System.out.println("Exception while reading a CSV file to check line count "+e.toString());
				e.printStackTrace();
				csvReader.close();
            }	
			
			
			
		}catch(Exception e) {
			status = "false";
			System.out.print("ThreadProcessCSV:::: createCSVViaLoadCommand:::: Exception while processing CSV "+e.toString());
			e.printStackTrace();
		}
		
		
		return status;
	}
	
	
	
	private String getTableColumnName(String db_url, String db_User, String db_pass, String table, String threadName) {
		String response = "";
		
		Connection conn = null;
		Statement stmt  = null;
		ResultSet rs    = null;
		String query    = null;

		
		try
		{
			query="select * from "+table+" order by sno desc limit 1";
					 
			
			
				
							
				System.out.println("["+threadName+"] Query API Log:  getTableColumnName: " +query);
			
						
			
			conn = new com.fapps.db.MySQLConnection().getConnection(db_url,
																	db_User,
																	db_pass);
			stmt = conn.createStatement();
			rs   = stmt.executeQuery( query );
			
			if (rs != null) {
		        ResultSetMetaData columns = rs.getMetaData();
		        int i = 0;
		        while (i < columns.getColumnCount()) {
		          i++;
		          System.out.print(columns.getColumnName(i) + "\t");
		          //columnNames.add(columns.getColumnName(i));
		          if(!columns.getColumnName(i).equals("sno")) {
		        	  response = response+","+columns.getColumnName(i);
		          }
		        }
			}
			
			response  = response.substring(1);
			System.out.println("["+threadName+"] Columns are: "+response);
			
			}catch( Exception ex )
			{
				System.out.println("["+threadName+"] EXCEPTION JAR: ::  getTableColumnName: "+ex.toString());
				response = "";
				ex.printStackTrace();
			}
			finally
			{
				try
				{
					if( conn != null )
					{
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
	
	
	
	private void updateScheduleTableTransactionInProcess(String scheduleTableName, String transactionTableName, String threadName, String sno, String db_URL, String db_User, String db_Pass ) {
		
		Connection conn = null;
		PreparedStatement ps = null;
		String query    = null;
		
		
try {
			
			
			String sync_status = "InProcess_EP";
			
			
			query = "update "+scheduleTableName+ " set sync_status=? where sno=? and table_name=?";
					
			
			
						
			conn = new com.fapps.db.MySQLConnection().getConnection(db_URL,
					db_User,
					db_Pass);
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
			
		}finally
		{
			try
			{
				if( conn != null )
				{
					if( ps != null )
						ps.close();
				
					conn.close();
				}
			}catch( Exception e ){}
		}
		
	}
	
	
	public String[] getSnoFromTransactionTable(String transactionTableName, String date, String date_column, String db_url, String db_User, String db_pass)
	{
		
		
		String[] snoRange  = null;
		String binaryDate = "";
		

		try
		{

			Source_Without_batching sbs = new Source_Without_batching();
			
			binaryDate = date_column+"~~"+date+"~~daily";

			snoRange = sbs.binarySearch("yes", binaryDate, transactionTableName, db_url, db_User, db_pass);


		}
		catch( Exception ex )
		{
			System.out.println("EXCEPTION JAR: getDataFromSourceTable: "+ex.toString());
			snoRange = null;
			ex.printStackTrace();
		}
		
		return snoRange;
	}
	
}
