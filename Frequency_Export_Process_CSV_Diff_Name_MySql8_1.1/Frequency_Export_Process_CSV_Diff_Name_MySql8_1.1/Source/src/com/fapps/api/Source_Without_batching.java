package com.fapps.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
//import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.fapps.db.SNORangeFinder;
import com.fapps.util.PrintUtilMsg;

public class Source_Without_batching {
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
					   									 db_pass);
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
							 db_pass);
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
			ex.printStackTrace();
		}
		return snoRangeReturn;
	}	
	
	/*private String currentDate() {
		String current_date = "";
		try {
			//SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd");
			Calendar c1 = Calendar.getInstance();
			Date date = new Date();
			c1.setTime(date);
			//Date nextDate = c1.getTime();
			current_date =sdf_date.format(c1.getTime());
		}catch(Exception E) {
			System.out.println("currentTime::   Exception while computing current date");
			E.printStackTrace();
		}
		return current_date;
	}*/
	
	/*private String getPreDateFromCurrentDate(int dayBack) {
		//String m = "-1";
		String current_date = "";
		try {
			//SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd");
			Calendar c1 = Calendar.getInstance();
			Date date = new Date();
			c1.setTime(date);
			c1.add(c1.DATE, -dayBack);
			//Date newDate = c1.getTime();
			current_date =sdf_date.format(c1.getTime());
		}catch(Exception E) {
			System.out.println("currentTime::   Exception while computing previous date from current date");
			E.printStackTrace();
		}
		return current_date;
	}*/
	
	/*private String currentTime() {
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
	}*/
	
	/*private boolean compareCurrentTimeWithScheduleTime(String current_date_time, String schedule_date_time) {
		boolean status = false;
		try {
			SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Calendar c1 = Calendar.getInstance();
			//Date date = new Date();
			//c1.setTime(date);
			Date currentDate = sdf_date.parse(current_date_time);
			Date scheduleDate = sdf_date.parse(schedule_date_time);
			if(currentDate.after(scheduleDate)) {
				status = true;
			}else {
				status = false;
			}
		}catch(Exception E) {
			System.out.println("currentTime::   Exception while computing current date");
			E.printStackTrace();
		}
		return status;
	}*/
	
	/*private String compareCurrentDateWithPreviousDates(String input_date, String date_minus_two, String date_minus_one, String today_date) {
		String run_what = "NA";
		try {
			SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd");
			Calendar c1 = Calendar.getInstance();
			//Date date = new Date();
			//c1.setTime(date);
			Date inputDate = sdf_date.parse(input_date);
			Date minus2Date = sdf_date.parse(date_minus_two);
			Date minus1Date = sdf_date.parse(date_minus_one);
			Date todayDate = sdf_date.parse(today_date);
			if(inputDate.before(minus2Date)) {
				run_what = "normal";
			}else if(inputDate.equals(minus2Date)) {
				run_what = "d-2";
			}else if(inputDate.equals(minus1Date)) {
				run_what = "d-1";
			}else if(inputDate.equals(todayDate)) {
				run_what = "d-0";
			}else {
				run_what="NA";
			}
		}catch(Exception E) {
			run_what = "NA";
			System.out.println("compareCurrentDateWithPreviousDates::   Exception while comparing input date with minus1 and minus2 dates");
			E.printStackTrace();
		}
		return run_what;
	}*/
	
	public ArrayList<String[]> getDataFromScheduleTable( String inputDate, String db_url, String db_User, String db_pass, String table, String limit ){
		ArrayList<String[]> data = null;
		Connection conn = null;
		Statement stmt  = null;
		ResultSet rs    = null;
		String query    = null;
		int rowCount = 0;
		int row = 0;
		try{
			/*String today_date = currentDate();
			String current_time = currentTime();
			String dateMinusOne = getPreDateFromCurrentDate(1);
			String dateMinusTwo = getPreDateFromCurrentDate(2);
			pu.sop("Query API Log:  getDataFromScheduleTable: ", "today date ["+today_date+"]");
			pu.sop("Query API Log:  getDataFromScheduleTable: ", "d-1 date ["+dateMinusOne+"]");
			pu.sop("Query API Log:  getDataFromScheduleTable: ", "d-2 date ["+dateMinusTwo+"]");
			String run_what = compareCurrentDateWithPreviousDates(inputDate, dateMinusTwo, dateMinusOne, today_date);
			pu.sop("Query API Log:  getDataFromScheduleTable: ", "Run What is ["+run_what+"]");
			 //DATE_FORMAT('2023-11-25 'schedule_time,'%Y-%m-%d HH:mm:ss')<DATE_FORMAT('"+current_time+"','%Y-%m-%d HH:mm:ss')"
			if(run_what.equals("normal")) {
				query="select sno,table_name, type, module, circle, tech, graph_id, last_export_timestamp, schedule_time, sync_status, last_import_timestamp, remarks, date_column, Date_to_Pick from "+table+""
					+ " where lower(sync_status)=lower('Init') and DATE_FORMAT(last_export_timestamp,'%Y-%m-%d')<DATE_FORMAT('"+inputDate+"','%Y-%m-%d')"
					+ " order by sno asc"; 
			}else if(run_what.equals("d-2")) {
				query="select sno,table_name, type, module, circle, tech, graph_id, last_export_timestamp, schedule_time, sync_status, last_import_timestamp, remarks, date_column, Date_to_Pick from "+table+""
						+ " where lower(sync_status)=lower('Init') and DATE_FORMAT(last_export_timestamp,'%Y-%m-%d')<DATE_FORMAT('"+inputDate+"','%Y-%m-%d')"
						+ " and (Date_to_Pick='d-2' or Date_to_Pick='d-1')"
						+ " order by sno asc";
			}else if(run_what.equals("d-1")) {
				query="select sno,table_name, type, module, circle, tech, graph_id, last_export_timestamp, schedule_time, sync_status, last_import_timestamp, remarks, date_column, Date_to_Pick from "+table+""
						+ " where lower(sync_status)=lower('Init') and DATE_FORMAT(last_export_timestamp,'%Y-%m-%d')<DATE_FORMAT('"+inputDate+"','%Y-%m-%d')"
						+ " and Date_to_Pick='d-1'"
						+ " order by sno asc";
			}else if(run_what.equals("d-0")){
				query="select sno,table_name, type, module, circle, tech, graph_id, last_export_timestamp, schedule_time, sync_status, last_import_timestamp, remarks, date_column, Date_to_Pick from "+table+""
						+ " where lower(sync_status)=lower('Init') and DATE_FORMAT(last_export_timestamp,'%Y-%m-%d')<DATE_FORMAT('"+inputDate+"','%Y-%m-%d')"
						+ " and Date_to_Pick='d-0'"
						+ " order by sno asc";
			}else {
				pu.sop("Query API Log:  getDataFromScheduleTable: ", "Not a valid input date as it is greater than today's date ");
			}*/
			query="select sno,table_name, type, module, circle, tech, graph_id, last_export_timestamp, schedule_time, sync_status, last_import_timestamp, remarks, date_column, maxSno from "+table+""
					+ " where lower(sync_status)=lower('Init') and Date_to_Pick = 'frequency' order by sno asc";
			pu.sop("Query API Log:  getDataFromScheduleTable: ", query);
			conn = new com.fapps.db.MySQLConnection().getConnection(db_url,db_User,db_pass);
			stmt = conn.createStatement();
			rs   = stmt.executeQuery( query );
			rs.last();
			rowCount = rs.getRow();
			rs.beforeFirst();
			pu.sop("Row Count", "Total rows:["+rowCount+"]");
			if( rowCount > 0 ){
				data = new ArrayList<String[]>();
				int colNumber = 0;
				String[] tableData = null;
				boolean status = false;
				while( rs.next() ){	
					//status = false;
					status = true;
					/*if(run_what.equals("d-2")) {
						// if input date with current time is greater than will pull the entry to execute further for d-2
						if(rs.getString("Date_to_Pick").equals("d-2")){
							status = compareCurrentTimeWithScheduleTime(inputDate+" "+current_time.split(" ")[1], inputDate+" "+rs.getString("schedule_time").split(" ")[1]+":00");
						}else {
							pu.sop("Query API Log:  getDataFromScheduleTable: ", "Date pick is  ["+rs.getString("Date_to_Pick")+"], so will be executes no matter about time check for input time");
							status = compareCurrentTimeWithScheduleTime(current_time, inputDate+" "+rs.getString("schedule_time").split(" ")[1]+":00");
						}	
					}else if(run_what.equals("d-1")){
						// if input date with current time is greater than will pull the entry to execute further for d-1
						status = compareCurrentTimeWithScheduleTime(inputDate+" "+current_time.split(" ")[1], inputDate+" "+rs.getString("schedule_time").split(" ")[1]+":00");
					}else {
						status = compareCurrentTimeWithScheduleTime(current_time, inputDate+" "+rs.getString("schedule_time").split(" ")[1]+":00");
					}*/
					if(status==true) {
						colNumber=0;
						tableData = new String[14]; 
						for(colNumber=0; colNumber< 14; colNumber++) {
							tableData[colNumber] = rs.getString(colNumber+1);
						}
						data.add(tableData);
						updateScheduleTableTransactionInProcess(table, tableData[1], ""+row,tableData[0], db_url, db_User, db_pass);
						row++;
					}
					if(row==Integer.parseInt(limit)) {
						break;
					}
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
	
	public boolean processScheduledTransactions(String scheduleTable, ArrayList<String[]> getDataFromScheduleTable, String  inputdate, String  threadLimit, String  eventTable, String  db_URL, String  db_User, String db_Pass, String filePath, String db_Name, String host) {
		boolean status = false;
		String updateScheduleTableStatus = "true"; 
		try {
			//Get ExecutorService from Executors utility class, thread pool size is ThreadLimit
			ExecutorService exe = Executors.newFixedThreadPool(Integer.parseInt(threadLimit));
			//create a list to hold the Future object associated with Callable
	        List<Future<String>> list = new ArrayList<Future<String>>();
			System.out.println("Length of threads to work "+getDataFromScheduleTable.size());
			String[] AL_Values = null;
			for(int i=0; i< getDataFromScheduleTable.size(); i++) {
				AL_Values = getDataFromScheduleTable.get(i);
				//Create CallableThread instance
			     Callable<String> callable = new ThreadProcessCSV_Without_batching(inputdate, AL_Values[0],AL_Values[1], AL_Values[2],
			    	AL_Values[3], AL_Values[4],AL_Values[5], AL_Values[12],db_URL, db_User, db_Pass, i, filePath, 
			    	scheduleTable, db_Name, host, eventTable, AL_Values[13]);
				Future<String> future = exe.submit(callable);
				list.add(future);
			}
			for(Future<String> fut : list){
	            try {
	                //print the return value of Future, notice the output delay in console
	                // because Future.get() waits for task to get completed
	            	updateScheduleTableStatus = "true";
	                System.out.println("["+fut.get().split(",")[0]+"] processScheduledTransactions:::: Response from CSV Thread::::: "+new Date()+ " :::: "+fut.get());
	                if(fut.get().split(",")[1].equals("failed")) {
	                	updateScheduleTableStatus = "false";
	                }else if(fut.get().split(",")[1].equals("success_0")) {
	                	updateScheduleTableStatus = "true_0";
	                }else {
	                	updateScheduleTableStatus = "true";
	                }
	            } catch (InterruptedException | ExecutionException e) {
	                e.printStackTrace();
	            }
	        }
			status = true;
			//shut down the executor service now
	        exe.shutdown();
		}catch(Exception e) {
			pu.sop("Exception:::: ", "Exception while processingScheduledTransactions:::: "+e.toString());
			e.printStackTrace();
		}
		return status;
	}
	
	private void updateScheduleTableTransactionInProcess(String scheduleTableName, String transactionTableName, String threadName, String sno, String db_URL, String db_User, String db_Pass ) {
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
	}		
}