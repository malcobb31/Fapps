

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import com.fapps.db.SNORangeFinder;
import com.fapps.util.PrintUtilMsg;

import java.sql.ResultSet;

public class Source_EyeQ 
{
	PrintUtilMsg pu = new PrintUtilMsg();
	
	
	private String[] binarySearch(String binarySearch, String date, String sourceTable, String db_url, String db_name, String db_pass ) {
		
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
				
				long[] snoRangeToFromDate  = null;
				
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
	
	private String currentDate() {
		String current_date = "";
		
		try {
			
			//SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd");
			Calendar c1 = Calendar.getInstance();
			
			Date date = new Date();
			
			
			c1.setTime(date);
			
			Date nextDate = c1.getTime();
			
			
			current_date =sdf_date.format(c1.getTime());
			
		}catch(Exception E) {
			System.out.println("currentTime::   Exception while computing current date");
			E.printStackTrace();
		}
		
		return current_date;
	}
	
	
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
	
	
	
	public String[][] getDataFromScheduleTable( String inputDate, String db_url, String db_name, String db_pass, String table, String limit )
	{
		String[][] data = null;
		Connection conn = null;
		Statement stmt  = null;
		ResultSet rs    = null;
		String query    = null;
		String[] snoRange  = null;
		String binaryDate = "";
		int rowCount = 0;
		int row = 0;
		
		
		try
		{
			//binaryDate = "MONTH~~"+date+"~~monthly";
			
			//snoRange = binarySearch("yes", binaryDate, "Target_Details", db_url, db_name, db_pass);
			
			
			String today_date = currentDate();
			String current_time = currentTime();
			
			query="select table_name, type, module, circle, tech, graph_id, last_export_timestamp, schedule_time, sych_status, last_import_timestamp, remarks from "+table+""
					+ " where sych_status='Init' and DATE_FORMAT(last_export_timestamp,'%Y-%m-%d')<DATE_FORMAT('"+today_date+"','%Y-%m-%d')"
					+ " and DATE_FORMAT(schedule_time,'%Y-%m-%d HH:mm:ss')<DATE_FORMAT('"+current_time+"','%Y-%m-%d HH:mm:ss')"
					+ " order by sno asc limit "+limit+""; 
			
			
				
							
				pu.sop("Query API Log:  getDataFromScheduleTable: ", query);
			
						
			
			conn = new com.fapps.db.MySQLConnection().getConnection(db_url,
																	db_name,
																	db_pass);
			stmt = conn.createStatement();
			rs   = stmt.executeQuery( query );
			rs.last();
			rowCount = rs.getRow();
			rs.beforeFirst();
			
			pu.sop("Row Count", "Total rows:["+rowCount+"]");
		
			if( rowCount > 0 ){
				data = new String[ rowCount][];
				data[0] = new String[11];
				
				while( rs.next() )
				{	
					data[row] = new String[]{ rs.getString("table_name"), //0
							rs.getString("type"),   //1
							rs.getString("module"),    //2
							rs.getString("circle"),   //3
							rs.getString("tech"),    //4
							rs.getString("graph_id"),    //5
							rs.getString("last_export_timestamp"),  //6
							rs.getString("schedule_time"),    //7
							rs.getString("sych_status"),    //8
							rs.getString("last_import_timestamp"),  //9
							rs.getString("remarks")   //10
							
							};						

         
					row++;
				}
			}	
		}
		catch( Exception ex )
		{
			System.out.println("EXCEPTION JAR: ::  getDataFromScheduleTable: "+ex.toString());
			data = null;
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
		return data;
	}
	
	
	
	public HashMap<String, ArrayList<String[]>> getDataFromSourceTable(String sourceTable, String date,
			  															String db_url,
			  															String db_name,
			  															String db_pass)
	{
		HashMap<String, ArrayList<String[]>> data = null;
		ArrayList<String[]> innerAL = null;
		Connection conn = null;
		Statement stmt  = null;
		ResultSet rs    = null;
		String query    = null;
		String[] snoRange  = null;
		String binaryDate = "";
		int rowCount = 0;
		int row = 0;
		
		
		try
		{
			
			binaryDate = "date~~"+date+"~~daily";
			
			snoRange = binarySearch("yes", binaryDate, sourceTable, db_url, db_name, db_pass);
			
			
			query="select date,LOWER(LOCATION) as LOCATION,OTINTIME,OTOUTTIME"
					+ " from "+sourceTable+""
					+ " where sno >= "+snoRange[0]+" and sno <= "+snoRange[1]+""
					+ " and DATE_FORMAT(date,'%Y-%m-%d')= DATE_FORMAT('"+date+"','%Y-%m-%d')";
			
					
				pu.sop("Query API Log: getDataFromSourceTable ", query);



				conn = new com.fapps.db.MySQLConnection().getConnection(db_url,
											db_name,
											db_pass);
				stmt = conn.createStatement();
				rs   = stmt.executeQuery( query );
				rs.last();
				rowCount = rs.getRow();
				rs.beforeFirst();
				
				pu.sop("Row Count", "Total rows:["+rowCount+"]");

				
				SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd HH:mm");
				//Calendar c1 = Calendar.getInstance();
				Date inDate = null;
				Date outDate = null;
				long diffMinutes = 0;
				
				if( rowCount > 0 ){
					
					
						
					String[] sourceTableColumns = new String[4];

					pu.sop("Column Count", "Total Columns:["+sourceTableColumns.length+"]");	
					
					data = new HashMap<String, ArrayList<String[]>>();
					//	data[0] = new String[sourceTableColumns.length];
					int colNumber=0;
					String[] tableData = null ;
					int count=0;

					while( rs.next() )
					{
						colNumber=0;
						diffMinutes = 0;
						//tableData="";
						tableData = new String[sourceTableColumns.length+1]; //adding one extra in length for Avg Time
						
						
						for(colNumber=0; colNumber< sourceTableColumns.length; colNumber++) {
							//tableData =tableData+""+rs.getString(colNumber+1)+",";
							tableData[colNumber] = rs.getString(colNumber+1);
						}
						
						try {
							inDate = sdf_date.parse(tableData[0]+" "+tableData[2].replaceAll(" ", "").trim());
							outDate = sdf_date.parse(tableData[0]+" "+tableData[3].replaceAll(" ", "").trim());
						
							diffMinutes = outDate.getTime()-inDate.getTime();
						
						
						
						
						
							tableData[4] = ""+diffMinutes/1000/60;
						}catch(Exception e) {
							tableData[4] = "0";
							System.out.println("Exception while calculating ayg time "+e.toString());
						}
						if(count==0) {
							innerAL = new ArrayList<String[]>();
							
							//System.out.println("First Time ["+firstPart+"]====["+SecondPart+"]");
							
							//innerHM.put(secondPart, tableData);
							innerAL.add(tableData);
							data.put(tableData[1], innerAL );
							
						}else {	
							//After first time assumung one record is there in hashmap
							if(data.get(tableData[1])!=null) {
								
								//System.out.println("Record Already Exist ["+firstPart+"]====["+SecondPart+"]");
								innerAL = data.get(tableData[1]);
								//System.out.println("The size of inner HM "+innerHM.size());
									
									
								 //main key found but inner key not found
								//System.out.println("Inner Key Does not exist ["+firstPart+"]====["+SecondPart+"]");
								innerAL.add(tableData);
								data.put(tableData[1], innerAL );
										 
										
							}else {//Main key not found
								//System.out.println("New Outer Key:::: ["+firstPart+"]====["+SecondPart+"]");
								innerAL = new ArrayList<String[]>();

								innerAL.add(tableData);
								data.put(tableData[1], innerAL );
							}
						
						}

						
						count++;
						
						
						
						//pu.sop("Tables data is ", "Haspmap "+tableData);
						//tableData = tableData.substring(0, tableData.length()-1);
						
						//data.put(rs.getString(key), new String[]{tableData	});
						//data.put(rs.getString("INDENT_DATE"), tableData	);
						
					}

				}
				
				try {
					
					System.out.println("getDataFromSourceTable:::: Size of Hashmap is: "+data.size()); 
				}catch(Exception e) {
					System.out.println("getDataFromSourceTable:::: Exception while calculation size of Hashmap: "+e.toString());
					e.printStackTrace();
				}
				
				
				
		}
		catch( Exception ex )
		{
			System.out.println("EXCEPTION JAR: getDataFromSourceTable: "+ex.toString());
			data = null;
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
		return data;
	}

	
	
	public boolean truncateTable(String Table, String date, String db_url, String db_name, String db_pass)
	{
		boolean data = false;
		Connection conn = null;
		PreparedStatement stmt  = null;
		
		String query    = null;
		
		
		
		try
		{
			
			query = "truncate table "+Table+"";

	
			pu.sop("Query API Log: truncateTable: ", query);



			conn = new com.fapps.db.MySQLConnection().getConnection(db_url,
											db_name,
											db_pass);
			stmt = conn.prepareStatement( query );	
			int rowsEffected = stmt.executeUpdate();
			pu.sop("data effected", "Query executed : "+rowsEffected);
			data = true;
			conn.commit();
			
			stmt.close();

			
				
		}
		catch( Exception ex )
		{
			System.out.println("EXCEPTION JAR: truncateTable:: "+ex.toString());
			data = false;
			ex.printStackTrace();
		}
		finally
		{
			try
			{
				if( conn != null )
				{
					
					if( stmt != null )
						stmt.close();
					conn.close();
				}
			}catch( Exception e ){}
		}
		return data;
	}
	
		
}