package com.fapps.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.fapps.util.PrintUtilMsg;

public class Events {
	PrintUtilMsg pu = new PrintUtilMsg();
	public void insertEpEvents(String inputDate,String insertEpEventStatus, String epEventTable, String threadName, 
			String type, String module, String circle, String tech, String tableNameTransaction, String sno, String start_time, String file_records, String event_name,
			String db_URL, String db_User, String db_Pass ) {
		Connection conn = null;
		PreparedStatement ps = null;
		String query    = null;
		try {
			//String sync_status = "Ep_Success";
			String input_date = inputDate.replaceAll("-", "_");
			String typeF = "";
			if(type.equalsIgnoreCase("r")) {
				typeF = "raw";
			}else {
				typeF = "process";
			}
			query="insert into "+epEventTable+"("
					+ "Export_Timetamp,"//1
					+ "Module,"    		//2
					+ "Circle,"         //3
					+ "Tech,"           //4
					+ "Table_Name,"     //5
					+ "File_Name,"      //6
					+ "Export_Status,"  //7
					+ "start_time,"   	//8
					+ "size,"    		//9
					+ "event_name)"   	//10
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
			ps.setString(10, event_name);
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
}
