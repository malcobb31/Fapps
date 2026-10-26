package com.fapps.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;

public class ProcessData {
	com.fapps.util.PrintUtilMsg pu = new com.fapps.util.PrintUtilMsg();
	Source ev = new Source();
	public boolean addSourceData(String sourceTable, String inputdate, String scheduleTable, String threadLimit, 
			String filePath, int batch, String db_URL, String db_User, String db_Pass, String db_Name, 
			String host, String eventTable, int rawLimit, String logServerIP, String version, String serverInfoTable) {
		System.out.println("Time:"+Source.currentTime()+" [Debug] Process_Name: EP Server_IP: "+logServerIP+" Process_Version: "+version+" Log_Level: EVT_LOG  EVT_Type: START Description: Start");
		ArrayList<String[]> getDataFromScheduleTable      = null;
		PreparedStatement ps = null;
		Connection conn = null;
		boolean status = false;
		try {	
			getDataFromScheduleTable = new Source().getDataFromScheduleTable(inputdate, db_URL, db_User, db_Pass, 
					scheduleTable, threadLimit, logServerIP, version );
			status = new Source().processScheduledTransactions(scheduleTable, getDataFromScheduleTable, inputdate, 
					threadLimit, eventTable, db_URL, db_User, db_Pass, filePath, db_Name, host, rawLimit, logServerIP,
					 version,serverInfoTable);
			
		}catch(Exception ex){
			System.out.println("EXCEPTION JAR: Airtel_Export_Process_CSV_1.0: "+ex.toString());
			ex.printStackTrace();
			status = false;
			//ex.printStackTrace();
		}finally{
			try {
				if (conn != null) {
					conn.close();
					if (ps != null) {
						ps.close();
					}
				}
			} catch (Exception e) {}
		}
		System.out.println("Time:"+Source.currentTime()+" [Debug] Process_Name: EP Server_IP: "+logServerIP+" Process_Version: "+version+" Log_Level: EVT_LOG  EVT_Type: End Description: End");
		return status;
	}
}