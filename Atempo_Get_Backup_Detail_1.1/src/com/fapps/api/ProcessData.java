
package com.fapps.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;

public class ProcessData {
	com.fapps.util.PrintUtilMsg pu = new com.fapps.util.PrintUtilMsg();
	public boolean addSourceData(String serverTable, String siteTable, String inputdate, String eventTable, String category, String db_URL, 
							String db_User, String db_Pass, String db_Name) {
		ArrayList<String[]> getDataFromSourceTable      = null;
		PreparedStatement ps = null;
		Connection conn = null;
		boolean status = false;
		try {	
			getDataFromSourceTable = new Source().getDataFromSourceTable(inputdate, db_URL, db_User, db_Pass, serverTable, siteTable);
			//pu.sop("call  processSourceTransactions", "");
			status = new Source().processSourceTransactions(serverTable, getDataFromSourceTable, inputdate, eventTable, 
					category, db_URL, db_User, db_Pass, db_Name);
		}catch(Exception ex){
			System.out.println("EXCEPTION JAR: Check_Data_Insertion_In_Table: "+ex.toString());
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
			} catch (Exception e) {
			}
		}
		return status;
	}
}