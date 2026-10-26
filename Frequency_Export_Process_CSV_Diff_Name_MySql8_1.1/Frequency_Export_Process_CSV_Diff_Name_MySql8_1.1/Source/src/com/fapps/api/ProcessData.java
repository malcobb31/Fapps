package com.fapps.api;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;

public class ProcessData {
    com.fapps.util.PrintUtilMsg pu = new com.fapps.util.PrintUtilMsg();
    
    public boolean addSourceData(String sourceTable, String inputdate, String scheduleTable, String dateColumnScheduleTable,String filePath, 
            String db_URL, String db_User, String db_Pass, String db_Name, String host, String eventTable, String batchSize) {
        ArrayList<String[]> getDataFromScheduleTable = null;
        PreparedStatement ps = null;
        Connection conn = null;
        boolean status = false;
        try {    
            int limit = 1300000; // Hardcoded limit
            int threadLimit = 10; // Hardcoded threadLimit
            getDataFromScheduleTable = new Source().getDataFromScheduleTable(db_URL, db_User, db_Pass, scheduleTable,dateColumnScheduleTable ,inputdate, limit, "sno");
            status = new Source().processScheduledTransactions(db_URL, db_User, db_Pass, scheduleTable, inputdate, 
                    inputdate, inputdate, limit, threadLimit, filePath, db_Name, host, eventTable, "sno", batchSize);
        } catch(Exception ex) {
            System.out.println("EXCEPTION JAR: Airtel_Export_Process_CSV_1.0: " + ex.toString());
            ex.printStackTrace();
            status = false;
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                    if (ps != null) {
                        ps.close();
                    }
                }
            } catch (Exception e) {}
        }
        return status;
    }
}