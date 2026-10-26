package com.fapps.api;

import com.fapps.db.MySQLConnection;
import com.fapps.util.PrintUtilMsg;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Source {
    PrintUtilMsg pu = new PrintUtilMsg();

    public static void main(String[] args) {
        try {
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String[] binarySearch(String needDateRange, String date, String sourceTable, String db_url, String db_user, String db_pass) {
        String[] snoRange = new String[4];
        try {
            String[] dateColumn = date.split("~~");
            String dateField = dateColumn[0];
            String dateValue = dateColumn[1];
            String[] dateAndInterval = dateColumn[1].split("#");
            String trendType = dateColumn[2];
            String interval = "1";
            if (dateAndInterval.length > 1) {
                interval = dateAndInterval[1];
            }
            dateValue = dateAndInterval[0];
            SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd");
            Date d = sdf_date.parse(dateValue);
            Calendar c1 = Calendar.getInstance();
            c1.setTime(d);
            String from_date = sdf_date.format(c1.getTime());
            String to_date = sdf_date.format(c1.getTime());
            if (trendType.equalsIgnoreCase("monthly")) {
                c1.add(Calendar.MONTH, Integer.parseInt(interval));
                c1.add(Calendar.DAY_OF_MONTH, -1);
                to_date = sdf_date.format(c1.getTime());
            }
            com.fapps.db.SNORangeFinder srf = new com.fapps.db.SNORangeFinder();
            long[] snoRangeLong = srf.getStartEndIdOfTable(sourceTable, db_url, db_user, db_pass);
            long[] finalRange = srf.getStartAndEndId(dateValue, snoRangeLong[0], snoRangeLong[1], sourceTable, dateField, trendType, "1", false, "sno", db_url, db_user, db_pass);
            snoRange[0] = String.valueOf(finalRange[0]);
            snoRange[1] = String.valueOf(finalRange[1]);
            snoRange[2] = from_date;
            snoRange[3] = to_date;
        } catch (Exception ex) {
            System.out.println("EXCEPTION JAR: binarySearch: " + ex.toString());
            ex.printStackTrace();
        }
        return snoRange;
    }
// limit check 
    public ArrayList<String[]> getDataFromScheduleTable(String db_url, String db_user, 
    		String db_pass, String tableNameSchedule, String dateColumnScheduleTable, 
    		String inputDate, int limit, String column_id) {
        ArrayList<String[]> response = new ArrayList<String[]>();
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String query = null;
        try {
            query = "select sno,table_name,type,module,circle,tech,maxSno,db_URL,db_user,db_pass,db_name,host,events_table,Date_to_Pick from " + tableNameSchedule + " where sync_status='Init' and Date_to_Pick='frequency' limit " + limit;
            System.out.println("Query API Log: getDataFromSourceTable: " + query);
            conn = new com.fapps.db.MySQLConnection().getConnection(db_url, db_user, db_pass);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            int rowCount = 0;
            rs.last();
            rowCount = rs.getRow();
            rs.beforeFirst();
            pu.sop("Row Count", "Total rows: [" + rowCount + "]");
            if (rowCount > 0) {
                while (rs.next()) {
                    String[] temp = new String[14];
                    temp[0] = rs.getString("sno");
                    temp[1] = rs.getString("table_name");
                    temp[2] = rs.getString("type");
                    temp[3] = rs.getString("module");
                    temp[4] = rs.getString("circle");
                    temp[5] = rs.getString("tech");
                    temp[6] = rs.getString("maxSno");
                    temp[7] = rs.getString("db_URL");
                    temp[8] = rs.getString("db_user");
                    temp[9] = rs.getString("db_pass");
                    temp[10] = rs.getString("db_name");
                    temp[11] = rs.getString("host");
                    temp[12] = rs.getString("events_table");
                    temp[13] = rs.getString("Date_to_Pick");
                    response.add(temp);
                }
            }
            updateScheduleTableTransactionInProcess(tableNameSchedule, response, db_url, db_user, db_pass);
        } catch (Exception ex) {
            System.out.println("EXCEPTION JAR: getDataFromSourceTable: " + ex.toString());
            response = null;
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    if (rs != null) rs.close();
                    if (stmt != null) stmt.close();
                    conn.close();
                }
            } catch (Exception e) {
            }
        }
        return response;
    }

    private void updateScheduleTableTransactionInProcess(String scheduleTableName, ArrayList<String[]> response, String db_URL, String db_User, String db_Pass) {
        Connection conn = null;
        java.sql.PreparedStatement ps = null;
        String query = null;
        try {
            String sync_status = "InProcess_EP";
            query = "update " + scheduleTableName + " set sync_status=? where sno=? and table_name=?";
            conn = new com.fapps.db.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(query);
            for (String[] temp : response) {
                ps.setString(1, sync_status);
                ps.setString(2, temp[0]);
                ps.setString(3, temp[1]);
                ps.addBatch();
                pu.sop("[Main]", ps.toString());
            }
            ps.executeBatch();
            conn.commit();
        } catch (Exception ex) {
            System.out.println("EXCEPTION JAR: updateScheduleTableTransactionInProcess: " + ex.toString());
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    if (ps != null) ps.close();
                    conn.close();
                }
            } catch (Exception e) {
            }
        }
    }

    public boolean processScheduledTransactions(String db_URL, String db_User, String db_Pass, String tableNameSchedule,
    		String dateColumnScheduleTable, String dateColumnTransactionTable, String inputDate, int limit, int threadLimit,
    		String filePath, String db_Name, String host, String eventTable, String column_id, String batchSize) {
        boolean status = true;
        ExecutorService executor = Executors.newFixedThreadPool(threadLimit);
        ArrayList<Future<String>> list = new ArrayList<Future<String>>();
        HashMap<String, String> possibleEvents = new HashMap<String, String>();
        try {
            long batchSizeLong = Long.parseLong(batchSize);
            ArrayList<String[]> scheduleData = getDataFromScheduleTable(db_URL, db_User, db_Pass, tableNameSchedule, dateColumnScheduleTable,
            		inputDate, limit, column_id);
            if (scheduleData != null && scheduleData.size() > 0) {
                for (int i = 0; i < scheduleData.size(); i++) {
                    String[] temp = scheduleData.get(i);
                    String tableNameTransaction = temp[1];
                    String type = temp[2];
                    String module = temp[3];
                    String circle = temp[4];
                    String tech = temp[5];
                    String maxSno = temp[6];
                    String dbURLTemp = temp[7];
                    String dbUserTemp = temp[8];
                    String dbPassTemp = temp[9];
                    String dbNameTemp = temp[10];
                    String hostTemp = temp[11];
                    String eventTableTemp = temp[12];

                    // Check if column_id exists
                    boolean columnExists = checkColumnExists(dbURLTemp, dbUserTemp, dbPassTemp, tableNameTransaction, column_id);
                    if (columnExists) {
                        // Process with sno ranges
                        String[] snoRange = binarySearch("yes", dateColumnTransactionTable + "~~" + inputDate + "~~daily", tableNameTransaction, dbURLTemp, dbUserTemp, dbPassTemp);
                        long startSno = Long.parseLong(maxSno == null || maxSno.equals("null") ? snoRange[0] : maxSno);
                        long endSno = Long.parseLong(snoRange[1]);
                        for (long batchStart = startSno; batchStart <= endSno; batchStart += batchSizeLong) {
                            long batchEnd = Math.min(batchStart + batchSizeLong - 1, endSno);
                            ThreadProcessCSV callable = new ThreadProcessCSV(inputDate, String.valueOf(batchStart), String.valueOf(batchEnd), tableNameTransaction, type, module, circle, tech, dateColumnTransactionTable, dbURLTemp, dbUserTemp, dbPassTemp, i, filePath, tableNameSchedule, dbNameTemp, hostTemp, eventTableTemp, batchSize);
                            Future<String> future = executor.submit(callable);
                            list.add(future);
                            possibleEvents.put(tableNameTransaction + "_" + batchStart, "Processing");
                        }
                    } else {
                        // Process with offset-based batching
                        long totalRecords = getTotalRecordCount(dbURLTemp, dbUserTemp, dbPassTemp, tableNameTransaction);
                        long startOffset = 100000; // Start from 100,001 (1L) as per assumption
                        for (long offset = startOffset; offset < totalRecords; offset += batchSizeLong) {
                            long batchEnd = Math.min(offset + batchSizeLong, totalRecords);
                            ThreadProcessCSV callable = new ThreadProcessCSV(inputDate, String.valueOf(offset), String.valueOf(batchEnd), tableNameTransaction, type, module, circle, tech, dateColumnTransactionTable, dbURLTemp, dbUserTemp, dbPassTemp, i, filePath, tableNameSchedule, dbNameTemp, hostTemp, eventTableTemp, batchSize);
                            Future<String> future = executor.submit(callable);
                            list.add(future);
                            possibleEvents.put(tableNameTransaction + "_" + offset, "Processing");
                        }
                    }
                }
                for (Future<String> fut : list) {
                    String result = fut.get();
                    String[] resultArray = result.split(",");
                    String event = resultArray[0] + "_" + resultArray[3];
                    String action = resultArray[1];
                    possibleEvents.put(event, action);
                    if (action.equals("failed")) status = false;
                }
                pu.printMap("Main", possibleEvents);
            } else {
                status = false;
            }
        } catch (Exception ex) {
            status = false;
            ex.printStackTrace();
        } finally {
            executor.shutdown();
        }
        return status;
    }

    private boolean checkColumnExists(String db_URL, String db_User, String db_Pass, String tableName, String column_id) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = new MySQLConnection().getConnection(db_URL, db_User, db_Pass);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 1");
            java.sql.ResultSetMetaData metaData = rs.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                if (metaData.getColumnName(i).equalsIgnoreCase(column_id)) {
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (Exception e) {
            }
        }
    }

    private long getTotalRecordCount(String db_URL, String db_User, String db_Pass, String tableName) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = new MySQLConnection().getConnection(db_URL, db_User, db_Pass);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT COUNT(*) AS total FROM " + tableName);
            if (rs.next()) {
                return rs.getLong("total");
            }
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (Exception e) {
            }
        }
    }
}