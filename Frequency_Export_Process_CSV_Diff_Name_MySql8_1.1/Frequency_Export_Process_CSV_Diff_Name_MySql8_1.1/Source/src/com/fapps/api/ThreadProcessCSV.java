package com.fapps.api;

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

import com.fapps.util.PrintUtilMsg;

public class ThreadProcessCSV implements java.util.concurrent.Callable<String> {
    Events ev = new Events();
    PrintUtilMsg pu = new PrintUtilMsg();
    private final String sno;
    private final String endSno;
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
    private final String eventTable;
    private final String batchSize; // Changed to String

    public ThreadProcessCSV(String inputDate, String sno, String endSno, String tableNameTransaction, String type, String module, String circle, String tech, String dateColumnTransactionTable,
            String db_URL, String db_User, String db_Pass, int threadName, String filePath, String tableNameSchedule, String db_Name, String host, String eventTable, String batchSize) {
        this.sno = sno;
        this.endSno = endSno;
        this.inputDate = inputDate;
        this.tableNameTransaction = tableNameTransaction;
        this.type = type;
        this.module = module;
        this.circle = circle;
        this.tech = tech;
        this.dateColumnTransactionTable = dateColumnTransactionTable;
        this.db_URL = db_URL;
        this.db_User = db_User;
        this.db_Pass = db_Pass;
        this.threadName = "T" + threadName;
        this.filePath = filePath;
        this.tableNameSchedule = tableNameSchedule;
        this.db_Name = db_Name;
        this.host = host;
        this.eventTable = eventTable;
        this.batchSize = batchSize; // Changed to String
    }

    @Override
    public String call() throws Exception {
        String final_data = "";
        String response = "error";
        System.out.println("ThreadProcessCSV:::: Starting " + threadName);
        String start_time = currentTime();
        String status = createCSVViaLoadCommand();
        System.out.println("[" + threadName + "] final status is " + status);
        if (status.split("EPBreak")[0].equals("true")) {
            response = "success";
        } else if (status.split("EPBreak")[0].equals("true_0")) {
            response = "success_0";
        } else {
            response = "failed";
        }
        final_data = threadName + "," + response + "," + this.tableNameTransaction + "," + this.sno + "," + this.type + "," + this.module + "," + this.circle + "," + this.tech + "," + start_time + "," + status.split("EPBreak")[1];
        updateStatusAndEvent(final_data);
        return final_data;
    }

    private String createCSVViaLoadCommand() {
        String status = "false";
        String file_records = "0";
        try {
            String query = "";
            String path = "";
            String path1 = "";
            String typeValue = "";
            String input_date = this.inputDate.replaceAll("-", "_");
            System.out.println("[" + threadName + "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: sno[" + this.sno + "], endSno[" + this.endSno + "], Table_name[" + this.tableNameTransaction + "], type[" + this.type + "], module[" + this.module + "], circle[" + this.circle + "], tech[" + this.tech + "]");
            ev.insertEpEvents(this.inputDate, "NA", this.eventTable, this.threadName, this.type, this.module, this.circle, this.tech, this.tableNameTransaction, this.sno, currentTime(), "NA", "event_start", this.db_URL, this.db_User, this.db_Pass);
            String[] columnNames = getTableColumnName(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction, this.threadName);
            String columnNamesHeader = columnNames[0];
            columnNamesHeader = "'" + columnNamesHeader.replaceAll(",", "','") + "'";
            if (this.type.equalsIgnoreCase("r")) {
                typeValue = "raw";
            } else {
                typeValue = "process";
            }
            path = filePath + "/" + typeValue + "/" + typeValue + "_" + this.module + "_" + this.circle + "_" + this.tech + "_" + this.sno + "_" + this.endSno + "_" + input_date + "_" + time() + ".csv";
            path1 = filePath + "/" + typeValue + "/" + typeValue + "_" + this.module + "_" + this.circle + "_" + this.tech + "_" + this.sno + "_" + this.endSno + "_" + input_date + "*";
            System.out.println("[" + threadName + "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: " + path);
            System.out.println("[" + threadName + "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: " + path1);
            Process p2 = null;
            try {
                p2 = Runtime.getRuntime().exec(new String[]{"sh", "remove_file.sh", path1});
                int exitCode = p2.waitFor();
                System.out.println("[" + threadName + "] ThreadProcessCSV:::: In createCSVViaLoadCommand::: exitCode" + exitCode);
            } catch (Exception ee) {
            } finally {
                try {
                    if (p2 != null) {
                        p2.destroy();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            boolean snoColumnExists = checkColumnExists(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction, "sno");
            long batchSizeLong = Long.parseLong(this.batchSize); // Parse batchSize to long
            if (snoColumnExists) {
                query = "mysql -u" + this.db_User + " --password=\"" + this.db_Pass + "\" -h" + host + " " + db_Name
                        + " -s -N -e  \"select " + columnNamesHeader + " union all select " + columnNames[1] + ""
                        + " from " + this.tableNameTransaction + " where sno>=" + this.sno + " and sno<=" + this.endSno + "\" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
            } else {
                long offset = Long.parseLong(this.sno);
                query = "mysql -u" + this.db_User + " --password=\"" + this.db_Pass + "\" -h" + host + " " + db_Name
                        + " -s -N -e  \"select " + columnNamesHeader + " union all select " + columnNames[1] + ""
                        + " from " + this.tableNameTransaction + " LIMIT " + batchSizeLong + " OFFSET " + offset + "\" | sed 's/\\t/\",\"/g;s/\\n//g;s/^/\"/g;s/$/\"/g;' >> '" + path + "'";
            }
            Process p = null;
            System.out.println("query to create file " + query);
            String[] commands = new String[]{"bash", "-c", query};
            try {
                p = Runtime.getRuntime().exec(commands);
                p.waitFor();
                System.out.println("[" + threadName + "] Success: file written ");
                status = "true";
            } catch (Exception ein) {
                status = "false";
                ein.printStackTrace();
            } finally {
                try {
                    if (p != null) {
                        p.destroy();
                    }
                } catch (Exception ee) {
                    ee.printStackTrace();
                }
            }
            status = "true_0";
            file_records = "0";
            Process p1 = null;
            BufferedReader br = null;
            try {
                p1 = Runtime.getRuntime().exec("wc -l " + path);
                br = new BufferedReader(new InputStreamReader(p1.getInputStream()));
                String data;
                while ((data = br.readLine()) != null) {
                    System.out.println("line: " + data);
                    if (Integer.parseInt(data.split(" ")[0]) > 1) {
                        file_records = data.split(" ")[0];
                        System.out.println("File Count is: " + data.split(" ")[0]);
                        status = "true";
                        if (snoColumnExists) {
                            updateMaxSno(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction, this.threadName, this.tableNameSchedule, Long.parseLong(this.endSno));
                        } else {
                            updateOffset(this.db_URL, this.db_User, this.db_Pass, this.tableNameTransaction, this.threadName, this.tableNameSchedule, Long.parseLong(this.endSno));
                        }
                    } else {
                        System.out.println("File Count is: " + data.split(" ")[0]);
                        status = "true_0";
                    }
                }
                p1.waitFor();
                System.out.println("exit: " + p1.exitValue());
            } catch (Exception e) {
                file_records = "0";
                status = "true_0";
                System.out.println("Exception while reading a CSV file to check line count " + e.toString());
                e.printStackTrace();
            } finally {
                try {
                    if (br != null) {
                        br.close();
                    }
                    if (p1 != null) {
                        p1.destroy();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            status = "false";
            System.out.print("ThreadProcessCSV:::: createCSVViaLoadCommand:::: Exception while processing CSV " + e.toString());
            e.printStackTrace();
        }
        return status + "EPBreak" + file_records;
    }

    private void updateMaxSno(String db_URL, String db_User, String db_Pass, String tableNameTransaction, String threadName, String tableNameSchedule, long tableMaxSno) {
        Connection conn = null;
        PreparedStatement ps = null;
        String query = null;
        try {
            query = "update " + tableNameSchedule + " set maxSno=? where table_name=?";
            conn = new com.fapps.db.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(query);
            ps.setString(1, Long.toString(tableMaxSno));
            ps.setString(2, tableNameTransaction);
            ps.addBatch();
            pu.sop("[" + threadName + "] update query after batch addition", ps.toString());
            ps.executeBatch();
            conn.commit();
        } catch (Exception ex) {
            System.out.println("[" + threadName + "] EXCEPTION JAR: updateMaxSno: [" + threadName + "]" + ex.toString());
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    if (ps != null) {
                        ps.close();
                    }
                    conn.close();
                }
            } catch (Exception e) {
            }
        }
    }

    private void updateOffset(String db_URL, String db_User, String db_Pass, String tableNameTransaction, String threadName, String tableNameSchedule, long offset) {
        Connection conn = null;
        PreparedStatement ps = null;
        String query = null;
        try {
            query = "update " + tableNameSchedule + " set maxOffset=? where table_name=?";
            conn = new com.fapps.db.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(query);
            ps.setString(1, Long.toString(offset));
            ps.setString(2, tableNameTransaction);
            ps.addBatch();
            pu.sop("[" + threadName + "] update offset query after batch addition", ps.toString());
            ps.executeBatch();
            conn.commit();
        } catch (Exception ex) {
            System.out.println("[" + threadName + "] EXCEPTION JAR: updateOffset: [" + threadName + "]" + ex.toString());
            ex.printStackTrace();
        } finally {
            try {
                if (conn != null) {
                    if (ps != null) {
                        ps.close();
                    }
                    conn.close();
                }
            } catch (Exception e) {
            }
        }
    }

    private long getTableMaxSno(String db_url, String db_User, String db_pass, String table, String threadName) {
        long response = 0;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String query = null;
        try {
            query = "select max(sno) as sno from " + table;
            System.out.println("[" + threadName + "] Query API Log:  getTableMaxSno: " + query);
            conn = new com.fapps.db.MySQLConnection().getConnection(db_url, db_User, db_pass);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            rs.last();
            int rowCount = rs.getRow();
            rs.beforeFirst();
            if (rowCount > 0) {
                while (rs.next()) {
                    response = rs.getLong("sno");
                }
            }
            System.out.println("[" + threadName + "] MaxSno: " + response);
        } catch (Exception ex) {
            System.out.println("[" + threadName + "] EXCEPTION JAR: ::  getTableMaxSno: " + ex.toString());
            response = 0;
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

    private String[] getTableColumnName(String db_url, String db_User, String db_pass, String table, String threadName) {
        String[] response = null;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String query = null;
        try {
            query = "select * from " + table + " order by sno desc limit 1";
            System.out.println("[" + threadName + "] Query API Log:  getTableColumnName: " + query);
            conn = new com.fapps.db.MySQLConnection().getConnection(db_url, db_User, db_pass);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            if (rs != null) {
                response = new String[2];
                ResultSetMetaData columns = rs.getMetaData();
                int i = 0;
                while (i < columns.getColumnCount()) {
                    i++;
                    System.out.print(columns.getColumnName(i) + "\t");
                    if (!columns.getColumnName(i).equals("sno")) {
                        response[0] = response[0] + "," + columns.getColumnName(i);
                        response[1] = response[1] + "," + "replace(" + columns.getColumnName(i) + ",'\\\"',\\\"'\\\")";
                    }
                }
                response[0] = response[0].replace("null", "");
                response[1] = response[1].replace("null", "");
                response[0] = response[0].substring(1);
                response[1] = response[1].substring(1);
                System.out.println("[" + threadName + "] Columns are: " + response);
            }
        } catch (Exception ex) {
            System.out.println("[" + threadName + "] EXCEPTION JAR: ::  getTableColumnName: " + ex.toString());
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

    private boolean checkColumnExists(String db_url, String db_User, String db_pass, String table, String column_id) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = new com.fapps.db.MySQLConnection().getConnection(db_url, db_User, db_pass);
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SELECT * FROM " + table + " LIMIT 1");
            ResultSetMetaData metaData = rs.getMetaData();
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

    public String[] getSnoFromTransactionTable(String transactionTableName, String date, String date_column, String db_url, String db_User, String db_pass) {
        String[] snoRange = null;
        try {
            Source sbs = new Source();
            String binaryDate = date_column + "~~" + date + "~~daily";
            snoRange = sbs.binarySearch("yes", binaryDate, transactionTableName, db_url, db_User, db_pass);
        } catch (Exception ex) {
            System.out.println("EXCEPTION JAR: getDataFromSourceTable: " + ex.toString());
            snoRange = null;
            ex.printStackTrace();
        }
        return snoRange;
    }

    private String currentTime() {
        String current_time = "";
        try {
            SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar c1 = Calendar.getInstance();
            Date date = new Date();
            c1.setTime(date);
            current_time = sdf_date.format(c1.getTime());
        } catch (Exception E) {
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
            current_time = sdf_date.format(c1.getTime());
        } catch (Exception E) {
            System.out.println("currentTime::   Exception while computing current date");
            E.printStackTrace();
        }
        return current_time;
    }

    private void updateStatusAndEvent(String final_data) {
        try {
            String updateScheduleTableStatus = "true";
            System.out.println("[" + final_data.split(",")[0] + "] processScheduledTransactions:::: Response from CSV Thread::::: " + new Date() + " :::: " + final_data);
            if (final_data.split(",")[1].equals("failed")) {
                updateScheduleTableStatus = "false";
            } else if (final_data.split(",")[1].equals("success_0")) {
                updateScheduleTableStatus = "true_0";
            } else {
                updateScheduleTableStatus = "true";
            }
            try {
                updateScheduleTableTransaction(updateScheduleTableStatus, this.tableNameSchedule, final_data.split(",")[2], final_data.split(",")[0], final_data.split(",")[3], this.db_URL, this.db_User, this.db_Pass, this.inputDate);
            } catch (Exception e) {
                e.printStackTrace();
            }
            ev.insertEpEvents(this.inputDate, updateScheduleTableStatus, this.eventTable, final_data.split(",")[0], final_data.split(",")[4], final_data.split(",")[5], final_data.split(",")[6], final_data.split(",")[7], final_data.split(",")[2], final_data.split(",")[3], final_data.split(",")[8], final_data.split(",")[9], "event_end", db_URL, db_User, db_Pass);
        } catch (Exception eee) {
            System.out.println("Exception while processing in updateStatusAndEvent function");
            eee.printStackTrace();
        }
    }

    private void updateScheduleTableTransaction(String status, String scheduleTableName, String transactionTableName, String threadName, String sno, String db_URL, String db_User, String db_Pass, String inputDate) {
        Connection conn = null;
        PreparedStatement ps = null;
        String query = null;
        String last_export_time = "";
        try {
            String sync_status = "";
            if (status.equalsIgnoreCase("true")) {
                sync_status = "Ready_for_Export_Fre";
            } else if (status.equalsIgnoreCase("true_0")) {
                sync_status = "Ready_for_Export_Fre_0";
            } else {
                sync_status = "Error_EP_Fre";
            }
            query = "update " + scheduleTableName + " set last_export_timestamp=?, sync_status=? where sno=? and table_name=?";
            conn = new com.fapps.db.MySQLConnection().getConnection(db_URL, db_User, db_Pass);
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(query);
            last_export_time = inputDate + " " + currentTime().split(" ")[1];
            ps.setString(1, last_export_time);
            ps.setString(2, sync_status);
            ps.setString(3, sno);
            ps.setString(4, transactionTableName);
            ps.addBatch();
            pu.sop("[" + threadName + "] Insert query after batch addition", ps.toString());
            ps.executeBatch();
            conn.commit();
        } catch (Exception ex) {
            System.out.println("[" + threadName + "] EXCEPTION JAR: updateTable: [" + threadName + "]" + ex.toString());
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
}