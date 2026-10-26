package call_processing_v2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * ProcessTCNData - Main Processing Logic for TCN Call Data
 *
 * Business Logic Flow:
 * ----------------------------------------------------------------------- 1.
 * Delete current-month data from destination table (same logic as ProcessData)
 * 2. Fetch Inbound call records : Call_Completed_Date, Agent_First_Name,
 * Caller_ID, Result (from TCN_Inbound_Call) 3. Fetch Manual call records :
 * Call_Completed_Date, Agent_First_Name, TCN_Dialed_Number, Result (from
 * TCN_Manual_Call_final) 4. Merge both lists – phone is the deduplicated key
 * (last 10 digits) 5. For each merged phone: a) Look up
 * Patient_Type_Age_wise_final → get MRD_NO, Visit_Date RULE: Visit_Date >=
 * Call_Completed_Date b) Look up TCN_Inbound_Call_tmp_extra2 → get
 * Inbound_Group_Name (same month) 6. Insert enriched rows into destination
 * table in batches of 5000
 * -----------------------------------------------------------------------
 *
 * Destination table columns (adjust to your actual DDL): sno (auto),
 * Call_Completed_Date, Agent_First_Name, Phone_Number, Result, Call_Source,
 * MRD_NO, Visit_Date, Patient_Type, Inbound_Group_Name, Process_Month
 */
public class ProcessTCNData_Old {

    private static final String            processName    = "TCN_Call_Processing";
    private static final String            processVersion = "2.0";
    private              boolean           debugEnabled   = false;
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp)
                .append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(processName).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(processVersion)
                .append(" Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                .append(" Description: ").append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }

    /**
     * Main entry point - called from TCNMain.
     *
     * @param tableInbound
     *            TCN_Inbound_Call
     * @param tableManual
     *            TCN_Manual_Call_final
     * @param tableOPD
     *            Patient_Type_Age_wise_final
     * @param tableExtra2
     *            TCN_Inbound_Call_tmp_extra2
     * @param destTable
     *            Destination / output table
     * @param processDate
     *            YYYY-MM-DD (D-1)
     * @param processMonth
     *            YYYY-MM
     * @param dbURL
     *            JDBC URL (e.g. jdbc:mysql://localhost:3306/mydb)
     * @param dbName
     *            DB username
     * @param dbPass
     *            DB password
     * @return boolean true = success
     */
    public boolean processTCNData(String tableInbound, String tableManual, String tableOPD, String tableExtra2,
            String destTable, String processDate, String processMonth, String dbURL, String dbName, String dbPass) {

        Connection conn = null;
        PreparedStatement ps = null;
        boolean status = false;
        int batchSize = 5000;
        int rowCount = 0;

        try {

            log("INFO", "STEP 1 - Deleting current month data from dest table: " + destTable
                    + " for month: " + processMonth, "", "DB_DELETE");

            boolean deleted = new TCN_Source().deleteCurrentMonthData(destTable, processMonth, dbURL, dbName, dbPass);

            log("INFO", deleted
                    ? "Current month data deleted from table: " + destTable
                    : "No current month data found to delete in table: " + destTable,
                    "", "DB_DELETE");

            // ================= FETCH =================

            log("INFO", "STEP 2 - Fetching inbound call data from table: " + tableInbound
                    + " for month: " + processMonth + " up to date: " + processDate, "", "DB_SELECT");

            HashMap<String, List<String[]>> inboundData =
                    new TCN_Source().getInboundCallData(
                            tableInbound, processMonth, processDate, dbURL, dbName, dbPass);

            log("INFO", "STEP 3 - Fetching manual call data from table: " + tableManual
                    + " for month: " + processMonth + " up to date: " + processDate, "", "DB_SELECT");

            HashMap<String, List<String[]>> manualData =
                    new TCN_Source().getManualCallData(
                            tableManual, processMonth, processDate, dbURL, dbName, dbPass);

            if (inboundData == null) inboundData = new HashMap<>();
            if (manualData == null)  manualData  = new HashMap<>();

            log("DEBUG", "Inbound phone keys fetched: " + inboundData.size()
                    + " Manual phone keys fetched: " + manualData.size(), "", "DB_SELECT");

            // ================= MERGE =================

            HashMap<String, List<String[]>> mergedData = new HashMap<>();

            for (Map.Entry<String, List<String[]>> e : inboundData.entrySet()) {
                mergedData.put(e.getKey(), new ArrayList<>(e.getValue()));
            }

            for (Map.Entry<String, List<String[]>> e : manualData.entrySet()) {
                List<String[]> list = mergedData.get(e.getKey());

                if (list == null) {
                    list = new ArrayList<String[]>();
                    mergedData.put(e.getKey(), list);
                }

                list.addAll(e.getValue());
            }

            log("INFO", "STEP 4 - Inbound and manual records merged. Total unique phone keys: "
                    + mergedData.size(), "", "DATA_MERGE");

            // ================= FETCH OPD + GROUP =================

            log("INFO", "STEP 5 - Fetching OPD visit data from table: " + tableOPD
                    + " for month: " + processMonth, "", "DB_SELECT");

            HashMap<String, List<String[]>> opdData = new TCN_Source().getOPDVisitData(tableOPD, processMonth,
                    processDate, dbURL, dbName, dbPass);

            log("INFO", "STEP 6 - Fetching inbound group names from table: " + tableExtra2
                    + " for month: " + processMonth, "", "DB_SELECT");

            HashMap<String, String> groupNameData = new TCN_Source().getInboundGroupName(tableExtra2, processMonth,
                    processDate, dbURL, dbName, dbPass);

            log("DEBUG", "OPD phone keys fetched: " + (opdData != null ? opdData.size() : 0)
                    + " Group name phone keys fetched: " + (groupNameData != null ? groupNameData.size() : 0),
                    "", "DB_SELECT");

            // ================= INSERT =================

            String insertQuery = "INSERT INTO " + destTable
                    + " (Call_Completed_Date, Agent_First_Name, Phone_Number, Result, "
                    + "Call_Source, MRD_NO, Visit_Date, Patient_Type, "
                    + "Cluster, Location, Inbound_Group_Name, Process_Month) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            log("DEBUG", "Prepared insert query for dest table: " + insertQuery, "", "DB_INSERT");

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(insertQuery);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

            log("INFO", "STEP 7 - Starting batch insert into dest table: " + destTable
                    + " Batch size: " + batchSize, "", "DB_INSERT");

            for (Map.Entry<String, List<String[]>> entry : mergedData.entrySet()) {

                String phone = entry.getKey();
                if (phone == null)
                    continue;

                phone = phone.trim();
                if (phone.length() != 10)
                    continue;

                List<String[]> records = entry.getValue();

                for (String[] callData : records) {

                    if (callData == null || callData.length < 6)
                        continue;

                    String callCompletedDate = callData[0];
                    String agentFirstName = callData[1];
                    String result = callData[3];
                    String callSource = callData[4];

                    String mrdNo = null;
                    String visitDate = null;
                    String patientType = null;
                    String cluster = null;
                    String location = null;

                    List<String[]> visits = (opdData != null) ? opdData.get(phone) : null;

                    if (visits != null) {
                        for (String[] visit : visits) {

                            String vDate = visit[1];

                            if (vDate != null && callCompletedDate != null) {
                                try {
                                    String callDateOnly = callCompletedDate.length() >= 10
                                            ? callCompletedDate.substring(0, 10) : callCompletedDate;

                                    java.util.Date visitDateObj = sdf.parse(vDate);
                                    java.util.Date callDateObj = sdf.parse(callDateOnly);

                                    if (!visitDateObj.before(callDateObj)) {
                                        mrdNo = visit[0];
                                        visitDate = vDate;
                                        patientType = visit[3];
                                        cluster = visit[4];
                                        location = visit[5];
                                        break;
                                    }

                                } catch (Exception e) {
                                    log("WARN", "Date parse error for phone: " + phone
                                            + " callCompletedDate: " + callCompletedDate
                                            + " visitDate: " + vDate
                                            + " Error: " + e.getMessage(), "", "DATE_PARSE");
                                }
                            }
                        }
                    }

                    String inboundGroupName = (groupNameData != null) ? groupNameData.get(phone) : null;

                    ps.setString(1, callCompletedDate);
                    ps.setString(2, agentFirstName);
                    ps.setString(3, phone);
                    ps.setString(4, result);
                    ps.setString(5, callSource);
                    ps.setString(6, mrdNo);
                    ps.setString(7, visitDate);
                    ps.setString(8, patientType);
                    ps.setString(9, cluster);
                    ps.setString(10, location);
                    ps.setString(11, inboundGroupName);
                    ps.setString(12, processMonth);

                    ps.addBatch();
                    rowCount++;

                    if (rowCount % batchSize == 0) {
                        ps.executeBatch();
                        conn.commit();
                        log("INFO", "Batch committed to dest table: " + destTable
                                + " Total rows inserted so far: " + rowCount, "", "DB_INSERT");
                    }
                }
            }

            if (rowCount % batchSize != 0) {
                ps.executeBatch();
                conn.commit();
            }

            status = true;

            log("INFO", "Processing complete. Dest_Table: " + destTable
                    + " Process_Month: " + processMonth
                    + " Total_Rows_Inserted: " + rowCount, "", "PROCESS_END");

        } catch (Exception ex) {
            log("ERROR", "Exception in processTCNData. Dest_Table: " + destTable
                    + " Process_Month: " + processMonth
                    + " Rows_Attempted: " + rowCount
                    + " Error: " + ex.toString(), "", "PROCESS_END");

            try {
                if (conn != null)
                    conn.rollback();
            } catch (Exception e) {
                log("ERROR", "Rollback failed after exception. Error: " + e.getMessage(),
                        "", "DB_INSERT");
            }

            status = false;

        } finally {
            try {
                if (ps != null)
                    ps.close();
                if (conn != null)
                    conn.close();
            } catch (Exception e) {
                log("WARN", "Failed to close DB resources cleanly. Error: " + e.getMessage(),
                        "", "DB_INSERT");
            }
        }

        return status;
    }
}
