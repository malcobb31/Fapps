package call_processing_v2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * TCN_Source - Data Retrieval Class for TCN Call Processing
 *
 * Methods:
 * 1. deleteCurrentMonthData()        - Delete existing month data from destination table
 * 2. getInboundCallData()            - Fetch Call_Completed_Date, Agent_First_Name, Caller_ID, Result from TCN_Inbound_Call
 * 3. getManualCallData()             - Fetch Call_Completed_Date, Agent_First_Name, TCN_Dialed_Number, Result from TCN_Manual_Call_final
 * 4. getOPDVisitData()               - Fetch MRD, Visit_Date from Patient_Type_Age_wise_final by phone (Visit_Date >= Call_Completed_Date)
 * 5. getInboundGroupName()           - Fetch Inbound_Group_Name from TCN_Inbound_Call_tmp_extra2 for same month by phone
 */
public class TCN_Source {

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

    // =========================================================================
    // BINARY SEARCH HELPER  (same pattern as Source_EyeQ)
    // =========================================================================

    /**
     * Binary Search helper - finds SNO range for date filtering.
     *
     * @param binarySearch "yes" to activate
     * @param date         "ColumnName~~Date~~TrendType"
     * @param sourceTable  Table name
     * @param db_url       DB URL
     * @param db_name      DB name
     * @param db_pass      DB password
     * @return String[4]   [minSNO, maxSNO, fromDate, toDate]
     */
    private String[] binarySearch(String binarySearch, String date, String sourceTable,
                                  String db_url, String db_name, String db_pass) {

        String[] snoRangeReturn = new String[4];

        try {
            log("DEBUG", "Binary search requested for table: " + sourceTable
                    + " Binary search active: " + binarySearch
                    + " Date param: " + date, "", "BINARY_SEARCH");

            if (binarySearch.equalsIgnoreCase("yes")) {

                String[] pull_data_details = date.split("~~");
                String dateColumn = pull_data_details[0];
                String dateValue  = pull_data_details[1];
                String trendType  = pull_data_details[2];

                String[] dates_details = dateValue.split("#");
                boolean hashExist = dates_details.length > 1;

                String from_date_final = "";
                String to_date_final   = "";
                int[]  snoRangeFinal   = new int[2];

                log("DEBUG", "Binary search params - DateColumn: " + dateColumn
                        + " DateValue: " + dateValue
                        + " TrendType: " + trendType
                        + " Table: " + sourceTable, "", "BINARY_SEARCH");

                // Get table min/max SNO
                int[] snoRange = new SNORangeFinder().getStartEndIdOfTable(sourceTable, db_url, db_name, db_pass);
                log("DEBUG", "Table SNO range - Table: " + sourceTable
                        + " Min_SNO: " + snoRange[0] + " Max_SNO: " + snoRange[1], "", "BINARY_SEARCH");

                if (trendType.toLowerCase().equals("monthly")
                        || (trendType.toLowerCase().equals("daily") && hashExist)) {

                    if (!hashExist) {
                        from_date_final = dateValue.substring(0, 8) + "01";
                        to_date_final   = dateValue;
                    } else if (hashExist && trendType.toLowerCase().equals("monthly")) {
                        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
                        java.util.Date shared_date = sdf.parse(dates_details[0]);
                        java.util.Calendar c = java.util.Calendar.getInstance();
                        c.setTime(shared_date);
                        c.add(java.util.Calendar.MONTH, -Integer.parseInt(dates_details[1]));
                        String from_year_month = sdf.format(c.getTime());
                        from_date_final = from_year_month.substring(0, 8) + "01";
                        to_date_final   = dates_details[0].substring(0, 8) + "01";
                    } else {
                        int numberOfDays = (Integer.parseInt(dates_details[1])) - 1;
                        java.util.Date shared_date = new java.text.SimpleDateFormat("yyyy-MM-dd").parse(dates_details[0]);
                        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
                        to_date_final = sdf.format(shared_date);
                        java.util.Calendar c = java.util.Calendar.getInstance();
                        c.setTime(shared_date);
                        c.add(java.util.Calendar.DATE, -numberOfDays);
                        from_date_final = sdf.format(c.getTime());
                    }

                    String[] dates = {from_date_final, to_date_final};

                    for (int i = 0; i < dates.length; i++) {
                        int[] snoRangeToFromDate = new SNORangeFinder().getStartAndEndId(
                                dates[i], snoRange[0], snoRange[1], sourceTable,
                                "DATE_FORMAT(" + dateColumn + ",'%Y-%m-%d')",
                                "daily", "120", false, "sno",
                                db_url, db_name, db_pass);

                        log("DEBUG", "Binary search SNO result - Date: " + dates[i]
                                + " Min_SNO: " + snoRangeToFromDate[0]
                                + " Max_SNO: " + snoRangeToFromDate[1], "", "BINARY_SEARCH");

                        if (i == 0) snoRangeFinal[0] = snoRangeToFromDate[0];
                        else        snoRangeFinal[1] = snoRangeToFromDate[1];
                    }

                    snoRangeReturn[0] = "" + snoRangeFinal[0];
                    snoRangeReturn[1] = "" + snoRangeFinal[1];
                    snoRangeReturn[2] = from_date_final;
                    snoRangeReturn[3] = to_date_final;

                } else {
                    snoRange = new SNORangeFinder().getStartAndEndId(
                            dateValue, snoRange[0], snoRange[1], sourceTable,
                            "DATE_FORMAT(" + dateColumn + ",'%Y-%m-%d')",
                            trendType.toLowerCase(), "120", false, "sno",
                            db_url, db_name, db_pass);

                    snoRangeReturn[0] = "" + snoRange[0];
                    snoRangeReturn[1] = "" + snoRange[1];
                    snoRangeReturn[2] = "NA";
                    snoRangeReturn[3] = "NA";
                }
            }

        } catch (Exception ex) {
            log("ERROR", "Exception in binarySearch. Table: " + sourceTable
                    + " Date: " + date + " Error: " + ex.toString(), "", "BINARY_SEARCH");
        }

        return snoRangeReturn;
    }

    // =========================================================================
    // 1. DELETE CURRENT MONTH DATA  (same logic as Source_EyeQ.deleteCurrentMonthData)
    // =========================================================================

    /**
     * Delete current-month records from the destination table, then reset AUTO_INCREMENT.
     * Logic:  DELETE rows WHERE DATE_FORMAT(Call_Completed_Date,'%Y-%m') = processMonth
     *         Then get MAX(sno) and ALTER TABLE AUTO_INCREMENT = maxSno + 1
     *
     * Example: table has 13 rows total, 3 belong to this month.
     *          After delete -&gt; 10 rows remain (the 3 this-month rows are removed).
     *          AUTO_INCREMENT resets so the next insert continues from row 11 onward.
     *
     * @param destTable    Destination table name
     * @param processMonth Month in YYYY-MM format
     * @param dbURL        DB URL
     * @param dbName       DB name
     * @param dbPass       DB password
     * @return boolean     true if successful
     */
    public boolean deleteCurrentMonthData(String destTable, String processMonth,
                                          String dbURL, String dbName, String dbPass) {

        Connection conn  = null;
        Statement  stmt  = null;
        boolean isDeleted = false;
        int deletedRows   = 0;

        String deleteQuery = "DELETE FROM " + destTable
                + " WHERE DATE_FORMAT(Call_Completed_Date, '%Y-%m') = '" + processMonth + "'";

        try {
            log("INFO", "Deleting month data from table: " + destTable
                    + " Month: " + processMonth
                    + " Query: " + deleteQuery, "", "DB_DELETE");

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement();
            deletedRows = stmt.executeUpdate(deleteQuery);

            log("INFO", "Delete complete. Table: " + destTable
                    + " Rows_Deleted: " + deletedRows, "", "DB_DELETE");

            // CRITICAL: Get MAX SNO and reset AUTO_INCREMENT to continue sequence
            String maxSnoQuery = "SELECT MAX(sno) as maxSno FROM " + destTable;
            ResultSet rs = stmt.executeQuery(maxSnoQuery);

            int maxSno = 0;
            if (rs.next()) {
                maxSno = rs.getInt("maxSno");
            }
            rs.close();

            log("DEBUG", "Current MAX SNO after delete. Table: " + destTable
                    + " Max_SNO: " + maxSno, "", "DB_DELETE");

            // Reset AUTO_INCREMENT to MAX SNO + 1
            String resetAutoIncrementQuery = "ALTER TABLE " + destTable
                    + " AUTO_INCREMENT = " + (maxSno + 1);
            stmt.executeUpdate(resetAutoIncrementQuery);

            log("INFO", "AUTO_INCREMENT reset. Table: " + destTable
                    + " New_Auto_Increment: " + (maxSno + 1), "", "DB_DELETE");

            isDeleted = true;

        } catch (Exception e) {
            log("ERROR", "Exception in deleteCurrentMonthData. Table: " + destTable
                    + " Month: " + processMonth + " Error: " + e.toString(), "", "DB_DELETE");
            isDeleted = false;
        } finally {
            try {
                if (conn != null) {
                    if (stmt != null) stmt.close();
                    conn.close();
                }
            } catch (Exception e) {
                log("WARN", "Failed to close DB resources in deleteCurrentMonthData. Error: "
                        + e.getMessage(), "", "DB_DELETE");
            }
        }

        return isDeleted;
    }

    // =========================================================================
    // 2. INBOUND CALL DATA  (TCN_Inbound_Call)
    // =========================================================================

    /**
     * Fetch records from TCN_Inbound_Call for the given month.
     * Columns fetched: Call_Completed_Date, Agent_First_Name, Caller_ID, Result
     * Phone key = last 10 digits of Caller_ID (cleaned of +/space/-)
     *
     * @param sourceTable  TCN_Inbound_Call table name
     * @param processMonth YYYY-MM
     * @param processDate  YYYY-MM-DD (D-1)
     * @param dbURL        DB URL
     * @param dbName       DB name
     * @param dbPass       DB password
     * @return HashMap&lt;CleanPhone10, String[]&gt;
     *         String[0] = Call_Completed_Date
     *         String[1] = Agent_First_Name
     *         String[2] = Caller_ID (original)
     *         String[3] = Result
     *         String[4] = "INBOUND"  (source tag)
     *         String[5] = CleanPhone10
     */
    public HashMap<String, List<String[]>> getInboundCallData(
            String sourceTable,
            String processMonth,
            String processDate,
            String dbURL,
            String dbName,
            String dbPass) {

        HashMap<String, List<String[]>> data = new HashMap<>();

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {

            String query = "SELECT Call_Completed_Date, Agent_First_Name, Caller_ID, Result, "
                    + "RIGHT(REPLACE(REPLACE(REPLACE(Caller_ID,'+',''),' ',''),'-',''),10) AS CleanPhone "
                    + "FROM " + sourceTable + " "
                    + "WHERE DATE_FORMAT(Call_Completed_Date,'%Y-%m')='" + processMonth + "'";

            log("INFO", "Fetching inbound call data. Table: " + sourceTable
                    + " Month: " + processMonth
                    + " Query: " + query, "", "DB_SELECT");

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);

            int rowCount = 0;
            int skipped  = 0;

            while (rs.next()) {

                String cleanPhone = rs.getString("CleanPhone");

                if (cleanPhone == null || cleanPhone.trim().length() != 10) {
                    skipped++;
                    continue;
                }

                String[] callData = new String[6];
                callData[0] = rs.getString("Call_Completed_Date");
                callData[1] = rs.getString("Agent_First_Name");
                callData[2] = rs.getString("Caller_ID");
                callData[3] = rs.getString("Result");
                callData[4] = "INBOUND";
                callData[5] = cleanPhone;

                List<String[]> list = data.get(cleanPhone);

                if (list == null) {
                    list = new ArrayList<String[]>();
                    data.put(cleanPhone, list);
                }

                list.add(callData);
                rowCount++;
            }

            log("INFO", "Inbound call data fetch complete. Table: " + sourceTable
                    + " Records_Loaded: " + rowCount
                    + " Records_Skipped_Invalid_Phone: " + skipped
                    + " Unique_Phone_Keys: " + data.size(), "", "DB_SELECT");

        } catch (Exception e) {
            log("ERROR", "Exception in getInboundCallData. Table: " + sourceTable
                    + " Month: " + processMonth + " Error: " + e.toString(), "", "DB_SELECT");
        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (Exception e) {
                log("WARN", "Failed to close DB resources in getInboundCallData. Error: "
                        + e.getMessage(), "", "DB_SELECT");
            }
        }

        return data;
    }

    // =========================================================================
    // 3. MANUAL CALL DATA  (TCN_Manual_Call_final)
    // =========================================================================

    /**
     * Fetch records from TCN_Manual_Call_final for the given month.
     * Columns fetched: Call_Completed_Date, Agent_First_Name, TCN_Dialed_Number, Result
     * Phone key = last 10 digits of TCN_Dialed_Number (cleaned of +/space/-)
     *
     * @param sourceTable  TCN_Manual_Call_final table name
     * @param processMonth YYYY-MM
     * @param processDate  YYYY-MM-DD (D-1)
     * @param dbURL        DB URL
     * @param dbName       DB name
     * @param dbPass       DB password
     * @return HashMap&lt;CleanPhone10, String[]&gt;
     *         String[0] = Call_Completed_Date
     *         String[1] = Agent_First_Name
     *         String[2] = TCN_Dialed_Number (original)
     *         String[3] = Result
     *         String[4] = "MANUAL"  (source tag)
     *         String[5] = CleanPhone10
     */
    public HashMap<String, List<String[]>> getManualCallData(
            String sourceTable,
            String processMonth,
            String processDate,
            String dbURL,
            String dbName,
            String dbPass) {

        HashMap<String, List<String[]>> data = new HashMap<>();

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        try {

            String query = "SELECT "
                    + "Call_Completed_Date, "
                    + "Agent_First_Name, "
                    + "TCN_Dialed_Number, "
                    + "Result, "
                    + "RIGHT(REPLACE(REPLACE(REPLACE(TCN_Dialed_Number, '+', ''), ' ', ''), '-', ''), 10) AS CleanPhone "
                    + "FROM " + sourceTable + " "
                    + "WHERE DATE_FORMAT(Call_Completed_Date, '%Y-%m') = '" + processMonth + "'";

            log("INFO", "Fetching manual call data. Table: " + sourceTable
                    + " Month: " + processMonth
                    + " Query: " + query, "", "DB_SELECT");

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);

            int rowCount = 0;

            while (rs.next()) {

                String cleanPhone = rs.getString("CleanPhone");

                //  STRICT FILTER (important)
                if (cleanPhone == null) continue;

                cleanPhone = cleanPhone.trim();

                if (cleanPhone.length() != 10) continue;

                String[] callData = new String[6];

                callData[0] = rs.getString("Call_Completed_Date");
                callData[1] = rs.getString("Agent_First_Name");
                callData[2] = rs.getString("TCN_Dialed_Number");
                callData[3] = rs.getString("Result");
                callData[4] = "MANUAL";
                callData[5] = cleanPhone;

                //  NO DEDUP - KEEP ALL RECORDS
                List<String[]> list = data.get(cleanPhone);

                if (list == null) {
                    list = new ArrayList<String[]>();
                    data.put(cleanPhone, list);
                }

                list.add(callData);

                rowCount++;
            }

            log("INFO", "Manual call data fetch complete. Table: " + sourceTable
                    + " Records_Loaded: " + rowCount
                    + " Unique_Phone_Keys: " + data.size(), "", "DB_SELECT");

        } catch (Exception ex) {
            log("ERROR", "Exception in getManualCallData. Table: " + sourceTable
                    + " Month: " + processMonth + " Error: " + ex.toString(), "", "DB_SELECT");
            data = null;

        } finally {
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (Exception e) {
                log("WARN", "Failed to close DB resources in getManualCallData. Error: "
                        + e.getMessage(), "", "DB_SELECT");
            }
        }

        return data;
    }

    // =========================================================================
    // 4. OPD VISIT DATA  (Patient_Type_Age_wise_final)
    // =========================================================================

    /**
     * Fetch OPD visit records from Patient_Type_Age_wise_final.
     * Match on last-10-digit phone (CONTACT_DETAILS / CONTACT_DETAILS1).
     * Rule: Visit_Date &gt;= Call_Completed_Date  (enforced in ProcessTCNData)
     *
     * Returns all visit rows per phone so the caller can apply the date check.
     *
     * @param sourceTable  Patient_Type_Age_wise_final table name
     * @param processMonth YYYY-MM
     * @param processDate  YYYY-MM-DD (D-1)
     * @param dbURL        DB URL
     * @param dbName       DB name
     * @param dbPass       DB password
     * @return HashMap&lt;CleanPhone10, List&lt;String[]&gt;&gt;
     *         String[0] = MRD_NO
     *         String[1] = Visit_Date  (YYYY-MM-DD)
     *         String[2] = REGISTERED_DATETIME
     *         String[3] = PATIENT_TYPE
     *         String[4] = Cluster
     *         String[5] = LOCATION_NAME
     */
    public HashMap<String, List<String[]>> getOPDVisitData(String sourceTable,
                                                           String processMonth,
                                                           String processDate,
                                                           String dbURL,
                                                           String dbName,
                                                           String dbPass) {

        HashMap<String, List<String[]>> data = null;
        Connection conn  = null;
        Statement  stmt  = null;
        ResultSet  rs    = null;
        String     query = null;
        int rowCount = 0;

        try {
            String firstDayOfMonth = processMonth + "-01";

            String binaryDate  = "VISIT_DATE~~" + firstDayOfMonth + "~~monthly";
            String[] snoRange  = binarySearch("yes", binaryDate, sourceTable, dbURL, dbName, dbPass);

            String binaryDate2 = "VISIT_DATE~~" + processDate + "~~daily";
            String[] snoRange2 = binarySearch("yes", binaryDate2, sourceTable, dbURL, dbName, dbPass);

            log("DEBUG", "OPD binary search SNO range - Table: " + sourceTable
                    + " From_SNO: " + snoRange[0] + " To_SNO: " + snoRange2[1], "", "BINARY_SEARCH");

            query = "SELECT "
                    + "RIGHT(REPLACE(REPLACE(REPLACE(CONTACT_DETAILS,  '+', ''), ' ', ''), '-', ''), 10) AS CD1, "
                    + "RIGHT(REPLACE(REPLACE(REPLACE(CONTACT_DETAILS1, '+', ''), ' ', ''), '-', ''), 10) AS CD2, "
                    + "MRD_NO, "
                    + "DATE_FORMAT(VISIT_DATE, '%Y-%m-%d') AS Visit_Date, "
                    + "REGISTERED_DATETIME, "
                    + "PATIENT_TYPE, "
                    + "Cluster, "
                    + "LOCATION_NAME "
                    + "FROM " + sourceTable + " "
                    + "WHERE sno >= " + snoRange[0] + " AND sno <= " + snoRange2[1] + " "
                    + "AND DATE_FORMAT(VISIT_DATE, '%Y-%m') = '" + processMonth + "'";

            log("INFO", "Fetching OPD visit data. Table: " + sourceTable
                    + " Month: " + processMonth
                    + " SNO_Range: " + snoRange[0] + " to " + snoRange2[1]
                    + " Query: " + query, "", "DB_SELECT");

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs   = stmt.executeQuery(query);

            rs.last();
            rowCount = rs.getRow();
            rs.beforeFirst();

            log("INFO", "OPD visit records found. Table: " + sourceTable
                    + " Total_Records: " + rowCount, "", "DB_SELECT");

            if (rowCount > 0) {
                data = new HashMap<String, List<String[]>>();

                while (rs.next()) {
                    String[] opdData = new String[6];
                    opdData[0] = rs.getString("MRD_NO");
                    opdData[1] = rs.getString("Visit_Date");
                    opdData[2] = rs.getString("REGISTERED_DATETIME");
                    opdData[3] = rs.getString("PATIENT_TYPE");
                    opdData[4] = rs.getString("Cluster");
                    opdData[5] = rs.getString("LOCATION_NAME");

                    String cd1 = rs.getString("CD1");
                    String cd2 = rs.getString("CD2");

                    if (cd1 != null && !cd1.trim().isEmpty()) {
                        if (!data.containsKey(cd1)) data.put(cd1, new ArrayList<String[]>());
                        data.get(cd1).add(opdData);
                    }
                    if (cd2 != null && !cd2.trim().isEmpty()) {
                        if (!data.containsKey(cd2)) data.put(cd2, new ArrayList<String[]>());
                        data.get(cd2).add(opdData);
                    }
                }

                log("DEBUG", "OPD data indexed by phone. Unique_Phone_Keys: " + data.size(), "", "DB_SELECT");
            }

        } catch (Exception ex) {
            log("ERROR", "Exception in getOPDVisitData. Table: " + sourceTable
                    + " Month: " + processMonth + " Error: " + ex.toString(), "", "DB_SELECT");
            data = null;
        } finally {
            try {
                if (conn != null) {
                    if (rs   != null) rs.close();
                    if (stmt != null) stmt.close();
                    conn.close();
                }
            } catch (Exception e) {
                log("WARN", "Failed to close DB resources in getOPDVisitData. Error: "
                        + e.getMessage(), "", "DB_SELECT");
            }
        }

        return data;
    }

    // =========================================================================
    // 5. INBOUND GROUP NAME  (TCN_Inbound_Call_tmp_extra2)
    // =========================================================================

    /**
     * Fetch Inbound_Group_Name from TCN_Inbound_Call_tmp_extra2 for the same month,
     * keyed by last-10-digit phone (CALLER_ID).
     *
     * @param sourceTable  TCN_Inbound_Call_tmp_extra2 table name
     * @param processMonth YYYY-MM
     * @param processDate  YYYY-MM-DD (D-1)
     * @param dbURL        DB URL
     * @param dbName       DB name
     * @param dbPass       DB password
     * @return HashMap&lt;CleanPhone10, String&gt;  Inbound_Group_Name per phone
     */
    public HashMap<String, String> getInboundGroupName(String sourceTable,
                                                       String processMonth,
                                                       String processDate,
                                                       String dbURL,
                                                       String dbName,
                                                       String dbPass) {

        HashMap<String, String> data = null;
        Connection conn  = null;
        Statement  stmt  = null;
        ResultSet  rs    = null;
        String     query = null;
        int rowCount = 0;

        try {
            String firstDayOfMonth = processMonth + "-01";

            String binaryDate  = "Call_Completed_Date~~" + firstDayOfMonth + "~~monthly";
            String[] snoRange  = binarySearch("yes", binaryDate, sourceTable, dbURL, dbName, dbPass);

            String binaryDate2 = "Call_Completed_Date~~" + processDate + "~~daily";
            String[] snoRange2 = binarySearch("yes", binaryDate2, sourceTable, dbURL, dbName, dbPass);

            log("DEBUG", "Inbound group binary search SNO range - Table: " + sourceTable
                    + " From_SNO: " + snoRange[0] + " To_SNO: " + snoRange2[1], "", "BINARY_SEARCH");

            query = "SELECT "
                    + "RIGHT(REPLACE(REPLACE(REPLACE(CALLER_ID, '+', ''), ' ', ''), '-', ''), 10) AS CleanPhone, "
                    + "Inbound_Group_Name "
                    + "FROM " + sourceTable + " "
                    + "WHERE sno >= " + snoRange[0] + " AND sno <= " + snoRange2[1] + " "
                    + "AND DATE_FORMAT(Call_Completed_Date, '%Y-%m') = '" + processMonth + "'";

            log("INFO", "Fetching inbound group names. Table: " + sourceTable
                    + " Month: " + processMonth
                    + " SNO_Range: " + snoRange[0] + " to " + snoRange2[1]
                    + " Query: " + query, "", "DB_SELECT");

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs   = stmt.executeQuery(query);

            rs.last();
            rowCount = rs.getRow();
            rs.beforeFirst();

            log("INFO", "Inbound group records found. Table: " + sourceTable
                    + " Total_Records: " + rowCount, "", "DB_SELECT");

            if (rowCount > 0) {
                data = new HashMap<String, String>();

                while (rs.next()) {
                    String cleanPhone = rs.getString("CleanPhone");
                    String groupName  = rs.getString("Inbound_Group_Name");

                    if (cleanPhone != null && !cleanPhone.trim().isEmpty()) {
                        // Keep first match per phone
                        if (!data.containsKey(cleanPhone)) {
                            data.put(cleanPhone, groupName);
                        }
                    }
                }

                log("DEBUG", "Inbound group data indexed by phone. Unique_Phone_Keys: "
                        + data.size(), "", "DB_SELECT");
            }

        } catch (Exception ex) {
            log("ERROR", "Exception in getInboundGroupName. Table: " + sourceTable
                    + " Month: " + processMonth + " Error: " + ex.toString(), "", "DB_SELECT");
            data = null;
        } finally {
            try {
                if (conn != null) {
                    if (rs   != null) rs.close();
                    if (stmt != null) stmt.close();
                    conn.close();
                }
            } catch (Exception e) {
                log("WARN", "Failed to close DB resources in getInboundGroupName. Error: "
                        + e.getMessage(), "", "DB_SELECT");
            }
        }

        return data;
    }
}
