package metadata_parser_v1;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * ProcessData - PostgreSQL Data Parser and Transfer
 * 
 * Business Logic Flow: 1. Parse and validate user arguments 2. Validate source
 * and destination tables exist 3. Validate column structures 4. Fetch data from
 * source table 5. Transform data (direct columns + JSON metadata) 6. Insert
 * into destination table
 */
public class metadata_parser_process {

    private static final String            processName    = "metadata-parser";
    private static final String            processVersion = "1.0";
    private static final boolean           debugEnabled   = true;
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

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
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    // Configuration flag for future enhancement
    private static final boolean SKIP_MISSING_COLUMNS = false; // Set to true to
                                                                // skip missing
                                                                // columns
                                                                // instead of
                                                                // stopping

    /**
     * Main processing method
     */
    public boolean parseAndTransferData(String dbURL, String dbUsername, String dbPassword, String sourceInput,
            String destTableName, String directColumns, int metadataCount, String destMetadataColumns,
            String sourceMetadataColumns, String jsonKeyNames) {

        boolean status = false;

        try {
            log("INFO", "===== START CSV/DB PROCESSING | Source: " + sourceInput +
                    " | Destination Table: " + destTableName + " =====", null, "FILE_PROCESS");

            boolean isCSV = sourceInput != null && sourceInput.toLowerCase().endsWith(".csv");

            // ===== STEP 1: Parse arguments =====
            String[] directColArray    = parseCommaSeparated(directColumns);
            String[] destMetaColArray  = parseCommaSeparated(destMetadataColumns);
            String[] sourceMetaGroups  = parseHashSeparated(sourceMetadataColumns);
            String[] jsonKeyGroups     = parseHashSeparated(jsonKeyNames);

            List<String[]> sourceMetaColumnsList = new ArrayList<>();
            List<String[]> jsonKeyNamesList      = new ArrayList<>();

            for (int i = 0; i < metadataCount; i++) {
                String[] sourceCols = parseCommaSeparated(sourceMetaGroups[i]);
                String[] jsonKeys   = parseCommaSeparated(jsonKeyGroups[i]);

                if (sourceCols.length != jsonKeys.length) {
                    log("ERROR", "Metadata column count mismatch at index " + i +
                            " | Source cols: " + sourceCols.length + " | JSON keys: " + jsonKeys.length, null, "VALIDATION");
                    return false;
                }

                sourceMetaColumnsList.add(sourceCols);
                jsonKeyNamesList.add(jsonKeys);
            }

            // ===== STEP 2: Validate destination table only =====
            if (!validateTableExists(destTableName, dbURL, dbUsername, dbPassword)) {
                log("ERROR", "Destination table not found: " + destTableName, null, "VALIDATION");
                return false;
            }

            HashSet<String> destTableColumns = getTableColumns(destTableName, dbURL, dbUsername, dbPassword);

            for (String col : directColArray) {
                if (!destTableColumns.contains(col.toLowerCase())) {
                    log("ERROR", "Direct column missing in destination table: " + col +
                            " | Table: " + destTableName, null, "VALIDATION");
                    return false;
                }
            }

            for (String col : destMetaColArray) {
                if (!destTableColumns.contains(col.toLowerCase())) {
                    log("ERROR", "Metadata column missing in destination table: " + col +
                            " | Table: " + destTableName, null, "VALIDATION");
                    return false;
                }
            }

            // ===== STEP 3: Fetch Data (CSV or DB) =====
            HashMap<String, String[]> sourceData;

            if (isCSV) {
                log("INFO", "Reading data from CSV file: " + sourceInput, null, "FILE_PROCESS");
                sourceData = readCSV(sourceInput);
            } else {
                log("INFO", "Reading data from database table: " + sourceInput, null, "DB_QUERY");
                sourceData = new metadata_parser_source().fetchSourceData(sourceInput, directColArray, sourceMetaColumnsList, dbURL,
                        dbUsername, dbPassword);
            }

            if (sourceData == null || sourceData.isEmpty()) {
                log("WARN", "No data found in source: " + sourceInput +
                        " | Records returned: 0", null, "DATA_FETCH");
                return false;
            }

            log("INFO", "Data fetch complete | Source: " + sourceInput +
                    " | Total records fetched: " + sourceData.size(), null, "DATA_FETCH");

            // ===== STEP 4: Insert (UNCHANGED LOGIC) =====
            boolean inserted = insertDataToDestination(sourceData, destTableName, directColArray, destMetaColArray,
                    sourceMetaColumnsList, jsonKeyNamesList, dbURL, dbUsername, dbPassword);

            if (!inserted) {
                log("ERROR", "Data insert failed | Source: " + sourceInput +
                        " | Destination table: " + destTableName, null, "DATA_INSERT");
                return false;
            }

            log("INFO", "Processing completed successfully | Source: " + sourceInput +
                    " | Destination table: " + destTableName + " | Records processed: " + sourceData.size(), null, "FILE_PROCESS");

            status = true;

        } catch (Exception ex) {
            log("ERROR", "Unhandled exception in parseAndTransferData | Source: " + sourceInput +
                    " | Error: " + ex.getMessage(), null, "APP_ERROR");
            status = false;
        }

        return status;
    }

    private HashMap<String, String[]> readCSV(String filePath) {

        HashMap<String, String[]> data = new java.util.LinkedHashMap<>();

        try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(filePath))) {

            String headerLine = br.readLine();
            if (headerLine == null) {
                log("WARN", "Empty CSV file - no header line found | File: " + filePath, null, "FILE_PROCESS");
                return null;
            }

            // Parse headers safely
            String[] headers = headerLine.split(",", -1);
            log("DEBUG", "CSV header parsed | File: " + filePath + " | Column count: " + headers.length, null, "FILE_PROCESS");

            String line;
            int count = 0;

            while ((line = br.readLine()) != null) {

                java.util.List<String> valuesList = new java.util.ArrayList<>();
                StringBuilder sb = new StringBuilder();
                boolean inQuotes = false;

                for (char c : line.toCharArray()) {
                    if (c == '"') {
                        inQuotes = !inQuotes;
                    } else if (c == ',' && !inQuotes) {
                        valuesList.add(sb.toString());
                        sb.setLength(0);
                    } else {
                        sb.append(c);
                    }
                }
                valuesList.add(sb.toString());

                String[] rawValues = valuesList.toArray(new String[0]);

                // Normalize row length to header length
                String[] finalRow = new String[headers.length];

                for (int i = 0; i < headers.length; i++) {
                    if (i < rawValues.length) {
                        finalRow[i] = rawValues[i];
                    } else {
                        finalRow[i] = null;
                    }
                }

                if (rawValues.length != headers.length) {
                    log("WARN", "Column count mismatch at row " + count +
                            " | File: " + filePath +
                            " | Expected: " + headers.length +
                            " | Found: " + rawValues.length +
                            " | Row padded with nulls for missing columns", null, "FILE_PROCESS");
                }

                data.put(String.valueOf(count), finalRow);
                count++;
            }

            log("INFO", "CSV read complete | File: " + filePath +
                    " | Total rows read: " + data.size(), null, "FILE_PROCESS");

        } catch (Exception e) {
            log("ERROR", "Failed to read CSV file: " + filePath + " | Error: " + e.getMessage(), null, "FILE_PROCESS");
            return null;
        }

        return data;
    }

    /**
     * Parse comma-separated string into array
     */
    private String[] parseCommaSeparated(String input) {
        if (input == null || input.trim().isEmpty()) {
            return new String[0];
        }

        String[] parts = input.split(",");
        for (int i = 0; i < parts.length; i++) {
            parts[i] = parts[i].trim();
        }

        return parts;
    }

    /**
     * Parse hash-separated string into array
     */
    private String[] parseHashSeparated(String input) {
        if (input == null || input.trim().isEmpty()) {
            return new String[0];
        }

        String[] parts = input.split("#");
        for (int i = 0; i < parts.length; i++) {
            parts[i] = parts[i].trim();
        }

        return parts;
    }

    /**
     * Validate if table exists
     */
    private boolean validateTableExists(String tableName, String dbURL, String dbUser, String dbPass) {

        Connection conn = null;
        boolean exists = false;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            String schemaName     = "public";
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            log("DEBUG", "Checking table existence | Schema: " + schemaName +
                    " | Table: " + actualTableName, null, "VALIDATION");

            String sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                ps.setString(2, actualTableName);

                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        exists = true;
                        log("INFO", "Table found | Schema: " + schemaName +
                                " | Table: " + actualTableName, null, "VALIDATION");
                    } else {
                        log("WARN", "Table not found | Schema: " + schemaName +
                                " | Table: " + actualTableName, null, "VALIDATION");
                    }
                }
            }

        } catch (Exception ex) {
            log("ERROR", "Exception during table existence check | Table: " + tableName +
                    " | Error: " + ex.getMessage(), null, "VALIDATION");
            exists = false;
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (Exception e) {
                log("ERROR", "Failed to close DB connection after table validation | Error: " + e.getMessage(), null, "VALIDATION");
            }
        }

        return exists;
    }

    /**
     * Get all columns of a table
     */
    private HashSet<String> getTableColumns(String tableName, String dbURL, String dbUser, String dbPass) {

        Connection conn = null;
        HashSet<String> columns = new HashSet<>();

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
            DatabaseMetaData metaData = conn.getMetaData();

            // Handle schema-qualified table names (e.g.,
            // billing.gst_registration_raw)
            String schemaName     = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            log("DEBUG", "Fetching column list | Schema: " + schemaName +
                    " | Table: " + actualTableName, null, "VALIDATION");

            // PostgreSQL stores table names in lowercase by default
            ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, null);

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                columns.add(columnName.toLowerCase());
                log("DEBUG", "Column found: " + columnName + " | Table: " + tableName, null, "VALIDATION");
            }

            rs.close();

            log("INFO", "Column list fetched | Table: " + tableName +
                    " | Total columns: " + columns.size(), null, "VALIDATION");

        } catch (Exception ex) {
            log("ERROR", "Failed to retrieve column list | Table: " + tableName +
                    " | Error: " + ex.getMessage(), null, "VALIDATION");
            columns = null;
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                log("ERROR", "Failed to close DB connection after fetching columns | Error: " + e.getMessage(), null, "VALIDATION");
            }
        }

        return columns;
    }

    /**
     * Validate data types can be converted to string
     */
    private boolean validateDataTypes(String sourceTableName, String destTableName, String[] directColumns,
            String[] destMetaColumns, String dbURL, String dbUser, String dbPass) {

        Connection conn = null;
        boolean valid = true;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
            DatabaseMetaData metaData = conn.getMetaData();

            // Check direct columns in both tables
            for (String col : directColumns) {
                // Check source
                String sourceType = getColumnType(metaData, sourceTableName.toLowerCase(), col.toLowerCase());
                if (sourceType == null) {
                    log("ERROR", "Unable to determine type for source column: " + col +
                            " | Source table: " + sourceTableName, null, "VALIDATION");
                    valid = false;
                    continue;
                }

                // Check destination
                String destType = getColumnType(metaData, destTableName.toLowerCase(), col.toLowerCase());
                if (destType == null) {
                    log("ERROR", "Unable to determine type for destination column: " + col +
                            " | Destination table: " + destTableName, null, "VALIDATION");
                    valid = false;
                    continue;
                }

                // Validate can convert to string
                if (!isConvertibleToString(sourceType)) {
                    log("ERROR", "Source column type cannot be converted to string | Column: " + col +
                            " | Type: " + sourceType + " | Source table: " + sourceTableName, null, "VALIDATION");
                    valid = false;
                    continue;
                }

                if (!isConvertibleToString(destType)) {
                    log("ERROR", "Destination column type cannot be converted to string | Column: " + col +
                            " | Type: " + destType + " | Destination table: " + destTableName, null, "VALIDATION");
                    valid = false;
                    continue;
                }

                log("DEBUG", "Column type validated | Column: " + col +
                        " | Source type: " + sourceType + " | Destination type: " + destType, null, "VALIDATION");
            }

            // Check destination metadata columns (should be text/varchar/jsonb)
            for (String col : destMetaColumns) {
                String destType = getColumnType(metaData, destTableName.toLowerCase(), col.toLowerCase());
                if (destType == null) {
                    log("ERROR", "Unable to determine type for destination metadata column: " + col +
                            " | Table: " + destTableName, null, "VALIDATION");
                    valid = false;
                    continue;
                }

                if (!isTextType(destType)) {
                    log("WARN", "Destination metadata column is not text/varchar/jsonb - JSON data may not fit | " +
                            "Column: " + col + " | Type: " + destType, null, "VALIDATION");
                }

                log("DEBUG", "Metadata column type validated | Column: " + col +
                        " | Type: " + destType, null, "VALIDATION");
            }

        } catch (Exception ex) {
            log("ERROR", "Exception during data type validation | Error: " + ex.getMessage(), null, "VALIDATION");
            valid = false;
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                log("ERROR", "Failed to close DB connection after type validation | Error: " + e.getMessage(), null, "VALIDATION");
            }
        }

        return valid;
    }

    /**
     * Get column data type
     */
    private String getColumnType(DatabaseMetaData metaData, String tableName, String columnName) throws Exception {

        // Handle schema-qualified table names
        String schemaName     = null;
        String actualTableName = tableName;

        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.");
            schemaName      = parts[0].toLowerCase();
            actualTableName = parts[1].toLowerCase();
        } else {
            actualTableName = tableName.toLowerCase();
        }

        ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, columnName);

        String type = null;
        if (rs.next()) {
            type = rs.getString("TYPE_NAME");
        }

        rs.close();

        return type;
    }

    /**
     * Check if data type can be converted to string
     */
    private boolean isConvertibleToString(String dataType) {

        if (dataType == null) {
            return false;
        }

        String type = dataType.toLowerCase();

        // PostgreSQL types that can be converted to string
        return type.contains("char") || type.contains("text") || type.contains("int") || type.contains("numeric")
                || type.contains("decimal") || type.contains("float") || type.contains("double")
                || type.contains("real") || type.contains("bool") || type.contains("date") || type.contains("time")
                || type.contains("json") || type.contains("uuid") || type.contains("serial") || type.contains("money");
    }

    /**
     * Check if data type is text-based (for JSON storage)
     */
    private boolean isTextType(String dataType) {

        if (dataType == null) {
            return false;
        }

        String type = dataType.toLowerCase();

        return type.contains("char") || type.contains("text") || type.contains("json");
    }

    /**
     * Insert data into destination table
     */
    
    /**
     * Insert data into destination table
     */
    private boolean insertDataToDestination(HashMap<String, String[]> sourceData, String destTableName,
            String[] directColumns, String[] destMetaColumns, List<String[]> sourceMetaColumnsList,
            List<String[]> jsonKeyNamesList, String dbURL, String dbUser, String dbPass) {

        Connection conn = null;
        PreparedStatement ps = null;
        boolean status = false;
        int batchSize = 200;
        int rowCount = 0;

        int inserted = 0;
        int skipped  = 0;
        int unknown  = 0;
        int totalRows = sourceData.size();

        long beforeCount = -1;
        long afterCount  = -1;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            // Get column types for destination table
            HashMap<String, String> columnTypes = getDestinationColumnTypes(destTableName, directColumns,
                    destMetaColumns, conn);

            if (columnTypes == null || columnTypes.size() == 0) {
                log("ERROR", "Could not fetch destination column types | Table: " + destTableName, null, "DATA_INSERT");
                return false;
            }

            // ===== GET ROW COUNT BEFORE INSERT =====
            try (PreparedStatement countPs = conn.prepareStatement("SELECT COUNT(*) FROM " + destTableName)) {
                try (ResultSet countRs = countPs.executeQuery()) {
                    if (countRs.next()) {
                        beforeCount = countRs.getLong(1);
                    }
                }
            }

            // Build INSERT query (table name used as-is for INSERT, PostgreSQL
            // will handle schema)
            StringBuilder insertQuery = new StringBuilder("INSERT INTO " + destTableName + " (");

            // Add direct columns
            for (int i = 0; i < directColumns.length; i++) {
                insertQuery.append(directColumns[i]);
                if (i < directColumns.length - 1 || destMetaColumns.length > 0) {
                    insertQuery.append(", ");
                }
            }

            // Add metadata columns
            for (int i = 0; i < destMetaColumns.length; i++) {
                insertQuery.append(destMetaColumns[i]);
                if (i < destMetaColumns.length - 1) {
                    insertQuery.append(", ");
                }
            }

            insertQuery.append(") VALUES (");

            // Add placeholders
            int totalColumns = directColumns.length + destMetaColumns.length;
            for (int i = 0; i < totalColumns; i++) {
                insertQuery.append("?");
                if (i < totalColumns - 1) {
                    insertQuery.append(", ");
                }
            }

            insertQuery.append(") ON CONFLICT DO NOTHING");

            log("DEBUG", "Prepared insert query | Table: " + destTableName +
                    " | Query: " + insertQuery.toString(), null, "DATA_INSERT");

            conn.setAutoCommit(false);
            ps = conn.prepareStatement(insertQuery.toString());

            // Get column count for source data array
            int directColCount = directColumns.length;

            // Process each row
            for (Map.Entry<String, String[]> entry : sourceData.entrySet()) {
                String[] record = entry.getValue();

                int paramIndex = 1;

                // Set direct column values with proper type casting
                for (int i = 0; i < directColumns.length; i++) {
                    String columnName = directColumns[i].toLowerCase();
                    String value      = (i < record.length) ? record[i] : null;
                    String columnType = columnTypes.get(columnName);

                    setParameterByType(ps, paramIndex, value, columnType, columnName);
                    paramIndex++;
                }

                // Set metadata JSON values
                int recordIndex = directColCount;
                for (int i = 0; i < destMetaColumns.length; i++) {
                    String[] sourceCols = sourceMetaColumnsList.get(i);
                    String[] jsonKeys   = jsonKeyNamesList.get(i);

                    // Build JSON object
                    StringBuilder jsonBuilder = new StringBuilder("{");

                    for (int j = 0; j < sourceCols.length; j++) {
                        String key   = jsonKeys[j];
                        String value = (recordIndex < record.length) ? record[recordIndex] : null;

                        // Escape quotes and backslashes in value for JSON
                        String escapedValue = value;
                        if (escapedValue != null) {
                            escapedValue = escapedValue.replace("\\", "\\\\").replace("\"", "\\\"");
                        }

                        jsonBuilder.append("\"").append(key).append("\":");

                        if (escapedValue != null) {
                            jsonBuilder.append("\"").append(escapedValue).append("\"");
                        } else {
                            jsonBuilder.append("null");
                        }

                        if (j < sourceCols.length - 1) {
                            jsonBuilder.append(",");
                        }

                        recordIndex++;
                    }

                    jsonBuilder.append("}");

                    String columnName = destMetaColumns[i].toLowerCase();
                    String columnType = columnTypes.get(columnName);

                    // For JSONB columns, use setObject with proper type
                    if (columnType != null && columnType.toLowerCase().contains("jsonb")) {
                        ps.setObject(paramIndex, jsonBuilder.toString(), java.sql.Types.OTHER);
                    } else {
                        ps.setString(paramIndex, jsonBuilder.toString());
                    }

                    paramIndex++;
                }

                ps.addBatch();
                rowCount++;

                // ===== EXECUTE MID-LOOP BATCHES - NO LOGGING, ACCUMULATE COUNTS =====
                if (rowCount % batchSize == 0) {
                    int[] results = ps.executeBatch();
                    for (int r : results) {
                        if      (r == 1)                             inserted++;
                        else if (r == 0)                             skipped++;
                        else if (r == java.sql.Statement.SUCCESS_NO_INFO) unknown++;
                    }
                }
            }

            // ===== EXECUTE FINAL REMAINING BATCH =====
            if (ps != null) {
                int[] results = ps.executeBatch();
                for (int r : results) {
                    if      (r == 1)                             inserted++;
                    else if (r == 0)                             skipped++;
                    else if (r == java.sql.Statement.SUCCESS_NO_INFO) unknown++;
                }
            }

            conn.commit();

            // ===== GET ROW COUNT AFTER INSERT =====
            try (PreparedStatement countPs = conn.prepareStatement("SELECT COUNT(*) FROM " + destTableName)) {
                try (ResultSet countRs = countPs.executeQuery()) {
                    if (countRs.next()) {
                        afterCount = countRs.getLong(1);
                    }
                }
            }

            status = true;

            // ===== SINGLE FINAL SUMMARY LOG =====
            log("INFO",
                "Insert Summary | Table: " + destTableName +
                " | Total Rows (CSV): " + totalRows +
                " | Inserted (new): " + inserted +
                " | Skipped (already exists): " + skipped +
                " | Unknown (driver): " + unknown +
                " | Rows Before: " + beforeCount +
                " | Rows After: " + afterCount +
                " | Net Added: " + (afterCount - beforeCount),
                null,
                "DATA_INSERT"
            );

        } catch (Exception ex) {
            log("ERROR", "Insert failed | Table: " + destTableName +
                    " | Rows processed before failure: " + rowCount +
                    " | Error: " + ex.getMessage(), null, "DATA_INSERT");

            try {
                if (conn != null)
                    conn.rollback();
                log("INFO", "Transaction rolled back successfully | Table: " + destTableName, null, "DATA_INSERT");
            } catch (Exception e) {
                log("ERROR", "Rollback failed | Table: " + destTableName +
                        " | Error: " + e.getMessage(), null, "DATA_INSERT");
            }

            status = false;

        } finally {
            try {
                if (conn != null) {
                    if (ps != null)
                        ps.close();
                    conn.close();
                }
            } catch (Exception e) {
                log("ERROR", "Failed to close DB resources after insert | Error: " + e.getMessage(), null, "DATA_INSERT");
            }
        }

        return status;
    }
//    private boolean insertDataToDestination(HashMap<String, String[]> sourceData, String destTableName,
//            String[] directColumns, String[] destMetaColumns, List<String[]> sourceMetaColumnsList,
//            List<String[]> jsonKeyNamesList, String dbURL, String dbUser, String dbPass) {
//
//        Connection conn = null;
//        PreparedStatement ps = null;
//        boolean status = false;
//        int batchSize = 200;
//        int rowCount = 0;
//
//        try {
//            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
//
//            // Get column types for destination table
//            HashMap<String, String> columnTypes = getDestinationColumnTypes(destTableName, directColumns,
//                    destMetaColumns, conn);
//
//            if (columnTypes == null || columnTypes.size() == 0) {
//                log("ERROR", "Could not fetch destination column types | Table: " + destTableName, null, "DATA_INSERT");
//                return false;
//            }
//
//            // Build INSERT query (table name used as-is for INSERT, PostgreSQL
//            // will handle schema)
//            StringBuilder insertQuery = new StringBuilder("INSERT INTO " + destTableName + " (");
//
//            // Add direct columns
//            for (int i = 0; i < directColumns.length; i++) {
//                insertQuery.append(directColumns[i]);
//                if (i < directColumns.length - 1 || destMetaColumns.length > 0) {
//                    insertQuery.append(", ");
//                }
//            }
//
//            // Add metadata columns
//            for (int i = 0; i < destMetaColumns.length; i++) {
//                insertQuery.append(destMetaColumns[i]);
//                if (i < destMetaColumns.length - 1) {
//                    insertQuery.append(", ");
//                }
//            }
//
//            insertQuery.append(") VALUES (");
//
//            // Add placeholders
//            int totalColumns = directColumns.length + destMetaColumns.length;
//            for (int i = 0; i < totalColumns; i++) {
//                insertQuery.append("?");
//                if (i < totalColumns - 1) {
//                    insertQuery.append(", ");
//                }
//            }
//
//            insertQuery.append(") ON CONFLICT DO NOTHING");
//
//            log("DEBUG", "Prepared insert query | Table: " + destTableName +
//                    " | Query: " + insertQuery.toString(), null, "DATA_INSERT");
//
//            conn.setAutoCommit(false);
//            ps = conn.prepareStatement(insertQuery.toString());
//
//            // Get column count for source data array
//            int directColCount = directColumns.length;
//
//            // Process each row
//            for (Map.Entry<String, String[]> entry : sourceData.entrySet()) {
//                String[] record = entry.getValue();
//
//                int paramIndex = 1;
//
//                // Set direct column values with proper type casting
//                for (int i = 0; i < directColumns.length; i++) {
//                    String columnName = directColumns[i].toLowerCase();
//                    String value      = (i < record.length) ? record[i] : null;
//                    String columnType = columnTypes.get(columnName);
//
//                    setParameterByType(ps, paramIndex, value, columnType, columnName);
//                    paramIndex++;
//                }
//
//                // Set metadata JSON values
//                int recordIndex = directColCount;
//                for (int i = 0; i < destMetaColumns.length; i++) {
//                    String[] sourceCols = sourceMetaColumnsList.get(i);
//                    String[] jsonKeys   = jsonKeyNamesList.get(i);
//
//                    // Build JSON object
//                    StringBuilder jsonBuilder = new StringBuilder("{");
//
//                    for (int j = 0; j < sourceCols.length; j++) {
//                        String key   = jsonKeys[j];
//                        String value = (recordIndex < record.length) ? record[recordIndex] : null;
//
//                        // Escape quotes and backslashes in value for JSON
//                        String escapedValue = value;
//                        if (escapedValue != null) {
//                            escapedValue = escapedValue.replace("\\", "\\\\").replace("\"", "\\\"");
//                        }
//
//                        jsonBuilder.append("\"").append(key).append("\":");
//
//                        if (escapedValue != null) {
//                            jsonBuilder.append("\"").append(escapedValue).append("\"");
//                        } else {
//                            jsonBuilder.append("null");
//                        }
//
//                        if (j < sourceCols.length - 1) {
//                            jsonBuilder.append(",");
//                        }
//
//                        recordIndex++;
//                    }
//
//                    jsonBuilder.append("}");
//
//                    String columnName = destMetaColumns[i].toLowerCase();
//                    String columnType = columnTypes.get(columnName);
//
//                    // For JSONB columns, use setObject with proper type
//                    if (columnType != null && columnType.toLowerCase().contains("jsonb")) {
//                        ps.setObject(paramIndex, jsonBuilder.toString(), java.sql.Types.OTHER);
//                    } else {
//                        ps.setString(paramIndex, jsonBuilder.toString());
//                    }
//
//                    paramIndex++;
//                }
//
//                ps.addBatch();
//                rowCount++;
//
//                if (rowCount % batchSize == 0) {
//                    ps.executeBatch();
//                    log("INFO", "Batch executed | Table: " + destTableName +
//                    		"| Rows attempted so far: " + rowCount +
//                            " | Total source rows: " + sourceData.size(), null, "BATCH_EXEC");
//                }
//            }
//
//            // Execute remaining batch
//            if (conn != null && ps != null) {
//            	int[] results = ps.executeBatch();
//            	int inserted = 0;
//            	int skipped = 0;
//            	int unknown = 0;
//
//            	for (int r : results) {
//            	    if (r == 1) {
//            	        inserted++;        // confirmed insert
//            	    } else if (r == 0) {
//            	        skipped++;         // conflict ignored
//            	    } else if (r == java.sql.Statement.SUCCESS_NO_INFO) {
//            	        unknown++;         // success but unknown count
//            	    }
//            	}
//                conn.commit();
//                status = true;
//
//                log("INFO",
//                	    "Insert summary | Table: " + destTableName +
//                	    " | Confirmed Inserted: " + inserted +
//                	    " | Skipped (PK conflict): " + skipped +
//                	    " | Unknown (driver success): " + unknown,
//                	    null,
//                	    "DATA_INSERT"
//                	);
//            }
//
//        } catch (Exception ex) {
//            log("ERROR", "Insert failed | Table: " + destTableName +
//                    " | Rows processed before failure: " + rowCount +
//                    " | Error: " + ex.getMessage(), null, "DATA_INSERT");
//
//            try {
//                if (conn != null)
//                    conn.rollback();
//                log("INFO", "Transaction rolled back successfully | Table: " + destTableName, null, "DATA_INSERT");
//            } catch (Exception e) {
//                log("ERROR", "Rollback failed | Table: " + destTableName +
//                        " | Error: " + e.getMessage(), null, "DATA_INSERT");
//            }
//
//            status = false;
//
//        } finally {
//            try {
//                if (conn != null) {
//                    if (ps != null)
//                        ps.close();
//                    conn.close();
//                }
//            } catch (Exception e) {
//                log("ERROR", "Failed to close DB resources after insert | Error: " + e.getMessage(), null, "DATA_INSERT");
//            }
//        }
//
//        return status;
//    }

    /**
     * Get destination column types
     */
    private HashMap<String, String> getDestinationColumnTypes(String tableName, String[] directColumns,
            String[] metaColumns, Connection conn) {

        HashMap<String, String> columnTypes = new HashMap<>();

        try {
            DatabaseMetaData metaData = conn.getMetaData();

            // Handle schema-qualified table names
            String schemaName     = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            // Get all columns we need
            String[] allColumns = new String[directColumns.length + metaColumns.length];
            System.arraycopy(directColumns, 0, allColumns, 0, directColumns.length);
            System.arraycopy(metaColumns, 0, allColumns, directColumns.length, metaColumns.length);

            for (String colName : allColumns) {
                ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, colName.toLowerCase());

                if (rs.next()) {
                    String dataType = rs.getString("TYPE_NAME");
                    columnTypes.put(colName.toLowerCase(), dataType);
                    log("DEBUG", "Column type resolved | Column: " + colName +
                            " | Type: " + dataType + " | Table: " + tableName, null, "VALIDATION");
                }

                rs.close();
            }

        } catch (Exception ex) {
            log("ERROR", "Failed to fetch destination column types | Table: " + tableName +
                    " | Error: " + ex.getMessage(), null, "VALIDATION");
            return null;
        }

        return columnTypes;
    }

    /**
     * Set PreparedStatement parameter based on column type
     */
    private void setParameterByType(PreparedStatement ps, int paramIndex, String value, String columnType,
            String columnName) throws Exception {

        // Only treat actual NULL as NULL, not empty strings or whitespace
        if (value == null) {
            // Handle null values based on type
            if (columnType != null) {
                String type = columnType.toLowerCase();
                if (type.contains("int") || type.contains("serial")) {
                    ps.setNull(paramIndex, java.sql.Types.BIGINT);
                } else if (type.contains("timestamp") || type.contains("date") || type.contains("time")) {
                    ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
                } else if (type.contains("numeric") || type.contains("decimal") || type.contains("float")
                        || type.contains("double")) {
                    ps.setNull(paramIndex, java.sql.Types.NUMERIC);
                } else if (type.contains("bool")) {
                    ps.setNull(paramIndex, java.sql.Types.BOOLEAN);
                } else {
                    ps.setNull(paramIndex, java.sql.Types.VARCHAR);
                }
            } else {
                ps.setNull(paramIndex, java.sql.Types.VARCHAR);
            }
            return;
        }

        // For empty strings or whitespace, insert as-is for string types
        // For numeric/date types, treat empty string as NULL
        if (value.trim().isEmpty()) {
            if (columnType != null) {
                String type = columnType.toLowerCase();

                // For string types, insert the blank value as-is
                if (type.contains("char") || type.contains("text")) {
                    ps.setString(paramIndex, value);
                    return;
                }

                // For non-string types, treat blank as NULL
                if (type.contains("int") || type.contains("serial")) {
                    ps.setNull(paramIndex, java.sql.Types.BIGINT);
                } else if (type.contains("timestamp") || type.contains("date") || type.contains("time")) {
                    ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
                } else if (type.contains("numeric") || type.contains("decimal") || type.contains("float")
                        || type.contains("double")) {
                    ps.setNull(paramIndex, java.sql.Types.NUMERIC);
                } else if (type.contains("bool")) {
                    ps.setNull(paramIndex, java.sql.Types.BOOLEAN);
                } else {
                    ps.setString(paramIndex, value);
                }
            } else {
                // If type unknown, insert blank as-is
                ps.setString(paramIndex, value);
            }
            return;
        }

        // Set value based on column type
        if (columnType != null) {
            String type = columnType.toLowerCase();

            try {
                if (type.contains("bigint") || type.contains("int8") || type.contains("bigserial")) {
                    ps.setLong(paramIndex, Long.parseLong(value.trim()));
                } else if (type.contains("int") || type.contains("serial")) {
                    ps.setInt(paramIndex, Integer.parseInt(value.trim()));
                } else if (type.contains("numeric") || type.contains("decimal")) {
                    ps.setBigDecimal(paramIndex, new java.math.BigDecimal(value.trim()));
                } else if (type.contains("float") || type.contains("double") || type.contains("real")) {
                    ps.setDouble(paramIndex, Double.parseDouble(value.trim()));
                } else if (type.contains("bool")) {
                    ps.setBoolean(paramIndex, Boolean.parseBoolean(value.trim()));
                } else if (type.contains("timestamp")) {
                    // Try to parse timestamp
                    try {
                        java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.trim());
                        ps.setTimestamp(paramIndex, ts);
                    } catch (IllegalArgumentException e) {
                        log("ERROR", "Timestamp parse failure | Column: " + columnName +
                                " | Expected type: " + columnType +
                                " | Received value: " + value +
                                " | Expected format: YYYY-MM-DD HH:MM:SS or YYYY-MM-DD HH:MM:SS.nnnnnnnnn" +
                                " | Likely cause: data type mismatch between source and destination", null, "DATA_INSERT");

                        // Throw exception to stop processing
                        throw new Exception("Data type mismatch: Column '" + columnName
                                + "' expects timestamp but got '" + value + "'");
                    }
                } else if (type.contains("date")) {
                    try {
                        java.sql.Date date = java.sql.Date.valueOf(value.trim());
                        ps.setDate(paramIndex, date);
                    } catch (IllegalArgumentException e) {
                        log("ERROR", "Date parse failure | Column: " + columnName +
                                " | Expected type: " + columnType +
                                " | Received value: " + value, null, "DATA_INSERT");
                        throw new Exception(
                                "Data type mismatch: Column '" + columnName + "' expects date but got '" + value + "'");
                    }
                } else if (type.contains("time")) {
                    try {
                        java.sql.Time time = java.sql.Time.valueOf(value.trim());
                        ps.setTime(paramIndex, time);
                    } catch (IllegalArgumentException e) {
                        log("ERROR", "Time parse failure | Column: " + columnName +
                                " | Expected type: " + columnType +
                                " | Received value: " + value, null, "DATA_INSERT");
                        throw new Exception(
                                "Data type mismatch: Column '" + columnName + "' expects time but got '" + value + "'");
                    }
                } else {
                    // Default to string - insert value as-is (including blank
                    // strings)
                    ps.setString(paramIndex, value);
                }
            } catch (NumberFormatException e) {
                log("ERROR", "Number format parse failure | Column: " + columnName +
                        " | Expected type: " + columnType +
                        " | Received value: " + value, null, "DATA_INSERT");
                throw new Exception("Data type mismatch: Column '" + columnName + "' expects " + columnType
                        + " but got '" + value + "'");
            }
        } else {
            // If type is unknown, set as string (including blank strings)
            ps.setString(paramIndex, value);
        }
    }
}
