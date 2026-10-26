package tp3_parser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;

/**
 * ProcessData - CSV to PostgreSQL Pipeline (TP3)
 *
 * Business Logic Flow: 1. Parse and validate user arguments 2. Validate target
 * table exists 3. Validate target column structures 4. Stream CSV data
 * row-by-row and insert into target table - Transform flag column values: I
 * FALSE, A TRUE - Commit per batch (batch size = 200)
 */
public class ProcessData {

    PrintUtilMsg pu = new PrintUtilMsg();

    /**
     * TP3 entry point: stream CSV file and transfer data to target PostgreSQL
     * table.
     *
     * Steps 2 and 4 (source table existence + column validation) are not
     * applicable — there is no DB source table. Steps 7+8 are combined: CSV is
     * streamed row-by-row via insertDataStream() — no full file load into
     * memory.
     *
     * @param dbURL
     *            - Database URL for target DB
     * @param dbUsername
     *            - Database username
     * @param dbPassword
     *            - Database password
     * @param targetTableName
     *            - Target table to insert into
     * @param sourceColumns
     *            - Comma-separated source CSV column names (direct copy
     *            columns)
     * @param targetColumns
     *            - Comma-separated target column names (must match
     *            sourceColumns count)
     * @param sourceCheckColumn
     *            - CSV header name of the flag column ('I'/'A')
     * @param targetBooleanColumn
     *            - Target column to store TRUE/FALSE
     * @param csvFilePath
     *            - Absolute path to input CSV file
     * @return true if data was successfully streamed and inserted, false
     *         otherwise
     */
    public boolean parseAndTransferDataFromCSV(String dbURL, String dbUsername, String dbPassword,
            String targetTableName, String sourceColumns, String targetColumns, String sourceCheckColumn,
            String targetBooleanColumn, String csvFilePath) {

        boolean status = false;

        try {
            if (csvFilePath == null || csvFilePath.trim().isEmpty()) {
                pu.log("ERROR",
                       "VALIDATION_FAILED - reason: csv_file_path_is_empty",
                       "VALIDATION");
                return false;
            }

            // ===== STEP 1: Parse and validate arguments =====
            pu.log("INFO",
                   "STEP_1_START - phase: parse_and_validate_arguments csv_file: " + csvFilePath,
                   "VALIDATION");

            String[] sourceColArray = parseCommaSeparated(sourceColumns);
            pu.log("DEBUG",
                   "STEP_1 - source_columns_count: " + sourceColArray.length
                   + " source_columns: " + sourceColumns,
                   "VALIDATION");

            String[] targetColArray = parseCommaSeparated(targetColumns);
            pu.log("DEBUG",
                   "STEP_1 - target_columns_count: " + targetColArray.length
                   + " target_columns: " + targetColumns,
                   "VALIDATION");

            if (sourceColArray.length != targetColArray.length) {
                pu.log("ERROR",
                       "STEP_1_FAILED - reason: column_count_mismatch"
                       + " source_count: " + sourceColArray.length
                       + " target_count: " + targetColArray.length,
                       "VALIDATION");
                return false;
            }

            pu.log("DEBUG",
                   "STEP_1 - source_check_column: " + sourceCheckColumn
                   + " target_boolean_column: " + targetBooleanColumn,
                   "VALIDATION");

            pu.log("INFO",
                   "STEP_1_COMPLETE - arguments validated successfully column_pairs: " + sourceColArray.length,
                   "VALIDATION");

            // ===== STEP 2: Validate target table exists =====
            pu.log("INFO",
                   "STEP_2_START - phase: validate_target_table table_name: " + targetTableName,
                   "VALIDATION");

            if (!validateTableExists(targetTableName, dbURL, dbUsername, dbPassword)) {
                pu.log("ERROR",
                       "STEP_2_FAILED - reason: target_table_does_not_exist table_name: " + targetTableName,
                       "VALIDATION");
                return false;
            }

            pu.log("INFO",
                   "STEP_2_COMPLETE - target_table_exists table_name: " + targetTableName,
                   "VALIDATION");

            // ===== STEP 3: Validate target table columns =====
            pu.log("INFO",
                   "STEP_3_START - phase: validate_target_table_columns table_name: " + targetTableName,
                   "VALIDATION");

            HashSet<String> targetTableColumns = getTableColumns(targetTableName, dbURL, dbUsername, dbPassword);

            if (targetTableColumns == null || targetTableColumns.size() == 0) {
                pu.log("ERROR",
                       "STEP_3_FAILED - reason: could_not_fetch_target_table_columns table_name: " + targetTableName,
                       "VALIDATION");
                return false;
            }

            pu.log("DEBUG",
                   "STEP_3 - target_table_column_count: " + targetTableColumns.size(),
                   "VALIDATION");

            for (String col : targetColArray) {
                if (!targetTableColumns.contains(col.toLowerCase())) {
                    pu.log("ERROR",
                           "STEP_3_FAILED - reason: target_column_not_found column: " + col
                           + " table: " + targetTableName,
                           "VALIDATION");
                    return false;
                }
                pu.log("DEBUG",
                       "STEP_3 - column_validated column: " + col,
                       "VALIDATION");
            }

            if (!targetTableColumns.contains(targetBooleanColumn.toLowerCase())) {
                pu.log("ERROR",
                       "STEP_3_FAILED - reason: target_boolean_column_not_found column: " + targetBooleanColumn
                       + " table: " + targetTableName,
                       "VALIDATION");
                return false;
            }
            pu.log("DEBUG",
                   "STEP_3 - boolean_column_validated column: " + targetBooleanColumn,
                   "VALIDATION");

            pu.log("INFO",
                   "STEP_3_COMPLETE - all_target_columns_validated column_count: " + targetColArray.length,
                   "VALIDATION");

            // ===== STEP 4: Stream CSV data and insert into target table =====
            // Streams row-by-row: no full file load into memory. Safe for large
            // CSV files.
            pu.log("INFO",
                   "STEP_4_START - phase: stream_csv_and_insert csv_file: " + csvFilePath
                   + " target_table: " + targetTableName,
                   "FILE_PROC");

            boolean inserted = insertDataStream(csvFilePath, sourceColArray, sourceCheckColumn, targetTableName,
                    targetColArray, targetBooleanColumn, dbURL, dbUsername, dbPassword);

            if (inserted) {
                pu.log("INFO",
                       "STEP_4_COMPLETE - data_inserted_successfully target_table: " + targetTableName,
                       "DB_INSERT");
            } else {
                pu.log("ERROR",
                       "STEP_4_FAILED - reason: insert_returned_false target_table: " + targetTableName,
                       "DB_INSERT");
                return false;
            }

            status = true;

            pu.log("INFO",
                   "PROCESSING_COMPLETE - csv_file: " + csvFilePath + " target_table: " + targetTableName,
                   "FILE_PROC");

        } catch (Exception ex) {
            pu.log("ERROR",
                   "PROCESSING_EXCEPTION - csv_file: " + csvFilePath + " reason: " + ex.getMessage(),
                   "FILE_PROC");
            status = false;
        }

        return status;
    }

    /**
     * Stream CSV file row-by-row and insert into target table.
     *
     * Design principles: - No full file loaded into memory: safe for CSV files
     * with lakhs of rows. - Reuses all existing helpers: setParameterByType,
     * setBooleanParameter, getTargetColumnTypes. - Uses CSVSourceData for RFC
     * 4180 compliant parsing (package-private in same package). - Commits every
     * batchSize (200) rows to bound memory and enable progress visibility. -
     * Each skip is logged immediately with file name + line number (no
     * accumulation list). - On fatal exception: rolls back current uncommitted
     * batch, closes all resources.
     *
     * @param csvFilePath
     *            - Absolute path to CSV input file
     * @param sourceColArray
     *            - Ordered array of CSV column names to read
     * @param sourceCheckColumn
     *            - CSV header name of the I/A flag column
     * @param targetTableName
     *            - Target PostgreSQL table
     * @param targetColArray
     *            - Ordered target column names (maps 1:1 to sourceColArray)
     * @param targetBooleanColumn
     *            - Target boolean column name
     * @param dbURL
     *            - DB connection URL
     * @param dbUser
     *            - DB username
     * @param dbPass
     *            - DB password
     * @return true if at least one row inserted and all batches committed;
     *         false on fatal error or zero inserts
     */
    private boolean insertDataStream(String csvFilePath, String[] sourceColArray, String sourceCheckColumn,
            String targetTableName, String[] targetColArray, String targetBooleanColumn, String dbURL, String dbUser,
            String dbPass) {

        Connection conn = null;
        PreparedStatement ps = null;
        BufferedReader br = null;
        boolean status = false;

        int totalRead    = 0;
        int successCount = 0;
        int skipCount    = 0;
        int lineNumber   = 1;

        long beforeCount = 0;
        long afterCount  = 0;
        int  inserted    = 0;
        int  skipped     = 0;
        int  unknown     = 0;

        CSVSourceData csvParser = new CSVSourceData();

        try {
            pu.log("INFO",
                   "CSV_STREAM_START - csv_file: " + csvFilePath,
                   "FILE_PROC");

            br = new BufferedReader(new FileReader(csvFilePath));

            // ===== HEADER =====
            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                pu.log("ERROR",
                       "CSV_STREAM_FAILED - csv_file: " + csvFilePath + " reason: empty_file_or_missing_header",
                       "FILE_PROC");
                return false;
            }

            String[] headers = csvParser.parseCsvLine(headerLine);
            // CLEAN HEADERS (REMOVE QUOTES)
            for (int i = 0; i < headers.length; i++) {
                if (headers[i] != null) {
                    headers[i] = headers[i].trim().replaceAll("^\"|\"$", "");
                }
            }

            pu.log("DEBUG",
                   "CSV_HEADER_PARSED - csv_file: " + csvFilePath + " header_column_count: " + headers.length,
                   "FILE_PROC");

            int[] sourceColIndices = new int[sourceColArray.length];
            for (int i = 0; i < sourceColArray.length; i++) {
                sourceColIndices[i] = csvParser.findColumnIndex(headers, sourceColArray[i]);
                if (sourceColIndices[i] == -1) {
                    pu.log("ERROR",
                           "CSV_STREAM_FAILED - csv_file: " + csvFilePath
                           + " reason: source_column_not_found_in_header column: " + sourceColArray[i],
                           "FILE_PROC");
                    return false;
                }
            }

            int checkColIndex = csvParser.findColumnIndex(headers, sourceCheckColumn);
            if (checkColIndex == -1) {
                pu.log("ERROR",
                       "CSV_STREAM_FAILED - csv_file: " + csvFilePath
                       + " reason: flag_column_not_found_in_header flag_column: " + sourceCheckColumn,
                       "FILE_PROC");
                return false;
            }

            int maxRequiredIndex = checkColIndex;
            for (int idx : sourceColIndices) {
                if (idx > maxRequiredIndex)
                    maxRequiredIndex = idx;
            }

            // ===== DB =====
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
            if (conn == null) {
                pu.log("ERROR",
                       "CSV_STREAM_FAILED - csv_file: " + csvFilePath + " reason: db_connection_failed",
                       "DB_CONNECT");
                return false;
            }

            conn.setAutoCommit(false);

            HashMap<String, String> columnTypes =
                    getTargetColumnTypes(targetTableName, targetColArray, targetBooleanColumn, conn);

            if (columnTypes == null || columnTypes.isEmpty()) {
                pu.log("ERROR",
                       "CSV_STREAM_FAILED - csv_file: " + csvFilePath
                       + " reason: failed_to_fetch_column_metadata table: " + targetTableName,
                       "DB_META");
                return false;
            }

            // ===== TABLE SIZE BEFORE INSERT =====
            String countQuery = "SELECT COUNT(*) FROM " + targetTableName;
            PreparedStatement cps = conn.prepareStatement(countQuery);
            ResultSet crs = cps.executeQuery();
            if (crs.next()) {
                beforeCount = crs.getLong(1);
            }
            crs.close();
            cps.close();

            pu.log("INFO",
                   "TABLE_SIZE_BEFORE_INSERT - table: " + targetTableName
                   + " rows_before: " + beforeCount,
                   "DB_INSERT");

            StringBuilder insertQuery = new StringBuilder();
            insertQuery.append("INSERT INTO ").append(targetTableName).append(" (");

            for (int i = 0; i < targetColArray.length; i++) {
                insertQuery.append(targetColArray[i]).append(", ");
            }
            insertQuery.append(targetBooleanColumn).append(") VALUES (");

            for (int i = 0; i < targetColArray.length + 1; i++) {
                insertQuery.append("?");
                if (i < targetColArray.length) insertQuery.append(", ");
            }
            insertQuery.append(") ON CONFLICT DO NOTHING");
            ps = conn.prepareStatement(insertQuery.toString());

            pu.log("DEBUG",
                   "DB_INSERT_PREPARED - target_table: " + targetTableName
                   + " query: " + insertQuery.toString(),
                   "DB_INSERT");

            // ===== STREAM =====
            String line;
            while ((line = br.readLine()) != null) {
                lineNumber++;

                if (line.trim().isEmpty())
                    continue;

                totalRead++;
                String[] fields = csvParser.parseCsvLine(line);

                if (fields.length <= maxRequiredIndex) {
                    pu.log("WARN",
                           "ROW_SKIPPED - csv_file: " + csvFilePath
                           + " line_number: " + lineNumber
                           + " reason: insufficient_columns found: " + fields.length
                           + " required_min_index: " + maxRequiredIndex,
                           "FILE_PROC");
                    skipCount++;
                    continue;
                }

                try {
                    int paramIndex = 1;

                    // direct cols
                    for (int i = 0; i < targetColArray.length; i++) {
                        String val = fields[sourceColIndices[i]];
                        String type = columnTypes.get(targetColArray[i].toLowerCase());
                        setParameterByType(ps, paramIndex++, val, type, targetColArray[i]);
                    }

                    // flag
                    String checkVal = fields[checkColIndex];
                    if (checkVal == null || checkVal.trim().isEmpty()) {
                        skipCount++;
                        continue;
                    }

                    String normalized = checkVal.trim().toUpperCase();
                    if (normalized.equals("I")) {
                        setBooleanParameter(ps, paramIndex, false, columnTypes.get(targetBooleanColumn.toLowerCase()));
                    } else if (normalized.equals("A")) {
                        setBooleanParameter(ps, paramIndex, true, columnTypes.get(targetBooleanColumn.toLowerCase()));
                    } else {
                        skipCount++;
                        continue;
                    }

                    ps.addBatch();
                    successCount++;

                } catch (Exception e) {
                    pu.log("WARN",
                           "ROW_SKIPPED - csv_file: " + csvFilePath
                           + " line_number: " + lineNumber
                           + " reason: row_processing_exception detail: " + e.getMessage(),
                           "FILE_PROC");
                    skipCount++;
                }
            }

            // ===== FINAL EXECUTION =====
            if (successCount == 0) {
                pu.log("ERROR",
                       "DB_INSERT_FAILED - csv_file: " + csvFilePath
                       + " reason: no_valid_rows_to_insert total_read: " + totalRead
                       + " skipped: " + skipCount,
                       "DB_INSERT");
                return false;
            }
            // ===== EXECUTE BATCH WITH RESULT INTERPRETATION =====
            int[] results = ps.executeBatch();

            for (int r : results) {
                if (r == 1)                                        inserted++;
                else if (r == 0)                                   skipped++;
                else if (r == java.sql.Statement.SUCCESS_NO_INFO)  unknown++;
            }

            conn.commit();

            // ===== TABLE SIZE AFTER INSERT =====
            PreparedStatement cps2 = conn.prepareStatement(countQuery);
            ResultSet crs2 = cps2.executeQuery();
            if (crs2.next()) {
                afterCount = crs2.getLong(1);
            }
            crs2.close();
            cps2.close();

            long netAdded = afterCount - beforeCount;

            // ===== SINGLE FINAL SUMMARY LOG =====
            pu.log("INFO",
                   "INSERT_SUMMARY - table: " + targetTableName
                   + " total_rows_read: " + totalRead
                   + " valid_rows: " + successCount
                   + " skipped_invalid: " + skipCount
                   + " inserted_confirmed: " + inserted
                   + " skipped_conflict: " + skipped
                   + " unknown_driver: " + unknown
                   + " rows_before: " + beforeCount
                   + " rows_after: " + afterCount
                   + " net_added: " + netAdded,
                   "DB_INSERT");

            // ===== SAFETY WARNING: no PK/UNIQUE constraint detected =====
            if (skipped == 0 && netAdded == successCount) {
                pu.log("WARN",
                       "NO_CONFLICT_DETECTED - possible_reason: missing_primary_key_or_unique_constraint",
                       "DB_INSERT");
            }

            // ===== STATUS =====
            if (totalRead == 0) {
                pu.log("WARN",
                       "CSV_STREAM_COMPLETE - csv_file: " + csvFilePath + " status: no_data_rows_found",
                       "FILE_PROC");
                status = false;
            } else if (successCount == 0) {
                pu.log("ERROR",
                       "CSV_STREAM_COMPLETE - csv_file: " + csvFilePath
                       + " status: zero_rows_inserted total_read: " + totalRead,
                       "DB_INSERT");
                status = false;
            } else {
                status = true;
            }

        } catch (Exception ex) {
            pu.log("ERROR",
                   "CSV_STREAM_EXCEPTION - csv_file: " + csvFilePath + " reason: " + ex.getMessage(),
                   "FILE_PROC");
            try {
                if (conn != null) {
                    conn.rollback();
                    pu.log("WARN",
                           "DB_ROLLBACK_EXECUTED - csv_file: " + csvFilePath
                           + " rows_in_failed_batch: " + successCount,
                           "DB_INSERT");
                }
            } catch (Exception e) {
                pu.log("ERROR",
                       "DB_ROLLBACK_FAILED - csv_file: " + csvFilePath + " reason: " + e.getMessage(),
                       "DB_INSERT");
            }
            status = false;

        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (Exception ignored) {
            }
            try {
                if (ps != null)
                    ps.close();
            } catch (Exception ignored) {
            }
            try {
                if (conn != null)
                    conn.close();
            } catch (Exception ignored) {
            }
        }

        return status;
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
     * Validate if table exists
     */
    private boolean validateTableExists(String tableName, String dbURL, String dbUser, String dbPass) {

        Connection conn = null;
        boolean exists = false;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
            if (conn == null) {
                pu.log("ERROR",
                       "TABLE_VALIDATION_FAILED - table: " + tableName + " reason: db_connection_failed",
                       "DB_META");
                return false;
            }

            DatabaseMetaData metaData = conn.getMetaData();

            // Handle schema-qualified table names (e.g., schema.table_name)
            String schemaName = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            pu.log("DEBUG",
                   "TABLE_VALIDATION_QUERY - schema: " + schemaName + " table: " + actualTableName,
                   "DB_META");

            // PostgreSQL stores table names in lowercase by default
            ResultSet rs = metaData.getTables(null, schemaName, actualTableName, new String[] { "TABLE" });

            if (rs.next()) {
                exists = true;
            }

            if (rs != null)
                rs.close();

        } catch (Exception ex) {
            pu.log("ERROR",
                   "TABLE_VALIDATION_EXCEPTION - table: " + tableName + " reason: " + ex.getMessage(),
                   "DB_META");
            exists = false;
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                pu.log("WARN",
                       "DB_CONNECTION_CLOSE_FAILED - context: validateTableExists reason: " + e.getMessage(),
                       "DB_META");
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
            if (conn == null) {
                pu.log("ERROR",
                       "GET_TABLE_COLUMNS_FAILED - table: " + tableName + " reason: db_connection_failed",
                       "DB_META");
                return null;
            }
            DatabaseMetaData metaData = conn.getMetaData();

            // Handle schema-qualified table names
            String schemaName = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            pu.log("DEBUG",
                   "GET_TABLE_COLUMNS_QUERY - schema: " + schemaName + " table: " + actualTableName,
                   "DB_META");

            // PostgreSQL stores table names in lowercase by default
            ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, null);

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                columns.add(columnName.toLowerCase());
                pu.log("DEBUG",
                       "GET_TABLE_COLUMNS_FOUND - column: " + columnName,
                       "DB_META");
            }

            pu.log("INFO",
                   "GET_TABLE_COLUMNS_COMPLETE - table: " + actualTableName + " column_count: " + columns.size(),
                   "DB_META");

            if (rs != null)
                rs.close();

        } catch (Exception ex) {
            pu.log("ERROR",
                   "GET_TABLE_COLUMNS_EXCEPTION - table: " + tableName + " reason: " + ex.getMessage(),
                   "DB_META");
            columns = null;
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception e) {
                pu.log("WARN",
                       "DB_CONNECTION_CLOSE_FAILED - context: getTableColumns reason: " + e.getMessage(),
                       "DB_META");
            }
        }

        return columns;
    }

    /**
     * Get target column types
     */
    private HashMap<String, String> getTargetColumnTypes(String tableName, String[] targetColumns,
            String booleanColumn, Connection conn) {

        HashMap<String, String> columnTypes = new HashMap<>();

        try {
            if (conn == null) {
                pu.log("ERROR",
                       "GET_COLUMN_TYPES_FAILED - table: " + tableName + " reason: db_connection_is_null",
                       "DB_META");
                return null;
            }
            DatabaseMetaData metaData = conn.getMetaData();

            // Handle schema-qualified table names
            String schemaName = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            // Get all columns we need
            String[] allColumns = new String[targetColumns.length + 1];
            System.arraycopy(targetColumns, 0, allColumns, 0, targetColumns.length);
            allColumns[targetColumns.length] = booleanColumn;

            for (String colName : allColumns) {
                ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, colName.toLowerCase());

                if (rs.next()) {
                    String dataType = rs.getString("TYPE_NAME");
                    columnTypes.put(colName.toLowerCase(), dataType);
                    pu.log("DEBUG",
                           "GET_COLUMN_TYPES - column: " + colName + " type: " + dataType,
                           "DB_META");
                }

                if (rs != null)
                    rs.close();
            }

            pu.log("INFO",
                   "GET_COLUMN_TYPES_COMPLETE - table: " + actualTableName
                   + " columns_resolved: " + columnTypes.size(),
                   "DB_META");

        } catch (Exception ex) {
            pu.log("ERROR",
                   "GET_COLUMN_TYPES_EXCEPTION - table: " + tableName + " reason: " + ex.getMessage(),
                   "DB_META");
            return null;
        }

        return columnTypes;
    }

    /**
     * Set PreparedStatement parameter based on column type
     */
    private void setParameterByType(PreparedStatement ps, int paramIndex, String value, String columnType,
            String columnName) throws Exception {

        if (value == null || value.trim().equalsIgnoreCase("NULL")) {
            ps.setNull(paramIndex, java.sql.Types.NULL);
            return;
        }

        if (value.trim().isEmpty()) {
            // existing empty logic
            if (columnType != null) {
                String type = columnType.toLowerCase();
                if (type.contains("int") || type.contains("serial")) {
                    ps.setNull(paramIndex, java.sql.Types.BIGINT);
                } else if (type.contains("timestamp") || type.contains("date") || type.contains("time")) {
                    ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
                } else if (type.contains("numeric")) {
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


        // Set value based on column type
        if (columnType != null) {
            String type = columnType.toLowerCase();

            try {
                if (type.contains("bigint") || type.contains("int8")) {
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
                    try {
                        java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.trim());
                        ps.setTimestamp(paramIndex, ts);
                    } catch (Exception e) {
                        ps.setString(paramIndex, value.trim());
                    }
                } else if (type.contains("date")) {
                    try {
                        java.sql.Date date = java.sql.Date.valueOf(value.trim());
                        ps.setDate(paramIndex, date);
                    } catch (Exception e) {
                        ps.setString(paramIndex, value.trim());
                    }
                } else if (type.contains("time")) {
                    try {
                        java.sql.Time time = java.sql.Time.valueOf(value.trim());
                        ps.setTime(paramIndex, time);
                    } catch (Exception e) {
                        ps.setString(paramIndex, value.trim());
                    }
                } else {
                    ps.setString(paramIndex, value);
                }
            } catch (NumberFormatException e) {
                pu.log("WARN",
                       "TYPE_COERCION_FALLBACK - column: " + columnName
                       + " expected_type: " + columnType
                       + " value: " + value
                       + " action: setting_as_string",
                       "DB_INSERT");
                ps.setString(paramIndex, value);
            }
        } else {
            ps.setString(paramIndex, value);
        }
    }

    /**
     * Set boolean parameter based on target column type
     */
    private void setBooleanParameter(PreparedStatement ps, int paramIndex, boolean value, String columnType)
            throws Exception {

        if (columnType != null) {
            String type = columnType.toLowerCase();

            if (type.contains("bool")) {
                // Target is BOOLEAN type - set as boolean
                ps.setBoolean(paramIndex, value);
            } else {
                // Target is VARCHAR/TEXT type - set as string
                ps.setString(paramIndex, value ? "TRUE" : "FALSE");
            }
        } else {
            // Default to string
            ps.setString(paramIndex, value ? "TRUE" : "FALSE");
        }
    }
}
