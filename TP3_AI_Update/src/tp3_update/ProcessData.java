package tp3_update;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * ProcessData - CSV to PostgreSQL Pipeline (TP3)
 *
 * Business Logic Flow: 1. Parse and validate user arguments 2. Validate target
 * table exists 3. Validate target column structures 4. Stream CSV data
 * row-by-row and UPDATE rows in target table by PK:
 *   - Transform flag column values: I to FALSE, A to TRUE
 *   - Commit per batch (batch size = 200)
 *   - Skip rows where any PK value is null/empty (no partial updates)
 *   - Zero-row updates (no matching DB row) are treated as valid — no error thrown
 *
 * PK Mapping format (parsed from config):
 *   csvPkColumns  : comma-separated CSV header names that identify the primary key
 *   sqlPkColumns  : comma-separated DB identifiers — either a direct column name
 *                   (e.g. "customer_id") or a metadata JSON key
 *                   (e.g. "metadata:branch_id") prefixed with "metadata:"
 *
 * Metadata JSONB column behaviour:
 *   - If the target table contains a column named "metadata", it is treated as a
 *     JSONB aggregate column and is NOT mapped 1:1 from sourceColArray.
 *   - Its value is rebuilt on every update from ALL sourceColArray entries whose
 *     CSV column name is NOT a metadata PK source column.
 *   - SQL merge pattern:  metadata = COALESCE(metadata, '{}'::jsonb) || ?::jsonb
 *     ensures that the PK key(s) stored inside the existing JSONB are preserved
 *     without fetching the row first.
 *   - Metadata PK source columns are excluded from the rebuilt JSON so the merge
 *     leaves their existing values untouched.
 */
public class ProcessData {

    PrintUtilMsg pu = new PrintUtilMsg();

    /**
     * TP3 entry point: stream CSV file and UPDATE rows in the target PostgreSQL
     * table using the supplied PK mapping.
     *
     * Steps 2 and 4 (source table existence + column validation) are not
     * applicable — there is no DB source table. Steps 7+8 are combined: CSV is
     * streamed row-by-row via updateDataStream() — no full file load into memory.
     *
     * @param dbURL               - Database URL for target DB
     * @param dbUsername          - Database username
     * @param dbPassword          - Database password
     * @param targetTableName     - Target table to update
     * @param sourceColumns       - Comma-separated source CSV column names (direct copy columns)
     * @param targetColumns       - Comma-separated target column names (must match sourceColumns count)
     * @param sourceCheckColumn   - CSV header name of the flag column ('I'/'A')
     * @param targetBooleanColumn - Target column to store TRUE/FALSE
     * @param csvPkColumns        - Comma-separated CSV header names that form the PK
     * @param sqlPkColumns        - Comma-separated SQL PK identifiers; may include
     *                              "metadata:key" entries for JSON-embedded PKs
     * @param csvFilePath         - Absolute path to input CSV file
     * @return true if data was successfully streamed and all batches committed,
     *         false otherwise
     */
    public boolean parseAndTransferDataFromCSV(String dbURL, String dbUsername, String dbPassword,
            String targetTableName, String sourceColumns, String targetColumns, String sourceCheckColumn,
            String targetBooleanColumn, String csvPkColumns, String sqlPkColumns, String csvFilePath) {

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

            // --- PK mapping validation (NEW) ---
            String[] csvPkColArray = parseCommaSeparated(csvPkColumns);
            String[] sqlPkColArray = parseCommaSeparated(sqlPkColumns);

            if (csvPkColArray.length == 0) {
                pu.log("ERROR",
                       "STEP_1_FAILED - reason: csv_pk_columns_is_empty at_least_one_pk_required",
                       "VALIDATION");
                return false;
            }
            if (sqlPkColArray.length == 0) {
                pu.log("ERROR",
                       "STEP_1_FAILED - reason: sql_pk_columns_is_empty at_least_one_pk_required",
                       "VALIDATION");
                return false;
            }
            if (csvPkColArray.length != sqlPkColArray.length) {
                pu.log("ERROR",
                       "STEP_1_FAILED - reason: pk_column_count_mismatch"
                       + " csv_pk_count: " + csvPkColArray.length
                       + " sql_pk_count: " + sqlPkColArray.length,
                       "VALIDATION");
                return false;
            }

            pu.log("DEBUG",
                   "STEP_1 - pk_mapping_csv: " + Arrays.toString(csvPkColArray)
                   + " pk_mapping_sql: " + Arrays.toString(sqlPkColArray),
                   "VALIDATION");

            pu.log("INFO",
                   "STEP_1_COMPLETE - arguments_validated column_pairs: " + sourceColArray.length
                   + " pk_pairs: " + csvPkColArray.length,
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
                // "metadata" is always handled as the aggregate JSONB column; skip direct check
                if (col.trim().toLowerCase().equals("metadata")) continue;
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

            // Determine whether the target table has a metadata JSONB aggregate column (NEW)
            boolean hasMetadataColumn = targetTableColumns.contains("metadata");
            pu.log("INFO",
                   "STEP_3 - metadata_jsonb_column_present: " + hasMetadataColumn
                   + " table: " + targetTableName,
                   "VALIDATION");

            // ===== STEP 4: Stream CSV data and UPDATE target table =====
            // Streams row-by-row: no full file load into memory. Safe for large CSV files.
            pu.log("INFO",
                   "STEP_4_START - phase: stream_csv_and_update csv_file: " + csvFilePath
                   + " target_table: " + targetTableName,
                   "FILE_PROC");

            boolean updated = updateDataStream(csvFilePath, sourceColArray, sourceCheckColumn, targetTableName,
                    targetColArray, targetBooleanColumn, csvPkColArray, sqlPkColArray, hasMetadataColumn,
                    dbURL, dbUsername, dbPassword);

            if (updated) {
                pu.log("INFO",
                       "STEP_4_COMPLETE - data_updated_successfully target_table: " + targetTableName,
                       "DB_UPDATE");
            } else {
                pu.log("ERROR",
                       "STEP_4_FAILED - reason: update_returned_false target_table: " + targetTableName,
                       "DB_UPDATE");
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

    // =========================================================================
    // Core streaming UPDATE method
    // =========================================================================

    /**
     * Stream CSV file row-by-row and UPDATE rows in the target table by PK.
     *
     * Design principles (unchanged from original insertDataStream):
     * - No full file loaded into memory: safe for CSV files with lakhs of rows.
     * - Reuses all existing helpers: setParameterByType, setBooleanParameter, getTargetColumnTypes.
     * - Uses CSVSourceData for RFC 4180 compliant parsing (package-private in same package).
     * - Commits every batchSize rows to bound memory and enable progress visibility.
     * - Each skip is logged immediately with file name + line number (no accumulation list).
     * - On fatal exception: rolls back current uncommitted batch, closes all resources.
     *
     * UPDATE-specific behaviour (new):
     * - PK values are validated BEFORE any parameter binding; rows with null/empty PKs are skipped.
     * - PK columns are excluded from the SET clause; they appear only in the WHERE clause.
     * - Zero-row updates (WHERE matches nothing) are treated as valid — no error thrown.
     * - Metadata JSONB column (if present): rebuilt from all source columns EXCEPT metadata PK
     *   source columns, then merged with existing via SQL: COALESCE(metadata,'{}')::jsonb || ?::jsonb
     *   This preserves existing PK key values inside the JSON without fetching the row first.
     *
     * Binding order per row:
     *   1. SET: direct non-PK target columns (positionally)
     *   2. SET: metadata JSONB aggregate (if hasMetadataColumn)
     *   3. SET: boolean flag column
     *   4. WHERE: all PK values (direct columns use type-aware binding;
     *              metadata keys always use setString for JSON text comparison)
     *
     * @param csvFilePath       - Absolute path to CSV input file
     * @param sourceColArray    - Ordered array of CSV column names to read
     * @param sourceCheckColumn - CSV header name of the I/A flag column
     * @param targetTableName   - Target PostgreSQL table
     * @param targetColArray    - Ordered target column names (maps 1:1 to sourceColArray)
     * @param targetBooleanColumn - Target boolean column name
     * @param csvPkCols         - CSV column names identifying the PK (from csvPkColumns)
     * @param sqlPkCols         - SQL PK identifiers; may be "colName" or "metadata:key"
     * @param hasMetadataColumn - true if the target table contains a "metadata" JSONB column
     * @param dbURL             - DB connection URL
     * @param dbUser            - DB username
     * @param dbPass            - DB password
     * @return true if at least one row was batched and all batches committed;
     *         false on fatal error or zero valid rows
     */
    private boolean updateDataStream(String csvFilePath, String[] sourceColArray, String sourceCheckColumn,
            String targetTableName, String[] targetColArray, String targetBooleanColumn,
            String[] csvPkCols, String[] sqlPkCols, boolean hasMetadataColumn,
            String dbURL, String dbUser, String dbPass) {

        Connection conn = null;
        PreparedStatement ps = null;
        BufferedReader br = null;
        boolean status = false;

        int totalRead = 0;
        int successCount = 0;
        int skipCount = 0;
        int lineNumber = 1;

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
                       "CSV_STREAM_FAILED - csv_file: " + csvFilePath
                       + " reason: empty_file_or_missing_header",
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
                   "CSV_HEADER_PARSED - csv_file: " + csvFilePath
                   + " header_column_count: " + headers.length,
                   "FILE_PROC");

            // Resolve source column indices (unchanged)
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

            // Resolve flag column index (unchanged)
            int checkColIndex = csvParser.findColumnIndex(headers, sourceCheckColumn);
            if (checkColIndex == -1) {
                pu.log("ERROR",
                       "CSV_STREAM_FAILED - csv_file: " + csvFilePath
                       + " reason: flag_column_not_found_in_header flag_column: " + sourceCheckColumn,
                       "FILE_PROC");
                return false;
            }

            // Resolve PK column indices from CSV headers (NEW)
            // findColumnIndex searches all headers — PK columns need not be in sourceColArray
            int[] csvPkIndices = new int[csvPkCols.length];
            for (int i = 0; i < csvPkCols.length; i++) {
                csvPkIndices[i] = csvParser.findColumnIndex(headers, csvPkCols[i]);
                if (csvPkIndices[i] == -1) {
                    pu.log("ERROR",
                           "CSV_STREAM_FAILED - csv_file: " + csvFilePath
                           + " reason: pk_column_not_found_in_csv_headers pk_column: " + csvPkCols[i],
                           "FILE_PROC");
                    return false;
                }
                pu.log("DEBUG",
                       "PK_INDEX_RESOLVED - pk_csv_column: " + csvPkCols[i]
                       + " resolved_to_csv_index: " + csvPkIndices[i],
                       "FILE_PROC");
            }

            // Compute maxRequiredIndex across source cols, check col, AND PK cols (NEW: PK cols included)
            int maxRequiredIndex = checkColIndex;
            for (int idx : sourceColIndices) {
                if (idx > maxRequiredIndex) maxRequiredIndex = idx;
            }
            for (int idx : csvPkIndices) {
                if (idx > maxRequiredIndex) maxRequiredIndex = idx;
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

            // ===== Classify PKs (NEW) =====
            //   directPkTargetCols  : lowercase SQL column names that are plain direct PKs
            //   metadataPkJsonKeys  : JSON key names extracted from "metadata:key" SQL PKs
            //   metadataPkCsvColSet : lowercase CSV source column names for metadata PKs
            //                         (excluded when building the metadata JSON to preserve via merge)
            HashSet<String> directPkTargetCols  = new HashSet<>();
            List<String>    metadataPkJsonKeys   = new ArrayList<>();
            HashSet<String> metadataPkCsvColSet  = new HashSet<>();

            for (int i = 0; i < sqlPkCols.length; i++) {
                String sqlPk = sqlPkCols[i].trim();
                if (sqlPk.toLowerCase().startsWith("metadata:")) {
                    String jsonKey = sqlPk.substring("metadata:".length()).trim();
                    metadataPkJsonKeys.add(jsonKey);
                    metadataPkCsvColSet.add(csvPkCols[i].trim().toLowerCase());
                    pu.log("DEBUG",
                           "PK_CLASSIFIED - csv_pk_column: " + csvPkCols[i]
                           + " mapped_to_metadata_json_key: " + jsonKey,
                           "DB_UPDATE");
                } else {
                    directPkTargetCols.add(sqlPk.toLowerCase());
                    pu.log("DEBUG",
                           "PK_CLASSIFIED - csv_pk_column: " + csvPkCols[i]
                           + " mapped_to_direct_column: " + sqlPk,
                           "DB_UPDATE");
                }
            }

            // ===== Build UPDATE query (NEW) =====
            //
            // SET clause order:
            //   1. direct non-PK target columns (positionally, matching sourceColArray)
            //   2. metadata JSONB column (if hasMetadataColumn)
            //   3. boolean flag column
            //
            // WHERE clause:
            //   direct PKs  -> colName = ?
            //   metadata PKs -> metadata->>'jsonKey' = ?
            //
            // PK columns are NEVER included in the SET clause.
            // The "metadata" named column (aggregate JSONB) is handled separately from
            // direct columns even if it appears in targetColArray — it is always skipped
            // in the direct SET loop and handled via the JSONB merge below.

            StringBuilder updateQuery = new StringBuilder("UPDATE ")
                    .append(targetTableName).append(" SET ");

            // Collect which targetColArray indices actually go into the SET clause
            List<Integer> setColIdxList = new ArrayList<>();
            boolean firstSet = true;

            for (int i = 0; i < targetColArray.length; i++) {
                String tCol = targetColArray[i].trim().toLowerCase();
                if (directPkTargetCols.contains(tCol)) {
                    pu.log("DEBUG",
                           "SET_CLAUSE - excluding_direct_pk_column: " + targetColArray[i],
                           "DB_UPDATE");
                    continue;
                }
                if (tCol.equals("metadata")) {
                    // Handled separately below as the aggregate JSONB column
                    continue;
                }
                setColIdxList.add(i);
                if (!firstSet) updateQuery.append(", ");
                updateQuery.append(targetColArray[i]).append(" = ?");
                firstSet = false;
            }

            // Convert to primitive array for use inside the hot loop
            int[] setColIdxArr = new int[setColIdxList.size()];
            for (int i = 0; i < setColIdxList.size(); i++) {
                setColIdxArr[i] = setColIdxList.get(i);
            }

            // metadata JSONB column:  COALESCE(existing, '{}') || new_json
            // excluded when "metadata" is itself a direct PK column (edge case guard)
            boolean metadataInSet = hasMetadataColumn && !directPkTargetCols.contains("metadata");
            if (metadataInSet) {
                if (!firstSet) updateQuery.append(", ");
                updateQuery.append("metadata = COALESCE(metadata, '{}'::jsonb) || ?::jsonb");
                firstSet = false;
                pu.log("DEBUG",
                       "SET_CLAUSE - metadata_jsonb_merge_included"
                       + " excluded_metadata_pk_source_cols: " + metadataPkCsvColSet,
                       "DB_UPDATE");
            }

            // Boolean flag column
            if (!firstSet) updateQuery.append(", ");
            updateQuery.append(targetBooleanColumn).append(" = ?");

            // WHERE clause
            updateQuery.append(" WHERE ");
            boolean firstWhere = true;
            for (int i = 0; i < sqlPkCols.length; i++) {
                if (!firstWhere) updateQuery.append(" AND ");
                String sqlPk = sqlPkCols[i].trim();
                if (sqlPk.toLowerCase().startsWith("metadata:")) {
                    String jsonKey = sqlPk.substring("metadata:".length()).trim();
                    updateQuery.append("metadata->>'").append(jsonKey).append("' = ?");
                } else {
                    updateQuery.append(sqlPk).append(" = ?");
                }
                firstWhere = false;
            }

            String finalQuery = updateQuery.toString();
            pu.log("DEBUG",
                   "DB_UPDATE_QUERY_BUILT - target_table: " + targetTableName
                   + " query: " + finalQuery,
                   "DB_UPDATE");
            ps = conn.prepareStatement(finalQuery);

            // ===== STREAM =====
            String line;
            while ((line = br.readLine()) != null) {
                lineNumber++;

                if (line.trim().isEmpty()) continue;

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
                    // --- PK validation: MUST happen before any parameter binding (NEW) ---
                    // If any PK value is null/empty, skip the row without adding to batch.
                    boolean pkValid = true;
                    String[] pkValues = new String[csvPkCols.length];
                    for (int i = 0; i < csvPkCols.length; i++) {
                        String pkVal = fields[csvPkIndices[i]];
                        if (pkVal == null || pkVal.trim().isEmpty()) {
                            pu.log("WARN",
                                   "ROW_SKIPPED - csv_file: " + csvFilePath
                                   + " line_number: " + lineNumber
                                   + " reason: pk_value_null_or_empty pk_csv_column: " + csvPkCols[i],
                                   "FILE_PROC");
                            pkValid = false;
                            break;
                        }
                        pkValues[i] = pkVal.trim();
                    }
                    if (!pkValid) {
                        skipCount++;
                        continue;
                    }

                    // --- Flag column validation (unchanged logic) ---
                    String checkVal = fields[checkColIndex];
                    if (checkVal == null || checkVal.trim().isEmpty()) {
                        skipCount++;
                        continue;
                    }
                    String normalized = checkVal.trim().toUpperCase();
                    if (!normalized.equals("I") && !normalized.equals("A")) {
                        skipCount++;
                        continue;
                    }

                    // --- Bind SET params (phase 1): direct non-PK columns ---
                    int paramIndex = 1;
                    for (int idx : setColIdxArr) {
                        String val  = fields[sourceColIndices[idx]];
                        String type = columnTypes.get(targetColArray[idx].toLowerCase());
                        setParameterByType(ps, paramIndex++, val, type, targetColArray[idx]);
                    }

                    // --- Bind SET params (phase 2): metadata JSONB aggregate (NEW) ---
                    // Build JSON from all sourceColArray entries except metadata PK source cols.
                    // The SQL merge (||) then preserves the PK key values already in the DB row.
                    if (metadataInSet) {
                        String metaJson = buildMetadataJson(
                                sourceColArray, sourceColIndices, fields, metadataPkCsvColSet);
                        ps.setString(paramIndex++, metaJson);
                    }

                    // --- Bind SET params (phase 3): boolean flag column ---
                    String boolType = columnTypes.get(targetBooleanColumn.toLowerCase());
                    if (normalized.equals("I")) {
                        setBooleanParameter(ps, paramIndex++, false, boolType);
                    } else {
                        setBooleanParameter(ps, paramIndex++, true, boolType);
                    }

                    // --- Bind WHERE params: PK values (NEW) ---
                    // Binding order mirrors the WHERE clause construction order above.
                    // direct PK cols -> type-aware binding (uses columnTypes map)
                    // metadata PK keys -> always setString (JSON text comparison)
                    for (int i = 0; i < sqlPkCols.length; i++) {
                        String sqlPk = sqlPkCols[i].trim();
                        String pkVal = pkValues[i]; // already trimmed and validated non-empty
                        if (sqlPk.toLowerCase().startsWith("metadata:")) {
                            ps.setString(paramIndex++, pkVal);
                        } else {
                            String colType = columnTypes.get(sqlPk.toLowerCase());
                            setParameterByType(ps, paramIndex++, pkVal, colType, sqlPk);
                        }
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
            // Zero-row DB updates (no matching row) are valid — not checked here.
            // Only "nothing reached the batch" is an error.
            if (successCount == 0) {
                pu.log("ERROR",
                       "DB_UPDATE_FAILED - csv_file: " + csvFilePath
                       + " reason: no_valid_rows_to_update all_rows_skipped"
                       + " total_read: " + totalRead + " skipped: " + skipCount,
                       "DB_UPDATE");
                return false;
            }
            ps.executeBatch();
            conn.commit();

            pu.log("INFO",
                   "DB_COMMIT_SUCCESS - csv_file: " + csvFilePath
                   + " target_table: " + targetTableName
                   + " rows_updated: " + successCount
                   + " rows_skipped: " + skipCount
                   + " total_rows_read: " + totalRead,
                   "DB_UPDATE");

            // ===== STATUS =====
            if (totalRead == 0) {
                pu.log("WARN",
                       "CSV_STREAM_COMPLETE - csv_file: " + csvFilePath
                       + " status: no_data_rows_found_in_file",
                       "FILE_PROC");
                status = false;
            } else if (successCount == 0) {
                pu.log("ERROR",
                       "CSV_STREAM_COMPLETE - csv_file: " + csvFilePath
                       + " status: zero_rows_reached_batch total_read: " + totalRead,
                       "DB_UPDATE");
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
                           "DB_UPDATE");
                }
            } catch (Exception e) {
                pu.log("ERROR",
                       "DB_ROLLBACK_FAILED - csv_file: " + csvFilePath + " reason: " + e.getMessage(),
                       "DB_UPDATE");
            }
            status = false;

        } finally {
            try { if (br   != null) br.close();   } catch (Exception ignored) {}
            try { if (ps   != null) ps.close();   } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }

        return status;
    }

    // =========================================================================
    // Metadata JSON helpers (NEW)
    // =========================================================================

    /**
     * Build a JSONB-compatible JSON object string from the source columns in the
     * current CSV row, using each source column name as the JSON key.
     *
     * Columns whose lowercase name appears in {@code excludedCsvCols} are omitted
     * so that their existing value in the database metadata column is preserved by
     * the SQL merge operator (||).
     *
     * All values are serialized as JSON strings; null/empty/literal-"null" values
     * are serialized as JSON null.
     *
     * @param sourceColArray   - ordered source column names
     * @param sourceColIndices - CSV field index for each source column (parallel array)
     * @param fields           - parsed fields for the current CSV row
     * @param excludedCsvCols  - lowercase CSV column names to exclude (metadata PK sources)
     * @return valid JSON object string, e.g. {"col1":"val1","col2":null}
     */
    private String buildMetadataJson(String[] sourceColArray, int[] sourceColIndices,
            String[] fields, HashSet<String> excludedCsvCols) {

        StringBuilder sb = new StringBuilder("{");
        boolean first = true;

        for (int i = 0; i < sourceColArray.length; i++) {
            String srcCol = sourceColArray[i].trim();

            // Skip metadata PK source columns — their keys are preserved by the SQL merge
            if (excludedCsvCols.contains(srcCol.toLowerCase())) continue;

            String val = (sourceColIndices[i] < fields.length) ? fields[sourceColIndices[i]] : null;

            if (!first) sb.append(",");
            sb.append("\"").append(escapeJsonString(srcCol)).append("\":");

            if (val == null || val.trim().isEmpty() || val.trim().equalsIgnoreCase("null")) {
                sb.append("null");
            } else {
                sb.append("\"").append(escapeJsonString(val.trim())).append("\"");
            }
            first = false;
        }

        sb.append("}");
        return sb.toString();
    }

    /**
     * Escape a raw string for safe inclusion as a JSON string value.
     * Handles backslash, double-quote, newline, carriage-return, and tab.
     */
    private String escapeJsonString(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    // =========================================================================
    // Unchanged helpers below — do not modify
    // =========================================================================

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
                   "GET_TABLE_COLUMNS_COMPLETE - table: " + actualTableName
                   + " column_count: " + columns.size(),
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
                       "DB_UPDATE");
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
