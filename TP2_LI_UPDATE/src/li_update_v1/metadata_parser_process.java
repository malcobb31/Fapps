package li_update_v1;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ProcessData - PostgreSQL Data Parser and Transfer
 *
 * REFACTORED: Fully column-name driven (no more index-based access).
 *
 * Core change: CSV rows are now stored as Map<String, String> (colName - value)
 * instead of String[]. All downstream methods use record.get(columnName).
 *
 * Business Logic Flow:
 *  1. Parse and validate arguments (including pkMapping)
 *  2. Validate destination table and columns exist
 *  3. Fetch data from CSV or DB
 *  4. Transform data (direct columns + JSON metadata)
 *  5. Batch-UPDATE rows in PostgreSQL using the PK mapping for the WHERE clause
 *
 * PK Mapping format (pkMapping parameter):
 *   "csvPk1,csvPk2 | psqlPk1,psqlPk2"
 *
 *   Direct column PK:    "cust_id | customer_id"
 *   Metadata JSON PK:    "col4 | metadata:col4"
 *   Mixed:               "cust_id,branch_id | customer_id,metadata:branch_id"
 *
 * For metadata JSON PKs:
 *  - WHERE clause uses PostgreSQL JSON extraction: metadata->>'jsonKey' = ?
 *  - The PK key is excluded from the rebuilt JSON and its DB value is preserved
 *    via a jsonb merge expression: meta_col = ?::jsonb || jsonb_build_object(...)
 */
public class metadata_parser_process {

    private static final boolean           debugEnabled   = true;
    private static final String            processName    = "TP2_PROCESS";
    private static final String            processVersion = "1.0";
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @SuppressWarnings("unused")
    private static final boolean SKIP_MISSING_COLUMNS = false;

    // =========================================================================
    // PK MAPPING  (new - supports both direct and metadata:jsonKey PKs)
    // =========================================================================

    /**
     * Represents one primary-key part from the config pkMapping string.
     *
     * csvColumn  - CSV column that carries the PK value (always lowercase).
     * psqlTarget - Either a plain DB column name ("customer_id") or a metadata
     *              JSON path ("metadata:branch_id").
     * isMetadata - true when psqlTarget starts with "metadata:".
     * jsonKey    - The bare JSON key name when isMetadata is true, otherwise null.
     * psqlColumn - Physical DB column:
     *                direct - the column name itself (e.g. "customer_id")
     *                metadata - always "metadata" (the JSON/JSONB column)
     */
    private static class PkMapping {
        final String  csvColumn;
        final String  psqlTarget;
        final boolean isMetadata;
        final String  jsonKey;    // non-null only when isMetadata == true
        final String  psqlColumn; // physical DB column name

        PkMapping(String csvColumn, String psqlTarget) {
            this.csvColumn  = csvColumn.toLowerCase().trim();
            this.psqlTarget = psqlTarget.trim();
            this.isMetadata = psqlTarget.trim().toLowerCase().startsWith("metadata:");
            if (this.isMetadata) {
                this.jsonKey    = psqlTarget.trim().substring("metadata:".length()).trim();
                this.psqlColumn = "metadata";
            } else {
                this.jsonKey    = null;
                this.psqlColumn = psqlTarget.trim().toLowerCase();
            }
        }

        @Override
        public String toString() {
            return "PkMapping{csv=" + csvColumn + ", psql=" + psqlTarget + "}";
        }
    }

    // =========================================================================
    // PUBLIC ENTRY POINT
    // =========================================================================

    /**
     * Main processing method.
     *
     * CHANGED: Added pkMapping parameter (required).
     * CHANGED: Calls updateDataInDestination() instead of insertDataToDestination().
     *
     * All other parameters are unchanged for backward compatibility with TP2_Main_V3.
     *
     * @param pkMapping  PK mapping string, format: "csvPk1,csvPk2 | psqlPk1,psqlPk2"
     *                   Use "metadata:jsonKey" on the PostgreSQL side for JSON column PKs.
     */
    public boolean parseAndTransferData(String dbURL, String dbUsername, String dbPassword,
            String sourceInput, String destTableName,
            String directColumns, int metadataCount,
            String destMetadataColumns, String sourceMetadataColumns, String jsonKeyNames,
            String pkMapping) {

        boolean status = false;
        long pipelineStart = System.currentTimeMillis();

        try {
            log("INFO",
                "UPDATE pipeline started - source=" + sourceInput
                + " | dest_table=" + destTableName,
                null, "PROCESS_START");

            // ===== STEP 0: Parse and validate PK mapping =====
            // Fail fast - an UPDATE without a WHERE is unsafe and not supported.
            if (pkMapping == null || pkMapping.trim().isEmpty()) {
                log("ERROR",
                    "pkMapping is required for UPDATE mode but was not provided. Aborting.",
                    null, "ERROR");
                return false;
            }

            List<PkMapping> pkMappings = parsePkMapping(pkMapping);
            if (pkMappings == null || pkMappings.isEmpty()) {
                log("ERROR",
                    "Failed to parse pkMapping - check format. Raw value: [" + pkMapping + "]",
                    null, "ERROR");
                return false;
            }

            log("INFO",
                "PK mappings resolved - count=" + pkMappings.size() + " | mappings=" + pkMappings,
                null, "PROCESS_START");

            boolean isCSV = sourceInput != null && sourceInput.toLowerCase().endsWith(".csv");

            // ===== STEP 1: Parse arguments =====
            String[]       directColArray   = parseCommaSeparated(directColumns);
            String[]       destMetaColArray = parseCommaSeparated(destMetadataColumns);
            String[]       sourceMetaGroups = parseHashSeparated(sourceMetadataColumns);
            String[]       jsonKeyGroups    = parseHashSeparated(jsonKeyNames);

            List<String[]> sourceMetaColumnsList = new ArrayList<>();
            List<String[]> jsonKeyNamesList       = new ArrayList<>();

            for (int i = 0; i < metadataCount; i++) {
                String[] sourceCols = parseCommaSeparated(sourceMetaGroups[i]);
                String[] jsonKeys   = parseCommaSeparated(jsonKeyGroups[i]);

                if (sourceCols.length != jsonKeys.length) {
                    log("ERROR",
                        "Metadata column/key count mismatch at metadata group index=" + i
                        + " | sourceCols count=" + sourceCols.length
                        + " | jsonKeys count=" + jsonKeys.length,
                        null, "ERROR");
                    return false;
                }

                sourceMetaColumnsList.add(sourceCols);
                jsonKeyNamesList.add(jsonKeys);
            }

            // ===== STEP 2: Validate destination table and columns =====
            if (!validateTableExists(destTableName, dbURL, dbUsername, dbPassword)) {
                log("ERROR",
                    "Destination table not found in database: " + destTableName,
                    null, "ERROR");
                return false;
            }

            HashSet<String> destTableColumns = getTableColumns(destTableName, dbURL, dbUsername, dbPassword);

            for (String col : directColArray) {
                if (!destTableColumns.contains(col.toLowerCase())) {
                    log("ERROR",
                        "Direct column missing in destination table=" + destTableName + " - col=" + col,
                        null, "ERROR");
                    return false;
                }
            }

            for (String col : destMetaColArray) {
                if (!destTableColumns.contains(col.toLowerCase())) {
                    log("ERROR",
                        "Metadata column missing in destination table=" + destTableName + " - col=" + col,
                        null, "ERROR");
                    return false;
                }
            }

            // ===== STEP 3: Fetch data (CSV or DB) - always Map<colName, value> =====
            HashMap<String, Map<String, String>> sourceData;

            if (isCSV) {
                log("INFO",
                    "Reading source data from CSV file: " + sourceInput,
                    null, "FILE_DETECTED");
                sourceData = readCSV(sourceInput);
            } else {
                log("INFO",
                    "Reading source data from database table: " + sourceInput,
                    null, "PROCESS_START");
                HashMap<String, String[]> arrayData = new metadata_parser_source()
                        .fetchSourceData(sourceInput, directColArray, sourceMetaColumnsList,
                                         dbURL, dbUsername, dbPassword);
                sourceData = convertArrayDataToMap(arrayData, directColArray, sourceMetaColumnsList);
            }

            if (sourceData == null) {
                log("WARN",
                    "No data returned from source (null result) - source=" + sourceInput,
                    null, "PROCESS_START");
                return false;
            }

            if (sourceData.isEmpty()) {
                log("INFO",
                    "Source data is empty - skipping UPDATE but marking as SUCCESS. source=" + sourceInput,
                    null, "PROCESS_START");
                return true;
            }

            log("INFO",
                "Source data fetched successfully - record count=" + sourceData.size()
                + " | source=" + sourceInput,
                null, "PROCESS_START");

            // ===== STEP 4: Update =====
            boolean updated = updateDataInDestination(
                    sourceData, destTableName,
                    directColArray, destMetaColArray,
                    sourceMetaColumnsList, jsonKeyNamesList,
                    pkMappings,
                    dbURL, dbUsername, dbPassword);

            if (!updated) {
                log("ERROR",
                    "UPDATE operation failed for dest_table=" + destTableName,
                    null, "UPDATE_FAILURE");
                return false;
            }

            long pipelineElapsed = System.currentTimeMillis() - pipelineStart;
            log("INFO",
                "UPDATE pipeline completed successfully - source=" + sourceInput
                + " | dest_table=" + destTableName
                + " | total pipeline time=" + pipelineElapsed + "ms",
                null, "UPDATE_SUCCESS");
            status = true;

        } catch (Exception ex) {
            log("ERROR",
                "Unhandled exception in parseAndTransferData - source=" + sourceInput
                + " | dest_table=" + destTableName + " - " + ex.getMessage(),
                null, "ERROR");
            status = false;
        }

        return status;
    }

    // =========================================================================
    // PK MAPPING PARSER
    // =========================================================================

    /**
     * Parse the pkMapping string into a list of PkMapping objects.
     *
     * Format: "csvPk1,csvPk2 | psqlPk1,psqlPk2"
     *   Left of '|'  - CSV column names (comma-separated, order matters).
     *   Right of '|' - PostgreSQL targets (comma-separated, same order).
     *                  Each target is either a plain column name or "metadata:jsonKey".
     *
     * The counts on both sides must match exactly.
     * Returns null on any parse error (caller must fail fast).
     */
    private List<PkMapping> parsePkMapping(String pkMappingStr) {

        if (pkMappingStr == null || pkMappingStr.trim().isEmpty()) {
            return null;
        }

        String[] sides = pkMappingStr.split("\\|", 2);
        if (sides.length != 2) {
            log("ERROR",
                "pkMapping must contain exactly one '|' separator. Got: [" + pkMappingStr + "]",
                null, "ERROR");
            return null;
        }

        String[] csvCols  = sides[0].split(",");
        String[] psqlCols = sides[1].split(",");

        if (csvCols.length != psqlCols.length) {
            log("ERROR",
                "CSV PK count (" + csvCols.length
                + ") does not match PostgreSQL PK count (" + psqlCols.length
                + "). Mapping: [" + pkMappingStr + "]",
                null, "ERROR");
            return null;
        }

        List<PkMapping> result = new ArrayList<>();

        for (int i = 0; i < csvCols.length; i++) {
            String csv  = csvCols[i].trim();
            String psql = psqlCols[i].trim();

            if (csv.isEmpty() || psql.isEmpty()) {
                log("ERROR",
                    "Empty PK entry at position=" + i + " in mapping: [" + pkMappingStr + "]",
                    null, "ERROR");
                return null;
            }

            result.add(new PkMapping(csv, psql));
        }

        return result;
    }

    // =========================================================================
    // CSV READING
    // =========================================================================

    /**
     * Read a pipe-delimited, optionally quoted CSV file.
     *
     * Returns HashMap<rowKey, Map<columnName, value>> instead of
     *         HashMap<rowKey, String[]>.  Each row is a LinkedHashMap so that
     *         insertion order (= header order) is preserved for debugging.
     *
     * Rules applied per cell:
     *  - Surrounding double-quotes are stripped.
     *  - Escaped double-quotes ("") inside a quoted field are collapsed to one.
     *  - "NULL" (case-insensitive) and blank values - stored as null.
     *  - Values are trimmed.
     *
     * @param filePath Absolute path to the CSV file.
     * @return Ordered map of row-index - (column-name - value), or null on error.
     */
    private HashMap<String, Map<String, String>> readCSV(String filePath) {

        HashMap<String, Map<String, String>> data = new LinkedHashMap<>();

        long startTime = System.currentTimeMillis();

        java.io.File csvFile = new java.io.File(filePath);
        log("INFO",
            "CSV processing started - file=" + filePath
            + " | size=" + csvFile.length() + " bytes",
            null, "FILE_DETECTED");

        try (java.io.BufferedReader br =
                     new java.io.BufferedReader(new java.io.FileReader(filePath))) {

            // ---- Read and parse header ----
            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                log("WARN",
                    "CSV file is empty or has no header row: " + filePath,
                    null, "FILE_DETECTED");
                return null;
            }

            String[] headers = parsePipeCsvLine(headerLine);
            log("INFO",
                "CSV header parsed - file=" + filePath + " | column count=" + headers.length,
                null, "FILE_DETECTED");

            // Lowercase header names once - avoids repeated toLowerCase() in the loop
            String[] lcHeaders = new String[headers.length];
            for (int i = 0; i < headers.length; i++) {
                lcHeaders[i] = (headers[i] != null) ? headers[i].trim().toLowerCase() : ("col_" + i);
            }

            // ---- Read data rows ----
            String line;
            int rowNum   = 0;
            int rowsRead = 0;
            int skipped  = 0;

            while ((line = br.readLine()) != null) {

                rowNum++;

                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] rawValues = parsePipeCsvLine(line);

                // Strict column-count check
                if (rawValues.length != lcHeaders.length) {
                    log("WARN",
                        "Skipping malformed row=" + rowNum
                        + " in file=" + filePath
                        + " | expected=" + lcHeaders.length
                        + " | got=" + rawValues.length,
                        null, "FILE_DETECTED");
                    skipped++;
                    continue;
                }

                Map<String, String> rowMap = new LinkedHashMap<>(lcHeaders.length * 2);

                for (int i = 0; i < lcHeaders.length; i++) {
                    rowMap.put(lcHeaders[i], cleanValue(rawValues[i]));
                }

                data.put(String.valueOf(rowNum), rowMap);
                rowsRead++;
            }

            long elapsedMs = System.currentTimeMillis() - startTime;

            log("INFO",
                "CSV read complete - file=" + filePath
                + " | total rows read=" + rowsRead
                + " | rows skipped=" + skipped
                + " | elapsed=" + elapsedMs + "ms",
                null, "FILE_DETECTED");

        } catch (Exception e) {
            log("ERROR",
                "Failed to read CSV file=" + filePath + " - " + e.getMessage(),
                null, "ERROR");
            return null;
        }

        return data;
    }

    // =========================================================================
    // DB-PATH ADAPTER
    // =========================================================================

    /**
     * Converts the String[]-based output of metadata_parser_source into the
     * same Map<colName, value> structure used by the CSV path.
     *
     * Column order in the String[] produced by metadata_parser_source is:
     *   [directCols...] [metaGroup0Cols...] [metaGroup1Cols...] ...
     *
     * This adapter reconstructs that mapping by name so that
     * updateDataInDestination() can always use record.get(columnName).
     *
     * metadata_parser_source.java is NOT modified.
     */
    private HashMap<String, Map<String, String>> convertArrayDataToMap(
            HashMap<String, String[]> arrayData,
            String[] directColArray,
            List<String[]> sourceMetaColumnsList) {

        if (arrayData == null) {
            return null;
        }

        HashMap<String, Map<String, String>> result = new LinkedHashMap<>(arrayData.size() * 2);

        for (Map.Entry<String, String[]> entry : arrayData.entrySet()) {

            String[]            record = entry.getValue();
            Map<String, String> rowMap = new LinkedHashMap<>();

            int idx = 0;

            for (String col : directColArray) {
                rowMap.put(col.toLowerCase(), (idx < record.length) ? record[idx] : null);
                idx++;
            }

            for (String[] metaCols : sourceMetaColumnsList) {
                for (String col : metaCols) {
                    rowMap.put(col.toLowerCase(), (idx < record.length) ? record[idx] : null);
                    idx++;
                }
            }

            result.put(entry.getKey(), rowMap);
        }

        return result;
    }

    // =========================================================================
    // UPDATE  (replaces insertDataToDestination)
    // =========================================================================

    /**
     * Batch-UPDATE all rows in the destination PostgreSQL table.
     *
     * Generated SQL structure:
     *   UPDATE destTable
     *   SET    non_pk_col1 = ?,
     *          non_pk_col2 = ?,
     *          meta_col    = ?::jsonb || jsonb_build_object('pk_key', meta_col->>'pk_key')
     *                        -- OR simply: meta_col = ?  (when no metadata PK in column)
     *   WHERE  direct_pk_col = ?
     *   AND    meta_col->>'json_pk_key' = ?
     *
     * Rules:
     *  - PK direct columns are excluded from the SET clause entirely.
     *  - Metadata JSON columns that contain a PK key use a PostgreSQL jsonb merge
     *    expression so the PK key value stored in the DB is never overwritten.
     *  - For non-jsonb metadata columns that contain a metadata PK, we fall back
     *    to setting the full JSON string (PK preservation requires jsonb).
     *  - Rows with any null/blank PK value are skipped with a WARN log.
     *  - Batch size, commit, and rollback behaviour are unchanged from the old INSERT.
     *
     * @param pkMappings  Parsed PK mapping - must not be null or empty.
     */
    private boolean updateDataInDestination(
            HashMap<String, Map<String, String>> sourceData,
            String destTableName,
            String[] directColumns, String[] destMetaColumns,
            List<String[]> sourceMetaColumnsList, List<String[]> jsonKeyNamesList,
            List<PkMapping> pkMappings,
            String dbURL, String dbUser, String dbPass) {

        Connection        conn         = null;
        PreparedStatement ps           = null;
        boolean           status       = false;
        int               batchSize    = 200;
        int               rowCount     = 0;
        int               skippedRows  = 0;
        int               totalUpdated = 0;
        int               totalNoMatch = 0;

        long updateStartTime = System.currentTimeMillis();

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            // ---- Fetch destination column types (single metadata query) ----
            HashMap<String, String> columnTypes =
                    getDestinationColumnTypes(destTableName, directColumns, destMetaColumns, conn);

            if (columnTypes == null || columnTypes.isEmpty()) {
                log("ERROR",
                    "Could not fetch destination column types for table=" + destTableName,
                    null, "ERROR");
                return false;
            }

            // ---- Classify PK parts into direct vs. metadata ----

            // Direct DB column names that are PK targets - excluded from SET clause.
            Set<String> directPkCols = new HashSet<>();

            // Metadata column name - set of JSON key names that are PK targets in
            // that column.  Used to build the jsonb merge expression and to skip
            // those keys when building the new JSON string.
            Map<String, Set<String>> metaPkKeysByCol = new HashMap<>();

            for (PkMapping pkm : pkMappings) {
                if (pkm.isMetadata) {
                    if (!metaPkKeysByCol.containsKey(pkm.psqlColumn)) {
                        metaPkKeysByCol.put(pkm.psqlColumn, new HashSet<>());
                    }
                    metaPkKeysByCol.get(pkm.psqlColumn).add(pkm.jsonKey);
                } else {
                    directPkCols.add(pkm.psqlColumn);
                }
            }

            // ---- Pre-compute non-PK direct column names (lowercased) ----
            // Avoids repeated toLowerCase() and containsKey() checks inside the row loop.
            List<String> nonPkDirectColsLc = new ArrayList<>();
            for (String col : directColumns) {
                if (!directPkCols.contains(col.toLowerCase())) {
                    nonPkDirectColsLc.add(col.toLowerCase());
                }
            }

            // ---- Pre-compute lowercased dest meta column names ----
            String[] destMetaColsLc = new String[destMetaColumns.length];
            for (int i = 0; i < destMetaColumns.length; i++) {
                destMetaColsLc[i] = destMetaColumns[i].toLowerCase();
            }

            // ---- Build UPDATE statement ----
            StringBuilder updateQuery = new StringBuilder("UPDATE ")
                    .append(destTableName).append(" SET ");

            boolean firstSetTerm = true;

            // SET clause - non-PK direct columns
            for (String colLc : nonPkDirectColsLc) {
                if (!firstSetTerm) updateQuery.append(", ");
                updateQuery.append(colLc).append(" = ?");
                firstSetTerm = false;
            }

            // SET clause - metadata JSON columns
            for (int i = 0; i < destMetaColumns.length; i++) {
                String destCol   = destMetaColumns[i];
                String destColLc = destMetaColsLc[i];

                Set<String> pkKeysInThisCol = metaPkKeysByCol.get(destColLc);
                boolean     hasMetaPk       = (pkKeysInThisCol != null && !pkKeysInThisCol.isEmpty());

                String  colType = columnTypes.get(destColLc);
                boolean isJsonb = colType != null && colType.toLowerCase().contains("jsonb");

                if (!firstSetTerm) updateQuery.append(", ");

                if (hasMetaPk && isJsonb) {
                    // Build a merge expression that:
                    //   1. Takes our new JSON (which excludes the PK key(s)) via ?::jsonb
                    //   2. Overlays it with the existing DB value for every PK key
                    // Result: all non-PK keys are updated; PK keys keep their DB values.
                    //
                    // SQL: meta_col = ?::jsonb || jsonb_build_object(
                    //          'pk1', meta_col->>'pk1',
                    //          'pk2', meta_col->>'pk2'
                    //      )
                    updateQuery.append(destCol).append(" = ?::jsonb || jsonb_build_object(");
                    boolean firstKey = true;
                    for (String pkKey : pkKeysInThisCol) {
                        if (!firstKey) updateQuery.append(", ");
                        updateQuery.append("'").append(pkKey).append("', ")
                                   .append(destCol).append("->>'").append(pkKey).append("'");
                        firstKey = false;
                    }
                    updateQuery.append(")");
                } else {
                    // Normal metadata column (no PK key, or column is not jsonb).
                    // For non-jsonb with a metadata PK this is a best-effort: the full
                    // JSON including the PK key value from the CSV is written (no DB
                    // preservation is possible without a subquery for text columns).
                    if (hasMetaPk && !isJsonb) {
                        log("ERROR",
                            "Metadata PK requires jsonb column type but found type=" + colType
                            + " for col=" + destCol + " in table=" + destTableName,
                            null, "ERROR");
                        return false;
                    }
                    updateQuery.append(destCol).append(" = ?");
                }

                firstSetTerm = false;
            }

            if (firstSetTerm) {
                log("ERROR",
                    "SET clause is empty - all direct columns are PKs and there are no metadata columns."
                    + " An UPDATE with no SET clause is invalid. table=" + destTableName,
                    null, "ERROR");
                return false;
            }

            // WHERE clause - direct PK columns first, then metadata JSON PKs
            updateQuery.append(" WHERE ");
            boolean firstWhereTerm = true;

            for (PkMapping pkm : pkMappings) {
                if (!pkm.isMetadata) {
                    if (!firstWhereTerm) updateQuery.append(" AND ");
                    updateQuery.append(pkm.psqlColumn).append(" = ?");
                    firstWhereTerm = false;
                }
            }

            for (PkMapping pkm : pkMappings) {
                if (pkm.isMetadata) {
                    if (!firstWhereTerm) updateQuery.append(" AND ");
                    // PostgreSQL JSON text extraction operator
                    updateQuery.append(pkm.psqlColumn)
                               .append("->>'").append(pkm.jsonKey).append("' = ?");
                    firstWhereTerm = false;
                }
            }

            log("INFO",
                "UPDATE started - table=" + destTableName
                + " | total records to update=" + sourceData.size()
                + " | batch size=" + batchSize,
                null, "UPDATE_START");

            log("DEBUG",
                "UPDATE query template - table=" + destTableName
                + " | template=" + updateQuery.toString(),
                null, "UPDATE_START");

            conn.setAutoCommit(false);
            ps = conn.prepareStatement(updateQuery.toString());

            boolean firstRowLogged = false;

            // ---- Process each row ----
            for (Map.Entry<String, Map<String, String>> entry : sourceData.entrySet()) {

                Map<String, String> record = entry.getValue();

                // ---- Debug: log first row summary only ----
                if (!firstRowLogged) {
                    log("DEBUG",
                        "First row sample - row key=" + entry.getKey()
                        + " | column count=" + record.size(),
                        null, "UPDATE_START");
                    firstRowLogged = true;
                }

                // ---- Soft row validation (non-blocking) ----
                if (!validateRow(record, directColumns, columnTypes)) {
                    log("WARN",
                        "Soft validation warning on row=" + entry.getKey()
                        + " - proceeding with UPDATE anyway",
                        null, "UPDATE_PROCESS");
                }

                // ---- PK value check - skip the row if any PK value is missing ----
                boolean pkMissing = false;
                for (PkMapping pkm : pkMappings) {
                    String pkVal = record.get(pkm.csvColumn);
                    if (pkVal == null || pkVal.trim().isEmpty()) {
                        pkMissing = true;
                        break;
                    }
                }
                if (pkMissing) {
                    skippedRows++;
                    continue;
                }

                int paramIndex = 1;

                // ================= SET: NON-PK DIRECT COLUMNS =================
                for (String colLc : nonPkDirectColsLc) {
                    String value      = record.get(colLc);
                    String columnType = columnTypes.get(colLc);
                    setParameterByType(ps, paramIndex, value, columnType, colLc);
                    paramIndex++;
                }

                // ================= SET: METADATA JSON COLUMNS =================
                for (int i = 0; i < destMetaColumns.length; i++) {

                    String[]    sourceCols  = sourceMetaColumnsList.get(i);
                    String[]    jsonKeys    = jsonKeyNamesList.get(i);
                    String      destColLc   = destMetaColsLc[i];

                    Set<String> pkKeysInThisCol = metaPkKeysByCol.get(destColLc);
                    boolean     hasMetaPk       = (pkKeysInThisCol != null && !pkKeysInThisCol.isEmpty());

                    String  colType = columnTypes.get(destColLc);
                    boolean isJsonb = colType != null && colType.toLowerCase().contains("jsonb");

                    // Build the JSON string.
                    //
                    // When hasMetaPk && isJsonb: omit the PK json key(s) from the
                    // string - the SQL expression restores them from the existing DB row.
                    //
                    // When hasMetaPk && !isJsonb (fallback): include all keys as usual
                    // since we cannot do a DB-side merge on a non-jsonb column.
                    StringBuilder json     = new StringBuilder("{");
                    boolean       firstKey = true;

                    for (int j = 0; j < sourceCols.length; j++) {

                        String srcColName = sourceCols[j].toLowerCase();
                        String jsonKey    = jsonKeys[j];

                        // Skip PK key when building the JSON for the jsonb merge path.
                        // The SQL expression will re-inject it from the current DB value.
                        if (hasMetaPk && isJsonb && pkKeysInThisCol.contains(jsonKey)) {
                            continue;
                        }

                        String value = record.get(srcColName);
                        if (value != null) {
                            value = value.replace("\\", "\\\\").replace("\"", "\\\"");
                        }

                        if (!firstKey) json.append(",");
                        json.append("\"").append(jsonKey).append("\":");
                        if (value != null) {
                            json.append("\"").append(value).append("\"");
                        } else {
                            json.append("null");
                        }
                        firstKey = false;
                    }

                    json.append("}");

                    // For both the plain (?) and cast (?::jsonb) placeholders in the SQL,
                    // setString() is correct: the ::jsonb cast in the SQL handles the type
                    // conversion, and plain text columns just receive the string directly.
                    ps.setString(paramIndex, json.toString());
                    paramIndex++;
                }

                // ================= WHERE: DIRECT PK COLUMNS =================
                for (PkMapping pkm : pkMappings) {
                    if (!pkm.isMetadata) {
                        String value      = record.get(pkm.csvColumn);
                        String columnType = columnTypes.get(pkm.psqlColumn);
                        setParameterByType(ps, paramIndex, value, columnType, pkm.psqlColumn);
                        paramIndex++;
                    }
                }

                // ================= WHERE: METADATA JSON PK COLUMNS =================
                for (PkMapping pkm : pkMappings) {
                    if (pkm.isMetadata) {
                        // The ->> operator returns text so we always bind as String.
                        String value = record.get(pkm.csvColumn);
                        ps.setString(paramIndex, value);
                        paramIndex++;
                    }
                }

                ps.addBatch();
                rowCount++;

                if (rowCount % batchSize == 0) {
                    int[] results = ps.executeBatch();

                    int batchUpdated = 0;
                    int batchNoMatch = 0;
                    for (int r : results) {
                        if (r == 0) batchNoMatch++;
                        else batchUpdated++;
                    }
                    totalUpdated += batchUpdated;
                    totalNoMatch += batchNoMatch;

                    conn.commit();

                    log("INFO",
                        "Batch executed - rows processed so far=" + rowCount
                        + " | batch updated=" + batchUpdated
                        + " | batch no-match=" + batchNoMatch
                        + " | running total updated=" + totalUpdated
                        + " | skipped (missing PK)=" + skippedRows,
                        null, "UPDATE_BATCH");
                }
            }

            // Execute remaining batch
            if (ps != null) {
                int[] results = ps.executeBatch();

                int batchUpdated = 0;
                int batchNoMatch = 0;
                for (int r : results) {
                    if (r == 0) batchNoMatch++;
                    else batchUpdated++;
                }
                totalUpdated += batchUpdated;
                totalNoMatch += batchNoMatch;

                ps.clearBatch();
                conn.commit();

                long elapsedMs = System.currentTimeMillis() - updateStartTime;

                log("INFO",
                    "Final batch executed - batch updated=" + batchUpdated
                    + " | batch no-match=" + batchNoMatch,
                    null, "UPDATE_BATCH");

                log("INFO",
                    "Update completed - table=" + destTableName
                    + " | total rows processed=" + rowCount
                    + " | total rows updated=" + totalUpdated
                    + " | no-match (not found in DB)=" + totalNoMatch
                    + " | skipped (missing PK)=" + skippedRows
                    + " | elapsed=" + elapsedMs + "ms",
                    null, "UPDATE_SUCCESS");

                status = true;
            }

        } catch (Exception ex) {
            log("ERROR",
                "Exception in updateDataInDestination for table=" + destTableName
                + " - " + ex.getMessage(),
                null, "UPDATE_FAILURE");
            try {
                if (conn != null) conn.rollback();
                log("INFO",
                    "Transaction rolled back for table=" + destTableName,
                    null, "UPDATE_FAILURE");
            } catch (Exception e) {
                log("ERROR",
                    "Failed to rollback transaction for table=" + destTableName
                    + " - " + e.getMessage(),
                    null, "ERROR");
            }
            status = false;
        } finally {
            try {
                if (ps   != null) ps.close();
                if (conn != null) conn.close();
            } catch (Exception e) {
                log("WARN",
                    "Failed to close DB resources for table=" + destTableName
                    + " - " + e.getMessage(),
                    null, "ERROR");
            }
        }

        return status;
    }

    // =========================================================================
    // ROW VALIDATION
    // =========================================================================

    /**
     * Soft-validate a single row against expected column types.
     *
     * Returns false on first type mismatch (non-blocking - caller decides
     * whether to skip or update the row).
     */
    private boolean validateRow(Map<String, String> record,
                                String[] directColumns,
                                HashMap<String, String> columnTypes) {

        for (String col : directColumns) {

            String colKey = col.toLowerCase();
            String val    = record.get(colKey);
            String type   = columnTypes.get(colKey);

            if (val == null || val.trim().isEmpty() || val.equalsIgnoreCase("NULL")) {
                continue;
            }

            if (type == null) {
                continue;
            }

            val = val.trim();

            try {

                if (type.contains("int")) {
                    if (!val.matches("-?\\d+")) {
                        return false;
                    }
                } else if (type.contains("numeric") || type.contains("decimal")) {
                    if (!val.matches("-?\\d+(\\.\\d+)?")) {
                        return false;
                    }
                } else if (type.contains("date")) {
                    if (val.equals("0")) {
                        continue;
                    }
                    if (!val.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        return false;
                    }
                } else if (type.contains("timestamp")) {
                    if (!val.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
                        return false;
                    }
                }

            } catch (Exception e) {
                return false;
            }
        }

        return true;
    }

    // =========================================================================
    // PIPE-CSV PARSER
    // =========================================================================

    /**
     * Parse a single pipe-delimited CSV line, respecting double-quoted fields
     * and escaped double-quotes ("" inside a quoted field).
     */
    private String[] parsePipeCsvLine(String line) {

        List<String> fields = new ArrayList<>();
        StringBuilder sb    = new StringBuilder();
        boolean inQuotes    = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    sb.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == '|' && !inQuotes) {
                fields.add(cleanValue(sb.toString()));
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }

        fields.add(cleanValue(sb.toString()));

        return fields.toArray(new String[0]);
    }

    /**
     * Trim and normalise a single cell value.
     *  - null input        - null
     *  - blank string      - null
     *  - "NULL" (any case) - null
     *  - otherwise         - trimmed value
     */
    private String cleanValue(String val) {
        if (val == null) return null;
        val = val.trim();
        if (val.isEmpty() || val.equalsIgnoreCase("NULL")) return null;
        return val;
    }

    // =========================================================================
    // ARGUMENT PARSERS
    // =========================================================================

    private String[] parseCommaSeparated(String input) {
        if (input == null || input.trim().isEmpty()) return new String[0];
        String[] parts = input.split(",");
        for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
        return parts;
    }

    private String[] parseHashSeparated(String input) {
        if (input == null || input.trim().isEmpty()) return new String[0];
        String[] parts = input.split("#");
        for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
        return parts;
    }

    // =========================================================================
    // TABLE / COLUMN METADATA HELPERS
    // =========================================================================

    private boolean validateTableExists(String tableName, String dbURL, String dbUser, String dbPass) {

        Connection conn   = null;
        boolean    exists = false;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            String schemaName      = "public";
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            log("DEBUG",
                "Checking table existence - schema=" + schemaName + " | table=" + actualTableName,
                null, "PROCESS_START");

            String sql = "SELECT 1 FROM information_schema.tables "
                       + "WHERE table_schema = ? AND table_name = ?";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                ps.setString(2, actualTableName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        exists = true;
                        log("INFO",
                            "Table found - schema=" + schemaName + " | table=" + actualTableName,
                            null, "PROCESS_START");
                    } else {
                        log("WARN",
                            "Table NOT found - schema=" + schemaName + " | table=" + actualTableName,
                            null, "ERROR");
                    }
                }
            }

        } catch (Exception ex) {
            log("ERROR",
                "Exception while validating table existence for table=" + tableName
                + " - " + ex.getMessage(),
                null, "ERROR");
        } finally {
            try { if (conn != null) conn.close(); } catch (Exception e) {
                log("WARN",
                    "Failed to close DB connection after table validation - " + e.getMessage(),
                    null, "ERROR");
            }
        }

        return exists;
    }

    private HashSet<String> getTableColumns(String tableName, String dbURL, String dbUser, String dbPass) {

        Connection      conn    = null;
        HashSet<String> columns = new HashSet<>();

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
            DatabaseMetaData meta = conn.getMetaData();

            String schemaName      = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            try (ResultSet rs = meta.getColumns(null, schemaName, actualTableName, null)) {
                while (rs.next()) {
                    String colName = rs.getString("COLUMN_NAME");
                    columns.add(colName.toLowerCase());
                }
            }

            log("INFO",
                "Column metadata loaded - table=" + actualTableName
                + " | total columns=" + columns.size(),
                null, "PROCESS_START");

        } catch (Exception ex) {
            log("ERROR",
                "Failed to retrieve columns for table=" + tableName + " - " + ex.getMessage(),
                null, "ERROR");
            columns = null;
        } finally {
            try { if (conn != null) conn.close(); } catch (Exception e) {
                log("WARN",
                    "Failed to close DB connection after getTableColumns - " + e.getMessage(),
                    null, "ERROR");
            }
        }

        return columns;
    }

    /**
     * Fetch column type metadata for the specified direct and meta columns.
     *
     * OPTIMIZED: Uses a single metadata.getColumns() call for all columns
     * instead of one call per column, reducing DB round-trips from N to 1.
     */
    private HashMap<String, String> getDestinationColumnTypes(String tableName,
            String[] directColumns, String[] metaColumns, Connection conn) {

        HashMap<String, String> columnTypes = new HashMap<>();

        try {
            DatabaseMetaData meta = conn.getMetaData();

            String schemaName      = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            // Build a set of the lowercased column names we need
            Set<String> neededCols = new HashSet<>();
            for (String col : directColumns) {
                neededCols.add(col.toLowerCase());
            }
            for (String col : metaColumns) {
                neededCols.add(col.toLowerCase());
            }

            // Single metadata query for all columns - avoids N separate round-trips
            try (ResultSet rs = meta.getColumns(null, schemaName, actualTableName, null)) {
                while (rs.next()) {
                    String colName = rs.getString("COLUMN_NAME").toLowerCase();
                    if (neededCols.contains(colName)) {
                        columnTypes.put(colName, rs.getString("TYPE_NAME"));
                    }
                }
            }

            log("INFO",
                "Column types resolved - table=" + actualTableName
                + " | resolved=" + columnTypes.size() + "/" + neededCols.size(),
                null, "PROCESS_START");

        } catch (Exception ex) {
            log("ERROR",
                "Failed to fetch destination column types for table=" + tableName
                + " - " + ex.getMessage(),
                null, "ERROR");
            return null;
        }

        return columnTypes;
    }

    // =========================================================================
    // TYPE-SAFE PARAMETER SETTER
    // =========================================================================

    private void setParameterByType(PreparedStatement ps, int paramIndex,
                                    String value, String columnType, String columnName)
            throws Exception {

        if (value == null || value.trim().isEmpty() || value.equalsIgnoreCase("NULL")) {
            ps.setNull(paramIndex, java.sql.Types.NULL);
            return;
        }

        String val = trimToColumnSize(value.trim(), columnType);

        if (columnType == null) {
            ps.setString(paramIndex, val);
            return;
        }

        String type = columnType.toLowerCase();

        try {

            if (type.contains("bigint")) {
                if (!val.matches("-?\\d+")) { ps.setNull(paramIndex, java.sql.Types.BIGINT); return; }
                ps.setLong(paramIndex, Long.parseLong(val));

            } else if (type.contains("int")) {
                if (!val.matches("-?\\d+")) { ps.setNull(paramIndex, java.sql.Types.INTEGER); return; }
                ps.setInt(paramIndex, Integer.parseInt(val));

            } else if (type.contains("numeric") || type.contains("decimal")) {
                if (!val.matches("-?\\d+(\\.\\d+)?")) { ps.setNull(paramIndex, java.sql.Types.NUMERIC); return; }
                ps.setBigDecimal(paramIndex, new java.math.BigDecimal(val));

            } else if (type.contains("timestamp")) {
                if (!val.matches("\\d{4}-\\d{2}-\\d{2}.*")) { ps.setNull(paramIndex, java.sql.Types.TIMESTAMP); return; }
                ps.setTimestamp(paramIndex, java.sql.Timestamp.valueOf(val));

            } else if (type.contains("date")) {
                if (val.equals("0") || !val.matches("\\d{4}-\\d{2}-\\d{2}")) {
                    ps.setNull(paramIndex, java.sql.Types.DATE); return;
                }
                ps.setDate(paramIndex, java.sql.Date.valueOf(val));

            } else if (type.contains("bool")) {
                ps.setBoolean(paramIndex, Boolean.parseBoolean(val));

            } else {
                ps.setString(paramIndex, val);
            }

        } catch (Exception e) {
            log("WARN",
                "Type parse failed - setting NULL. val='" + val + "' | col=" + columnName
                + " | type=" + columnType + " - " + e.getMessage(),
                null, "UPDATE_PROCESS");
            ps.setNull(paramIndex, java.sql.Types.NULL);
        }
    }

    private String trimToColumnSize(String value, String columnType) {
        if (value == null || columnType == null) return value;
        try {
            if (columnType.toLowerCase().contains("varchar")) {
                int start = columnType.indexOf("(");
                int end   = columnType.indexOf(")");
                if (start > 0 && end > start) {
                    int size = Integer.parseInt(columnType.substring(start + 1, end));
                    if (value.length() > size) return value.substring(0, size);
                }
            }
        } catch (Exception e) { /* ignore */ }
        return value;
    }

    // =========================================================================
    // LEGACY HELPERS  (kept for completeness; not used in main flow)
    // =========================================================================

    private boolean validateDataTypes(String sourceTableName, String destTableName,
            String[] directColumns, String[] destMetaColumns,
            String dbURL, String dbUser, String dbPass) {

        Connection conn  = null;
        boolean    valid = true;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
            DatabaseMetaData meta = conn.getMetaData();

            for (String col : directColumns) {
                String srcType  = getColumnType(meta, sourceTableName.toLowerCase(), col.toLowerCase());
                String destType = getColumnType(meta, destTableName.toLowerCase(),   col.toLowerCase());

                if (srcType == null || destType == null) { valid = false; continue; }
                if (!isConvertibleToString(srcType) || !isConvertibleToString(destType)) { valid = false; }
            }

            for (String col : destMetaColumns) {
                String destType = getColumnType(meta, destTableName.toLowerCase(), col.toLowerCase());
                if (destType == null) { valid = false; continue; }
                if (!isTextType(destType)) {
                    log("WARN",
                        "Metadata column '" + col + "' has type '" + destType
                        + "' which is not text/jsonb - JSON storage may fail",
                        null, "PROCESS_START");
                }
            }

        } catch (Exception ex) {
            log("ERROR",
                "Exception in validateDataTypes - src=" + sourceTableName
                + " | dest=" + destTableName + " - " + ex.getMessage(),
                null, "ERROR");
            valid = false;
        } finally {
            try { if (conn != null) conn.close(); } catch (Exception e) {
                log("WARN",
                    "Failed to close DB connection after validateDataTypes - " + e.getMessage(),
                    null, "ERROR");
            }
        }

        return valid;
    }

    private String getColumnType(DatabaseMetaData meta, String tableName, String columnName)
            throws Exception {

        String schemaName      = null;
        String actualTableName = tableName;

        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.");
            schemaName      = parts[0].toLowerCase();
            actualTableName = parts[1].toLowerCase();
        }

        try (ResultSet rs = meta.getColumns(null, schemaName, actualTableName, columnName)) {
            return rs.next() ? rs.getString("TYPE_NAME") : null;
        }
    }

    private boolean isConvertibleToString(String dataType) {
        if (dataType == null) return false;
        String t = dataType.toLowerCase();
        return t.contains("char") || t.contains("text")    || t.contains("int")     ||
               t.contains("numeric") || t.contains("decimal") || t.contains("float")  ||
               t.contains("double")  || t.contains("real")    || t.contains("bool")   ||
               t.contains("date")    || t.contains("time")    || t.contains("json")   ||
               t.contains("uuid")    || t.contains("serial")  || t.contains("money");
    }

    private boolean isTextType(String dataType) {
        if (dataType == null) return false;
        String t = dataType.toLowerCase();
        return t.contains("char") || t.contains("text") || t.contains("json");
    }

    // =========================================================================
    // LOGGING
    // =========================================================================

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
}
