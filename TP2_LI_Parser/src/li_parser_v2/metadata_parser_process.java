package li_parser_v2;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * ProcessData - PostgreSQL Data Parser and Transfer
 *
 * REFACTORED: Fully column-name driven (no more index-based access).
 *
 * Core change: CSV rows are now stored as Map<String, String> (colName → value)
 * instead of String[]. All downstream methods use record.get(columnName).
 *
 * Business Logic Flow:
 *  1. Parse and validate arguments
 *  2. Validate destination table and columns exist
 *  3. Fetch data from CSV or DB
 *  4. Transform data (direct columns + JSON metadata)
 *  5. Batch-insert into PostgreSQL
 */
public class metadata_parser_process {

    PrintUtilMsg pu = new PrintUtilMsg();

    @SuppressWarnings("unused")
    private static final boolean SKIP_MISSING_COLUMNS = false;

    // =========================================================================
    // PUBLIC ENTRY POINT
    // =========================================================================

    /**
     * Main processing method. Signature UNCHANGED for backward compatibility.
     */
    public boolean parseAndTransferData(String dbURL, String dbUsername, String dbPassword,
            String sourceInput, String destTableName,
            String directColumns, int metadataCount,
            String destMetadataColumns, String sourceMetadataColumns, String jsonKeyNames) {

        boolean status = false;

        try {
            pu.sop("parseAndTransferData", "===== START CSV/DB PROCESSING =====");

            boolean isCSV = sourceInput != null && sourceInput.toLowerCase().endsWith(".csv");

            // ===== STEP 1: Parse arguments =====
            String[]       directColArray  = parseCommaSeparated(directColumns);
            String[]       destMetaColArray = parseCommaSeparated(destMetadataColumns);
            String[]       sourceMetaGroups = parseHashSeparated(sourceMetadataColumns);
            String[]       jsonKeyGroups    = parseHashSeparated(jsonKeyNames);

            List<String[]> sourceMetaColumnsList = new ArrayList<>();
            List<String[]> jsonKeyNamesList       = new ArrayList<>();

            for (int i = 0; i < metadataCount; i++) {
                String[] sourceCols = parseCommaSeparated(sourceMetaGroups[i]);
                String[] jsonKeys   = parseCommaSeparated(jsonKeyGroups[i]);

                if (sourceCols.length != jsonKeys.length) {
                    pu.sop("parseAndTransferData", "ERROR: Metadata column/key count mismatch at group " + i);
                    return false;
                }

                sourceMetaColumnsList.add(sourceCols);
                jsonKeyNamesList.add(jsonKeys);
            }

            // ===== STEP 2: Validate destination table and columns =====
            if (!validateTableExists(destTableName, dbURL, dbUsername, dbPassword)) {
                pu.sop("parseAndTransferData", "ERROR: Destination table not found: " + destTableName);
                return false;
            }

            HashSet<String> destTableColumns = getTableColumns(destTableName, dbURL, dbUsername, dbPassword);

            for (String col : directColArray) {
                if (!destTableColumns.contains(col.toLowerCase())) {
                    pu.sop("parseAndTransferData", "ERROR: Direct column missing in destination: " + col);
                    return false;
                }
            }

            for (String col : destMetaColArray) {
                if (!destTableColumns.contains(col.toLowerCase())) {
                    pu.sop("parseAndTransferData", "ERROR: Metadata column missing in destination: " + col);
                    return false;
                }
            }

            // ===== STEP 3: Fetch data (CSV or DB) → always Map<colName, value> =====
            // NOTE: Both CSV and DB paths are normalised into the same type so that
            //       insertDataToDestination() stays purely column-name driven.
            HashMap<String, Map<String, String>> sourceData;

            if (isCSV) {
                pu.sop("parseAndTransferData", "Reading from CSV: " + sourceInput);
                // readCSV() is now column-name driven — returns Map<colName, value> per row
                sourceData = readCSV(sourceInput);
            } else {
                pu.sop("parseAndTransferData", "Reading from DB: " + sourceInput);
                // metadata_parser_source returns String[] — adapt to Map without modifying it
                HashMap<String, String[]> arrayData = new metadata_parser_source()
                        .fetchSourceData(sourceInput, directColArray, sourceMetaColumnsList,
                                         dbURL, dbUsername, dbPassword);
                sourceData = convertArrayDataToMap(arrayData, directColArray, sourceMetaColumnsList);
            }

            if (sourceData == null) {
                pu.sop("parseAndTransferData", "No data found (NULL)");
                return false;
            }

            if (sourceData.isEmpty()) {
                pu.sop("parseAndTransferData", "Empty file — skipping insert but marking SUCCESS");
                return true;
            }

            pu.sop("parseAndTransferData", "Records fetched: " + sourceData.size());

            // ===== STEP 4: Insert =====
            boolean inserted = insertDataToDestination(
                    sourceData, destTableName,
                    directColArray, destMetaColArray,
                    sourceMetaColumnsList, jsonKeyNamesList,
                    dbURL, dbUsername, dbPassword);

            if (!inserted) {
                pu.sop("parseAndTransferData", "Insert failed");
                return false;
            }

            pu.sop("parseAndTransferData", "SUCCESS");
            status = true;

        } catch (Exception ex) {
            System.out.println("EXCEPTION in parseAndTransferData: " + ex.toString());
            ex.printStackTrace();
            status = false;
        }

        return status;
    }

    // =========================================================================
    // CSV READING  (REFACTORED — column-name driven)
    // =========================================================================

    /**
     * Read a pipe-delimited, optionally quoted CSV file.
     *
     * CHANGED: Returns HashMap<rowKey, Map<columnName, value>> instead of
     *          HashMap<rowKey, String[]>.  Each row is a LinkedHashMap so that
     *          insertion order (= header order) is preserved for debugging.
     *
     * Rules applied per cell:
     *  - Surrounding double-quotes are stripped.
     *  - Escaped double-quotes ("") inside a quoted field are collapsed to one.
     *  - "NULL" (case-insensitive) and blank values → stored as null.
     *  - Values are trimmed.
     *
     * @param filePath Absolute path to the CSV file.
     * @return Ordered map of row-index → (column-name → value), or null on error.
     */
    private HashMap<String, Map<String, String>> readCSV(String filePath) {

        // LinkedHashMap preserves file row order
        HashMap<String, Map<String, String>> data = new LinkedHashMap<>();

        try (java.io.BufferedReader br =
                     new java.io.BufferedReader(new java.io.FileReader(filePath))) {

            // ---- Read and parse header ----
            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                pu.sop("readCSV", "Empty CSV file: " + filePath);
                return null;
            }

            String[] headers = parsePipeCsvLine(headerLine);
            pu.sop("readCSV", "CSV columns found: " + headers.length);

            // Lowercase header names once — avoids repeated toLowerCase() in the loop
            String[] lcHeaders = new String[headers.length];
            for (int i = 0; i < headers.length; i++) {
                lcHeaders[i] = (headers[i] != null) ? headers[i].trim().toLowerCase() : ("col_" + i);
            }

            // ---- Read data rows ----
            String line;
            int rowNum    = 0;  // counts every line (including skipped)
            int inserted  = 0;  // counts rows actually stored
            int skipped   = 0;

            while ((line = br.readLine()) != null) {

                rowNum++;

                if (line.trim().isEmpty()) {
                    continue;  // skip blank lines silently
                }

                String[] rawValues = parsePipeCsvLine(line);

                // Strict column-count check
                if (rawValues.length != lcHeaders.length) {
                    pu.sop("readCSV", "SKIP row " + rowNum
                            + " | expected=" + lcHeaders.length
                            + " | got=" + rawValues.length
                            + " | raw=" + line);
                    skipped++;
                    continue;
                }

                // Build column-name → value map for this row.
                // Use LinkedHashMap to preserve column order (helpful for debugging).
                Map<String, String> rowMap = new LinkedHashMap<>(lcHeaders.length * 2);

                for (int i = 0; i < lcHeaders.length; i++) {
                    // cleanValue handles: trim, NULL→null, empty→null
                    rowMap.put(lcHeaders[i], cleanValue(rawValues[i]));
                }

                data.put(String.valueOf(rowNum), rowMap);
                inserted++;
            }

            pu.sop("readCSV", "Total rows stored: " + inserted + " | skipped: " + skipped);

        } catch (Exception e) {
            System.out.println("EXCEPTION in readCSV: " + e.toString());
            e.printStackTrace();
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
     * insertDataToDestination() can always use record.get(columnName).
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

            String[]           record = entry.getValue();
            Map<String, String> rowMap = new LinkedHashMap<>();

            int idx = 0;

            // Map direct columns
            for (String col : directColArray) {
                rowMap.put(col.toLowerCase(), (idx < record.length) ? record[idx] : null);
                idx++;
            }

            // Map source metadata columns (same order as metadata_parser_source builds them)
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
    // INSERT  (REFACTORED — column-name driven)
    // =========================================================================

    /**
     * Batch-insert all rows into the destination PostgreSQL table.
     *
     * CHANGED signature: sourceData is now HashMap<String, Map<String,String>>.
     * CHANGED: direct column values fetched via record.get(columnName).
     * CHANGED: metadata JSON values fetched via record.get(sourceColumnName).
     * All other logic (batch size, commit, rollback, column-type routing) is
     * UNCHANGED.
     */
    private boolean insertDataToDestination(
            HashMap<String, Map<String, String>> sourceData,
            String destTableName,
            String[] directColumns, String[] destMetaColumns,
            List<String[]> sourceMetaColumnsList, List<String[]> jsonKeyNamesList,
            String dbURL, String dbUser, String dbPass) {

        Connection        conn   = null;
        PreparedStatement ps     = null;
        boolean           status = false;
        int batchSize  = 200;
        int rowCount   = 0;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            // ---- Fetch destination column types ----
            HashMap<String, String> columnTypes =
                    getDestinationColumnTypes(destTableName, directColumns, destMetaColumns, conn);

            if (columnTypes == null || columnTypes.isEmpty()) {
                pu.sop("insertDataToDestination", "ERROR: Could not fetch destination column types");
                return false;
            }

            // ---- Build INSERT statement ----
            StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(destTableName).append(" (");

            for (int i = 0; i < directColumns.length; i++) {
                insertQuery.append(directColumns[i]);
                if (i < directColumns.length - 1 || destMetaColumns.length > 0) {
                    insertQuery.append(", ");
                }
            }

            for (int i = 0; i < destMetaColumns.length; i++) {
                insertQuery.append(destMetaColumns[i]);
                if (i < destMetaColumns.length - 1) {
                    insertQuery.append(", ");
                }
            }

            insertQuery.append(") VALUES (");

            int totalCols = directColumns.length + destMetaColumns.length;
            for (int i = 0; i < totalCols; i++) {
                insertQuery.append("?");
                if (i < totalCols - 1) {
                    insertQuery.append(", ");
                }
            }
            insertQuery.append(")");

            pu.sop("insertDataToDestination", "Insert query: " + insertQuery.toString());

            conn.setAutoCommit(false);
            ps = conn.prepareStatement(insertQuery.toString());

            // ---- Process each row ----
            boolean firstRowLogged = false;

            for (Map.Entry<String, Map<String, String>> entry : sourceData.entrySet()) {

                Map<String, String> record = entry.getValue();

                // ---- Debug: log first row column mapping ----
                if (!firstRowLogged) {
                    pu.sop("DEBUG_FIRST_ROW", "Row key=" + entry.getKey() + " | columns=" + record.size());
                    for (String col : directColumns) {
                        pu.sop("COLUMN_MAPPING", "Col '" + col + "' = " + record.get(col.toLowerCase()));
                    }
                    firstRowLogged = true;
                }

                // ---- Validation (non-blocking: logs warning but continues) ----
                if (!validateRow(record, directColumns, columnTypes)) {
                    pu.sop("ROW_WARNING", "Soft validation warning on row " + entry.getKey()
                            + " — inserting anyway");
                }

                int paramIndex = 1;

                // ================= DIRECT COLUMNS =================
                // CHANGED: record.get(columnName) instead of record[i]
                for (String directCol : directColumns) {

                    String columnName = directCol.toLowerCase();
                    // Fetch by column name — eliminates all index-alignment issues
                    String value      = record.get(columnName);
                    String columnType = columnTypes.get(columnName);

                    setParameterByType(ps, paramIndex, value, columnType, columnName);
                    paramIndex++;
                }

                // ================= METADATA JSON COLUMNS =================
                // CHANGED: values fetched by source column NAME, not array offset
                for (int i = 0; i < destMetaColumns.length; i++) {

                    String[]      sourceCols = sourceMetaColumnsList.get(i);
                    String[]      jsonKeys   = jsonKeyNamesList.get(i);

                    StringBuilder json = new StringBuilder("{");

                    for (int j = 0; j < sourceCols.length; j++) {

                        String srcColName = sourceCols[j].toLowerCase();
                        String jsonKey    = jsonKeys[j];

                        // CHANGED: look up value by column name — no recordIndex shifting
                        String value = record.get(srcColName);

                        // Escape backslashes then double-quotes for valid JSON string
                        if (value != null) {
                            value = value.replace("\\", "\\\\").replace("\"", "\\\"");
                        }

                        json.append("\"").append(jsonKey).append("\":");

                        if (value != null) {
                            json.append("\"").append(value).append("\"");
                        } else {
                            json.append("null");
                        }

                        if (j < sourceCols.length - 1) {
                            json.append(",");
                        }
                    }

                    json.append("}");

                    String destColName = destMetaColumns[i].toLowerCase();
                    String columnType  = columnTypes.get(destColName);

                    if (columnType != null && columnType.toLowerCase().contains("jsonb")) {
                        ps.setObject(paramIndex, json.toString(), java.sql.Types.OTHER);
                    } else {
                        ps.setString(paramIndex, json.toString());
                    }

                    paramIndex++;
                }

                ps.addBatch();
                rowCount++;

                if (rowCount % batchSize == 0) {
                    ps.executeBatch();
                    pu.sop("insertDataToDestination", "Batch executed — rows so far: " + rowCount);
                }
            }

            // Execute remaining batch
            if (ps != null) {
                ps.executeBatch();
                conn.commit();
                status = true;
                pu.sop("insertDataToDestination", "===== ALL DATA INSERTED SUCCESSFULLY =====");
                pu.sop("insertDataToDestination", "Total rows inserted: " + rowCount);
            }

        } catch (Exception ex) {
            System.out.println("EXCEPTION in insertDataToDestination: " + ex.toString());
            ex.printStackTrace();
            try { if (conn != null) conn.rollback(); } catch (Exception e) { e.printStackTrace(); }
            status = false;
        } finally {
            try {
                if (ps   != null) ps.close();
                if (conn != null) conn.close();
            } catch (Exception e) { e.printStackTrace(); }
        }

        return status;
    }

    // =========================================================================
    // ROW VALIDATION  (REFACTORED — column-name driven)
    // =========================================================================

    /**
     * Soft-validate a single row against expected column types.
     *
     * CHANGED signature: record is now Map<String,String> instead of String[].
     * CHANGED: values fetched via record.get(col) instead of record[i].
     *
     * Returns false on first type mismatch (non-blocking — caller decides
     * whether to skip or insert the row).
     */
    private boolean validateRow(Map<String, String> record,
                                String[] directColumns,
                                HashMap<String, String> columnTypes) {

        for (String col : directColumns) {

            String colKey = col.toLowerCase();

            // CHANGED: column-name lookup, no index math
            String val  = record.get(colKey);
            String type = columnTypes.get(colKey);

            // Skip nulls and blanks — handled downstream by setParameterByType
            if (val == null || val.trim().isEmpty() || val.equalsIgnoreCase("NULL")) {
                continue;
            }

            // Skip if type unknown (can't validate without type info)
            if (type == null) {
                continue;
            }

            val = val.trim();

            try {

                if (type.contains("int")) {
                    if (!val.matches("-?\\d+")) {
                        pu.sop("ROW_VALIDATION", "Invalid INT  | col=" + colKey + " | val=" + val);
                        return false;
                    }
                } else if (type.contains("numeric") || type.contains("decimal")) {
                    if (!val.matches("-?\\d+(\\.\\d+)?")) {
                        pu.sop("ROW_VALIDATION", "Invalid DECIMAL | col=" + colKey + " | val=" + val);
                        return false;
                    }
                } else if (type.contains("date")) {
                    if (val.equals("0")) {
                        continue; // treated as NULL downstream
                    }
                    if (!val.matches("\\d{4}-\\d{2}-\\d{2}")) {
                        pu.sop("ROW_VALIDATION", "Invalid DATE | col=" + colKey + " | val=" + val);
                        return false;
                    }
                } else if (type.contains("timestamp")) {
                    if (!val.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
                        pu.sop("ROW_VALIDATION", "Invalid TIMESTAMP | col=" + colKey + " | val=" + val);
                        return false;
                    }
                }

            } catch (Exception e) {
                pu.sop("ROW_VALIDATION", "Exception validating col=" + colKey + ": " + e.getMessage());
                return false;
            }
        }

        return true;
    }

    // =========================================================================
    // PIPE-CSV PARSER  (UNCHANGED LOGIC — kept here for completeness)
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
                // Escaped double-quote: "" inside a quoted field → single "
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    sb.append('"');
                    i++; // skip the second quote
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

        fields.add(cleanValue(sb.toString())); // last field

        return fields.toArray(new String[0]);
    }

    /**
     * Trim and normalise a single cell value.
     *  - null input       → null
     *  - blank string     → null
     *  - "NULL" (any case)→ null
     *  - otherwise        → trimmed value
     */
    private String cleanValue(String val) {
        if (val == null) return null;
        val = val.trim();
        if (val.isEmpty() || val.equalsIgnoreCase("NULL")) return null;
        return val;
    }

    // =========================================================================
    // ARGUMENT PARSERS  (UNCHANGED)
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
    // TABLE / COLUMN METADATA HELPERS  (UNCHANGED)
    // =========================================================================

    private boolean validateTableExists(String tableName, String dbURL, String dbUser, String dbPass) {

        Connection conn   = null;
        boolean    exists = false;

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

            pu.sop("validateTableExists",
                "Checking schema=" + schemaName + " table=" + actualTableName);

            String sql = "SELECT 1 FROM information_schema.tables "
                       + "WHERE table_schema = ? AND table_name = ?";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                ps.setString(2, actualTableName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        exists = true;
                        pu.sop("validateTableExists", "Table FOUND");
                    } else {
                        pu.sop("validateTableExists", "Table NOT FOUND");
                    }
                }
            }

        } catch (Exception ex) {
            pu.sop("validateTableExists", "EXCEPTION: " + ex.toString());
            ex.printStackTrace();
        } finally {
            try { if (conn != null) conn.close(); } catch (Exception e) { e.printStackTrace(); }
        }

        return exists;
    }

    private HashSet<String> getTableColumns(String tableName, String dbURL, String dbUser, String dbPass) {

        Connection        conn    = null;
        HashSet<String>   columns = new HashSet<>();

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
            DatabaseMetaData meta = conn.getMetaData();

            String schemaName     = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            pu.sop("getTableColumns", "schema=" + schemaName + " table=" + actualTableName);

            try (ResultSet rs = meta.getColumns(null, schemaName, actualTableName, null)) {
                while (rs.next()) {
                    String colName = rs.getString("COLUMN_NAME");
                    columns.add(colName.toLowerCase());
                    pu.sop("getTableColumns", "  col: " + colName);
                }
            }

        } catch (Exception ex) {
            System.out.println("EXCEPTION in getTableColumns: " + ex.toString());
            ex.printStackTrace();
            columns = null;
        } finally {
            try { if (conn != null) conn.close(); } catch (Exception e) { e.printStackTrace(); }
        }

        return columns;
    }

    private HashMap<String, String> getDestinationColumnTypes(String tableName,
            String[] directColumns, String[] metaColumns, Connection conn) {

        HashMap<String, String> columnTypes = new HashMap<>();

        try {
            DatabaseMetaData meta = conn.getMetaData();

            String schemaName     = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            String[] allColumns = new String[directColumns.length + metaColumns.length];
            System.arraycopy(directColumns, 0, allColumns, 0, directColumns.length);
            System.arraycopy(metaColumns,   0, allColumns, directColumns.length, metaColumns.length);

            for (String colName : allColumns) {
                try (ResultSet rs = meta.getColumns(null, schemaName, actualTableName, colName.toLowerCase())) {
                    if (rs.next()) {
                        String dataType = rs.getString("TYPE_NAME");
                        columnTypes.put(colName.toLowerCase(), dataType);
                        pu.sop("getDestinationColumnTypes", "col=" + colName + " type=" + dataType);
                    }
                }
            }

        } catch (Exception ex) {
            System.out.println("EXCEPTION in getDestinationColumnTypes: " + ex.toString());
            ex.printStackTrace();
            return null;
        }

        return columnTypes;
    }

    // =========================================================================
    // TYPE-SAFE PARAMETER SETTER  (UNCHANGED)
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
            pu.sop("TYPE_FIX", "Parse failed '" + val + "' → NULL for col=" + columnName
                   + " type=" + columnType);
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
                    pu.sop("validateDataTypes", "WARNING: meta col '" + col + "' type '" + destType
                           + "' is not text/jsonb");
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            valid = false;
        } finally {
            try { if (conn != null) conn.close(); } catch (Exception e) { e.printStackTrace(); }
        }

        return valid;
    }

    private String getColumnType(DatabaseMetaData meta, String tableName, String columnName)
            throws Exception {

        String schemaName     = null;
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
}
