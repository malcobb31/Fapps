package li_update_v1;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


public class TP2_update_main_V1 {

    // =========================================================================
    // CONSTANTS
    // =========================================================================

    private static final String PROCESS_VERSION  = "1.0";
    private static final String DEFAULT_FLAG_COL = "flag";

    // =========================================================================
    // LOGGING INFRASTRUCTURE
    // =========================================================================

    private static final boolean           debugEnabled   = true;
    private static final String            processName    = "TP2";
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // =========================================================================
    // DEPENDENCIES
    // =========================================================================

    private static final TP2_FileSorter  sorter   = new TP2_FileSorter();
    private static final TP2_CSVSplitter splitter = new TP2_CSVSplitter();

    // =========================================================================
    // PARSED PARAMS  (one block = one set of transformation rules for one flag)
    // =========================================================================

    /**
     * Holds the six fields that describe how one CSV split should be
     * transformed and updated. A config row produces 1 or 2 of these:
     *  - 1 block   same params applied to both L and I splits
     *  - 2 blocks  blocks[0]  L split, blocks[1]  I split
     *
     * CHANGED: Added pkMapping field (field index [5] in the pipe-delimited block).
     */
    static class ParsedParams {

        final String directColumns;   // comma-separated dest column names
        final int    metadataCount;   // 0 = direct-only; 1+ = hybrid
        final String destMetaCols;    // comma-separated dest JSON column names
        final String sourceMetaCols;  // hash-separated groups of source column names
        final String jsonKeys;        // hash-separated groups of JSON key names
        final String pkMapping;       // NEW: "csvPk1,csvPk2 | psqlPk1,psqlPk2"

        ParsedParams(String directColumns, int metadataCount,
                     String destMetaCols, String sourceMetaCols, String jsonKeys,
                     String pkMapping) {
            this.directColumns  = directColumns  != null ? directColumns  : "";
            this.metadataCount  = metadataCount;
            this.destMetaCols   = destMetaCols   != null ? destMetaCols   : "";
            this.sourceMetaCols = sourceMetaCols != null ? sourceMetaCols : "";
            this.jsonKeys       = jsonKeys       != null ? jsonKeys       : "";
            this.pkMapping      = pkMapping      != null ? pkMapping      : "";
        }

        @Override
        public String toString() {
            return "ParsedParams{"
                + "direct="    + directColumns
                + ", metaCnt=" + metadataCount
                + ", destMeta="  + destMetaCols
                + ", srcMeta="   + sourceMetaCols
                + ", jsonKeys="  + jsonKeys
                + ", pkMapping=" + pkMapping
                + "}";
        }
    }

    // =========================================================================
    // ENTRY POINT
    // =========================================================================

    public static void main(String[] args) {

        log("INFO",
            "TP2 CSV Routing Engine starting. Version: " + PROCESS_VERSION,
            null, "PROCESS_START");

        if (args.length < 8) {
            log("ERROR",
                "Insufficient arguments provided. Required: MySQL URL, MySQL Username, MySQL Password, "
                + "Config Table Name, PostgreSQL URL, PostgreSQL Username, PostgreSQL Password, tm_id filter. "
                + "Optional: Flag column name (default: flag), Split output dir (default: source_path/split_tmp)",
                null, "PROCESS_START");
            return;
        }

        String mysqlURL    = args[0];
        String mysqlUser   = args[1];
        String mysqlPass   = args[2];
        String configTable = args[3];
        String pgURL       = args[4];
        String pgUser      = args[5];
        String pgPass      = args[6];
        String tmId        = args[7];
        String flagColName = (args.length >= 9  && !args[8].isEmpty()) ? args[8] : DEFAULT_FLAG_COL;
        String splitOutDir = (args.length >= 10 && !args[9].isEmpty()) ? args[9] : null;

        log("INFO",
            "TP2 startup parameters - MySQL URL: " + mysqlURL
            + " | MySQL User: " + mysqlUser
            + " | Config Table: " + configTable
            + " | PG URL: " + pgURL
            + " | PG User: " + pgUser
            + " | tm_id: " + tmId
            + " | Flag Column: " + flagColName
            + " | Split Out Dir: " + (splitOutDir != null ? splitOutDir : "<source_path/split_tmp>"),
            null, "PROCESS_START");

        try {
            processAllConfigs(
                mysqlURL, mysqlUser, mysqlPass, configTable,
                pgURL, pgUser, pgPass,
                tmId, flagColName, splitOutDir
            );
        } catch (Exception e) {
            log("ERROR",
                "FATAL: Unhandled exception in TP2 main: " + e.getMessage(),
                null, "ERROR");
        }

        log("INFO", "TP2 processing completed.", null, "PROCESS_START");
    }

    // =========================================================================
    // CONFIG LOOP
    // =========================================================================

    private static void processAllConfigs(
            String mysqlURL, String mysqlUser, String mysqlPass, String configTable,
            String pgURL, String pgUser, String pgPass,
            String tmId, String flagColName, String splitOutDirOverride) {

        Connection mysqlConn = null;

        try {
            mysqlConn = new MySQLConnection().getConnection(mysqlURL, mysqlUser, mysqlPass);

            if (mysqlConn == null) {
                log("ERROR", "Cannot connect to MySQL config database", null, "ERROR");
                return;
            }

            String query =
                "SELECT source_path, destination_table_name, destination_path, parameters " +
                "FROM " + configTable +
                " WHERE tm_id = ? AND is_active = 1";

            PreparedStatement ps = mysqlConn.prepareStatement(query);
            ps.setString(1, tmId);

            log("DEBUG",
                "Executing config query on table: " + configTable + " | tm_id=" + tmId,
                null, "PROCESS_START");

            ResultSet rs        = ps.executeQuery();
            int       configCount = 0;

            while (rs.next()) {
                configCount++;

                String sourcePath = rs.getString("source_path");
                String destTables = rs.getString("destination_table_name");
                String destPath   = rs.getString("destination_path");
                String parameters = rs.getString("parameters");

                log("INFO",
                    "Config row " + configCount + " loaded"
                    + " | source_path: " + sourcePath
                    + " | destination_table: " + destTables
                    + " | destination_path: " + destPath,
                    null, "PROCESS_START");

                if (sourcePath == null || destTables == null || destPath == null
                        || parameters == null || parameters.trim().isEmpty()) {
                    log("WARN",
                        "Config row " + configCount + " is incomplete - skipping."
                        + " source_path=" + sourcePath
                        + " | dest_table=" + destTables
                        + " | dest_path=" + destPath
                        + " | parameters=" + parameters,
                        null, "ERROR");
                    continue;
                }

                File splitOutDir = (splitOutDirOverride != null)
                    ? new File(splitOutDirOverride)
                    : new File(sourcePath, "split_tmp");

                processConfigRow(
                    sourcePath, destTables, destPath, parameters,
                    pgURL, pgUser, pgPass,
                    flagColName, splitOutDir
                );
            }

            rs.close();
            ps.close();

            if (configCount == 0) {
                log("WARN",
                    "No active config rows found for tm_id=" + tmId,
                    null, "PROCESS_START");
            } else {
                log("INFO",
                    "Config processing complete. Total config rows processed: " + configCount,
                    null, "PROCESS_START");
            }

        } catch (Exception e) {
            log("ERROR",
                "Error reading config table " + configTable + ": " + e.getMessage(),
                null, "ERROR");
        } finally {
            try { if (mysqlConn != null) mysqlConn.close(); } catch (Exception ignored) {}
        }
    }

    // =========================================================================
    // PER-CONFIG-ROW PROCESSING
    // =========================================================================

    private static void processConfigRow(
            String sourcePath, String destTables, String destPath, String parameters,
            String pgURL, String pgUser, String pgPass,
            String flagColName, File splitOutDir) {

        // ---- STEP 1: Parse parameter blocks
        ParsedParams[] paramBlocks = parseMultiBlockParameters(parameters);

        if (paramBlocks == null || paramBlocks.length == 0) {
            log("ERROR",
                "Failed to parse parameters - skipping config row. raw=" + parameters,
                null, "ERROR");
            return;
        }

        ParsedParams lParams = paramBlocks[0];
        ParsedParams iParams = (paramBlocks.length > 1) ? paramBlocks[1] : paramBlocks[0];

        boolean sameBlock = (paramBlocks.length == 1);
        log("DEBUG",
            "Parameter mode: " + (sameBlock ? "single-block (shared)" : "multi-block (L/I separate)")
            + " | L params: " + lParams
            + " | I params: " + iParams,
            null, "PROCESS_START");

        // ---- STEP 2: Determine table routing
        String[] tables = destTables.split(",", -1);
        String tableForL;
        String tableForI;

        if (tables.length == 1) {
            tableForL = tables[0].trim();
            tableForI = tables[0].trim();
            log("INFO",
                "Single-table routing: both L and I rows will be routed to table=" + tableForL,
                null, "ROUTING_DECISION");
        } else {
            tableForL = tables[0].trim();
            tableForI = tables[1].trim();
            log("INFO",
                "Dual-table routing: L rows to table=" + tableForL + " | I rows to table=" + tableForI,
                null, "ROUTING_DECISION");
        }

        // ---- STEP 3: Scan source directory
        File sourceDir = new File(sourcePath);
        if (!sourceDir.exists() || !sourceDir.isDirectory()) {
            log("ERROR",
                "Source path is invalid or not a directory: " + sourcePath,
                null, "ERROR");
            return;
        }

        File[] csvFiles = sourceDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (csvFiles == null || csvFiles.length == 0) {
            log("WARN",
                "No CSV files found in source directory: " + sourcePath,
                null, "FILE_DETECTED");
            return;
        }

        sorter.sortByTimestamp(csvFiles);

        log("INFO",
            "Found " + csvFiles.length + " CSV file(s) in " + sourcePath + " - processing in timestamp order",
            null, "FILE_DETECTED");

        for (File masterFile : csvFiles) {
            log("INFO",
                "Processing file: " + masterFile.getName()
                + " | size=" + masterFile.length() + " bytes",
                null, "FILE_DETECTED");

            processSingleFile(
                masterFile, tableForL, tableForI, destPath,
                lParams, iParams,
                pgURL, pgUser, pgPass,
                flagColName, splitOutDir
            );
        }
    }

    // =========================================================================
    // PER-FILE PROCESSING
    // =========================================================================

    private static void processSingleFile(
            File masterFile,
            String tableForL, String tableForI,
            String destPath,
            ParsedParams lParams, ParsedParams iParams,
            String pgURL, String pgUser, String pgPass,
            String flagColName, File splitOutDir) {

        String fileName = masterFile.getName();

        // ---- STEP 1: Split CSV
        TP2_CSVSplitter.SplitResult splitResult;

        try {
            splitResult = splitter.split(masterFile, flagColName, splitOutDir);
        } catch (TP2_CSVSplitter.SplitException se) {
            log("ERROR",
                "Split failed for file=" + fileName + " - " + se.getMessage(),
                null, "CSV_SPLIT");
            log("WARN",
                "Master file NOT moved due to split failure: " + fileName,
                null, "FILE_NOT_MOVED");
            return;
        }

        File lFile = splitResult.lFile;
        File iFile = splitResult.iFile;

        log("INFO",
            "Split complete: file=" + fileName
            + " | L rows=" + splitResult.lRowCount
            + " | I rows=" + splitResult.iRowCount
            + " | skipped=" + splitResult.skippedRowCount,
            null, "CSV_SPLIT");

        // ---- STEP 2: TP1 for L split
        boolean lSuccess = callTP1(
            pgURL, pgUser, pgPass,
            lFile.getAbsolutePath(),
            tableForL,
            lParams,
            "L", fileName
        );

        boolean iSuccess = callTP1(
            pgURL, pgUser, pgPass,
            iFile.getAbsolutePath(),
            tableForI,
            iParams,
            "I", fileName
        );

        if (lSuccess && iSuccess) {
            moveFile(masterFile, destPath);
            deleteSilently(lFile);
            deleteSilently(iFile);
        } else {
            log("WARN",
                "Master file NOT moved - L_success=" + lSuccess
                + " | I_success=" + iSuccess
                + " | file=" + fileName,
                null, "FILE_NOT_MOVED");
        }
    }

    // =========================================================================
    // TP1 DELEGATION
    // =========================================================================

    /**
     * Call TP1 (metadata_parser_process) for one split file using the
     * ParsedParams block that belongs to that split's flag type.
     *
     * CHANGED: passes params.pkMapping as the new 11th argument to
     *          parseAndTransferData().
     *
     * @param splitLabel     "L" or "I" - for logging only.
     * @param masterFileName Original master filename - for logging only.
     * @return true if TP1 reported success, false otherwise.
     */
    private static boolean callTP1(
            String pgURL, String pgUser, String pgPass,
            String splitFilePath, String destTable,
            ParsedParams params,
            String splitLabel, String masterFileName) {

        log("INFO",
            "Calling TP1 for split=" + splitLabel + " of file=" + masterFileName
            + " | dest_table=" + destTable
            + " | directCols=" + params.directColumns
            + " | metaCount=" + params.metadataCount
            + " | pkMapping=" + params.pkMapping,
            null, "TP1_CALL");

        boolean result;

        try {
            result = new metadata_parser_process().parseAndTransferData(
                pgURL, pgUser, pgPass,
                splitFilePath,
                destTable,
                params.directColumns,
                params.metadataCount,
                params.destMetaCols,
                params.sourceMetaCols,
                params.jsonKeys,
                params.pkMapping      // required for UPDATE WHERE clause
            );
        } catch (Exception e) {
            log("ERROR",
                "TP1 threw exception for split=" + splitLabel + " of file=" + masterFileName
                + " - " + e.getMessage(),
                null, "UPDATE_FAILURE");
            return false;
        }

        if (result) {
            log("INFO",
                "TP1 succeeded for split=" + splitLabel + " of file=" + masterFileName
                + " | dest_table=" + destTable,
                null, "UPDATE_SUCCESS");
        } else {
            log("ERROR",
                "TP1 failed for split=" + splitLabel + " of file=" + masterFileName
                + " | dest_table=" + destTable,
                null, "UPDATE_FAILURE");
        }

        return result;
    }

    // =========================================================================
    // MULTI-BLOCK PARAMETER PARSING
    // =========================================================================

    static ParsedParams[] parseMultiBlockParameters(String parameters) {

        if (parameters == null || parameters.trim().isEmpty()) {
            log("ERROR",
                "Parameters string is null or empty - cannot parse",
                null, "ERROR");
            return null;
        }

        String trimmed = parameters.trim();

        if (trimmed.startsWith("{")) {
            return parseMultiBlockBraceFormat(trimmed);
        } else {
            ParsedParams single = parseParameterBlock(trimmed);
            return (single != null) ? new ParsedParams[]{ single } : null;
        }
    }

    /**
     * Parse a brace-delimited string into one or two parameter blocks.
     *
     * Strategy: scan for balanced brace pairs to extract block content,
     * then parse each block as a pipe-delimited parameter string.
     * A comma (or whitespace) between closing '}' and next '{' is treated
     * as a separator and skipped.
     *
     * Supports nested braces inside block content (depth-tracked).
     *
     * @param input Trimmed parameter string that starts with '{'.
     * @return ParsedParams array (length 1 or 2), or null on parse failure.
     */
    private static ParsedParams[] parseMultiBlockBraceFormat(String input) {

        List<String> blockContents = new ArrayList<>();
        int i = 0;

        while (i < input.length()) {
            char c = input.charAt(i);

            if (c == ',' || c == ' ' || c == '\t') {
                i++;
                continue;
            }

            if (c == '{') {
                int depth        = 0;
                int contentStart = i + 1;
                int closePos     = -1;

                for (int j = i; j < input.length(); j++) {
                    if      (input.charAt(j) == '{') depth++;
                    else if (input.charAt(j) == '}') {
                        depth--;
                        if (depth == 0) { closePos = j; break; }
                    }
                }

                if (closePos == -1) {
                    log("ERROR",
                        "Unmatched opening brace at index " + i + " in parameter string: " + input,
                        null, "ERROR");
                    return null;
                }

                String content = input.substring(contentStart, closePos).trim();
                blockContents.add(content);
                i = closePos + 1;

            } else {
                log("WARN",
                    "Unexpected character '" + c + "' at index " + i + " in parameter string - skipping",
                    null, "ERROR");
                i++;
            }
        }

        if (blockContents.isEmpty()) {
            log("ERROR",
                "No brace-delimited blocks found in parameter string: " + input,
                null, "ERROR");
            return null;
        }

        if (blockContents.size() > 2) {
            log("WARN",
                blockContents.size() + " parameter blocks found - only the first 2 will be used",
                null, "ERROR");
        }

        List<ParsedParams> result = new ArrayList<>();

        for (int b = 0; b < Math.min(blockContents.size(), 2); b++) {
            String content = blockContents.get(b);
            ParsedParams p = parseParameterBlock(content);

            if (p == null) {
                log("ERROR",
                    "Failed to parse parameter block[" + b + "]: " + content,
                    null, "ERROR");
                return null;
            }

            result.add(p);
            log("DEBUG",
                "Parameter block[" + b + "] parsed successfully: " + p,
                null, "PROCESS_START");
        }

        return result.toArray(new ParsedParams[0]);
    }

    /**
     * Parse a single pipe-delimited parameter string into a ParsedParams.
     *
     * Format:
     *   directCols | metaCount | destMetaCols | sourceMetaCols | jsonKeys | pkMapping
     *   [0]          [1]         [2]             [3]              [4]        [5]
     *
     * Fields [2]-[4] are optional (metadata_flag=0 case).
     * Field  [5] pkMapping is required for UPDATE mode; an absent or blank value
     * is stored as-is and will cause a fail-fast error in parseAndTransferData.
     *
     * @param block Raw block content, without surrounding braces.
     * @return ParsedParams, or null if the string is null/empty.
     */
    static ParsedParams parseParameterBlock(String block) {

        if (block == null || block.trim().isEmpty()) {
            log("ERROR",
                "Parameter block is null or empty - cannot parse",
                null, "ERROR");
            return null;
        }

        // Limit=6: fields [0]-[4] consume the first 5 pipes; field [5] (pkMapping)
        // absorbs the rest of the string including the '|' inside "csvPk | psqlPk".
        // Using -1 here was the bug: it split pkMapping's own '|' into parts[5] and
        // parts[6], so parts[5] only captured the CSV-side and the SQL-side was lost.
        String[] parts = block.split("\\|", 6);

        String directColumns  = parts.length > 0 ? parts[0].trim() : "";
        int    metadataCount  = parts.length > 1 ? safeParseInt(parts[1], 0) : 0;
        String destMetaCols   = parts.length > 2 ? parts[2].trim() : "";
        String sourceMetaCols = parts.length > 3 ? parts[3].trim() : "";
        String jsonKeys       = parts.length > 4 ? parts[4].trim() : "";
        // NEW: field [5] - pkMapping
        String pkMapping      = parts.length > 5 ? parts[5].trim() : "";

        if (directColumns.isEmpty()) {
            log("WARN",
                "directColumns is empty in parameter block - this will produce an empty UPDATE. block=[" + block + "]",
                null, "ERROR");
        }

        if (metadataCount > 0 && (destMetaCols.isEmpty() || sourceMetaCols.isEmpty() || jsonKeys.isEmpty())) {
            log("WARN",
                "metadataCount=" + metadataCount
                + " but one or more of destMetaCols/sourceMetaCols/jsonKeys is empty. block=[" + block + "]",
                null, "ERROR");
        }

        if (pkMapping.isEmpty()) {
            log("WARN",
                "pkMapping is empty in parameter block - parseAndTransferData will reject this. block=[" + block + "]",
                null, "ERROR");
        }

        return new ParsedParams(directColumns, metadataCount, destMetaCols, sourceMetaCols, jsonKeys, pkMapping);
    }

    // =========================================================================
    // FILE MOVE
    // =========================================================================

    private static void moveFile(File masterFile, String destPath) {
        try {
            Path targetDir = Paths.get(destPath);

            if (!Files.exists(targetDir)) {
                Files.createDirectories(targetDir);
            }

            Files.move(
                masterFile.toPath(),
                targetDir.resolve(masterFile.getName()),
                StandardCopyOption.REPLACE_EXISTING
            );

            log("INFO",
                "Master file moved successfully - file=" + masterFile.getName()
                + " | dest=" + destPath,
                null, "FILE_MOVED");

        } catch (Exception e) {
            log("ERROR",
                "Failed to move file=" + masterFile.getName()
                + " to dest=" + destPath + " - " + e.getMessage(),
                null, "ERROR");
        }
    }

    // =========================================================================
    // UTILITIES
    // =========================================================================

    private static int safeParseInt(String s, int defaultValue) {
        if (s == null || s.trim().isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static void deleteSilently(File f) {
        try {
            if (f != null && f.exists()) f.delete();
        } catch (Exception ignored) {}
    }

    // =========================================================================
    // LOGGING
    // =========================================================================

    private static void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp)
                .append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(processName).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(PROCESS_VERSION)
                .append(" Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                .append(" Description: ").append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }
}
