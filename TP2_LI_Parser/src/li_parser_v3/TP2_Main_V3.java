package li_parser_v3;

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

public class TP2_Main_V3 {

    // =========================================================================
    // CONSTANTS
    // =========================================================================

    private static final String PROCESS_VERSION  = "1.0";
    private static final String DEFAULT_FLAG_COL = "flag";

    // =========================================================================
    // LOGGING INFRASTRUCTURE
    // =========================================================================

    private static final String PROCESS_NAME = "TP2";
    private static final boolean debugEnabled = false;
    private static final DateTimeFormatter LOG_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String SERVER_IP;
    static {
        String ip;
        try {
            ip = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            ip = "UNKNOWN";
        }
        SERVER_IP = ip;
    }

    // NOTE: log() is static because all methods in this class are static.
    private static void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(LOG_FORMAT);

        StringBuilder logBuilder = new StringBuilder(512);
        logBuilder.append("Time:").append(timestamp)
                  .append(" [").append(capitalize(level)).append("] ")
                  .append("Process_Name: ").append(PROCESS_NAME).append(" ");

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
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    // =========================================================================
    // DEPENDENCIES
    // =========================================================================

    private static final TP2_FileSorter  sorter   = new TP2_FileSorter();
    private static final TP2_CSVSplitter splitter = new TP2_CSVSplitter();

    // =========================================================================
    // PARSED PARAMS  (one block = one set of transformation rules for one flag)
    // =========================================================================

    /**
     * Holds the five fields that describe how one CSV split should be
     * transformed and inserted. A config row produces 1 or 2 of these:
     *  - 1 block  - same params applied to both L and I splits
     *  - 2 blocks - blocks[0] - L split, blocks[1] - I split
     */
    static class ParsedParams {

        final String directColumns;   // comma-separated dest column names
        final int    metadataCount;   // 0 = direct-only; 1+ = hybrid
        final String destMetaCols;    // comma-separated dest JSON column names
        final String sourceMetaCols;  // hash-separated groups of source column names
        final String jsonKeys;        // hash-separated groups of JSON key names

        ParsedParams(String directColumns, int metadataCount,
                     String destMetaCols, String sourceMetaCols, String jsonKeys) {
            this.directColumns  = directColumns  != null ? directColumns  : "";
            this.metadataCount  = metadataCount;
            this.destMetaCols   = destMetaCols   != null ? destMetaCols   : "";
            this.sourceMetaCols = sourceMetaCols != null ? sourceMetaCols : "";
            this.jsonKeys       = jsonKeys       != null ? jsonKeys       : "";
        }

        @Override
        public String toString() {
            return "ParsedParams{"
                + "direct="    + directColumns
                + ", metaCnt=" + metadataCount
                + ", destMeta="  + destMetaCols
                + ", srcMeta="   + sourceMetaCols
                + ", jsonKeys="  + jsonKeys
                + "}";
        }
    }

    // =========================================================================
    // ENTRY POINT
    // =========================================================================

    public static void main(String[] args) {

        log("INFO",
            "TP2 CSV Routing Engine starting. Version: " + PROCESS_VERSION,
            SERVER_IP, TP2_Logger.EVT_PROCESS_START);

        if (args.length < 8) {
            log("ERROR",
                "Insufficient arguments. Required 8 minimum: "
                + "[1] MySQL URL, [2] MySQL Username, [3] MySQL Password, "
                + "[4] Config Table Name, [5] PostgreSQL URL, [6] PostgreSQL Username, "
                + "[7] PostgreSQL Password, [8] tm_id filter. "
                + "Optional: [9] Flag column name (default: flag), "
                + "[10] Split output dir (default: source_path/split_tmp). "
                + "Received: " + args.length + " argument(s). Process aborted.",
                SERVER_IP, TP2_Logger.EVT_PROCESS_START);
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
            "TP2 startup arguments resolved."
            + " MySQL_URL: " + mysqlURL
            + " | MySQL_User: " + mysqlUser
            + " | Config_Table: " + configTable
            + " | PG_URL: " + pgURL
            + " | PG_User: " + pgUser
            + " | tm_id: " + tmId
            + " | Flag_Column: " + flagColName
            + " | Split_Out_Dir: " + (splitOutDir != null ? splitOutDir : "<source_path/split_tmp>"),
            SERVER_IP, TP2_Logger.EVT_PROCESS_START);

        try {
            processAllConfigs(
                mysqlURL, mysqlUser, mysqlPass, configTable,
                pgURL, pgUser, pgPass,
                tmId, flagColName, splitOutDir
            );
        } catch (Exception e) {
            log("ERROR",
                "FATAL: Unhandled exception in TP2_Main.main. Cause: " + e.getMessage(),
                SERVER_IP, TP2_Logger.EVT_ERROR);
        }

        log("INFO",
            "TP2 processing completed.",
            SERVER_IP, TP2_Logger.EVT_PROCESS_START);
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
                log("ERROR",
                    "Cannot connect to MySQL config database. URL: " + mysqlURL,
                    SERVER_IP, TP2_Logger.EVT_ERROR);
                return;
            }

            String query =
                "SELECT source_path, destination_table_name, destination_path, parameters " +
                "FROM " + configTable +
                " WHERE tm_id = ? AND is_active = 1";

            PreparedStatement ps = mysqlConn.prepareStatement(query);
            ps.setString(1, tmId);

            log("DEBUG",
                "Executing config query on table: " + configTable + " | tm_id filter: " + tmId,
                SERVER_IP, TP2_Logger.EVT_PROCESS_START);

            ResultSet rs  = ps.executeQuery();
            int configCount = 0;

            while (rs.next()) {
                configCount++;

                String sourcePath = rs.getString("source_path");
                String destTables = rs.getString("destination_table_name");
                String destPath   = rs.getString("destination_path");
                String parameters = rs.getString("parameters");

                log("INFO",
                    "Config row " + configCount + " loaded."
                    + " source_path: " + sourcePath
                    + " | destination_table_name: " + destTables
                    + " | destination_path: " + destPath
                    + " | parameters length: " + (parameters != null ? parameters.length() : 0) + " chars",
                    SERVER_IP, TP2_Logger.EVT_PROCESS_START);

                if (sourcePath == null || destTables == null || destPath == null
                        || parameters == null || parameters.trim().isEmpty()) {
                    log("WARN",
                        "Config row " + configCount + " is incomplete — one or more required fields are null or empty. Skipping.",
                        SERVER_IP, TP2_Logger.EVT_ERROR);
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
                    "No active config rows found for tm_id: " + tmId
                    + " in table: " + configTable,
                    SERVER_IP, TP2_Logger.EVT_PROCESS_START);
            } else {
                log("INFO",
                    "Config loop complete. Total config rows processed: " + configCount
                    + " | tm_id: " + tmId,
                    SERVER_IP, TP2_Logger.EVT_PROCESS_START);
            }

        } catch (Exception e) {
            log("ERROR",
                "Error reading config from table: " + configTable
                + " | tm_id: " + tmId
                + " | Cause: " + e.getMessage(),
                SERVER_IP, TP2_Logger.EVT_ERROR);
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

        // ---- STEP 1: Parse parameter blocks ----------------------------------
        // Returns 1 block - same params for both L and I.
        // Returns 2 blocks - blocks[0] = L params, blocks[1] = I params.
        ParsedParams[] paramBlocks = parseMultiBlockParameters(parameters);

        if (paramBlocks == null || paramBlocks.length == 0) {
            log("ERROR",
                "Failed to parse parameters — skipping config row."
                + " raw parameters length: " + parameters.length() + " chars",
                SERVER_IP, TP2_Logger.EVT_ERROR);
            return;
        }

        ParsedParams lParams = paramBlocks[0];
        ParsedParams iParams = (paramBlocks.length > 1) ? paramBlocks[1] : paramBlocks[0];

        boolean sameBlock = (paramBlocks.length == 1);
        log("INFO",
            "Parameter blocks parsed. Mode: " + (sameBlock ? "single-block (shared for L and I)" : "multi-block (separate L and I)")
            + " | L params: " + lParams
            + " | I params: " + iParams,
            SERVER_IP, TP2_Logger.EVT_ROUTING);

        // ---- STEP 2: Determine table routing ---------------------------------
        String[] tables = destTables.split(",", -1);
        String tableForL;
        String tableForI;

        if (tables.length == 1) {
            tableForL = tables[0].trim();
            tableForI = tables[0].trim();
            log("INFO",
                "Single-table routing: both L and I splits route to table: " + tableForL,
                SERVER_IP, TP2_Logger.EVT_ROUTING);
        } else {
            tableForL = tables[0].trim();
            tableForI = tables[1].trim();
            log("INFO",
                "Dual-table routing: L split routes to: " + tableForL
                + " | I split routes to: " + tableForI,
                SERVER_IP, TP2_Logger.EVT_ROUTING);
        }

        // ---- STEP 3: Scan source directory -----------------------------------
        File sourceDir = new File(sourcePath);
        if (!sourceDir.exists() || !sourceDir.isDirectory()) {
            log("ERROR",
                "Source path is invalid or not a directory: " + sourcePath,
                SERVER_IP, TP2_Logger.EVT_ERROR);
            return;
        }

        File[] csvFiles = sourceDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (csvFiles == null || csvFiles.length == 0) {
            log("WARN",
                "No CSV files found in source directory: " + sourcePath,
                SERVER_IP, TP2_Logger.EVT_FILE_DETECTED);
            return;
        }

        // ---- Sort: earliest timestamp first ----------------------------------
        sorter.sortByTimestamp(csvFiles);

        log("INFO",
            "Found " + csvFiles.length + " CSV file(s) in: " + sourcePath
            + " | Sorted by timestamp ascending.",
            SERVER_IP, TP2_Logger.EVT_FILE_DETECTED);

        // ---- Process each file -----------------------------------------------
        for (File masterFile : csvFiles) {
            log("INFO",
                "Starting processing for file: " + masterFile.getName()
                + " | size: " + masterFile.length() + " bytes"
                + " | path: " + masterFile.getAbsolutePath(),
                SERVER_IP, TP2_Logger.EVT_FILE_DETECTED);

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

        // ---- STEP 1: Split CSV -----------------------------------------------
        TP2_CSVSplitter.SplitResult splitResult;

        try {
            splitResult = splitter.split(masterFile, flagColName, splitOutDir);
        } catch (TP2_CSVSplitter.SplitException se) {
            log("ERROR",
                "Split failed for file: " + fileName + " | Cause: " + se.getMessage(),
                SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);
            log("WARN",
                "Master file will NOT be moved due to split failure. File: " + fileName,
                SERVER_IP, TP2_Logger.EVT_FILE_NOT_MOVED);
            return;
        }

        File lFile = splitResult.lFile;
        File iFile = splitResult.iFile;

        log("INFO",
            "Split result for: " + fileName
            + " | L rows: " + splitResult.lRowCount
            + " | I rows: " + splitResult.iRowCount
            + " | skipped rows: " + splitResult.skippedRowCount
            + " | L file: " + lFile.getName()
            + " | I file: " + iFile.getName(),
            SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);

        // ---- STEP 2: TP1 for L split (using L-specific params) ---------------
        boolean lSuccess = callTP1(
            pgURL, pgUser, pgPass,
            lFile.getAbsolutePath(),
            tableForL,
            lParams,
            "L", fileName
        );

        // ---- STEP 3: TP1 for I split (using I-specific params) ---------------
        boolean iSuccess = callTP1(
            pgURL, pgUser, pgPass,
            iFile.getAbsolutePath(),
            tableForI,
            iParams,
            "I", fileName
        );

        // ---- STEP 4: Move master file ONLY if both splits succeeded ----------
        if (lSuccess && iSuccess) {
            moveFile(masterFile, destPath);
            deleteSilently(lFile);
            deleteSilently(iFile);
        } else {
            log("WARN",
                "Master file will NOT be moved — one or more splits failed."
                + " L_success: " + lSuccess
                + " | I_success: " + iSuccess
                + " | file: " + fileName,
                SERVER_IP, TP2_Logger.EVT_FILE_NOT_MOVED);
        }
    }

    // =========================================================================
    // TP1 DELEGATION
    // =========================================================================

    /**
     * Call TP1 (metadata_parser_process) for one split file using the
     * ParsedParams block that belongs to that split's flag type.
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
            "Invoking TP1 for split label: " + splitLabel
            + " | master file: " + masterFileName
            + " | split file: " + splitFilePath
            + " | destination table: " + destTable
            + " | directCols: " + params.directColumns
            + " | metaCount: " + params.metadataCount,
            SERVER_IP, TP2_Logger.EVT_TP1_CALL);

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
                params.jsonKeys
            );
        } catch (Exception e) {
            log("ERROR",
                "TP1 threw an exception for split label: " + splitLabel
                + " | master file: " + masterFileName
                + " | destination table: " + destTable
                + " | Cause: " + e.getMessage(),
                SERVER_IP, TP2_Logger.EVT_INSERT_FAILURE);
            return false;
        }

        if (result) {
            log("INFO",
                "TP1 completed successfully for split label: " + splitLabel
                + " | master file: " + masterFileName
                + " | destination table: " + destTable,
                SERVER_IP, TP2_Logger.EVT_INSERT_SUCCESS);
        } else {
            log("ERROR",
                "TP1 reported failure for split label: " + splitLabel
                + " | master file: " + masterFileName
                + " | destination table: " + destTable,
                SERVER_IP, TP2_Logger.EVT_INSERT_FAILURE);
        }

        return result;
    }

    // =========================================================================
    // MULTI-BLOCK PARAMETER PARSING  (STEP 5 IMPLEMENTATION)
    // =========================================================================

    /**
     * Entry point: detect format and dispatch to the correct parser.
     *
     * Format A - Single-block (legacy pipe format):
     *   directCols|metaCount|destMetaCols|sourceMetaCols|jsonKeys
     *   - ParsedParams[1]; same block applied to both L and I.
     *
     * Format B - Multi-block (new brace format):
     *   {block_for_L},{block_for_I}
     *   - ParsedParams[2]; [0] - L, [1] - I.
     *   If only one brace block is present, ParsedParams[1] is returned.
     *
     * @param parameters Raw value from the config table.
     * @return Array of 1 or 2 ParsedParams, or null on failure.
     */
    static ParsedParams[] parseMultiBlockParameters(String parameters) {

        if (parameters == null || parameters.trim().isEmpty()) {
            log("ERROR",
                "parseMultiBlockParameters: parameters string is null or empty.",
                SERVER_IP, TP2_Logger.EVT_ERROR);
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

            // Skip separators between blocks: comma, space, tab
            if (c == ',' || c == ' ' || c == '\t') {
                i++;
                continue;
            }

            if (c == '{') {
                // Find matching closing brace using depth tracking
                int depth = 0;
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
                        "parseMultiBlockBraceFormat: Unmatched opening brace at index " + i
                        + " in parameter string of length: " + input.length(),
                        SERVER_IP, TP2_Logger.EVT_ERROR);
                    return null;
                }

                String content = input.substring(contentStart, closePos).trim();
                blockContents.add(content);
                i = closePos + 1;   // advance past '}'

            } else {
                // Unexpected character outside braces - log and skip
                log("WARN",
                    "parseMultiBlockBraceFormat: Unexpected character [" + c + "] at index " + i
                    + " outside brace block — skipping character.",
                    SERVER_IP, TP2_Logger.EVT_ERROR);
                i++;
            }
        }

        if (blockContents.isEmpty()) {
            log("ERROR",
                "parseMultiBlockBraceFormat: No valid brace blocks found in parameter string.",
                SERVER_IP, TP2_Logger.EVT_ERROR);
            return null;
        }

        if (blockContents.size() > 2) {
            log("WARN",
                "parseMultiBlockBraceFormat: Found " + blockContents.size()
                + " brace blocks — only the first 2 will be used.",
                SERVER_IP, TP2_Logger.EVT_ERROR);
        }

        List<ParsedParams> result = new ArrayList<>();

        for (int b = 0; b < Math.min(blockContents.size(), 2); b++) {
            String content = blockContents.get(b);
            ParsedParams p = parseParameterBlock(content);

            if (p == null) {
                log("ERROR",
                    "parseMultiBlockBraceFormat: Failed to parse block[" + b + "]."
                    + " Content length: " + content.length() + " chars.",
                    SERVER_IP, TP2_Logger.EVT_ERROR);
                return null;
            }

            result.add(p);
            log("DEBUG",
                "parseMultiBlockBraceFormat: Block[" + b + "] parsed successfully. Result: " + p,
                SERVER_IP, TP2_Logger.EVT_ROUTING);
        }

        return result.toArray(new ParsedParams[0]);
    }

    /**
     * Parse a single pipe-delimited parameter string into a ParsedParams.
     *
     * Format:
     *   directCols | metaCount | destMetaCols | sourceMetaCols | jsonKeys
     *   [0]          [1]         [2]             [3]              [4]
     *
     * Fields [2]-[4] are optional and default to empty string (metadata_flag=0 case).
     * Trailing pipes are required for fields to be absent, e.g. "colA,colB|0|||"
     * or simply "colA,colB|0" - both parse cleanly.
     *
     * @param block Raw block content, without surrounding braces.
     * @return ParsedParams, or null if the string is null/empty.
     */
    static ParsedParams parseParameterBlock(String block) {

        if (block == null || block.trim().isEmpty()) {
            log("ERROR",
                "parseParameterBlock: block string is null or empty.",
                SERVER_IP, TP2_Logger.EVT_ERROR);
            return null;
        }

        // -1 limit preserves trailing empty tokens (e.g. "a|0|||" gives ["a","0","","",""])
        String[] parts = block.split("\\|", -1);

        String directColumns  = parts.length > 0 ? parts[0].trim() : "";
        int    metadataCount  = parts.length > 1 ? safeParseInt(parts[1], 0) : 0;
        String destMetaCols   = parts.length > 2 ? parts[2].trim() : "";
        String sourceMetaCols = parts.length > 3 ? parts[3].trim() : "";
        String jsonKeys       = parts.length > 4 ? parts[4].trim() : "";

        if (directColumns.isEmpty()) {
            log("WARN",
                "parseParameterBlock: directColumns is empty — this will produce an empty INSERT."
                + " block length: " + block.length() + " chars.",
                SERVER_IP, TP2_Logger.EVT_ERROR);
        }

        if (metadataCount > 0 && (destMetaCols.isEmpty() || sourceMetaCols.isEmpty() || jsonKeys.isEmpty())) {
            log("WARN",
                "parseParameterBlock: metadataCount=" + metadataCount
                + " but one or more of destMetaCols, sourceMetaCols, or jsonKeys is empty."
                + " destMetaCols_empty: " + destMetaCols.isEmpty()
                + " | sourceMetaCols_empty: " + sourceMetaCols.isEmpty()
                + " | jsonKeys_empty: " + jsonKeys.isEmpty(),
                SERVER_IP, TP2_Logger.EVT_ERROR);
        }

        return new ParsedParams(directColumns, metadataCount, destMetaCols, sourceMetaCols, jsonKeys);
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
                "Master file moved successfully."
                + " File: " + masterFile.getName()
                + " | Destination: " + destPath,
                SERVER_IP, TP2_Logger.EVT_FILE_MOVED);

        } catch (Exception e) {
            log("ERROR",
                "Failed to move master file."
                + " File: " + masterFile.getName()
                + " | Destination: " + destPath
                + " | Cause: " + e.getMessage(),
                SERVER_IP, TP2_Logger.EVT_ERROR);
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
}
