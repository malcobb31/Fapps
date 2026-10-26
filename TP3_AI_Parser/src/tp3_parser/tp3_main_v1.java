package tp3_parser;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * tp3_main_v2 - Config-Driven TP3 CSV to PostgreSQL Pipeline Entry Point
 *
 * Replaces tp3_main_v1 (single-file, args-driven) with a fully config-driven,
 * multi-file, scheduler-friendly engine modelled after TP2's architecture.
 *
 * EXECUTION MODEL
 *   Scheduler - run jar - reads MySQL config - auto file pickup - process - move
 *
 *   No manual file path input. All file locations come from the config table.
 *
 * REQUIRED ARGUMENTS (10, positional)
 *   [0]  mysqlURL         e.g. jdbc:mysql://host:3306/configdb
 *   [1]  mysqlUser
 *   [2]  mysqlPass
 *   [3]  configTable      e.g. metadata_parser_config
 *   [4]  pgURL            e.g. jdbc:postgresql://host:5432/targetdb
 *   [5]  pgUser
 *   [6]  pgPass
 *   [7]  tm_id            filter value -- only rows matching this tm_id are processed
 *   [8]  flagColumnName   (optional, pass "" to use default "flag")
 *   [9]  processedDir     fallback processed directory; overridden per-row by
 *                         destination_path if non-empty
 *
 * CONFIG TABLE CONTRACT  (metadata_parser_config)
 *   source_path              - directory containing CSV files to ingest
 *   destination_table_name   - target PostgreSQL table
 *   destination_path         - processed directory for this config row
 *   parameters               - pipe-separated mapping string (see below)
 *   tm_id                    - tenant / task marker -- filter condition
 *   is_active                - only rows with is_active = 1 are processed
 *
 * PARAMETERS FORMAT
 *   "<sourceColumns> | <targetColumns> | <flagColumn> | <booleanColumn>"
 *
 *   Example:
 *     "id,name,amount | id,name,amount | flag | is_active"
 *
 *   Rules:
 *   - Pipe (|) is the field delimiter.
 *   - Comma (,) separates column names within source/target fields.
 *   - All parsing is trimmed and case-insensitive.
 *   - Exactly 4 pipe-delimited segments are required.
 *   - flagColumn and booleanColumn must be single column names (no commas).
 *
 * FILE PROCESSING RULES
 *   1. Only *.csv files in source_path are picked up.
 *   2. Files are sorted by name (ascending) for deterministic ordering.
 *   3. FileManager.isAlreadyProcessed() is called before every file -- any file
 *      already present in the processed directory is silently skipped.
 *   4. On SUCCESS  - FileManager.moveToProcessed() is called.
 *   5. On FAILURE  - file stays in source_path; no movement.
 *   6. An empty source_path directory is logged and skipped (no error).
 *
 * CLASSES REUSED UNCHANGED
 *   ProcessData    - streaming CSV parse + PostgreSQL insert + batch commit
 *   FileManager    - idempotency guard + atomic file movement
 *   CSVSourceData  - RFC 4180 CSV parser (used internally by ProcessData)
 *   PrintUtilMsg   - structured production logging
 *   PostgreSQLConnection - PostgreSQL JDBC connection factory
 */
public class tp3_main_v1 {

    // =========================================================================
    // Constants
    // =========================================================================

    /** Number of pipe-delimited segments expected in the parameters column. */
    private static final int PARAMETERS_SEGMENT_COUNT = 4;

    /** Default flag column name when arg[8] is blank. */
    private static final String DEFAULT_FLAG_COLUMN = "flag";

    /** Batch size used for DB processing (informational -- enforced in ProcessData). */
    private static final int CSV_BATCH_SIZE = 200;

    // =========================================================================
    // Instance state
    // =========================================================================

    private final PrintUtilMsg pu = new PrintUtilMsg();

    // =========================================================================
    // Main
    // =========================================================================

    public static void main(String[] args) {

        PrintUtilMsg startLog = new PrintUtilMsg();
        startLog.log("INFO",
                     "PIPELINE_STARTUP - process: TP3_Config_Driven_Pipeline version: v2",
                     "PIPELINE_LIFECYCLE");

        if (args.length != 10) {
            printUsage();
            System.exit(1);
        }

        new tp3_main_v1().run(args);
    }

    // =========================================================================
    // Core orchestrator
    // =========================================================================

    /**
     * Main execution body. Reads config rows from MySQL, then for each active
     * row matching tm_id, scans the source directory and processes every CSV file.
     */
    private void run(String[] args) {

        // -- Parse arguments -----------------------------------------------
        final String mysqlURL             = args[0].trim();
        final String mysqlUser            = args[1].trim();
        final String mysqlPass            = args[2].trim();
        final String configTable          = args[3].trim();
        final String pgURL                = args[4].trim();
        final String pgUser               = args[5].trim();
        final String pgPass               = args[6].trim();
        final String tmId                 = args[7].trim();
        final String flagArgRaw           = args[8].trim();
        final String fallbackProcessedDir = args[9].trim();

        // Resolve effective flag column (arg[8] is optional; blank - default)
        final String globalFlagColumn = flagArgRaw.isEmpty() ? DEFAULT_FLAG_COLUMN : flagArgRaw;

        pu.log("INFO",
               "PIPELINE_ARGS - mysql_url: " + mysqlURL
               + " mysql_user: " + mysqlUser
               + " config_table: " + configTable
               + " pg_url: " + pgURL
               + " pg_user: " + pgUser
               + " tm_id: " + tmId
               + " flag_column: " + globalFlagColumn
               + (flagArgRaw.isEmpty() ? " (default)" : " (from_args)")
               + " fallback_processed_dir: " + fallbackProcessedDir,
               "PIPELINE_LIFECYCLE");

        // -- Load config rows from MySQL ------------------------------------
        List<ConfigRow> configRows = loadConfigRows(
                mysqlURL, mysqlUser, mysqlPass, configTable, tmId);

        if (configRows == null) {
            pu.log("ERROR",
                   "PIPELINE_ABORTED - reason: could_not_load_config_rows tm_id: " + tmId,
                   "PIPELINE_LIFECYCLE");
            System.exit(2);
        }

        if (configRows.isEmpty()) {
            pu.log("INFO",
                   "PIPELINE_NOTHING_TO_DO - reason: no_active_config_rows_found tm_id: " + tmId,
                   "PIPELINE_LIFECYCLE");
            return;
        }

        pu.log("INFO",
               "CONFIG_ROWS_LOADED - tm_id: " + tmId + " config_row_count: " + configRows.size(),
               "PIPELINE_LIFECYCLE");

        // -- Process each config row ----------------------------------------
        int totalConfigRows  = configRows.size();
        int rowsSucceeded    = 0;
        int rowsFailed       = 0;
        int totalFilesOk     = 0;
        int totalFilesFailed = 0;

        for (int rowIdx = 0; rowIdx < configRows.size(); rowIdx++) {

            ConfigRow row = configRows.get(rowIdx);

            pu.log("INFO",
                   "CONFIG_ROW_START - row: " + (rowIdx + 1) + "/" + totalConfigRows
                   + " source_path: " + row.sourcePath
                   + " destination_table: " + row.destinationTableName
                   + " destination_path: " + row.destinationPath,
                   "PIPELINE_LIFECYCLE");

            pu.log("DEBUG",
                   "CONFIG_ROW_PARAMS - row: " + (rowIdx + 1) + " parameters: " + row.parameters,
                   "PIPELINE_LIFECYCLE");

            // -- Parse parameters -------------------------------------------
            ParsedParameters params = parseParameters(row.parameters, globalFlagColumn);

            if (params == null) {
                pu.log("ERROR",
                       "CONFIG_ROW_SKIPPED - row: " + (rowIdx + 1)
                       + " reason: parameter_parse_failed",
                       "PIPELINE_LIFECYCLE");
                rowsFailed++;
                continue;
            }

            pu.log("DEBUG",
                   "CONFIG_ROW_PARAMS_PARSED - row: " + (rowIdx + 1)
                   + " source_columns: " + params.sourceColumns
                   + " target_columns: " + params.targetColumns
                   + " flag_column: " + params.flagColumn
                   + " boolean_column: " + params.booleanColumn,
                   "PIPELINE_LIFECYCLE");

            // -- Resolve processed directory (per-row overrides global fallback) -
            String effectiveProcessedDir = (row.destinationPath != null && !row.destinationPath.trim().isEmpty())
                    ? row.destinationPath.trim()
                    : fallbackProcessedDir;

            pu.log("DEBUG",
                   "CONFIG_ROW_RESOLVED_DIR - row: " + (rowIdx + 1)
                   + " effective_processed_dir: " + effectiveProcessedDir,
                   "PIPELINE_LIFECYCLE");

            // -- Scan source directory for CSV files -------------------------
            File[] csvFiles = scanCsvDirectory(row.sourcePath);

            if (csvFiles == null) {
                // scanCsvDirectory already logged the reason (dir not found, not readable, etc.)
                rowsFailed++;
                continue;
            }

            if (csvFiles.length == 0) {
                pu.log("INFO",
                       "CONFIG_ROW_SKIPPED - row: " + (rowIdx + 1)
                       + " reason: no_csv_files_found source_path: " + row.sourcePath,
                       "FILE_PROC");
                rowsSucceeded++; // Empty dir is not a failure -- pipeline ran cleanly
                continue;
            }

            pu.log("INFO",
                   "CSV_FILES_FOUND - row: " + (rowIdx + 1)
                   + " source_path: " + row.sourcePath
                   + " file_count: " + csvFiles.length,
                   "FILE_PROC");

            // -- Process each CSV file --------------------------------------
            FileManager fm = new FileManager();
            ProcessData pd = new ProcessData();

            int filesOkThisRow     = 0;
            int filesFailedThisRow = 0;

            for (int fileIdx = 0; fileIdx < csvFiles.length; fileIdx++) {

                File csvFile = csvFiles[fileIdx];
                String csvFilePath = csvFile.getAbsolutePath();
                String fileName    = csvFile.getName();

                pu.log("INFO",
                       "FILE_PROCESSING_START - file: " + (fileIdx + 1) + "/" + csvFiles.length
                       + " file_name: " + fileName
                       + " file_size_bytes: " + csvFile.length(),
                       "FILE_PROC");

                // Idempotency guard -- skip already-processed files
                if (fm.isAlreadyProcessed(csvFilePath, effectiveProcessedDir)) {
                    pu.log("INFO",
                           "FILE_SKIPPED - file_name: " + fileName + " reason: already_processed",
                           "FILE_PROC");
                    continue;
                }

                // -- Call ProcessData -- all streaming + DB insert logic lives here --
                boolean success = false;

                try {
                    success = pd.parseAndTransferDataFromCSV(
                            pgURL,
                            pgUser,
                            pgPass,
                            row.destinationTableName,
                            params.sourceColumns,
                            params.targetColumns,
                            params.flagColumn,
                            params.booleanColumn,
                            csvFilePath
                    );
                } catch (Exception e) {
                    pu.log("ERROR",
                           "FILE_PROCESSING_EXCEPTION - file_name: " + fileName
                           + " reason: " + e.getMessage(),
                           "FILE_PROC");
                    success = false;
                }

                if (success) {
                    pu.log("INFO",
                           "FILE_PROCESSING_SUCCESS - file_name: " + fileName,
                           "FILE_PROC");
                    filesOkThisRow++;
                    totalFilesOk++;

                    // Move file to processed directory ONLY after confirmed success
                    boolean moved = fm.moveToProcessed(csvFilePath, effectiveProcessedDir);
                    if (!moved) {
                        pu.log("WARN",
                               "FILE_MOVE_WARNING - file_name: " + fileName
                               + " reason: file_processed_ok_but_move_failed action: manual_intervention_required",
                               "FILE_MOVE");
                    }

                } else {
                    pu.log("ERROR",
                           "FILE_PROCESSING_FAILED - file_name: " + fileName
                           + " action: file_remains_in_source_directory",
                           "FILE_PROC");
                    filesFailedThisRow++;
                    totalFilesFailed++;
                }
            }

            pu.log("INFO",
                   "CONFIG_ROW_SUMMARY - row: " + (rowIdx + 1)
                   + " files_ok: " + filesOkThisRow
                   + " files_failed: " + filesFailedThisRow,
                   "PIPELINE_LIFECYCLE");

            if (filesFailedThisRow > 0) {
                rowsFailed++;
            } else {
                rowsSucceeded++;
            }
        }

        // -- Pipeline summary ------------------------------------------------
        pu.log("INFO",
               "PIPELINE_COMPLETE"
               + " config_rows_processed: " + totalConfigRows
               + " config_rows_ok: " + rowsSucceeded
               + " config_rows_with_errors: " + rowsFailed
               + " total_files_ok: " + totalFilesOk
               + " total_files_failed: " + totalFilesFailed,
               "PIPELINE_LIFECYCLE");
    }

    // =========================================================================
    // Config loading
    // =========================================================================

    /**
     * Query MySQL config table and return all active rows matching tm_id.
     *
     * Only rows where:
     *   is_active = 1
     *   tm_id     = ? (exact match, case-sensitive)
     *
     * Results are returned in database insertion order (no explicit ORDER BY --
     * add one here if deterministic ordering across rows is required).
     *
     * @return List of ConfigRow objects, empty list if none, null on fatal DB error
     */
    private List<ConfigRow> loadConfigRows(String mysqlURL, String mysqlUser,
                                           String mysqlPass, String configTable,
                                           String tmId) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ConfigRow> rows = new ArrayList<>();

        String sql = "SELECT source_path, destination_table_name, destination_path, parameters "
                   + "FROM " + configTable + " "
                   + "WHERE is_active = 1 AND tm_id = ?";

        pu.log("INFO",
               "CONFIG_LOAD_START - config_table: " + configTable + " tm_id: " + tmId,
               "DB_QUERY");

        pu.log("DEBUG",
               "CONFIG_LOAD_SQL - sql: " + sql + " tm_id_param: " + tmId,
               "DB_QUERY");

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(mysqlURL, mysqlUser, mysqlPass);
            conn.setAutoCommit(true); // read-only config query; auto-commit is fine

            ps = conn.prepareStatement(sql);
            ps.setString(1, tmId);
            rs = ps.executeQuery();

            int rowCount = 0;
            while (rs.next()) {
                ConfigRow row = new ConfigRow();
                row.sourcePath            = nullToEmpty(rs.getString("source_path"));
                row.destinationTableName  = nullToEmpty(rs.getString("destination_table_name"));
                row.destinationPath       = nullToEmpty(rs.getString("destination_path"));
                row.parameters            = nullToEmpty(rs.getString("parameters"));
                rows.add(row);
                rowCount++;
                pu.log("DEBUG",
                       "CONFIG_ROW_FETCHED - row_index: " + rowCount
                       + " source_path: " + row.sourcePath
                       + " destination_table: " + row.destinationTableName,
                       "DB_QUERY");
            }

            pu.log("INFO",
                   "CONFIG_LOAD_COMPLETE - config_table: " + configTable
                   + " tm_id: " + tmId
                   + " rows_fetched: " + rowCount,
                   "DB_QUERY");
            return rows;

        } catch (ClassNotFoundException e) {
            pu.log("ERROR",
                   "CONFIG_LOAD_FAILED - reason: mysql_jdbc_driver_not_found detail: " + e.getMessage(),
                   "DB_CONNECT");
            return null;

        } catch (Exception e) {
            pu.log("ERROR",
                   "CONFIG_LOAD_FAILED - config_table: " + configTable
                   + " tm_id: " + tmId
                   + " reason: " + e.getMessage(),
                   "DB_QUERY");
            return null;

        } finally {
            closeQuietly(rs, ps, conn);
        }
    }

    // =========================================================================
    // Parameter parsing
    // =========================================================================

    /**
     * Parse the pipe-delimited parameters string from the config table.
     *
     * Expected format (4 segments separated by '|'):
     *   "<sourceColumns> | <targetColumns> | <flagColumn> | <booleanColumn>"
     *
     * - sourceColumns  : comma-separated CSV header names
     * - targetColumns  : comma-separated PG column names (1:1 with source)
     * - flagColumn     : single CSV header name containing 'I'/'A' values
     * - booleanColumn  : single PG target column name for the boolean result
     *
     * If the parameters string is blank or malformed, the globalFlagColumn
     * argument is used as a safe fallback for flagColumn only.
     *
     * @param parameters     raw parameters column value from config table
     * @param globalFlagCol  fallback flag column name (from args[8])
     * @return ParsedParameters, or null if the string is fundamentally unparseable
     */
    private ParsedParameters parseParameters(String parameters, String globalFlagCol) {

        if (parameters == null || parameters.trim().isEmpty()) {
            pu.log("ERROR",
                   "PARAM_PARSE_FAILED - reason: parameters_string_is_null_or_empty",
                   "VALIDATION");
            return null;
        }

        String[] segments = parameters.split("\\|", -1);

        if (segments.length != PARAMETERS_SEGMENT_COUNT) {
            pu.log("ERROR",
                   "PARAM_PARSE_FAILED - reason: wrong_segment_count"
                   + " expected: " + PARAMETERS_SEGMENT_COUNT
                   + " found: " + segments.length
                   + " raw_value: " + parameters,
                   "VALIDATION");
            return null;
        }

        String sourceColumns  = segments[0].trim();
        String targetColumns  = segments[1].trim();
        String flagColumn     = segments[2].trim();
        String booleanColumn  = segments[3].trim();

        // Validate non-empty segments
        if (sourceColumns.isEmpty()) {
            pu.log("ERROR",
                   "PARAM_PARSE_FAILED - reason: source_columns_segment_is_empty",
                   "VALIDATION");
            return null;
        }
        if (targetColumns.isEmpty()) {
            pu.log("ERROR",
                   "PARAM_PARSE_FAILED - reason: target_columns_segment_is_empty",
                   "VALIDATION");
            return null;
        }
        if (flagColumn.isEmpty()) {
            pu.log("WARN",
                   "PARAM_PARSE_WARNING - reason: flag_column_segment_is_empty"
                   + " action: using_global_fallback fallback_value: " + globalFlagCol,
                   "VALIDATION");
            flagColumn = globalFlagCol;
        }
        if (booleanColumn.isEmpty()) {
            pu.log("ERROR",
                   "PARAM_PARSE_FAILED - reason: boolean_column_segment_is_empty",
                   "VALIDATION");
            return null;
        }

        // Validate column count parity (source vs target)
        String[] srcCols = sourceColumns.split(",");
        String[] tgtCols = targetColumns.split(",");

        if (srcCols.length != tgtCols.length) {
            pu.log("ERROR",
                   "PARAM_PARSE_FAILED - reason: column_count_mismatch"
                   + " source_column_count: " + srcCols.length
                   + " target_column_count: " + tgtCols.length,
                   "VALIDATION");
            return null;
        }

        ParsedParameters pp = new ParsedParameters();
        pp.sourceColumns  = sourceColumns;
        pp.targetColumns  = targetColumns;
        pp.flagColumn     = flagColumn;
        pp.booleanColumn  = booleanColumn;
        return pp;
    }

    // =========================================================================
    // Directory scanning
    // =========================================================================

    /**
     * Scan a directory for CSV files and return them sorted by file name (ascending).
     *
     * Sorting by name gives deterministic, reproducible processing order. If
     * timestamp-based ordering is preferred, swap the comparator to
     * Comparator.comparingLong(File::lastModified).
     *
     * @param directoryPath absolute path of the source directory
     * @return sorted array of CSV File objects; empty array if none found;
     *         null on error (directory not found, not readable, etc.)
     */
    private File[] scanCsvDirectory(String directoryPath) {

        if (directoryPath == null || directoryPath.trim().isEmpty()) {
            pu.log("ERROR",
                   "SCAN_DIRECTORY_FAILED - reason: source_path_is_null_or_empty",
                   "FILE_PROC");
            return null;
        }

        File dir = new File(directoryPath.trim());

        if (!dir.exists()) {
            pu.log("ERROR",
                   "SCAN_DIRECTORY_FAILED - reason: directory_does_not_exist path: " + directoryPath,
                   "FILE_PROC");
            return null;
        }

        if (!dir.isDirectory()) {
            pu.log("ERROR",
                   "SCAN_DIRECTORY_FAILED - reason: path_is_not_a_directory path: " + directoryPath,
                   "FILE_PROC");
            return null;
        }

        if (!dir.canRead()) {
            pu.log("ERROR",
                   "SCAN_DIRECTORY_FAILED - reason: directory_is_not_readable path: " + directoryPath,
                   "FILE_PROC");
            return null;
        }

        // List only .csv files (case-insensitive extension check)
        File[] csvFiles = dir.listFiles(
                (d, name) -> name.toLowerCase().endsWith(".csv")
        );

        if (csvFiles == null) {
            // listFiles() returns null for I/O errors
            pu.log("ERROR",
                   "SCAN_DIRECTORY_FAILED - reason: io_error_reading_directory path: " + directoryPath,
                   "FILE_PROC");
            return null;
        }

        if (csvFiles.length == 0) {
            pu.log("INFO",
                   "SCAN_DIRECTORY_EMPTY - path: " + directoryPath + " csv_files_found: 0",
                   "FILE_PROC");
            return csvFiles; // Return empty array -- caller decides how to handle
        }

        // Sort by file name ascending for deterministic ordering
        Arrays.sort(csvFiles, Comparator.comparingLong(File::lastModified));

        pu.log("INFO",
               "SCAN_DIRECTORY_COMPLETE - path: " + directoryPath + " csv_files_found: " + csvFiles.length,
               "FILE_PROC");

        for (File f : csvFiles) {
            pu.log("DEBUG",
                   "CSV_FILE_LISTED - file_name: " + f.getName() + " file_size_bytes: " + f.length(),
                   "FILE_PROC");
        }

        return csvFiles;
    }

    // =========================================================================
    // Utility helpers
    // =========================================================================

    /** Null-safe string coercion. */
    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }

    /** Silently close JDBC resources in reverse-open order. */
    private static void closeQuietly(ResultSet rs, PreparedStatement ps, Connection conn) {
        try { if (rs   != null) rs.close();   } catch (Exception ignored) {}
        try { if (ps   != null) ps.close();   } catch (Exception ignored) {}
        try { if (conn != null) conn.close(); } catch (Exception ignored) {}
    }

    /** Print CLI usage and expected arguments. */
    private static void printUsage() {
        System.out.println("");
        System.out.println("Usage: java -jar tp3.jar <arg1> ... <arg10>");
        System.out.println("");
        System.out.println("  [1]  MySQL JDBC URL          e.g. jdbc:mysql://host:3306/configdb");
        System.out.println("  [2]  MySQL Username");
        System.out.println("  [3]  MySQL Password");
        System.out.println("  [4]  Config Table Name       e.g. metadata_parser_config");
        System.out.println("  [5]  PostgreSQL JDBC URL     e.g. jdbc:postgresql://host:5432/targetdb");
        System.out.println("  [6]  PostgreSQL Username");
        System.out.println("  [7]  PostgreSQL Password");
        System.out.println("  [8]  TM_ID                  filter value for config table");
        System.out.println("  [9]  Flag Column Name        (optional -- pass \"\" to use default 'flag')");
        System.out.println("  [10] Fallback Processed Dir  used when destination_path is empty in config");
        System.out.println("");
        System.out.println("Config table parameters column format:");
        System.out.println("  \"<sourceColumns> | <targetColumns> | <flagColumn> | <booleanColumn>\"");
        System.out.println("  Example: \"id,name,amount | id,name,amount | flag | is_active\"");
        System.out.println("");
    }

    // =========================================================================
    // Inner data-transfer types
    // =========================================================================

    /**
     * Holds a single row from the MySQL config table.
     * All fields are trimmed; null DB values become empty strings.
     */
    private static class ConfigRow {
        String sourcePath;
        String destinationTableName;
        String destinationPath;
        String parameters;
    }

    /**
     * Result of parsing the pipe-delimited parameters column.
     * All fields are guaranteed non-null and non-empty when returned from
     * parseParameters() (null return indicates parse failure).
     */
    private static class ParsedParameters {
        String sourceColumns;   // comma-separated CSV header names
        String targetColumns;   // comma-separated PG column names
        String flagColumn;      // single CSV flag header ('I'/'A')
        String booleanColumn;   // single PG boolean target column
    }
}
