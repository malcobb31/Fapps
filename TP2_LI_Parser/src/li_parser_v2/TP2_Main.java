package li_parser_v2;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * TP2_Main - Configuration-Driven CSV Routing Engine (TP2)
 *
 * Flow per active config row:
 *  1. Scan source_path for CSV files.
 *  2. Sort files by embedded timestamp (natural sort, earliest first).
 *  3. For each file:
 *     a. Split into *_L.csv and *_I.csv by flag column.
 *     b. Determine destination table(s) from destination_table_name.
 *     c. Call TP1 (metadata_parser_process.parseAndTransferData) for each split.
 *     d. Move master file to destination_path ONLY if BOTH splits succeed.
 *
 * Command-line arguments (positional):
 *  1  mysqlURL
 *  2  mysqlUser
 *  3  mysqlPass
 *  4  configTable          (e.g. metadata_parser_config)
 *  5  pgURL
 *  6  pgUser
 *  7  pgPass
 *  8  tmId                 (filter: only rows matching this tm_id, e.g. "TP2")
 *  9  flagColumnName       (name of the flag column in CSV, default "flag")
 * 10  splitOutputDir       (optional: directory to write split CSVs; default = source_path)
 */
public class TP2_Main {

    // ========================================================================
    // CONSTANTS
    // ========================================================================

    private static final String PROCESS_VERSION  = "1.0";
    private static final String DEFAULT_FLAG_COL = "flag";

    // ========================================================================
    // DEPENDENCIES
    // ========================================================================

    private static final TP2_Logger     logger   = new TP2_Logger();
    private static final PrintUtilMsg   pu       = new PrintUtilMsg();
    private static final TP2_FileSorter sorter   = new TP2_FileSorter();
    private static final TP2_CSVSplitter splitter = new TP2_CSVSplitter();

    // ========================================================================
    // ENTRY POINT
    // ========================================================================

    public static void main(String[] args) {

        logger.info(TP2_Logger.EVT_PROCESS_START,
            "TP2 CSV Routing Engine starting. Version: " + PROCESS_VERSION);

        if (args.length < 8) {
            pu.sop("TP2_MAIN", "Insufficient arguments. Required (in order):");
            pu.sop("TP2_MAIN", "  1. MySQL URL");
            pu.sop("TP2_MAIN", "  2. MySQL Username");
            pu.sop("TP2_MAIN", "  3. MySQL Password");
            pu.sop("TP2_MAIN", "  4. Config Table Name");
            pu.sop("TP2_MAIN", "  5. PostgreSQL URL");
            pu.sop("TP2_MAIN", "  6. PostgreSQL Username");
            pu.sop("TP2_MAIN", "  7. PostgreSQL Password");
            pu.sop("TP2_MAIN", "  8. tm_id filter (e.g. TP2)");
            pu.sop("TP2_MAIN", "  9. (Optional) Flag column name   [default: flag]");
            pu.sop("TP2_MAIN", " 10. (Optional) Split output dir   [default: source_path]");
            logger.error(TP2_Logger.EVT_ERROR, "Process aborted — insufficient arguments");
            return;
        }

        String mysqlURL      = args[0];
        String mysqlUser     = args[1];
        String mysqlPass     = args[2];
        String configTable   = args[3];
        String pgURL         = args[4];
        String pgUser        = args[5];
        String pgPass        = args[6];
        String tmId          = args[7];
        String flagColName   = (args.length >= 9  && !args[8].isEmpty()) ? args[8] : DEFAULT_FLAG_COL;
        String splitOutDir   = (args.length >= 10 && !args[9].isEmpty()) ? args[9] : null;

        // ---- Log resolved arguments ----
        pu.sop("TP2_MAIN", "======== TP2 ARGUMENTS ========");
        pu.sop("TP2_MAIN", "MySQL URL       : " + mysqlURL);
        pu.sop("TP2_MAIN", "MySQL User      : " + mysqlUser);
        pu.sop("TP2_MAIN", "Config Table    : " + configTable);
        pu.sop("TP2_MAIN", "PG URL          : " + pgURL);
        pu.sop("TP2_MAIN", "PG User         : " + pgUser);
        pu.sop("TP2_MAIN", "tm_id filter    : " + tmId);
        pu.sop("TP2_MAIN", "Flag Column     : " + flagColName);
        pu.sop("TP2_MAIN", "Split Out Dir   : " + (splitOutDir != null ? splitOutDir : "<same as source_path>"));
        pu.sop("TP2_MAIN", "================================");

        try {
            processAllConfigs(
                mysqlURL, mysqlUser, mysqlPass, configTable,
                pgURL, pgUser, pgPass,
                tmId, flagColName, splitOutDir
            );
        } catch (Exception e) {
            logger.error(TP2_Logger.EVT_ERROR, "FATAL: Unhandled exception in TP2_Main.main: " + e.getMessage());
            e.printStackTrace();
        }

        logger.info(TP2_Logger.EVT_PROCESS_START, "TP2 processing completed.");
    }

    // ========================================================================
    // CONFIG LOOP
    // ========================================================================

    private static void processAllConfigs(
            String mysqlURL, String mysqlUser, String mysqlPass, String configTable,
            String pgURL, String pgUser, String pgPass,
            String tmId, String flagColName, String splitOutDirOverride) {

        Connection mysqlConn = null;

        try {
            mysqlConn = new MySQLConnection().getConnection(mysqlURL, mysqlUser, mysqlPass);

            if (mysqlConn == null) {
                logger.error(TP2_Logger.EVT_ERROR, "Cannot connect to MySQL config database");
                return;
            }

            String query =
                "SELECT source_path, destination_table_name, destination_path, parameters " +
                "FROM " + configTable +
                " WHERE tm_id = ? AND is_active = 1";

            PreparedStatement ps = mysqlConn.prepareStatement(query);
            ps.setString(1, tmId);

            pu.sop("TP2_MAIN", "Config query: " + query + " [tm_id=" + tmId + "]");

            ResultSet rs = ps.executeQuery();

            int configCount = 0;

            while (rs.next()) {
                configCount++;
                String sourcePath  = rs.getString("source_path");
                String destTables  = rs.getString("destination_table_name");
                String destPath    = rs.getString("destination_path");
                String parameters  = rs.getString("parameters");

                pu.sop("TP2_MAIN", "--- CONFIG ROW " + configCount + " ---");
                pu.sop("TP2_MAIN", "source_path             : " + sourcePath);
                pu.sop("TP2_MAIN", "destination_table_name  : " + destTables);
                pu.sop("TP2_MAIN", "destination_path        : " + destPath);

                if (sourcePath == null || destTables == null || destPath == null || parameters == null
                        || parameters.trim().isEmpty()) {
                    logger.warn(TP2_Logger.EVT_ERROR,
                        "Config row " + configCount + " is incomplete — skipping");
                    continue;
                }

                // Resolve split output directory
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
                logger.warn(TP2_Logger.EVT_PROCESS_START,
                    "No active config rows found for tm_id=" + tmId);
            }

        } catch (Exception e) {
            logger.error(TP2_Logger.EVT_ERROR, "Error reading config: " + e.getMessage());
            e.printStackTrace();
        } finally {
            try { if (mysqlConn != null) mysqlConn.close(); } catch (Exception ignored) {}
        }
    }

    // ========================================================================
    // PER-CONFIG-ROW PROCESSING
    // ========================================================================

    private static void processConfigRow(
            String sourcePath, String destTables, String destPath, String parameters,
            String pgURL, String pgUser, String pgPass,
            String flagColName, File splitOutDir) {

        // ---- Parse parameters field ----
        String[] parts         = parameters.split("\\|", -1);
        String directColumns   = parts.length > 0 ? parts[0].trim() : "";
        int    metadataCount   = parts.length > 1 ? safeParseInt(parts[1], 0) : 0;
        String destMetaCols    = parts.length > 2 ? parts[2].trim() : "";
        String sourceMetaCols  = parts.length > 3 ? parts[3].trim() : "";
        String jsonKeys        = parts.length > 4 ? parts[4].trim() : "";

        // ---- Determine table routing ----
        String[] tables = destTables.split(",", -1);
        String tableForL;
        String tableForI;

        if (tables.length == 1) {
            // Single table: both L and I go to the same table
            tableForL = tables[0].trim();
            tableForI = tables[0].trim();
            logger.info(TP2_Logger.EVT_ROUTING,
                "Single-table routing. Both L and I → " + tableForL);
        } else {
            // Two tables: L → first, I → second
            tableForL = tables[0].trim();
            tableForI = tables[1].trim();
            logger.info(TP2_Logger.EVT_ROUTING,
                "Dual-table routing. L → " + tableForL + " | I → " + tableForI);
        }

        // ---- Scan source directory ----
        File sourceDir = new File(sourcePath);
        if (!sourceDir.exists() || !sourceDir.isDirectory()) {
            logger.error(TP2_Logger.EVT_ERROR, "Source path invalid or not a directory: " + sourcePath);
            return;
        }

        File[] csvFiles = sourceDir.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

        if (csvFiles == null || csvFiles.length == 0) {
            logger.warn(TP2_Logger.EVT_FILE_DETECTED, "No CSV files found in: " + sourcePath);
            return;
        }

        // ---- Natural sort: earliest timestamp first ----
        sorter.sortByTimestamp(csvFiles);

        logger.info(TP2_Logger.EVT_FILE_DETECTED,
            "Found " + csvFiles.length + " CSV file(s) in " + sourcePath);

        // ---- Process each file ----
        for (File masterFile : csvFiles) {

            logger.info(TP2_Logger.EVT_FILE_DETECTED,
                "Processing file: " + masterFile.getName()
                + " | size=" + masterFile.length() + " bytes");

            processSingleFile(
                masterFile, tableForL, tableForI, destPath,
                directColumns, metadataCount, destMetaCols, sourceMetaCols, jsonKeys,
                pgURL, pgUser, pgPass,
                flagColName, splitOutDir
            );
        }
    }

    // ========================================================================
    // PER-FILE PROCESSING
    // ========================================================================

    private static void processSingleFile(
            File masterFile,
            String tableForL, String tableForI,
            String destPath,
            String directColumns, int metadataCount,
            String destMetaCols, String sourceMetaCols, String jsonKeys,
            String pgURL, String pgUser, String pgPass,
            String flagColName, File splitOutDir) {

        String fileName = masterFile.getName();
        TP2_CSVSplitter.SplitResult splitResult = null;

        // ---- STEP 1: Split the CSV ----
        try {
            splitResult = splitter.split(masterFile, flagColName, splitOutDir);
        } catch (TP2_CSVSplitter.SplitException se) {
            logger.error(TP2_Logger.EVT_CSV_SPLIT,
                "Split failed for " + fileName + ": " + se.getMessage());
            logger.warn(TP2_Logger.EVT_FILE_NOT_MOVED,
                "Master file NOT moved due to split failure: " + fileName);
            return;
        }

        File lFile = splitResult.lFile;
        File iFile = splitResult.iFile;

        logger.info(TP2_Logger.EVT_CSV_SPLIT,
            "Split completed for " + fileName
            + " | L=" + splitResult.lRowCount
            + " | I=" + splitResult.iRowCount
            + " | skipped=" + splitResult.skippedRowCount);

        // ---- STEP 2: Call TP1 for L split ----
        boolean lSuccess = callTP1(
            pgURL, pgUser, pgPass,
            lFile.getAbsolutePath(),
            tableForL,
            directColumns, metadataCount, destMetaCols, sourceMetaCols, jsonKeys,
            "L", fileName
        );

        // ---- STEP 3: Call TP1 for I split ----
        boolean iSuccess = callTP1(
            pgURL, pgUser, pgPass,
            iFile.getAbsolutePath(),
            tableForI,
            directColumns, metadataCount, destMetaCols, sourceMetaCols, jsonKeys,
            "I", fileName
        );

        // ---- STEP 4: Move master file only if BOTH succeeded ----
        if (lSuccess && iSuccess) {
            moveFile(masterFile, destPath);
        } else {
            logger.warn(TP2_Logger.EVT_FILE_NOT_MOVED,
                "Master file NOT moved — L_success=" + lSuccess + " I_success=" + iSuccess
                + " | file=" + fileName);
        }

        // ---- Cleanup split files ----
        if (lSuccess && iSuccess) {
            deleteSilently(lFile);
            deleteSilently(iFile);
        }
    }

    // ========================================================================
    // TP1 DELEGATION
    // ========================================================================

    /**
     * Delegates a single split file to TP1 (metadata_parser_process).
     *
     * @param splitLabel "L" or "I" — used only for logging.
     * @param masterFileName Original master filename — used only for logging.
     * @return true if TP1 returned success, false otherwise.
     */
    private static boolean callTP1(
            String pgURL, String pgUser, String pgPass,
            String splitFilePath, String destTable,
            String directColumns, int metadataCount,
            String destMetaCols, String sourceMetaCols, String jsonKeys,
            String splitLabel, String masterFileName) {

        logger.info(TP2_Logger.EVT_TP1_CALL,
            "Calling TP1 for " + splitLabel + " split of " + masterFileName
            + " → table=" + destTable);

        boolean result;

        try {
            result = new metadata_parser_process().parseAndTransferData(
                pgURL, pgUser, pgPass,
                splitFilePath,
                destTable,
                directColumns,
                metadataCount,
                destMetaCols,
                sourceMetaCols,
                jsonKeys
            );
        } catch (Exception e) {
            logger.error(TP2_Logger.EVT_INSERT_FAILURE,
                "TP1 threw exception for " + splitLabel + " split of " + masterFileName
                + ": " + e.getMessage());
            return false;
        }

        if (result) {
            logger.info(TP2_Logger.EVT_INSERT_SUCCESS,
                "TP1 succeeded for " + splitLabel + " split of " + masterFileName
                + " → table=" + destTable);
        } else {
            logger.error(TP2_Logger.EVT_INSERT_FAILURE,
                "TP1 failed for " + splitLabel + " split of " + masterFileName
                + " → table=" + destTable);
        }

        return result;
    }

    // ========================================================================
    // FILE MOVE
    // ========================================================================

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

            logger.info(TP2_Logger.EVT_FILE_MOVED,
                "Master file moved to " + destPath + " | file=" + masterFile.getName());

        } catch (Exception e) {
            logger.error(TP2_Logger.EVT_ERROR,
                "Failed to move master file " + masterFile.getName()
                + " to " + destPath + ": " + e.getMessage());
        }
    }

    // ========================================================================
    // UTILITIES
    // ========================================================================

    private static int safeParseInt(String s, int defaultValue) {
        try {
            return Integer.parseInt(s.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @SuppressWarnings("unused")
    private static void deleteSilently(File f) {
        try {
            if (f != null && f.exists()) {
                f.delete();
            }
        } catch (Exception ignored) {}
    }
}
