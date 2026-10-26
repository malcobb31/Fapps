package tp4_parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ProcessData - TP4 ETL orchestration.
 *
 * Parameters column format (combined):
 *   <srcRegiCols,srcCorrCols> | <dstCols> || <tp1DirectCols> | <tp1MetaCount> | <tp1DestMetaCols> | <tp1SrcMetaCols> | <tp1JsonKeys>
 *
 * The "||" boundary separates the TP4 address-split spec from the TP1 metadata spec.
 * If "||" is absent, the TP1 spec defaults to all intermediate CSV columns as direct
 * columns, metadataCount=0, no JSON metadata.
 *
 * Pipeline per CSV file:
 *   Stage 1 : Read source CSV  split each row into Regi + Corr  write intermediate CSV
 *   Stage 2 : TP1 metadata flow on intermediate CSV (port of metadata_parser_process)  insert into PostgreSQL
 *   On success : move intermediate CSV to destination_path, delete original source CSV
 *   On failure : delete intermediate CSV (if exists), leave original source CSV for retry
 */
public class ProcessData {

    // =========================================================================
    // Constants
    // =========================================================================

    private static final String  ADD_TYPE_COLUMN    = "add_type";
    private static final String  SAME_AS_CORR_COLUMN = "same_as_correspondence_address";
    private static final String  ADD_TYPE_REGI       = "Regi";
    private static final String  ADD_TYPE_CORR       = "Corr";
    private static final String  INTERMEDIATE_PREFIX = "tp4_out_";

    private static final int     TP1_BATCH_SIZE      = 200;

    private static final String  PROCESS_NAME    = "TP4_ADDRESS_PARSER";
    private static final String  PROCESS_VERSION = "1.0";
    private static final boolean DEBUG_ENABLED   = true;

    private static final DateTimeFormatter LOG_FORMAT =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    // =========================================================================
    // Inner DTOs
    // =========================================================================

    /** TP4-specific address-split specification parsed from parameters column. */
    private static class Tp4Spec {
        String[] srcRegiCols;  // first half of source address columns
        String[] srcCorrCols;  // second half of source address columns
        String[] dstCols;      // shared destination address column names
    }

    /** Full combined spec (TP4 split + TP1 metadata). */
    private static class CombinedSpec {
        Tp4Spec tp4;
        String  tp1DirectCols;    // null/empty  derive from intermediate CSV header
        int     tp1MetaCount;
        String  tp1DestMetaCols;
        String  tp1SrcMetaCols;
        String  tp1JsonKeys;
    }

    // =========================================================================
    // Entry point
    // =========================================================================

    public boolean run(String mysqlUrl, String mysqlUser, String mysqlPass,
                       String configTable,
                       String pgUrl, String pgUser, String pgPass,
                       String tmId, String flagColumn) {

        long startTime = System.currentTimeMillis();
        log("INFO", "=== TP4 Parser PROCESS START === tmId=" + tmId
                + " | flagColumn=" + flagColumn, null, "PROCESS_START");

        if (!validateMysqlConnection(mysqlUrl, mysqlUser, mysqlPass)) {
            log("ERROR", "MySQL connection validation FAILED. Aborting.", null, "VALIDATION_FAIL");
            return false;
        }
        if (!validatePgConnection(pgUrl, pgUser, pgPass)) {
            log("ERROR", "PostgreSQL connection validation FAILED. Aborting.", null, "VALIDATION_FAIL");
            return false;
        }

        List<SourceData.ConfigRow> configs =
                new SourceData().fetchConfigRows(mysqlUrl, mysqlUser, mysqlPass, configTable, tmId);

        if (configs == null || configs.isEmpty()) {
            log("ERROR", "No config rows found for tm_id=" + tmId, null, "CONFIG_EMPTY");
            return false;
        }
        log("INFO", "Config rows loaded: " + configs.size() + " | tm_id=" + tmId, null, "CONFIG_LOADED");

        boolean overallSuccess = true;
        int successCount = 0;
        int failCount    = 0;

        for (SourceData.ConfigRow cfg : configs) {
            log("INFO", "===== CONFIG START | sno=" + cfg.sno
                    + " | destTable=" + cfg.destinationTableName + " =====", null, "CONFIG_START");

            boolean ok = processConfig(cfg, pgUrl, pgUser, pgPass, flagColumn);

            if (ok) {
                successCount++;
                log("INFO", "CONFIG SUCCESS | sno=" + cfg.sno, null, "CONFIG_SUCCESS");
            } else {
                failCount++;
                overallSuccess = false;
                log("ERROR", "CONFIG FAILED  | sno=" + cfg.sno, null, "CONFIG_FAIL");
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        log("INFO", "=== TP4 Parser PROCESS END === elapsed=" + elapsed + "ms"
                + " | configSuccess=" + successCount + " | configFailed=" + failCount
                + " | overallSuccess=" + overallSuccess, null, "PROCESS_END");

        return overallSuccess;
    }

    // =========================================================================
    // Process one config row
    // =========================================================================

    private boolean processConfig(SourceData.ConfigRow cfg,
                                   String pgUrl, String pgUser, String pgPass,
                                   String flagColumn) {

        // --- Parse parameters ---
        if (cfg.parameters == null || cfg.parameters.trim().isEmpty()) {
            log("ERROR", "Parameters null/empty | sno=" + cfg.sno, null, "PARAM_PARSE");
            return false;
        }
        CombinedSpec spec = parseParameters(cfg.parameters, cfg.sno);
        if (spec == null) {
            log("ERROR", "Parameter parsing failed | sno=" + cfg.sno, null, "PARAM_PARSE_FAIL");
            return false;
        }
        log("INFO", "Parameters parsed | sno=" + cfg.sno
                + " | srcRegiCols=" + Arrays.toString(spec.tp4.srcRegiCols)
                + " | srcCorrCols=" + Arrays.toString(spec.tp4.srcCorrCols)
                + " | dstCols="     + Arrays.toString(spec.tp4.dstCols)
                + " | tp1MetaCount=" + spec.tp1MetaCount, null, "PARAM_PARSED");

        // --- Validate source path ---
        if (cfg.sourcePath == null || cfg.sourcePath.trim().isEmpty()) {
            log("ERROR", "Source path null/empty | sno=" + cfg.sno, null, "VALIDATION");
            return false;
        }
        File sourceDir = new File(cfg.sourcePath.trim());
        if (!sourceDir.exists() || !sourceDir.isDirectory()) {
            log("ERROR", "Source directory invalid | path=" + cfg.sourcePath
                    + " | sno=" + cfg.sno, null, "VALIDATION");
            return false;
        }

        // --- Validate destination table name ---
        if (cfg.destinationTableName == null || cfg.destinationTableName.trim().isEmpty()) {
            log("ERROR", "Destination table name null/empty | sno=" + cfg.sno, null, "VALIDATION");
            return false;
        }

        // --- Validate / create destination path ---
        if (cfg.destinationPath == null || cfg.destinationPath.trim().isEmpty()) {
            log("ERROR", "Destination path null/empty | sno=" + cfg.sno, null, "VALIDATION");
            return false;
        }
        File destDir = new File(cfg.destinationPath.trim());
        if (!destDir.exists()) {
            if (!destDir.mkdirs()) {
                log("ERROR", "Cannot create destination directory | path=" + cfg.destinationPath
                        + " | sno=" + cfg.sno, null, "VALIDATION");
                return false;
            }
            log("INFO", "Destination directory created | path=" + cfg.destinationPath
                    + " | sno=" + cfg.sno, null, "FILE_PROCESS");
        }

        // --- Validate destination table exists in PostgreSQL ---
        if (!validateTableExists(cfg.destinationTableName, pgUrl, pgUser, pgPass)) {
            log("ERROR", "Destination table not found | table=" + cfg.destinationTableName
                    + " | sno=" + cfg.sno, null, "VALIDATION");
            return false;
        }

        // --- List source CSV files (exclude intermediate files from prior runs) ---
        File[] csvFiles = sourceDir.listFiles((dir, name) ->
                name.toLowerCase().endsWith(".csv") && !name.startsWith(INTERMEDIATE_PREFIX));

        if (csvFiles == null || csvFiles.length == 0) {
            log("WARN", "No CSV files found in source directory | path=" + cfg.sourcePath
                    + " | sno=" + cfg.sno, null, "FILE_PROCESS");
            return true;  // nothing to do is not a failure
        }

        Arrays.sort(csvFiles, Comparator.comparing(File::getName));
        log("INFO", "CSV files found: " + csvFiles.length + " | sourcePath=" + cfg.sourcePath
                + " | destTable=" + cfg.destinationTableName + " | sno=" + cfg.sno, null, "FILE_PROCESS");

        boolean configSuccess = true;
        int fileSuccessCount  = 0;
        int fileFailCount     = 0;

        for (File csvFile : csvFiles) {
            log("INFO", "--- FILE START | file=" + csvFile.getName()
                    + " | size=" + csvFile.length() + " bytes | sno=" + cfg.sno + " ---",
                    null, "FILE_START");

            long fileStart = System.currentTimeMillis();
            boolean fileOk = runFilePipeline(csvFile, cfg, spec, pgUrl, pgUser, pgPass, flagColumn);
            long fileElapsed = System.currentTimeMillis() - fileStart;

            if (fileOk) {
                fileSuccessCount++;
                log("INFO", "--- FILE SUCCESS | file=" + csvFile.getName()
                        + " | elapsed=" + fileElapsed + "ms | sno=" + cfg.sno + " ---",
                        null, "FILE_END");
            } else {
                fileFailCount++;
                configSuccess = false;
                log("ERROR", "--- FILE FAILED  | file=" + csvFile.getName()
                        + " | elapsed=" + fileElapsed + "ms | sno=" + cfg.sno + " ---",
                        null, "FILE_END");
            }
        }

        log("INFO", "Config row done | sno=" + cfg.sno
                + " | filesSuccess=" + fileSuccessCount
                + " | filesFailed="  + fileFailCount, null, "CONFIG_SUMMARY");

        return configSuccess;
    }

    // =========================================================================
    // Full pipeline for one CSV file
    // =========================================================================

    private boolean runFilePipeline(File srcFile,
                                     SourceData.ConfigRow cfg,
                                     CombinedSpec spec,
                                     String pgUrl, String pgUser, String pgPass,
                                     String flagColumn) {

        File intermediate = new File(srcFile.getParentFile(),
                INTERMEDIATE_PREFIX + srcFile.getName());

        // ---- STAGE 1: CSV split transformation ----
        log("INFO", "STAGE 1 START: CSV split transformation | src=" + srcFile.getName()
                + " | out=" + intermediate.getName() + " | sno=" + cfg.sno, null, "STAGE1_START");

        long s1 = System.currentTimeMillis();
        int[] counts = stage1Transform(srcFile, intermediate, spec.tp4, flagColumn, cfg.sno);

        if (counts == null) {
            log("ERROR", "STAGE 1 FAILED | file=" + srcFile.getName()
                    + " | sno=" + cfg.sno, null, "STAGE1_FAIL");
            silentDelete(intermediate);
            return false;
        }

        log("INFO", "STAGE 1 DONE | file=" + srcFile.getName()
                + " | sourceDataRows=" + counts[0]
                + " | outputRows="    + counts[1]
                + " | malformed="     + counts[2]
                + " | skipped="       + counts[3]
                + " | elapsed="       + (System.currentTimeMillis() - s1) + "ms"
                + " | sno="           + cfg.sno, null, "STAGE1_END");

        if (counts[1] == 0) {
            log("WARN", "Stage 1 produced 0 output rows; skipping DB insert | file="
                    + srcFile.getName() + " | sno=" + cfg.sno, null, "STAGE1_EMPTY");
            silentDelete(intermediate);
            // File had no usable data; still move it to prevent endless reprocessing
            moveFile(srcFile, new File(cfg.destinationPath.trim()), cfg.sno);
            return true;
        }

        // ---- STAGE 2: TP1 metadata flow + DB insert ----
        log("INFO", "STAGE 2 START: TP1 metadata flow | file=" + intermediate.getName()
                + " | destTable=" + cfg.destinationTableName + " | sno=" + cfg.sno,
                null, "STAGE2_START");

        long s2 = System.currentTimeMillis();
        boolean stage2Ok = stage2Tp1Flow(
                intermediate.getAbsolutePath(),
                cfg.destinationTableName,
                spec.tp1DirectCols,
                spec.tp1MetaCount,
                spec.tp1DestMetaCols,
                spec.tp1SrcMetaCols,
                spec.tp1JsonKeys,
                pgUrl, pgUser, pgPass,
                cfg.sno);

        log("INFO", "STAGE 2 " + (stage2Ok ? "SUCCESS" : "FAILED") + " | file="
                + intermediate.getName() + " | elapsed="
                + (System.currentTimeMillis() - s2) + "ms | sno=" + cfg.sno,
                null, stage2Ok ? "STAGE2_END" : "STAGE2_FAIL");

        if (!stage2Ok) {
            // Leave intermediate in place for debugging; original source stays for retry
            log("ERROR", "Stage 2 failed - original source CSV not moved | file="
                    + srcFile.getName() + " | sno=" + cfg.sno, null, "STAGE2_FAIL");
            return false;
        }

        // ---- Move intermediate to dest, delete original source ----
        boolean moved = moveFile(intermediate, new File(cfg.destinationPath.trim()), cfg.sno);
        if (!moved) {
            log("ERROR", "File move failed after successful insert | file="
                    + intermediate.getName() + " | sno=" + cfg.sno, null, "FILE_MOVE_FAIL");
            return false;
        }

        silentDelete(srcFile);
        return true;
    }

    // =========================================================================
    // STAGE 1 — CSV split transformation
    // =========================================================================

    /**
     * Reads srcFile, splits every data row into one Regi row and one Corr row,
     * writes the result to intermediateFile.
     *
     * Output CSV column order:
     *   [pass-through cols ...] [dstCols ...] [add_type] [same_as_correspondence_address]
     *
     * Pass-through columns = all source columns not in the regi/corr address sets
     *                        and not the flag column.
     *
     * @return int[]{sourceDataRows, outputRows, malformedRows, skippedRows}
     *         or null on fatal error (missing header, missing required columns, IO failure).
     */
    private int[] stage1Transform(File srcFile,
                                   File intermediateFile,
                                   Tp4Spec tp4,
                                   String flagColumn,
                                   int sno) {

        int sourceDataRows = 0;
        int outputRows     = 0;
        int malformedRows  = 0;
        int skippedRows    = 0;

        try (BufferedReader br     = new BufferedReader(new FileReader(srcFile));
             PrintWriter    writer = new PrintWriter(new BufferedWriter(new FileWriter(intermediateFile)))) {

            // ---- Parse header ----
            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                log("ERROR", "CSV has no header | file=" + srcFile.getName()
                        + " | sno=" + sno, null, "HEADER_MISSING");
                return null;
            }

            String[] headers     = splitCsvLine(headerLine);
            Map<String, Integer> headerIndex = buildHeaderIndex(headers);

            log("DEBUG", "Source header parsed | file=" + srcFile.getName()
                    + " | columns=" + headers.length + " | sno=" + sno, null, "STAGE1");

            // ---- Validate required source address columns ----
            List<String> missing = new ArrayList<>();
            for (String c : tp4.srcRegiCols) { if (!headerIndex.containsKey(c)) missing.add("regi:" + c); }
            for (String c : tp4.srcCorrCols) { if (!headerIndex.containsKey(c)) missing.add("corr:" + c); }
            if (!missing.isEmpty()) {
                log("ERROR", "Required source columns missing in CSV header | missing=" + missing
                        + " | file=" + srcFile.getName() + " | sno=" + sno, null, "HEADER_INVALID");
                return null;
            }

            // ---- Flag column (warn-only if absent) ----
            boolean flagPresent = headerIndex.containsKey(flagColumn);
            if (!flagPresent) {
                log("WARN", "Flag column '" + flagColumn
                        + "' not found in CSV header; same_as_correspondence_address defaults to false"
                        + " | file=" + srcFile.getName() + " | sno=" + sno, null, "FLAG_COL_MISSING");
            }

            // ---- Identify pass-through columns ----
            // Exclude: source address cols, flag col, and the hardcoded output col names
            // (prevents collisions if source CSV happens to have those names).
            Set<String> exclude = new HashSet<>();
            for (String c : tp4.srcRegiCols)  exclude.add(c);
            for (String c : tp4.srcCorrCols)  exclude.add(c);
            exclude.add(flagColumn);
            exclude.add(ADD_TYPE_COLUMN);
            exclude.add(SAME_AS_CORR_COLUMN);
            // Also exclude destination column names to prevent duplicate columns
            for (String c : tp4.dstCols) exclude.add(c);

            List<String>  ptHeaders = new ArrayList<>();
            List<Integer> ptIndices = new ArrayList<>();
            for (int i = 0; i < headers.length; i++) {
                if (!exclude.contains(headers[i])) {
                    ptHeaders.add(headers[i]);
                    ptIndices.add(i);
                }
            }

            log("DEBUG", "Pass-through columns identified | count=" + ptHeaders.size()
                    + " | cols=" + ptHeaders + " | sno=" + sno, null, "STAGE1");

            // ---- Write intermediate CSV header ----
            // Order: passThroughCols..., dstCols..., add_type, same_as_correspondence_address
            StringBuilder hdr = new StringBuilder();
            for (String h : ptHeaders)    { hdr.append(h).append(","); }
            for (String d : tp4.dstCols)  { hdr.append(d).append(","); }
            hdr.append(ADD_TYPE_COLUMN).append(",").append(SAME_AS_CORR_COLUMN);
            writer.println(hdr.toString());

            log("DEBUG", "Intermediate CSV header written | header=" + hdr
                    + " | sno=" + sno, null, "STAGE1");

            // ---- Process data rows ----
            String line;
            int lineNum = 1;

            while ((line = br.readLine()) != null) {
                lineNum++;
                if (line.trim().isEmpty()) {
                    log("DEBUG", "Blank line skipped | line=" + lineNum
                            + " | sno=" + sno, null, "STAGE1");
                    skippedRows++;
                    continue;
                }
                sourceDataRows++;

                String[] raw = splitCsvLine(line);
                if (raw == null || raw.length == 0) {
                    malformedRows++;
                    log("WARN", "Malformed row (null parse) | line=" + lineNum
                            + " | file=" + srcFile.getName() + " | sno=" + sno, null, "ROW_MALFORMED");
                    continue;
                }

                // Normalise to header length (pad with nulls if short, truncate if long)
                if (raw.length < headers.length) {
                    malformedRows++;
                    log("WARN", "Column count mismatch | line=" + lineNum
                            + " | expected=" + headers.length + " | got=" + raw.length
                            + " | padded with nulls | file=" + srcFile.getName()
                            + " | sno=" + sno, null, "ROW_MALFORMED");
                }
                String[] vals = new String[headers.length];
                for (int i = 0; i < headers.length; i++) {
                    vals[i] = (i < raw.length) ? raw[i] : null;
                }

                // Flag value  same_as_correspondence_address
                boolean sameAsCorr = false;
                if (flagPresent) {
                    Integer fi = headerIndex.get(flagColumn);
                    if (fi != null) {
                        String fv = safeGet(vals, fi);
                        sameAsCorr = "on".equalsIgnoreCase(fv);
                    }
                }

                // Pass-through values
                String[] ptVals = new String[ptHeaders.size()];
                for (int i = 0; i < ptIndices.size(); i++) {
                    ptVals[i] = safeGet(vals, ptIndices.get(i));
                }

                // Extract regi address values
                String[] regiVals = extractAddressValues(vals, headerIndex, tp4.srcRegiCols);
                if (regiVals == null) {
                    skippedRows++;
                    log("WARN", "Skipped row: could not extract regi values | line=" + lineNum
                            + " | sno=" + sno, null, "ROW_SKIP");
                    continue;
                }

                // Extract corr address values
                String[] corrVals = extractAddressValues(vals, headerIndex, tp4.srcCorrCols);
                if (corrVals == null) {
                    skippedRows++;
                    log("WARN", "Skipped row: could not extract corr values | line=" + lineNum
                            + " | sno=" + sno, null, "ROW_SKIP");
                    continue;
                }

                // Write Regi row
                writer.println(buildOutputRow(ptVals, regiVals, ADD_TYPE_REGI, sameAsCorr));
                outputRows++;

                // Write Corr row
                writer.println(buildOutputRow(ptVals, corrVals, ADD_TYPE_CORR, sameAsCorr));
                outputRows++;
            }

            writer.flush();
            log("INFO", "Stage 1 complete | file=" + srcFile.getName()
                    + " | sourceDataRows=" + sourceDataRows
                    + " | outputRows="     + outputRows
                    + " | malformed="      + malformedRows
                    + " | skipped="        + skippedRows
                    + " | sno="            + sno, null, "STAGE1");

        } catch (IOException ex) {
            log("ERROR", "IOException in Stage 1 | file=" + srcFile.getName()
                    + " | sno=" + sno + " | Error: " + ex.getMessage(), null, "STAGE1_EXCEPTION");
            return null;
        } catch (Exception ex) {
            log("ERROR", "Exception in Stage 1 | file=" + srcFile.getName()
                    + " | sno=" + sno + " | Error: " + ex.getMessage(), null, "STAGE1_EXCEPTION");
            return null;
        }

        return new int[]{sourceDataRows, outputRows, malformedRows, skippedRows};
    }

    /** Extract address column values from a source row, in srcCols order. */
    private String[] extractAddressValues(String[] rowVals,
                                           Map<String, Integer> headerIndex,
                                           String[] srcCols) {
        String[] result = new String[srcCols.length];
        for (int i = 0; i < srcCols.length; i++) {
            Integer idx = headerIndex.get(srcCols[i]);
            if (idx == null) return null;
            result[i] = safeGet(rowVals, idx);
        }
        return result;
    }

    /** Build one output CSV row string. */
    private String buildOutputRow(String[] ptVals, String[] addrVals,
                                   String addType, boolean sameAsCorr) {
        StringBuilder sb = new StringBuilder();
        for (String v : ptVals)   { sb.append(escapeCsv(v)).append(","); }
        for (String v : addrVals) { sb.append(escapeCsv(v)).append(","); }
        sb.append(escapeCsv(addType)).append(",");
        sb.append(sameAsCorr ? "true" : "false");
        return sb.toString();
    }

    // =========================================================================
    // STAGE 2 — TP1 metadata flow (ported from metadata_parser_process)
    // =========================================================================

    /**
     * Reads the intermediate CSV produced by Stage 1, validates the destination
     * table, assembles ordered record arrays (direct cols first, then source
     * metadata cols), and batch-inserts into PostgreSQL.
     *
     * This is a port of metadata_parser_process.parseAndTransferData() adapted
     * to use header-aware reading so the intermediate CSV column order is
     * independent of the TP1 positional-access requirement.
     */
    private boolean stage2Tp1Flow(String csvFilePath,
                                   String destTableName,
                                   String tp1DirectColsStr,
                                   int    tp1MetaCount,
                                   String tp1DestMetaColsStr,
                                   String tp1SrcMetaColsStr,
                                   String tp1JsonKeysStr,
                                   String pgUrl, String pgUser, String pgPass,
                                   int sno) {

        log("INFO", "TP1 Flow | csv=" + csvFilePath
                + " | destTable="   + destTableName
                + " | directCols="  + tp1DirectColsStr
                + " | metaCount="   + tp1MetaCount
                + " | sno="         + sno, null, "TP1_FLOW_START");

        try {
            // ---- Parse TP1 spec ----
            String[] srcMetaGroups = parseHashSeparated(tp1SrcMetaColsStr);
            String[] jsonKeyGroups = parseHashSeparated(tp1JsonKeysStr);

            List<String[]> srcMetaColsList  = new ArrayList<>();
            List<String[]> jsonKeyNamesList = new ArrayList<>();

            for (int i = 0; i < tp1MetaCount; i++) {
                if (i >= srcMetaGroups.length || i >= jsonKeyGroups.length) {
                    log("ERROR", "Metadata group index out of bounds | index=" + i
                            + " | metaCount=" + tp1MetaCount + " | sno=" + sno,
                            null, "TP1_PARAM_ERROR");
                    return false;
                }
                String[] srcCols  = parseCommaSeparated(srcMetaGroups[i]);
                String[] jsonKeys = parseCommaSeparated(jsonKeyGroups[i]);
                if (srcCols.length != jsonKeys.length) {
                    log("ERROR", "Metadata group " + i + " column/key count mismatch"
                            + " | srcCols=" + srcCols.length
                            + " | jsonKeys=" + jsonKeys.length + " | sno=" + sno,
                            null, "TP1_PARAM_ERROR");
                    return false;
                }
                srcMetaColsList.add(srcCols);
                jsonKeyNamesList.add(jsonKeys);
            }

            // ---- Read intermediate CSV header ----
            String[] csvHeaders = readCsvHeader(csvFilePath);
            if (csvHeaders == null) {
                log("ERROR", "Cannot read intermediate CSV header | file=" + csvFilePath
                        + " | sno=" + sno, null, "TP1_CSV_ERROR");
                return false;
            }
            Map<String, Integer> headerIndex = buildHeaderIndex(csvHeaders);

            log("DEBUG", "Intermediate CSV columns=" + csvHeaders.length
                    + " | headers=" + Arrays.toString(csvHeaders) + " | sno=" + sno,
                    null, "TP1_FLOW");

            // ---- Determine direct columns ----
            // If not specified in TP1 spec, default to all intermediate CSV columns.
            String[] directCols;
            if (tp1DirectColsStr == null || tp1DirectColsStr.trim().isEmpty()) {
                directCols = csvHeaders;
                log("INFO", "tp1DirectCols defaulting to all CSV headers | count="
                        + directCols.length + " | sno=" + sno, null, "TP1_FLOW");
            } else {
                directCols = parseCommaSeparated(tp1DirectColsStr);
            }
            String[] destMetaCols = parseCommaSeparated(tp1DestMetaColsStr);

            // ---- Validate destination table columns ----
            HashSet<String> destTableCols = getTableColumns(destTableName, pgUrl, pgUser, pgPass);
            if (destTableCols == null || destTableCols.isEmpty()) {
                log("ERROR", "Could not fetch destination table columns | table=" + destTableName
                        + " | sno=" + sno, null, "TP1_VALIDATION");
                return false;
            }

            for (String col : directCols) {
                if (!destTableCols.contains(col.toLowerCase())) {
                    log("ERROR", "Direct column missing in destination table | col=" + col
                            + " | table=" + destTableName + " | sno=" + sno, null, "TP1_VALIDATION");
                    return false;
                }
            }
            for (String col : destMetaCols) {
                if (!destTableCols.contains(col.toLowerCase())) {
                    log("ERROR", "Dest metadata column missing in destination table | col=" + col
                            + " | table=" + destTableName + " | sno=" + sno, null, "TP1_VALIDATION");
                    return false;
                }
            }

            // ---- Validate that all required columns exist in intermediate CSV header ----
            for (String col : directCols) {
                if (!headerIndex.containsKey(col)) {
                    log("ERROR", "Direct column not found in intermediate CSV header | col=" + col
                            + " | file=" + csvFilePath + " | sno=" + sno, null, "TP1_VALIDATION");
                    return false;
                }
            }
            for (String[] srcCols : srcMetaColsList) {
                for (String col : srcCols) {
                    if (!headerIndex.containsKey(col)) {
                        log("ERROR", "Source metadata column not found in intermediate CSV header | col="
                                + col + " | file=" + csvFilePath + " | sno=" + sno,
                                null, "TP1_VALIDATION");
                        return false;
                    }
                }
            }

            // ---- Build ordered column list for reading ----
            // Record array layout expected by insertDataToDestination:
            //   record[0..directCols.length-1]             direct column values
            //   record[directCols.length..]                source metadata column values (group order)
            List<String> orderedCols = new ArrayList<>();
            for (String c : directCols)           orderedCols.add(c);
            for (String[] grp : srcMetaColsList)  for (String c : grp) orderedCols.add(c);

            // ---- Read CSV data (header-aware, ordered by orderedCols) ----
            HashMap<String, String[]> sourceData =
                    readCsvOrdered(csvFilePath, orderedCols, headerIndex, sno);

            if (sourceData == null || sourceData.isEmpty()) {
                log("WARN", "No data rows in intermediate CSV | file=" + csvFilePath
                        + " | sno=" + sno, null, "TP1_FLOW");
                return false;
            }
            log("INFO", "TP1 CSV read complete | file=" + csvFilePath
                    + " | rows=" + sourceData.size() + " | sno=" + sno, null, "TP1_FLOW");

            // ---- Insert into destination table ----
            return insertDataToDestination(
                    sourceData, destTableName,
                    directCols, destMetaCols,
                    srcMetaColsList, jsonKeyNamesList,
                    pgUrl, pgUser, pgPass, sno);

        } catch (Exception ex) {
            log("ERROR", "Exception in Stage 2 TP1 Flow | file=" + csvFilePath
                    + " | sno=" + sno + " | Error: " + ex.getMessage(), null, "TP1_EXCEPTION");
            return false;
        }
    }

    // =========================================================================
    // CSV reading helpers
    // =========================================================================

    /** Read only the header line of a CSV file. Returns null on error. */
    private String[] readCsvHeader(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line = br.readLine();
            if (line == null || line.trim().isEmpty()) return null;
            return splitCsvLine(line);
        } catch (Exception ex) {
            log("ERROR", "Failed to read CSV header | file=" + filePath
                    + " | Error: " + ex.getMessage(), null, "CSV_READ");
            return null;
        }
    }

    /**
     * Read CSV data (skipping header) and return a LinkedHashMap keyed by row
     * index.  Each value is a String[] of length orderedCols.size() whose
     * elements correspond to orderedCols[i], looked up by column name from the
     * CSV header.  This decouples the physical CSV column order from the logical
     * order expected by the TP1 positional insert logic.
     */
    private HashMap<String, String[]> readCsvOrdered(String filePath,
                                                      List<String> orderedCols,
                                                      Map<String, Integer> headerIndex,
                                                      int sno) {
        HashMap<String, String[]> data = new LinkedHashMap<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            br.readLine(); // skip header

            String line;
            int count  = 0;
            int rowNum = 0;

            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;
                rowNum++;

                String[] raw = splitCsvLine(line);
                String[] ordered = new String[orderedCols.size()];

                for (int i = 0; i < orderedCols.size(); i++) {
                    Integer idx = headerIndex.get(orderedCols.get(i));
                    if (idx != null && raw != null && idx < raw.length) {
                        String v = raw[idx];
                        ordered[i] = (v != null && !v.trim().isEmpty()) ? v.trim() : null;
                    } else {
                        ordered[i] = null;
                    }
                }

                data.put(String.valueOf(count), ordered);
                count++;
            }

            log("DEBUG", "CSV ordered read | file=" + filePath
                    + " | rows=" + data.size() + " | sno=" + sno, null, "CSV_READ");

        } catch (Exception ex) {
            log("ERROR", "Failed to read CSV ordered | file=" + filePath
                    + " | sno=" + sno + " | Error: " + ex.getMessage(), null, "CSV_READ");
            return null;
        }

        return data;
    }

    // =========================================================================
    // TP1 Insert (ported from metadata_parser_process.insertDataToDestination)
    // =========================================================================

    private boolean insertDataToDestination(HashMap<String, String[]> sourceData,
                                             String   destTableName,
                                             String[] directColumns,
                                             String[] destMetaColumns,
                                             List<String[]> srcMetaColumnsList,
                                             List<String[]> jsonKeyNamesList,
                                             String pgUrl, String pgUser, String pgPass,
                                             int sno) {

        Connection        conn      = null;
        PreparedStatement ps        = null;
        boolean           status    = false;
        int               rowCount  = 0;
        int               inserted  = 0;
        int               skipped   = 0;
        int               unknown   = 0;
        int               totalRows = sourceData.size();
        long              beforeCnt = -1;
        long              afterCnt  = -1;

        log("INFO", "Insert START | table=" + destTableName
                + " | totalRows=" + totalRows + " | sno=" + sno, null, "DATA_INSERT");

        try {
            conn = getPgConnection(pgUrl, pgUser, pgPass);
            if (conn == null) {
                log("ERROR", "Cannot open PostgreSQL connection for insert | sno=" + sno,
                        null, "DATA_INSERT");
                return false;
            }

            HashMap<String, String> colTypes =
                    getDestinationColumnTypes(destTableName, directColumns, destMetaColumns, conn);
            if (colTypes == null || colTypes.isEmpty()) {
                log("ERROR", "Could not fetch destination column types | table=" + destTableName
                        + " | sno=" + sno, null, "DATA_INSERT");
                return false;
            }

            // Row count before
            try (PreparedStatement c = conn.prepareStatement("SELECT COUNT(*) FROM " + destTableName);
                 ResultSet r = c.executeQuery()) {
                if (r.next()) beforeCnt = r.getLong(1);
            }

            // Build INSERT SQL
            StringBuilder sql = new StringBuilder("INSERT INTO ").append(destTableName).append(" (");
            for (int i = 0; i < directColumns.length; i++) {
                sql.append(directColumns[i]);
                if (i < directColumns.length - 1 || destMetaColumns.length > 0) sql.append(", ");
            }
            for (int i = 0; i < destMetaColumns.length; i++) {
                sql.append(destMetaColumns[i]);
                if (i < destMetaColumns.length - 1) sql.append(", ");
            }
            sql.append(") VALUES (");
            int totalCols = directColumns.length + destMetaColumns.length;
            for (int i = 0; i < totalCols; i++) {
                sql.append("?");
                if (i < totalCols - 1) sql.append(", ");
            }
            sql.append(") ON CONFLICT DO NOTHING");

            log("DEBUG", "Insert SQL | table=" + destTableName + " | sql=" + sql
                    + " | sno=" + sno, null, "DATA_INSERT");

            conn.setAutoCommit(false);
            ps = conn.prepareStatement(sql.toString());

            for (Map.Entry<String, String[]> entry : sourceData.entrySet()) {
                String[] record = entry.getValue();
                int      pIdx   = 1;

                // Direct column values (positional: record[i]  directColumns[i])
                for (int i = 0; i < directColumns.length; i++) {
                    String colName = directColumns[i].toLowerCase();
                    String value   = (i < record.length) ? record[i] : null;
                    String colType = colTypes.get(colName);
                    setParameterByType(ps, pIdx, value, colType, colName, sno);
                    pIdx++;
                }

                // Metadata JSON values
                // record[directColumns.length + offset]  source metadata columns in group order
                int recIdx = directColumns.length;
                for (int i = 0; i < destMetaColumns.length; i++) {
                    String[] srcCols  = srcMetaColumnsList.get(i);
                    String[] jsonKeys = jsonKeyNamesList.get(i);

                    StringBuilder json = new StringBuilder("{");
                    for (int j = 0; j < srcCols.length; j++) {
                        String key = jsonKeys[j];
                        String val = (recIdx < record.length) ? record[recIdx] : null;
                        String esc = (val != null)
                                ? val.replace("\\", "\\\\").replace("\"", "\\\"")
                                : null;
                        json.append("\"").append(key).append("\":");
                        if (esc != null) json.append("\"").append(esc).append("\"");
                        else             json.append("null");
                        if (j < srcCols.length - 1) json.append(",");
                        recIdx++;
                    }
                    json.append("}");

                    String colType = colTypes.get(destMetaColumns[i].toLowerCase());
                    if (colType != null && colType.toLowerCase().contains("jsonb"))
                        ps.setObject(pIdx, json.toString(), Types.OTHER);
                    else
                        ps.setString(pIdx, json.toString());
                    pIdx++;
                }

                ps.addBatch();
                rowCount++;

                if (rowCount % TP1_BATCH_SIZE == 0) {
                    int[] res = ps.executeBatch();
                    for (int r : res) {
                        if      (r == 1)                           inserted++;
                        else if (r == 0)                           skipped++;
                        else if (r == Statement.SUCCESS_NO_INFO)   unknown++;
                    }
                    log("INFO", "Mid-loop batch executed | rowsSoFar=" + rowCount
                            + " | sno=" + sno, null, "DATA_INSERT");
                }
            }

            // Final remaining batch
            if (ps != null) {
                int[] res = ps.executeBatch();
                for (int r : res) {
                    if      (r == 1)                           inserted++;
                    else if (r == 0)                           skipped++;
                    else if (r == Statement.SUCCESS_NO_INFO)   unknown++;
                }
            }

            conn.commit();
            log("INFO", "Transaction committed | table=" + destTableName
                    + " | sno=" + sno, null, "DATA_INSERT");

            // Row count after
            try (PreparedStatement c = conn.prepareStatement("SELECT COUNT(*) FROM " + destTableName);
                 ResultSet r = c.executeQuery()) {
                if (r.next()) afterCnt = r.getLong(1);
            }

            status = true;

            log("INFO", "Insert Summary | table=" + destTableName
                    + " | totalRows="   + totalRows
                    + " | inserted="    + inserted
                    + " | skipped="     + skipped
                    + " | unknown="     + unknown
                    + " | rowsBefore="  + beforeCnt
                    + " | rowsAfter="   + afterCnt
                    + " | netAdded="    + (afterCnt - beforeCnt)
                    + " | sno="         + sno, null, "DATA_INSERT");

        } catch (Exception ex) {
            log("ERROR", "Insert failed | table=" + destTableName
                    + " | rowsBeforeFailure=" + rowCount
                    + " | sno=" + sno + " | Error: " + ex.getMessage(), null, "DATA_INSERT");
            try {
                if (conn != null) conn.rollback();
                log("INFO", "Transaction rolled back | table=" + destTableName
                        + " | sno=" + sno, null, "DATA_INSERT");
            } catch (Exception e) {
                log("ERROR", "Rollback failed | sno=" + sno
                        + " | Error: " + e.getMessage(), null, "DATA_INSERT");
            }
            status = false;
        } finally {
            try { if (ps   != null) ps.close();   } catch (Exception ignored) {}
            try { if (conn != null) conn.close();  } catch (Exception ignored) {}
        }

        return status;
    }

    // =========================================================================
    // DB validation helpers (ported from metadata_parser_process)
    // =========================================================================

    private boolean validateTableExists(String tableName, String pgUrl, String pgUser, String pgPass) {
        Connection conn = null;
        try {
            conn = getPgConnection(pgUrl, pgUser, pgPass);
            if (conn == null) return false;

            String schema = "public";
            String table  = tableName;
            if (tableName.contains(".")) {
                String[] p = tableName.split("\\.", 2);
                schema = p[0].toLowerCase();
                table  = p[1].toLowerCase();
            } else {
                table = tableName.toLowerCase();
            }

            String sql = "SELECT 1 FROM information_schema.tables"
                    + " WHERE table_schema = ? AND table_name = ?";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schema);
                ps.setString(2, table);
                try (ResultSet rs = ps.executeQuery()) {
                    boolean exists = rs.next();
                    log(exists ? "INFO" : "WARN",
                            "Table " + (exists ? "found" : "not found")
                                    + " | schema=" + schema + " | table=" + table,
                            null, "VALIDATION");
                    return exists;
                }
            }
        } catch (Exception ex) {
            log("ERROR", "Table existence check failed | table=" + tableName
                    + " | Error: " + ex.getMessage(), null, "VALIDATION");
            return false;
        } finally {
            closeQuietly(null, null, conn);
        }
    }

    private HashSet<String> getTableColumns(String tableName,
                                             String pgUrl, String pgUser, String pgPass) {
        HashSet<String> cols = new HashSet<>();
        Connection conn = null;
        try {
            conn = getPgConnection(pgUrl, pgUser, pgPass);
            if (conn == null) return cols;

            String schema = null;
            String table  = tableName;
            if (tableName.contains(".")) {
                String[] p = tableName.split("\\.", 2);
                schema = p[0].toLowerCase();
                table  = p[1].toLowerCase();
            } else {
                table = tableName.toLowerCase();
            }

            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
                while (rs.next()) cols.add(rs.getString("COLUMN_NAME").toLowerCase());
            }
            log("DEBUG", "Table columns fetched | table=" + tableName
                    + " | count=" + cols.size(), null, "VALIDATION");
        } catch (Exception ex) {
            log("ERROR", "Failed to fetch table columns | table=" + tableName
                    + " | Error: " + ex.getMessage(), null, "VALIDATION");
        } finally {
            closeQuietly(null, null, conn);
        }
        return cols;
    }

    /** Fetch column name  type_name for the given set of column names. */
    private HashMap<String, String> getDestinationColumnTypes(String tableName,
                                                               String[] directCols,
                                                               String[] metaCols,
                                                               Connection conn) {
        HashMap<String, String> types = new HashMap<>();
        try {
            String schema = null;
            String table  = tableName;
            if (tableName.contains(".")) {
                String[] p = tableName.split("\\.", 2);
                schema = p[0].toLowerCase();
                table  = p[1].toLowerCase();
            } else {
                table = tableName.toLowerCase();
            }

            DatabaseMetaData meta = conn.getMetaData();

            String[] all = new String[directCols.length + metaCols.length];
            System.arraycopy(directCols, 0, all, 0, directCols.length);
            System.arraycopy(metaCols,   0, all, directCols.length, metaCols.length);

            for (String col : all) {
                try (ResultSet rs = meta.getColumns(null, schema, table, col.toLowerCase())) {
                    if (rs.next()) types.put(col.toLowerCase(), rs.getString("TYPE_NAME"));
                }
            }
        } catch (Exception ex) {
            log("ERROR", "Failed to fetch column types | table=" + tableName
                    + " | Error: " + ex.getMessage(), null, "DATA_INSERT");
            return null;
        }
        return types;
    }

    /** Type-aware PreparedStatement parameter setter (ported from TP1). */
    private void setParameterByType(PreparedStatement ps, int idx,
                                     String value, String colType,
                                     String colName, int sno) throws Exception {
        if (value == null || value.isEmpty()) {
            if (colType != null) {
                String t = colType.toLowerCase();
                if (t.contains("int") || t.contains("serial"))
                    ps.setNull(idx, Types.BIGINT);
                else if (t.contains("numeric") || t.contains("decimal")
                        || t.contains("float") || t.contains("double") || t.contains("real"))
                    ps.setNull(idx, Types.NUMERIC);
                else if (t.contains("bool"))
                    ps.setNull(idx, Types.BOOLEAN);
                else if (t.contains("timestamp") || t.contains("date") || t.contains("time"))
                    ps.setNull(idx, Types.TIMESTAMP);
                else if (value != null)
                    ps.setString(idx, value); // empty string for text types
                else
                    ps.setNull(idx, Types.VARCHAR);
            } else {
                if (value != null) ps.setString(idx, value);
                else               ps.setNull(idx, Types.VARCHAR);
            }
            return;
        }

        if (colType != null) {
            String t = colType.toLowerCase();
            try {
                if (t.contains("bigint") || t.contains("int8") || t.contains("bigserial"))
                    ps.setLong(idx, Long.parseLong(value.trim()));
                else if (t.contains("int") || t.contains("serial"))
                    ps.setInt(idx, Integer.parseInt(value.trim()));
                else if (t.contains("numeric") || t.contains("decimal"))
                    ps.setBigDecimal(idx, new BigDecimal(value.trim()));
                else if (t.contains("float") || t.contains("double") || t.contains("real"))
                    ps.setDouble(idx, Double.parseDouble(value.trim()));
                else if (t.contains("bool")) {
                    String v = value.trim().toLowerCase();
                    ps.setBoolean(idx, "true".equals(v) || "on".equals(v)
                            || "1".equals(v) || "yes".equals(v));
                } else if (t.contains("timestamp")) {
                    try { ps.setTimestamp(idx, Timestamp.valueOf(value.trim())); }
                    catch (IllegalArgumentException e) {
                        log("ERROR", "Timestamp parse fail | col=" + colName
                                + " | value=" + value + " | sno=" + sno, null, "TYPE_ERROR");
                        throw new Exception("Type mismatch: col='" + colName
                                + "' expects timestamp, got '" + value + "'");
                    }
                } else if (t.contains("date")) {
                    try { ps.setDate(idx, Date.valueOf(value.trim())); }
                    catch (IllegalArgumentException e) {
                        log("ERROR", "Date parse fail | col=" + colName
                                + " | value=" + value + " | sno=" + sno, null, "TYPE_ERROR");
                        throw new Exception("Type mismatch: col='" + colName
                                + "' expects date, got '" + value + "'");
                    }
                } else if (t.contains("time")) {
                    try { ps.setTime(idx, Time.valueOf(value.trim())); }
                    catch (IllegalArgumentException e) {
                        log("ERROR", "Time parse fail | col=" + colName
                                + " | value=" + value + " | sno=" + sno, null, "TYPE_ERROR");
                        throw new Exception("Type mismatch: col='" + colName
                                + "' expects time, got '" + value + "'");
                    }
                } else {
                    ps.setString(idx, value);
                }
            } catch (NumberFormatException e) {
                log("ERROR", "Number format fail | col=" + colName
                        + " | type=" + colType + " | value=" + value
                        + " | sno=" + sno, null, "TYPE_ERROR");
                throw new Exception("Type mismatch: col='" + colName + "' expects "
                        + colType + ", got '" + value + "'");
            }
        } else {
            ps.setString(idx, value);
        }
    }

    // =========================================================================
    // Parameter parsing
    // =========================================================================

    /**
     * Parses the combined parameters string.
     *
     * Format:
     *   <srcRegiCols,srcCorrCols> | <dstCols> [|| <tp1DirectCols> | <tp1MetaCount> | <tp1DestMetaCols> | <tp1SrcMetaCols> | <tp1JsonKeys>]
     *
     * The "||" boundary separates TP4 spec (left) from optional TP1 spec (right).
     */
    private CombinedSpec parseParameters(String parameters, int sno) {
        if (parameters == null || parameters.trim().isEmpty()) {
            log("ERROR", "Parameters string null/empty | sno=" + sno, null, "PARAM_PARSE");
            return null;
        }

        // Split TP4 spec from TP1 spec
        String tp4Raw  = parameters;
        String tp1Raw  = null;
        int doublePipe = parameters.indexOf("||");
        if (doublePipe >= 0) {
            tp4Raw = parameters.substring(0, doublePipe).trim();
            tp1Raw = parameters.substring(doublePipe + 2).trim();
        }

        // --- Parse TP4 spec: "srcCols | dstCols" ---
        String[] tp4Parts = tp4Raw.split("\\|");
        if (tp4Parts.length != 2) {
            log("ERROR", "TP4 spec must have exactly one '|' separator | tp4Spec=" + tp4Raw
                    + " | sno=" + sno, null, "PARAM_PARSE");
            return null;
        }

        String[] allSrcCols = Arrays.stream(tp4Parts[0].split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
        String[] dstCols    = Arrays.stream(tp4Parts[1].split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);

        if (allSrcCols.length == 0) {
            log("ERROR", "TP4 source columns are empty | sno=" + sno, null, "PARAM_PARSE");
            return null;
        }
        if (dstCols.length == 0) {
            log("ERROR", "TP4 destination columns are empty | sno=" + sno, null, "PARAM_PARSE");
            return null;
        }
        if (allSrcCols.length % 2 != 0) {
            log("ERROR", "TP4 source column count must be even (regi + corr halves)"
                    + " | count=" + allSrcCols.length + " | sno=" + sno, null, "PARAM_PARSE");
            return null;
        }

        int      half       = allSrcCols.length / 2;
        String[] srcRegiCols = Arrays.copyOfRange(allSrcCols, 0, half);
        String[] srcCorrCols = Arrays.copyOfRange(allSrcCols, half, allSrcCols.length);

        if (srcRegiCols.length != dstCols.length) {
            log("ERROR", "TP4 regi/corr column count (" + srcRegiCols.length
                    + ") must equal destination column count (" + dstCols.length + ")"
                    + " | sno=" + sno, null, "PARAM_PARSE");
            return null;
        }

        Tp4Spec tp4Spec    = new Tp4Spec();
        tp4Spec.srcRegiCols = srcRegiCols;
        tp4Spec.srcCorrCols = srcCorrCols;
        tp4Spec.dstCols     = dstCols;

        // --- Parse TP1 spec (optional): "directCols | metaCount | destMeta | srcMeta | jsonKeys" ---
        String tp1DirectCols   = null;
        int    tp1MetaCount    = 0;
        String tp1DestMetaCols = "";
        String tp1SrcMetaCols  = "";
        String tp1JsonKeys     = "";

        if (tp1Raw != null && !tp1Raw.isEmpty()) {
            String[] tp1Parts = tp1Raw.split("\\|", -1);
            tp1DirectCols   = tp1Parts.length > 0 ? tp1Parts[0].trim() : null;
            if (tp1Parts.length > 1) {
                try {
                    tp1MetaCount = Integer.parseInt(tp1Parts[1].trim());
                } catch (NumberFormatException e) {
                    log("WARN", "tp1MetaCount parse failed; defaulting to 0 | value="
                            + tp1Parts[1] + " | sno=" + sno, null, "PARAM_PARSE");
                    tp1MetaCount = 0;
                }
            }
            tp1DestMetaCols = tp1Parts.length > 2 ? tp1Parts[2].trim() : "";
            tp1SrcMetaCols  = tp1Parts.length > 3 ? tp1Parts[3].trim() : "";
            tp1JsonKeys     = tp1Parts.length > 4 ? tp1Parts[4].trim() : "";
        }

        CombinedSpec spec    = new CombinedSpec();
        spec.tp4             = tp4Spec;
        spec.tp1DirectCols   = (tp1DirectCols == null || tp1DirectCols.isEmpty()) ? null : tp1DirectCols;
        spec.tp1MetaCount    = tp1MetaCount;
        spec.tp1DestMetaCols = tp1DestMetaCols;
        spec.tp1SrcMetaCols  = tp1SrcMetaCols;
        spec.tp1JsonKeys     = tp1JsonKeys;

        return spec;
    }

    // =========================================================================
    // File operations
    // =========================================================================

    private boolean moveFile(File src, File destDir, int sno) {
        try {
            if (!destDir.exists()) Files.createDirectories(destDir.toPath());
            java.nio.file.Path target = destDir.toPath().resolve(src.getName());
            Files.move(src.toPath(), target, StandardCopyOption.REPLACE_EXISTING);
            log("INFO", "File moved | from=" + src.getAbsolutePath()
                    + " | to=" + target + " | sno=" + sno, null, "FILE_MOVED");
            return true;
        } catch (IOException ex) {
            log("ERROR", "File move failed | file=" + src.getName()
                    + " | dest=" + destDir.getAbsolutePath()
                    + " | sno=" + sno + " | Error: " + ex.getMessage(), null, "FILE_MOVE_FAIL");
            return false;
        }
    }

    private void silentDelete(File f) {
        if (f == null || !f.exists()) return;
        try {
            boolean deleted = f.delete();
            log("DEBUG", "File " + (deleted ? "deleted" : "delete failed")
                    + " | path=" + f.getAbsolutePath(), null, "FILE_DELETE");
        } catch (Exception ex) {
            log("WARN", "Exception deleting file | path=" + f.getAbsolutePath()
                    + " | Error: " + ex.getMessage(), null, "FILE_DELETE");
        }
    }

    // =========================================================================
    // CSV helpers
    // =========================================================================

    private String[] splitCsvLine(String line) {
        if (line == null) return new String[0];
        List<String> tokens  = new ArrayList<>();
        StringBuilder sb     = new StringBuilder();
        boolean inQuotes     = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    sb.append('"'); i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                tokens.add(sb.toString().trim());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        tokens.add(sb.toString().trim());
        return tokens.toArray(new String[0]);
    }

    private Map<String, Integer> buildHeaderIndex(String[] headers) {
        Map<String, Integer> map = new LinkedHashMap<>();
        for (int i = 0; i < headers.length; i++) {
            String h = (headers[i] != null) ? headers[i].trim() : "";
            if (!map.containsKey(h)) {
                map.put(h, i);
            } else {
                log("WARN", "Duplicate CSV header name '" + h
                        + "' at index " + i + "; first occurrence kept", null, "HEADER_WARN");
            }
        }
        return map;
    }

    private String safeGet(String[] arr, int idx) {
        if (arr == null || idx < 0 || idx >= arr.length) return null;
        String v = arr[idx];
        return (v == null || v.trim().isEmpty()) ? null : v.trim();
    }

    private String escapeCsv(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"")
                || value.contains("\n") || value.contains("\r")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }

    private String[] parseCommaSeparated(String input) {
        if (input == null || input.trim().isEmpty()) return new String[0];
        return Arrays.stream(input.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toArray(String[]::new);
    }

    private String[] parseHashSeparated(String input) {
        if (input == null || input.trim().isEmpty()) return new String[0];
        return Arrays.stream(input.split("#"))
                .map(String::trim).toArray(String[]::new);
    }

    // =========================================================================
    // Connection helpers
    // =========================================================================

    private Connection getPgConnection(String url, String user, String pass) {
        try {
            Class.forName("org.postgresql.Driver");
            Connection conn = DriverManager.getConnection(url, user, pass);
            conn.setAutoCommit(false);
            log("DEBUG", "PostgreSQL connection established | URL=" + url, null, "DB_CONNECT");
            return conn;
        } catch (ClassNotFoundException ex) {
            log("ERROR", "PostgreSQL JDBC driver not found | Error: " + ex.getMessage(),
                    null, "DB_CONNECT");
            return null;
        } catch (Exception ex) {
            log("ERROR", "PostgreSQL connection failed | URL=" + url
                    + " | Error: " + ex.getMessage(), null, "DB_CONNECT");
            return null;
        }
    }

    private boolean validateMysqlConnection(String url, String user, String pass) {
        log("INFO", "Validating MySQL connection | URL=" + url, null, "VALIDATION_START");
        Connection conn = null;
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(url, user, pass);
            log("INFO", "MySQL connection validated successfully", null, "VALIDATION_OK");
            return true;
        } catch (Exception ex) {
            log("ERROR", "MySQL connection FAILED | URL=" + url
                    + " | Error: " + ex.getMessage(), null, "VALIDATION_FAIL");
            return false;
        } finally {
            closeQuietly(null, null, conn);
        }
    }

    private boolean validatePgConnection(String url, String user, String pass) {
        log("INFO", "Validating PostgreSQL connection | URL=" + url, null, "VALIDATION_START");
        Connection conn = getPgConnection(url, user, pass);
        if (conn == null) {
            log("ERROR", "PostgreSQL connection FAILED", null, "VALIDATION_FAIL");
            return false;
        }
        closeQuietly(null, null, conn);
        log("INFO", "PostgreSQL connection validated successfully", null, "VALIDATION_OK");
        return true;
    }

    private void closeQuietly(ResultSet rs, Statement stmt, Connection conn) {
        try { if (rs   != null) rs.close();   } catch (Exception ignored) {}
        try { if (stmt != null) stmt.close();  } catch (Exception ignored) {}
        try { if (conn != null) conn.close();  } catch (Exception ignored) {}
    }

    // =========================================================================
    // Logging
    // =========================================================================

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !DEBUG_ENABLED) return;
        String timestamp = LocalDateTime.now().format(LOG_FORMAT);
        StringBuilder sb = new StringBuilder(512);
        sb.append("Time:").append(timestamp)
          .append(" [").append(capitalize(level)).append("] ")
          .append("Process_Name: ").append(PROCESS_NAME).append(" ");
        if (serverIP != null && !serverIP.isEmpty())
            sb.append("Server_IP: ").append(serverIP).append(" ");
        sb.append("Process_Version: ").append(PROCESS_VERSION)
          .append(" Log_Level: EVT_LOG ")
          .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
          .append(" Description: ").append(message != null ? message : "");
        System.out.println(sb.toString().trim());
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }
}
