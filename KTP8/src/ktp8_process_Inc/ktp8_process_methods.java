package ktp8_process_Inc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class ktp8_process_methods {

    private final String processName = "KYC_RELATIONS_PARSER_UPDATE";
    private final String processVersion = "1.0.0";
    private final boolean debugEnabled = true;
    private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String TARGET_TABLE = "kyc.kyc_legal_entity_related_parties";
    private static final int PARALLEL_FILES = 5;
    private static final int DEFAULT_BATCH_SIZE = 100000;

    // Headers that are mapped to real columns (or used as match keys) and therefore
    // excluded from becoming "extra" top-level entries when building the metadata map.
    // krl_id is intentionally kept as an explicit key inside metadata JSON as well
    // (matches existing data shape), so it stays in MAPPED_HEADERS to avoid duplication
    // via the generic "everything else goes to metadata" loop -- we add it back in
    // explicitly when building the metadata map.
    private static final Set<String> MAPPED_HEADERS = new HashSet<String>();

    static {
        MAPPED_HEADERS.add("krl_kyc_no");
        MAPPED_HEADERS.add("krl_id");
        MAPPED_HEADERS.add("krl_relation_kyc_no");
        MAPPED_HEADERS.add("krl_relation_code");
        MAPPED_HEADERS.add("krl_rel_din");
        MAPPED_HEADERS.add("krl_rel_type_other");
        MAPPED_HEADERS.add("krl_rel_status");
        MAPPED_HEADERS.add("krl_created_date");
        MAPPED_HEADERS.add("krl_updated_date");
        MAPPED_HEADERS.add("krl_created_by");
        MAPPED_HEADERS.add("krl_updated_by");
        MAPPED_HEADERS.add("krl_rel_name");
    }

    public void processDirectory(String inputDirectory,
                                 String processedDirectory,
                                 String jdbcUrl,
                                 String dbUser,
                                 String dbPass,
                                 String serverIP) {

        File dir = new File(inputDirectory);

        File[] csvFiles = dir.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File d, String name) {
                return name != null && name.toLowerCase().endsWith(".csv");
            }
        });

        if (csvFiles == null || csvFiles.length == 0) {
            log("WARN", "No CSV files found in directory: " + inputDirectory, serverIP, "VALIDATION");
            return;
        }

        Arrays.sort(csvFiles);

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            log("ERROR", "PostgreSQL driver not found: " + safeMsg(e), serverIP, "FATAL");
            log("INFO",
                    "Directory summary: total_files=" + csvFiles.length
                            + " | completed_files=0"
                            + " | retained_files=0"
                            + " | empty_files=0"
                            + " | header_only_files=0"
                            + " | error_files=" + csvFiles.length
                            + " | total_rows=0"
                            + " | updated=0"
                            + " | not_found=0"
                            + " | skipped_requirement_legal_entity_prefix=0"
                            + " | db_failed=0"
                            + " | row_errors=0"
                            + " | batch_failures=0"
                            + " | blank_lines=0",
                    serverIP,
                    "END");
            return;
        }

        int poolSize = Math.min(PARALLEL_FILES, csvFiles.length);
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        List<Future<FileResult>> futures = new ArrayList<Future<FileResult>>();

        log("INFO",
                "Directory start. files=" + csvFiles.length + " | parallel_files=" + poolSize,
                serverIP,
                "START");

        for (final File csvFile : csvFiles) {
            if (csvFile == null || !csvFile.isFile()) {
                continue;
            }

            futures.add(executor.submit(new Callable<FileResult>() {
                @Override
                public FileResult call() {
                    return processFile(csvFile, processedDirectory, jdbcUrl, dbUser, dbPass, serverIP);
                }
            }));
        }

        int totalFiles = 0;
        int completedFiles = 0;
        int retainedFiles = 0;
        int emptyFiles = 0;
        int headerOnlyFiles = 0;
        int errorFiles = 0;

        long totalDataRows = 0;
        long totalUpdated = 0;
        long totalNotFound = 0;
        long totalRequirementSkips = 0;
        long totalDbFailures = 0;
        long totalRowErrors = 0;
        long totalBatchFailures = 0;
        long totalBlankLines = 0;

        for (Future<FileResult> future : futures) {
            try {
                FileResult result = future.get();
                if (result == null) {
                    continue;
                }

                totalFiles++;
                totalDataRows += result.dataRows;
                totalUpdated += result.updated;
                totalNotFound += result.notFound;
                totalRequirementSkips += result.requirementSkips;
                totalDbFailures += result.dbFailures;
                totalRowErrors += result.rowErrors;
                totalBatchFailures += result.batchFailures;
                totalBlankLines += result.blankLines;

                if (result.statusCode == 0) {
                    completedFiles++;
                } else if (result.statusCode == 1) {
                    emptyFiles++;
                } else if (result.statusCode == 2) {
                    headerOnlyFiles++;
                } else if (result.statusCode == 4) {
                    errorFiles++;
                } else {
                    retainedFiles++;
                }

            } catch (Exception e) {
                errorFiles++;
                log("ERROR", "Directory task failed: " + safeMsg(e), serverIP, "ERROR");
            }
        }

        executor.shutdown();

        log("INFO",
                "Directory summary: total_files=" + totalFiles
                        + " | completed_files=" + completedFiles
                        + " | retained_files=" + retainedFiles
                        + " | empty_files=" + emptyFiles
                        + " | header_only_files=" + headerOnlyFiles
                        + " | error_files=" + errorFiles
                        + " | total_rows=" + totalDataRows
                        + " | updated=" + totalUpdated
                        + " | not_found=" + totalNotFound
                        + " | skipped_requirement_legal_entity_prefix=" + totalRequirementSkips
                        + " | db_failed=" + totalDbFailures
                        + " | row_errors=" + totalRowErrors
                        + " | batch_failures=" + totalBatchFailures
                        + " | blank_lines=" + totalBlankLines,
                serverIP,
                "END");
    }

    private FileResult processFile(File csvFile,
                                   String processedDirectory,
                                   String jdbcUrl,
                                   String dbUser,
                                   String dbPass,
                                   String serverIP) {

        FileResult result = new FileResult(csvFile.getName());

        ScanResult scan = scanFile(csvFile);
        result.dataRows = scan.dataRows;
        result.blankLines = scan.blankLines;

        if (scan.emptyFile) {
            result.statusCode = 1;
            result.statusText = "EMPTY_FILE";
            log("WARN", "Skipping empty file: " + csvFile.getName(), serverIP, "EMPTY_FILE");
            logFileSummary(result, serverIP);
            return result;
        }

        if (scan.headerOnly) {
            result.statusCode = 2;
            result.statusText = "HEADER_ONLY";
            log("WARN", "Skipping header-only file: " + csvFile.getName(), serverIP, "EMPTY_FILE");
            logFileSummary(result, serverIP);
            return result;
        }

        int effectiveBatchSize = Math.min(DEFAULT_BATCH_SIZE, Math.max(1, scan.dataRows));
        result.effectiveBatchSize = effectiveBatchSize;

        String updateSql = buildUpdateSql();

        log("INFO",
                "File start: " + csvFile.getName()
                        + " | total_rows=" + scan.dataRows
                        + " | effective_batch_size=" + effectiveBatchSize,
                serverIP,
                "FILE_START");

        log("INFO", "Update SQL: " + updateSql, serverIP, "SQL");

        Connection conn = null;
        PreparedStatement ps = null;
        BufferedReader br = null;

        try {
            conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
            conn.setAutoCommit(false);
            ps = conn.prepareStatement(updateSql);

            br = new BufferedReader(new FileReader(csvFile));

            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                result.statusCode = 1;
                result.statusText = "EMPTY_FILE";
                log("WARN", "Skipping empty file: " + csvFile.getName(), serverIP, "EMPTY_FILE");
                logFileSummary(result, serverIP);
                return result;
            }

            String[] headers = splitCsvLine(headerLine);
            List<PreparedRow> batchBuffer = new ArrayList<PreparedRow>(effectiveBatchSize);

            String line;
            int lineNo = 1;

            while ((line = br.readLine()) != null) {
                lineNo++;

                if (line.trim().isEmpty()) {
                    continue;
                }

                Map<String, String> rowMap = buildRowMap(headers, splitCsvLine(line));

                try {
                    String legalEntityCkyc = cleanValue(getValue(rowMap, "krl_kyc_no"));

                    if (!startsWith789(legalEntityCkyc)) {
                        result.requirementSkips++;
                        continue;
                    }

                    PreparedRow preparedRow = buildPreparedRow(rowMap, lineNo);
                    batchBuffer.add(preparedRow);

                    if (batchBuffer.size() >= effectiveBatchSize) {
                        boolean ok = flushBatch(batchBuffer, conn, ps, result, serverIP, csvFile.getName());
                        if (!ok || result.fatalError) {
                            break;
                        }
                    }

                } catch (Exception rowEx) {
                    result.rowErrors++;
                    result.fatalError = true;
                    result.fatalReason = "Row prepare failed at line " + lineNo + " : " + safeMsg(rowEx);
                    break;
                }
            }

            if (!result.fatalError && !batchBuffer.isEmpty()) {
                boolean ok = flushBatch(batchBuffer, conn, ps, result, serverIP, csvFile.getName());
                if (!ok || result.fatalError) {
                    // handled below
                }
            }

            if (result.fatalError) {
                try {
                    conn.rollback();
                } catch (Exception ignore) {
                }

                result.statusCode = 4;
                result.statusText = "ERROR";
                result.updated = 0;
                result.stagedUpdated = 0;

                log("ERROR",
                        "File error: " + csvFile.getName()
                                + " | reason=" + result.fatalReason,
                        serverIP,
                        "FILE_ERROR");

                logFileSummary(result, serverIP);
                return result;
            }

            result.updated = result.stagedUpdated;

            if (result.updated > 0) {
                boolean moved = moveFile(csvFile, processedDirectory);
                result.moved = moved;

                if (moved) {
                    result.statusCode = 0;
                    result.statusText = "COMPLETED_MOVED";
                    log("INFO", "File moved successfully: " + csvFile.getName(), serverIP, "FILE_MOVE");
                } else {
                    result.statusCode = 3;
                    result.statusText = "MOVE_FAILED";
                    log("ERROR", "File processed but move failed: " + csvFile.getName(), serverIP, "FILE_MOVE");
                }
            } else {
                result.statusCode = 3;
                result.statusText = "RETAINED_NO_UPDATES";
                log("WARN",
                        "File retained in source directory. file=" + csvFile.getName()
                                + " | updated=0"
                                + " | not_found=" + result.notFound
                                + " | skipped_requirement_legal_entity_prefix=" + result.requirementSkips,
                        serverIP,
                        "FILE_RETAIN");
            }

            try {
                conn.commit();
            } catch (Exception e) {
                result.statusCode = 4;
                result.statusText = "ERROR";
                result.fatalError = true;
                result.fatalReason = "Commit failed: " + safeMsg(e);
                log("ERROR", "File error: " + csvFile.getName() + " | reason=" + result.fatalReason, serverIP, "FILE_ERROR");
            }

        } catch (Exception e) {
            result.statusCode = 4;
            result.statusText = "ERROR";
            result.fatalError = true;
            result.fatalReason = safeMsg(e);
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (Exception ignore) {
            }
            log("ERROR", "Parser failed for file " + csvFile.getName() + ": " + safeMsg(e), serverIP, "FATAL");
        } finally {
            closeQuietly(br);
            closeQuietly(ps);
            closeQuietly(conn);
        }

        logFileSummary(result, serverIP);
        return result;
    }

    private ScanResult scanFile(File csvFile) {
        ScanResult scan = new ScanResult();
        BufferedReader br = null;

        try {
            br = new BufferedReader(new FileReader(csvFile));

            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                scan.emptyFile = true;
                return scan;
            }

            String line;
            int dataRows = 0;
            int blankLines = 0;

            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    blankLines++;
                } else {
                    dataRows++;
                }
            }

            scan.dataRows = dataRows;
            scan.blankLines = blankLines;
            scan.headerOnly = (dataRows == 0);

        } catch (Exception e) {
            scan.error = true;
        } finally {
            closeQuietly(br);
        }

        return scan;
    }

    /**
     * Executes the batch of UPDATE statements. Because each statement in an UPDATE
     * batch reports its own affected-row-count, we inspect the int[] returned by
     * executeBatch() to tell "matched & updated" apart from "no matching row"
     * (SUCCESS_NO_INFO / 0) so not_found can be tracked accurately.
     */
    private boolean flushBatch(List<PreparedRow> batchBuffer,
                               Connection conn,
                               PreparedStatement ps,
                               FileResult result,
                               String serverIP,
                               String fileName) {

        if (batchBuffer == null || batchBuffer.isEmpty()) {
            return true;
        }

        try {
            ps.clearBatch();

            for (PreparedRow row : batchBuffer) {
                bindRow(ps, row);
                ps.addBatch();
            }

            int[] counts = ps.executeBatch();

            for (int c : counts) {
                if (c == Statement_SUCCESS_NO_INFO) {
                    // Driver couldn't report an exact count; assume it matched.
                    result.stagedUpdated++;
                } else if (c > 0) {
                    result.stagedUpdated += c;
                } else {
                    // c == 0 -> UPDATE ran but WHERE clause matched no row.
                    result.notFound++;
                }
            }

            batchBuffer.clear();
            return true;

        } catch (BatchUpdateException batchEx) {
            result.batchFailures++;
            result.fatalError = true;
            result.fatalReason = extractDbMessage(batchEx);

            log("ERROR",
                    "Batch failed for file=" + fileName
                            + " | batch_size=" + batchBuffer.size()
                            + " | reason=" + result.fatalReason,
                    serverIP,
                    "BATCH_ERROR");

            try {
                conn.rollback();
            } catch (Exception ignore) {
            }

            batchBuffer.clear();
            return false;

        } catch (SQLException sqlEx) {
            result.dbFailures++;
            result.fatalError = true;
            result.fatalReason = extractDbMessage(sqlEx);

            log("ERROR",
                    "Batch failed for file=" + fileName
                            + " | batch_size=" + batchBuffer.size()
                            + " | reason=" + result.fatalReason,
                    serverIP,
                    "BATCH_ERROR");

            try {
                conn.rollback();
            } catch (Exception ignore) {
            }

            batchBuffer.clear();
            return false;

        } catch (Exception ex) {
            result.rowErrors++;
            result.fatalError = true;
            result.fatalReason = safeMsg(ex);

            log("ERROR",
                    "Batch failed for file=" + fileName
                            + " | batch_size=" + batchBuffer.size()
                            + " | reason=" + result.fatalReason,
                    serverIP,
                    "BATCH_ERROR");

            try {
                conn.rollback();
            } catch (Exception ignore) {
            }

            batchBuffer.clear();
            return false;
        }
    }

    // java.sql.Statement.SUCCESS_NO_INFO is -2; kept as a local constant so this
    // file has no additional import beyond what's already here.
    private static final int Statement_SUCCESS_NO_INFO = -2;

    private PreparedRow buildPreparedRow(Map<String, String> rowMap, int lineNo) {

        String legalEntityCkyc = cleanValue(getValue(rowMap, "krl_kyc_no"));
        String krlId = cleanValue(getValue(rowMap, "krl_id"));
        Long relatedPersonCkyc = parseLong(getValue(rowMap, "krl_relation_kyc_no"));
        String relationCode = cleanValue(getValue(rowMap, "krl_relation_code"));
        String directorId = cleanValue(getValue(rowMap, "krl_rel_din"));
        String otherRelationDesc = cleanValue(getValue(rowMap, "krl_rel_type_other"));
        Boolean isActive = parseBooleanStatus(getValue(rowMap, "krl_rel_status"));
        Timestamp createdAt = parseTimestamp(getValue(rowMap, "krl_created_date"));
        Timestamp updatedAt = parseTimestamp(getValue(rowMap, "krl_updated_date"));
        String createdBy = cleanValue(getValue(rowMap, "krl_created_by"));
        String updatedBy = cleanValue(getValue(rowMap, "krl_updated_by"));
        String name = cleanValue(getValue(rowMap, "krl_rel_name"));

        String relationshipType = mapRelationshipType(relationCode);
        String metadataJson = toJson(buildMetadataMap(rowMap, krlId));

        return new PreparedRow(
                lineNo,
                legalEntityCkyc,
                krlId,
                relatedPersonCkyc,
                relationshipType,
                directorId,
                otherRelationDesc,
                isActive,
                createdAt,
                updatedAt,
                createdBy,
                updatedBy,
                name,
                metadataJson);
    }

    /**
     * Binds SET columns first (in the order they appear in buildUpdateSql()),
     * then binds the WHERE-clause match keys last: legal_entity_ckyc and
     * metadata->>'krl_id'.
     */
    private void bindRow(PreparedStatement ps, PreparedRow row) throws SQLException {
        int idx = 1;

        // related_person_ckyc (BIGINT)
        if (row.relatedPersonCkyc == null) {
            ps.setNull(idx++, Types.BIGINT);
        } else {
            ps.setLong(idx++, row.relatedPersonCkyc.longValue());
        }

        ps.setString(idx++, row.relationshipType);

        ps.setNull(idx++, Types.NUMERIC); // ownership_percentage

        if (row.directorId == null) {
            ps.setNull(idx++, Types.VARCHAR);
        } else {
            ps.setString(idx++, row.directorId);
        }

        if (row.otherRelationshipDescription == null) {
            ps.setNull(idx++, Types.VARCHAR);
        } else {
            ps.setString(idx++, row.otherRelationshipDescription);
        }

        ps.setNull(idx++, Types.DATE); // appointment_date
        ps.setNull(idx++, Types.DATE); // resignation_date

        if (row.isActive == null) {
            ps.setNull(idx++, Types.BOOLEAN);
        } else {
            ps.setBoolean(idx++, row.isActive.booleanValue());
        }

        if (row.createdAt == null) {
            ps.setNull(idx++, Types.TIMESTAMP);
        } else {
            ps.setTimestamp(idx++, row.createdAt);
        }

        if (row.updatedAt == null) {
            ps.setNull(idx++, Types.TIMESTAMP);
        } else {
            ps.setTimestamp(idx++, row.updatedAt);
        }

        if (row.createdBy == null) {
            ps.setNull(idx++, Types.VARCHAR);
        } else {
            ps.setString(idx++, row.createdBy);
        }

        if (row.updatedBy == null) {
            ps.setNull(idx++, Types.VARCHAR);
        } else {
            ps.setString(idx++, row.updatedBy);
        }

        if (row.name == null) {
            ps.setNull(idx++, Types.VARCHAR);
        } else {
            ps.setString(idx++, row.name);
        }

        ps.setNull(idx++, Types.VARCHAR); // controlling_interest
        ps.setObject(idx++, row.metadataJson, Types.OTHER); // metadata (full replace, JSONB)

        // ---- WHERE clause bind params ----
        if (row.legalEntityCkyc == null) {
            ps.setNull(idx++, Types.BIGINT);
        } else {
            ps.setLong(idx++, Long.parseLong(row.legalEntityCkyc));
        }

        if (row.krlId == null) {
            ps.setNull(idx++, Types.VARCHAR);
        } else {
            ps.setString(idx++, row.krlId);
        }
    }

    private boolean moveFile(File sourceFile, String processedDirectory) {
        try {
            File targetDir = new File(processedDirectory);
            if (!targetDir.exists()) {
                targetDir.mkdirs();
            }

            File targetFile = new File(targetDir, sourceFile.getName());
            Files.move(sourceFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * UPDATE version of the original INSERT statement.
     * rp_id is never touched (identity/PK stays as-is on update).
     * Match keys: legal_entity_ckyc + metadata->>'krl_id' (JSONB text extraction).
     */
    private String buildUpdateSql() {
        return "UPDATE " + TARGET_TABLE + " SET "
                + "related_person_ckyc = ?, "
                + "relationship_type = ?, "
                + "ownership_percentage = ?, "
                + "director_identification_number = ?, "
                + "other_relationship_description = ?, "
                + "appointment_date = ?, "
                + "resignation_date = ?, "
                + "is_active = ?, "
                + "created_at = ?, "
                + "updated_at = ?, "
                + "created_by = ?, "
                + "updated_by = ?, "
                + "name = ?, "
                + "controlling_interest = ?, "
                + "metadata = ? "
                + "WHERE legal_entity_ckyc = ? "
                + "AND metadata ->> 'krl_id' = ?";
    }

    private Map<String, String> buildRowMap(String[] headers, String[] values) {
        Map<String, String> map = new LinkedHashMap<String, String>();

        int max = Math.max(headers.length, values.length);
        for (int i = 0; i < max; i++) {
            String header = i < headers.length ? normalize(headers[i]) : "";
            String value = i < values.length ? cleanValue(unquote(values[i])) : null;

            if (header.length() > 0) {
                map.put(header, value);
            }
        }

        return map;
    }

    /**
     * Builds the metadata JSON map. krl_id is a match key (also bound separately
     * in the WHERE clause) but is still written into metadata explicitly, matching
     * the existing data shape (metadata.krl_id present alongside all other
     * unmapped krl_* columns).
     */
    private Map<String, String> buildMetadataMap(Map<String, String> rowMap, String krlId) {
        Map<String, String> meta = new LinkedHashMap<String, String>();

        meta.put("krl_id", krlId);

        for (Map.Entry<String, String> e : rowMap.entrySet()) {
            if (!MAPPED_HEADERS.contains(e.getKey())) {
                meta.put(e.getKey().toLowerCase(), e.getValue());
            }
        }

        return meta;
    }

    private String getValue(Map<String, String> rowMap, String key) {
        if (rowMap == null || key == null) {
            return null;
        }
        return rowMap.get(normalize(key));
    }

    private String mapRelationshipType(String code) {
        String c = cleanValue(code);
        if (c == null) {
            return null;
        }

        if ("04".equals(c)) return "DIRECTOR";
        if ("05".equals(c)) return "PROMOTER";
        if ("06".equals(c)) return "KARTA";
        if ("07".equals(c)) return "TRUSTEE";
        if ("08".equals(c)) return "PARTNER";
        if ("09".equals(c)) return "PROPRIETOR";
        if ("10".equals(c)) return "COURT_APPOINTED_OFFICIAL";
        if ("11".equals(c)) return "BENEFICIARY";
        if ("12".equals(c)) return "AUTHORIZED_SIGNATORY";
        if ("13".equals(c)) return "BENEFICIAL_OWNER";
        if ("14".equals(c)) return "POWER_OF_ATTORNEY_HOLDER";
        if ("15".equals(c)) return "OTHER";

        return c;
    }

    private Boolean parseBooleanStatus(String value) {
        String v = cleanValue(value);
        if (v == null) {
            return null;
        }

        if ("1".equals(v) || "01".equals(v) || "true".equalsIgnoreCase(v) || "yes".equalsIgnoreCase(v)) {
            return Boolean.TRUE;
        }

        if ("0".equals(v) || "00".equals(v) || "false".equalsIgnoreCase(v) || "no".equalsIgnoreCase(v)) {
            return Boolean.FALSE;
        }

        return null;
    }

    private boolean startsWith789(String value) {
        String v = cleanValue(value);
        if (v == null || v.isEmpty()) {
            return false;
        }

        char c = v.charAt(0);
        return c == '7' || c == '8' || c == '9';
    }

    private Timestamp parseTimestamp(String value) {
        String v = cleanValue(value);
        if (v == null) {
            return null;
        }

        try {
            return Timestamp.valueOf(LocalDateTime.parse(v, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        } catch (Exception ignore) {
        }

        try {
            return Timestamp.valueOf(LocalDateTime.parse(v, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")));
        } catch (Exception ignore) {
        }

        try {
            return Timestamp.valueOf(LocalDateTime.parse(v, DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        } catch (Exception ignore) {
        }

        try {
            return Timestamp.from(OffsetDateTime.parse(v).toInstant());
        } catch (Exception ignore) {
        }

        try {
            LocalDate date = LocalDate.parse(v, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            return Timestamp.valueOf(date.atStartOfDay());
        } catch (Exception ignore) {
        }

        try {
            LocalDate date = LocalDate.parse(v, DateTimeFormatter.ofPattern("yyyy/MM/dd"));
            return Timestamp.valueOf(date.atStartOfDay());
        } catch (Exception ignore) {
        }

        return null;
    }

    private String toJson(Map<String, String> map) {
        StringBuilder json = new StringBuilder("{");
        int i = 0;

        for (Map.Entry<String, String> e : map.entrySet()) {
            if (i++ > 0) {
                json.append(",");
            }

            json.append("\"").append(escapeJson(e.getKey())).append("\":");

            if (e.getValue() == null) {
                json.append("null");
            } else {
                json.append("\"").append(escapeJson(e.getValue())).append("\"");
            }
        }

        json.append("}");
        return json.toString();
    }

    private String[] splitCsvLine(String line) {
        List<String> result = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == '|' && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(ch);
            }
        }

        result.add(current.toString());
        return result.toArray(new String[result.size()]);
    }

    private String normalize(String value) {
        String v = unquote(value);
        if (v == null) {
            return "";
        }
        return v.trim().toLowerCase();
    }

    private String cleanValue(String value) {
        if (value == null) {
            return null;
        }

        String v = value.trim();
        if (v.length() == 0) {
            return null;
        }

        if ("\\N".equalsIgnoreCase(v)) {
            return null;
        }

        return v;
    }

    private String unquote(String value) {
        if (value == null) {
            return null;
        }

        String v = value.trim();
        if (v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
            v = v.substring(1, v.length() - 1);
        }
        return v;
    }

    private String safeMsg(Exception e) {
        return e == null ? "" : e.getMessage();
    }

    private String extractDbMessage(Exception e) {
        if (e == null) {
            return "";
        }

        if (e instanceof BatchUpdateException) {
            BatchUpdateException b = (BatchUpdateException) e;
            if (b.getNextException() != null && b.getNextException().getMessage() != null) {
                return b.getNextException().getMessage();
            }
        }

        if (e.getCause() != null && e.getCause().getMessage() != null) {
            return e.getCause().getMessage();
        }

        return e.getMessage() == null ? "" : e.getMessage();
    }

    private void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception ignore) {
            }
        }
    }

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
            return;
        }

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp).append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(processName).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(processVersion).append(" ")
                .append("Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                .append(" Description: ")
                .append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private void logFileSummary(FileResult result, String serverIP) {
        String statusText = getStatusText(result.statusCode);

        log("INFO",
                "Summary Column: file=" + result.fileName
                        + " | status=" + statusText
                        + " | data_rows=" + result.dataRows
                        + " | updated=" + result.updated
                        + " | not_found=" + result.notFound
                        + " | skipped_total=" + (result.requirementSkips + result.dbFailures + result.rowErrors + result.batchFailures)
                        + " | skipped_requirement_legal_entity_prefix=" + result.requirementSkips
                        + " | db_failed=" + result.dbFailures
                        + " | row_errors=" + result.rowErrors
                        + " | batch_failures=" + result.batchFailures
                        + " | blank_lines=" + result.blankLines
                        + " | effective_batch_size=" + result.effectiveBatchSize
                        + " | moved=" + result.moved,
                serverIP,
                "FILE_SUMMARY");
    }

    private String getStatusText(int statusCode) {
        if (statusCode == 0) return "COMPLETED_MOVED";
        if (statusCode == 1) return "EMPTY_FILE";
        if (statusCode == 2) return "HEADER_ONLY";
        if (statusCode == 3) return "RETAINED_NO_UPDATES";
        return "ERROR";
    }

    private String capitalize(String value) {
        if (value == null || value.trim().isEmpty()) {
            return "";
        }
        String v = value.trim().toLowerCase();
        return Character.toUpperCase(v.charAt(0)) + v.substring(1);
    }

    private static class ScanResult {
        boolean emptyFile = false;
        boolean headerOnly = false;
        boolean error = false;
        int dataRows = 0;
        int blankLines = 0;
    }

    private Long parseLong(String value) {
        String v = cleanValue(value);

        if (v == null) {
            return null;
        }

        try {
            return Long.valueOf(v);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid BIGINT value: '" + v + "'",
                    e
            );
        }
    }

    private static class PreparedRow {
        final int lineNo;
        final String legalEntityCkyc;
        final String krlId;
        final Long relatedPersonCkyc;
        final String relationshipType;
        final String directorId;
        final String otherRelationshipDescription;
        final Boolean isActive;
        final Timestamp createdAt;
        final Timestamp updatedAt;
        final String createdBy;
        final String updatedBy;
        final String name;
        final String metadataJson;

        PreparedRow(int lineNo,
                    String legalEntityCkyc,
                    String krlId,
                    Long relatedPersonCkyc,
                    String relationshipType,
                    String directorId,
                    String otherRelationshipDescription,
                    Boolean isActive,
                    Timestamp createdAt,
                    Timestamp updatedAt,
                    String createdBy,
                    String updatedBy,
                    String name,
                    String metadataJson) {
            this.lineNo = lineNo;
            this.legalEntityCkyc = legalEntityCkyc;
            this.krlId = krlId;
            this.relatedPersonCkyc = relatedPersonCkyc;
            this.relationshipType = relationshipType;
            this.directorId = directorId;
            this.otherRelationshipDescription = otherRelationshipDescription;
            this.isActive = isActive;
            this.createdAt = createdAt;
            this.updatedAt = updatedAt;
            this.createdBy = createdBy;
            this.updatedBy = updatedBy;
            this.name = name;
            this.metadataJson = metadataJson;
        }
    }

    private String escapeJson(String value) {

        if (value == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < value.length(); i++) {

            char c = value.charAt(i);

            switch (c) {

            case '\\':
                sb.append("\\\\");
                break;

            case '"':
                sb.append("\\\"");
                break;

            case '\b':
                sb.append("\\b");
                break;

            case '\f':
                sb.append("\\f");
                break;

            case '\n':
                sb.append("\\n");
                break;

            case '\r':
                sb.append("\\r");
                break;

            case '\t':
                sb.append("\\t");
                break;

            default:
                if (c < 32) {
                    sb.append(String.format("\\u%04x", (int) c));
                } else {
                    sb.append(c);
                }
            }
        }

        return sb.toString();
    }

    private static class FileResult {
        final String fileName;
        int dataRows = 0;
        int blankLines = 0;
        int updated = 0;
        int stagedUpdated = 0;
        int notFound = 0;
        int requirementSkips = 0;
        int dbFailures = 0;
        int rowErrors = 0;
        int batchFailures = 0;
        int effectiveBatchSize = 0;
        int statusCode = 4;
        String statusText = "ERROR";
        boolean moved = false;
        boolean fatalError = false;
        String fatalReason = "";

        FileResult(String fileName) {
            this.fileName = fileName;
        }
    }
}