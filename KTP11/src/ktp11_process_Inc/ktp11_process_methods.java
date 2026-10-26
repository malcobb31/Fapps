package ktp11_process_Inc;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ktp11_process_methods {

    private final String inputPath;
    private final String processedDir;
    private final String targetJdbcUrl;
    private final String targetDbUser;
    private final String targetDbPassword;
    private final String appJdbcUrl;
    private final String appDbUser;
    private final String appDbPassword;
    private final int configuredBatchSize;
    private final String serverIP;

    private final String processName = "FI_THRESHOLD_LIMITS_ETL_UPDATE";
    private final String processVersion = "1.0.0";
    private final boolean debugEnabled = false;
    private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String TARGET_TABLE = "kyc.re_download_limit";

    // java.sql.Statement.SUCCESS_NO_INFO, kept local to avoid an extra import.
    private static final int STATEMENT_SUCCESS_NO_INFO = -2;

    /**
     * UPDATE version of the original INSERT_SQL.
     * id is never touched on update. Match key: re_id (unique on the table),
     * which is resolved per-row from ftl_fi_code via the app DB lookup map
     * (reportingEntityMap), not read directly from the CSV.
     * metadata is a FULL replace here (unlike ktp8/ktp9), per requirement.
     */
    private static final String UPDATE_SQL =
            "UPDATE kyc.re_download_limit SET " +
            "institution_type = ?, " +
            "daily_limit = ?, " +
            "is_manual = ?, " +
            "first_download_at = ?, " +
            "last_calculated_at = ?, " +
            "created_at = ?, " +
            "updated_at = ?, " +
            "metadata = ?::jsonb " +
            "WHERE re_id = ?";

    private static final String SQL_REPORTING_ENTITY_ALL =
            "SELECT fi_code, id FROM re_mgmt.reporting_entity";

    private final Map<String, String> reportingEntityMap = new HashMap<String, String>();

    public ktp11_process_methods(String inputPath,
                                 String processedDir,
                                 String targetJdbcUrl,
                                 String targetDbUser,
                                 String targetDbPassword,
                                 String appJdbcUrl,
                                 String appDbUser,
                                 String appDbPassword,
                                 int configuredBatchSize,
                                 String serverIP) {
        this.inputPath = inputPath;
        this.processedDir = processedDir;
        this.targetJdbcUrl = targetJdbcUrl;
        this.targetDbUser = targetDbUser;
        this.targetDbPassword = targetDbPassword;
        this.appJdbcUrl = appJdbcUrl;
        this.appDbUser = appDbUser;
        this.appDbPassword = appDbPassword;
        this.configuredBatchSize = configuredBatchSize <= 0 ? 20 : configuredBatchSize;
        this.serverIP = serverIP;
    }

    public void process() {
        log("INFO", "Process start: input_path=" + inputPath + " target_table=" + TARGET_TABLE, serverIP, "PROCESS_START");

        File source = new File(inputPath);
        List<File> files = collectInputFiles(source);

        if (source.isDirectory()) {
            log("INFO", "Directory start: directory=" + source.getAbsolutePath() + " total_files=" + files.size(), serverIP, "DIR_START");
        } else {
            log("INFO", "Directory start: directory=" + source.getAbsolutePath() + " total_files=1", serverIP, "DIR_START");
        }

        if (!loadAppLookupMaps()) {
            log("ERROR", "Fatal error: app lookup load failed", serverIP, "FATAL_ERROR");
            return;
        }

        DirectoryAggregate aggregate = new DirectoryAggregate();

        if (files.isEmpty()) {
            log("INFO", buildDirectorySummaryLine(aggregate, 0), serverIP, "DIR_SUMMARY");
            return;
        }

        int threadCount = files.size() < 5 ? files.size() : 5;
        if (threadCount <= 0) {
            threadCount = 1;
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<FileProcessResult>> futures = new ArrayList<Future<FileProcessResult>>();

        for (final File file : files) {
            futures.add(executor.submit(new Callable<FileProcessResult>() {
                @Override
                public FileProcessResult call() {
                    return processSingleFile(file);
                }
            }));
        }

        try {
            for (Future<FileProcessResult> future : futures) {
                FileProcessResult result;
                try {
                    result = future.get();
                } catch (Exception e) {
                    result = new FileProcessResult();
                    result.fileName = "UNKNOWN";
                    result.status = "ERROR";
                    result.errorMessage = rootCauseMessage(e);
                }
                aggregate.add(result);
            }
        } finally {
            executor.shutdown();
        }

        log("INFO", buildDirectorySummaryLine(aggregate, files.size()), serverIP, "DIR_SUMMARY");
    }

    private boolean loadAppLookupMaps() {
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            connection = DriverManager.getConnection(appJdbcUrl, appDbUser, appDbPassword);
            log("INFO", "Lookup load start: sql=" + SQL_REPORTING_ENTITY_ALL, serverIP, "LOOKUP");

            ps = connection.prepareStatement(SQL_REPORTING_ENTITY_ALL);
            rs = ps.executeQuery();

            while (rs.next()) {
                String fiCode = rs.getString("fi_code");
                String id = rs.getString("id");

                if (fiCode == null) {
                    continue;
                }

                fiCode = fiCode.trim();
                if (fiCode.length() == 0) {
                    continue;
                }

                if (id != null) {
                    id = id.trim();
                }

                if (id != null && id.length() > 0) {
                    reportingEntityMap.put(fiCode, id);
                }
            }

            log("INFO", "Lookup load complete: reporting_entity=" + reportingEntityMap.size(), serverIP, "LOOKUP");
            return true;

        } catch (SQLException e) {
            log("ERROR", "Lookup load failed: reason=" + rootCauseMessage(e), serverIP, "FATAL_ERROR");
            return false;

        } finally {
            closeQuietly(rs);
            closeQuietly(ps);
            closeQuietly(connection);
        }
    }

    private List<File> collectInputFiles(File source) {
        List<File> files = new ArrayList<File>();

        if (source == null || !source.exists()) {
            return files;
        }

        if (source.isFile()) {
            files.add(source);
            return files;
        }

        File[] csvFiles = source.listFiles(new java.io.FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name == null) {
                    return false;
                }
                return name.toLowerCase(Locale.ENGLISH).endsWith(".csv");
            }
        });

        if (csvFiles == null || csvFiles.length == 0) {
            return files;
        }

        Arrays.sort(csvFiles, new Comparator<File>() {
            @Override
            public int compare(File f1, File f2) {
                return f1.getName().compareToIgnoreCase(f2.getName());
            }
        });

        for (File file : csvFiles) {
            if (file != null && file.isFile()) {
                files.add(file);
            }
        }

        return files;
    }

    private FileProcessResult processSingleFile(File file) {
        FileProcessResult result = new FileProcessResult();
        result.fileName = file.getName();

        log("INFO", "File start: file=" + file.getName(), serverIP, "FILE_START");

        FileScanResult scanResult;
        try {
            scanResult = scanFile(file);
        } catch (IOException e) {
            result.status = "ERROR";
            result.errorMessage = rootCauseMessage(e);
            log("ERROR", "File error: file=" + file.getName() + " reason=" + rootCauseMessage(e), serverIP, "FATAL_ERROR");
            log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
            return result;
        }

        result.dataRows = scanResult.dataRows;
        result.blankLines = scanResult.blankLines;

        if (scanResult.empty) {
            result.status = "EMPTY";
            log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
            return result;
        }

        if (scanResult.headerOnly) {
            result.status = "HEADER_ONLY";
            log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
            return result;
        }

        result.effectiveBatchSize = scanResult.dataRows < configuredBatchSize ? (int) scanResult.dataRows : configuredBatchSize;
        if (result.effectiveBatchSize <= 0) {
            result.effectiveBatchSize = 1;
        }

        Connection targetConnection = null;
        PreparedStatement ps = null;
        BufferedReader reader = null;

        long updatedLocal = 0L;
        long notFoundLocal = 0L;
        long skippedLocal = 0L;
        long dbFailedLocal = 0L;
        long rowErrorsLocal = 0L;
        long batchFailuresLocal = 0L;

        int pendingBatchCount = 0;
        long rowNumber = 0L;

        try {
            targetConnection = DriverManager.getConnection(targetJdbcUrl, targetDbUser, targetDbPassword);
            targetConnection.setAutoCommit(false);

            log("INFO", "Update SQL: file=" + file.getName() + " sql=" + UPDATE_SQL, serverIP, "SQL");

            ps = targetConnection.prepareStatement(UPDATE_SQL);
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));

            String line;
            boolean headerFound = false;
            HeaderInfo headerInfo = null;

            while ((line = reader.readLine()) != null) {
                line = stripBom(line);

                if (line == null || line.trim().length() == 0) {
                    continue;
                }

                if (!headerFound) {
                    headerInfo = buildHeaderInfo(line);
                    headerFound = true;
                    continue;
                }

                rowNumber++;

                List<String> values = parsePipeLine(line);
                trimTrailingEmptyValues(values);

                while (values.size() < headerInfo.headers.size()) {
                    values.add(null);
                }

                if (values.size() > headerInfo.headers.size()) {
                    boolean onlyTrailingBlanks = true;
                    for (int i = headerInfo.headers.size(); i < values.size(); i++) {
                        String extra = values.get(i);
                        if (extra != null && extra.trim().length() > 0) {
                            onlyTrailingBlanks = false;
                            break;
                        }
                    }
                    if (onlyTrailingBlanks) {
                        while (values.size() > headerInfo.headers.size()) {
                            values.remove(values.size() - 1);
                        }
                    } else {
                        throw new IllegalArgumentException("Row has more columns than header");
                    }
                }

                TargetRow targetRow = buildTargetRow(headerInfo, values, file.getName(), rowNumber);

                if (targetRow.reId == null) {
                    // fi_code did not resolve to a known re_id -- soft skip, not fatal.
                    notFoundLocal++;
                    log("WARN",
                            "Reporting entity not found: file=" + file.getName()
                                    + " row=" + rowNumber
                                    + " fi_code=" + targetRow.sourceFiCode,
                            serverIP,
                            "RE_ID_NOT_FOUND");
                    continue;
                }

                setPreparedStatementValues(ps, targetRow);
                ps.addBatch();
                pendingBatchCount++;

                if (pendingBatchCount >= result.effectiveBatchSize) {
                    int[] counts = executeBatch(ps, pendingBatchCount);
                    long[] tally = tallyBatchCounts(counts);
                    updatedLocal += tally[0];
                    notFoundLocal += tally[1];
                    pendingBatchCount = 0;
                }
            }

            if (!headerFound) {
                result.status = "EMPTY";
                log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
                return result;
            }

            if (pendingBatchCount > 0) {
                int[] counts = executeBatch(ps, pendingBatchCount);
                long[] tally = tallyBatchCounts(counts);
                updatedLocal += tally[0];
                notFoundLocal += tally[1];
            }

            targetConnection.commit();

            result.status = "SUCCESS";
            result.inserted = updatedLocal;
            result.notFound = notFoundLocal;
            result.skipped = skippedLocal;
            result.dbFailed = dbFailedLocal;
            result.rowErrors = rowErrorsLocal;
            result.batchFailures = batchFailuresLocal;

            result.moved = moveFile(file);
            if (result.moved) {
                log("INFO", "File moved: file=" + file.getName() + " destination=" + new File(processedDir, file.getName()).getAbsolutePath(), serverIP, "FILE_MOVED");
            } else {
                result.status = "ERROR";
                log("ERROR", "File error: file=" + file.getName() + " reason=move failed", serverIP, "FATAL_ERROR");
            }

            log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
            return result;

        } catch (BatchFailureException e) {
            rollbackQuietly(targetConnection);
            result.status = "ERROR";
            result.inserted = 0L;
            result.notFound = 0L;
            result.skipped = skippedLocal;
            result.dbFailed = e.batchSize;
            result.rowErrors = rowErrorsLocal;
            result.batchFailures = batchFailuresLocal + 1L;
            log("ERROR", "Batch error: file=" + file.getName() + " batch_size=" + e.batchSize + " reason=" + e.getMessage(), serverIP, "BATCH_FAILURE");
            log("ERROR", "File error: file=" + file.getName() + " reason=" + rootCauseMessage(e), serverIP, "FATAL_ERROR");
            log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
            return result;

        } catch (IllegalArgumentException e) {
            rollbackQuietly(targetConnection);
            result.status = "ERROR";
            result.inserted = 0L;
            result.notFound = 0L;
            result.skipped = skippedLocal;
            result.dbFailed = dbFailedLocal;
            result.rowErrors = rowErrorsLocal + 1L;
            result.batchFailures = batchFailuresLocal;
            log("ERROR", "File error: file=" + file.getName() + " reason=" + rootCauseMessage(e), serverIP, "FATAL_ERROR");
            log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
            return result;

        } catch (SQLException e) {
            rollbackQuietly(targetConnection);
            result.status = "ERROR";
            result.inserted = 0L;
            result.notFound = 0L;
            result.skipped = skippedLocal;
            result.dbFailed = dbFailedLocal;
            result.rowErrors = rowErrorsLocal;
            result.batchFailures = batchFailuresLocal;
            log("ERROR", "File error: file=" + file.getName() + " reason=" + rootCauseMessage(e), serverIP, "FATAL_ERROR");
            log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
            return result;

        } catch (IOException e) {
            rollbackQuietly(targetConnection);
            result.status = "ERROR";
            result.inserted = 0L;
            result.notFound = 0L;
            result.skipped = skippedLocal;
            result.dbFailed = dbFailedLocal;
            result.rowErrors = rowErrorsLocal;
            result.batchFailures = batchFailuresLocal;
            log("ERROR", "File error: file=" + file.getName() + " reason=" + rootCauseMessage(e), serverIP, "FATAL_ERROR");
            log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
            return result;

        } catch (Exception e) {
            rollbackQuietly(targetConnection);
            result.status = "ERROR";
            result.inserted = 0L;
            result.notFound = 0L;
            result.skipped = skippedLocal;
            result.dbFailed = dbFailedLocal;
            result.rowErrors = rowErrorsLocal;
            result.batchFailures = batchFailuresLocal;
            log("ERROR", "File error: file=" + file.getName() + " reason=" + rootCauseMessage(e), serverIP, "FATAL_ERROR");
            log("INFO", buildFileSummaryLine(result), serverIP, "FILE_SUMMARY");
            return result;

        } finally {
            closeQuietly(reader);
            closeQuietly(ps);
            closeQuietly(targetConnection);
        }
    }

    private boolean moveFile(File sourceFile) {
        try {
            File destinationDir = new File(processedDir);
            File destinationFile = new File(destinationDir, sourceFile.getName());
            Files.move(sourceFile.toPath(), destinationFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private FileScanResult scanFile(File file) throws IOException {
        FileScanResult result = new FileScanResult();
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));
            String line;
            boolean headerFound = false;

            while ((line = reader.readLine()) != null) {
                line = stripBom(line);

                if (line == null || line.trim().length() == 0) {
                    result.blankLines++;
                    continue;
                }

                if (!headerFound) {
                    headerFound = true;
                    result.headerLine = line;
                    continue;
                }

                result.dataRows++;
            }

            if (!headerFound) {
                result.empty = true;
            } else if (result.dataRows == 0) {
                result.headerOnly = true;
            }
        } finally {
            closeQuietly(reader);
        }

        return result;
    }

    private HeaderInfo buildHeaderInfo(String headerLine) {
        if (headerLine == null) {
            throw new IllegalArgumentException("Header line is null");
        }

        List<String> parsed = parsePipeLine(headerLine);
        trimTrailingEmptyValues(parsed);

        if (parsed.isEmpty()) {
            throw new IllegalArgumentException("Header is empty");
        }

        HeaderInfo info = new HeaderInfo();
        info.headers = new ArrayList<String>();
        info.indexMap = new HashMap<String, Integer>();

        for (int i = 0; i < parsed.size(); i++) {
            String raw = parsed.get(i);
            String header = raw == null ? null : cleanValue(raw);
            if (header == null || header.trim().length() == 0) {
                throw new IllegalArgumentException("Blank header column found");
            }

            header = header.trim();
            info.headers.add(header);

            String normalized = normalize(header);
            if (!info.indexMap.containsKey(normalized)) {
                info.indexMap.put(normalized, Integer.valueOf(i));
            }
        }

        return info;
    }

    private TargetRow buildTargetRow(HeaderInfo headerInfo, List<String> values, String fileName, long rowNumber) {
        Map<String, String> rowMap = new LinkedHashMap<String, String>();

        for (int i = 0; i < headerInfo.headers.size(); i++) {
            String header = headerInfo.headers.get(i);
            String normalized = normalize(header);
            String value = i < values.size() ? values.get(i) : null;
            rowMap.put(normalized, cleanValue(value));
        }

        TargetRow row = new TargetRow();

        String srcFiCode = firstNonEmptyValue(rowMap, "ftl_fi_code");
        row.sourceFiCode = srcFiCode;

        if (srcFiCode == null || srcFiCode.trim().length() == 0) {
            row.reId = null;
        } else {
            row.reId = lookupReportingEntityId(srcFiCode);
        }

        row.institutionType = null;
        row.dailyLimit = parseIntegerValue(firstNonEmptyValue(rowMap, "ftl_threashold", "ftl_threshold", "daily_limit"), "daily_limit", fileName);
        row.isManual = false;
        row.firstDownloadAt = parseTimestampValue(firstNonEmptyValue(rowMap, "first_download_at"), false, "first_download_at", fileName);
        row.lastCalculatedAt = parseTimestampValue(firstNonEmptyValue(rowMap, "last_calculated_at"), false, "last_calculated_at", fileName);

        row.updatedAt = parseTimestampValue(
                firstNonEmptyValue(rowMap, "ftl_updated_date", "updated_at"),
                false,
                "updated_at",
                fileName);

        row.createdAt = parseTimestampValue(
                firstNonEmptyValue(rowMap, "ftl_created_date", "created_at"),
                false,
                "created_at",
                fileName);

        if (row.createdAt == null) {
            row.createdAt = row.updatedAt;
        }

        if (row.updatedAt == null) {
            row.updatedAt = row.createdAt;
        }

        if (row.createdAt == null) {
            row.createdAt = new Timestamp(System.currentTimeMillis());
        }

        if (row.updatedAt == null) {
            row.updatedAt = row.createdAt;
        }

        row.metadataJson = buildMetadataJson(headerInfo, rowMap);
        return row;
    }

    /**
     * Resolves fi_code -> re_id via the app DB lookup map loaded at startup.
     * Unlike the original INSERT version, this does NOT fall back to using the
     * fi_code itself as a substitute re_id -- for an UPDATE, a lookup miss means
     * there is no known target row, so it must resolve to null (soft skip) rather
     * than silently generating a WHERE clause that can't match anything real.
     */
    private String lookupReportingEntityId(String fiCode) {

        String value = cleanValue(fiCode);

        if (value == null) {
            return null;
        }

        return reportingEntityMap.get(value);
    }

    private String buildMetadataJson(HeaderInfo headerInfo, Map<String, String> rowMap) {
        Map<String, String> metadata = new LinkedHashMap<String, String>();

        for (int i = 0; i < headerInfo.headers.size(); i++) {
            String header = headerInfo.headers.get(i);
            String normalized = normalize(header);

            if (isMappedHeader(normalized)) {
                continue;
            }

            String value = rowMap.get(normalized);
            metadata.put(normalized, value);
        }

        return toJson(metadata);
    }

    private boolean isMappedHeader(String normalizedHeader) {
        if (normalizedHeader == null) {
            return false;
        }

        return "ftl_created_by".equals(normalizedHeader)
                || "ftl_created_date".equals(normalizedHeader)
                || "ftl_fi_code".equals(normalizedHeader)
                || "ftl_min_balance".equals(normalizedHeader)
                || "ftl_min_balance_email_status".equals(normalizedHeader)
                || "ftl_status".equals(normalizedHeader)
                || "ftl_status_updated_date".equals(normalizedHeader)
                || "ftl_threashold".equals(normalizedHeader)
                || "ftl_threshold".equals(normalizedHeader)
                || "ftl_threshold_email_status".equals(normalizedHeader)
                || "ftl_updated_date".equals(normalizedHeader)
                || "institution_type".equals(normalizedHeader)
                || "daily_limit".equals(normalizedHeader)
                || "is_manual".equals(normalizedHeader)
                || "first_download_at".equals(normalizedHeader)
                || "last_calculated_at".equals(normalizedHeader)
                || "created_at".equals(normalizedHeader)
                || "updated_at".equals(normalizedHeader);
    }

    private void setPreparedStatementValues(PreparedStatement ps, TargetRow row) throws SQLException {

        if (row.institutionType == null || row.institutionType.trim().length() == 0) {
            ps.setNull(1, java.sql.Types.VARCHAR);
        } else {
            ps.setString(1, row.institutionType);
        }
        ps.setInt(2, row.dailyLimit);
        ps.setBoolean(3, row.isManual);

        if (row.firstDownloadAt == null) {
            ps.setNull(4, java.sql.Types.TIMESTAMP);
        } else {
            ps.setTimestamp(4, row.firstDownloadAt);
        }

        if (row.lastCalculatedAt == null) {
            ps.setNull(5, java.sql.Types.TIMESTAMP);
        } else {
            ps.setTimestamp(5, row.lastCalculatedAt);
        }

        if (row.createdAt == null) {
            throw new IllegalArgumentException("created_at is null");
        }
        ps.setTimestamp(6, row.createdAt);

        if (row.updatedAt == null) {
            throw new IllegalArgumentException("updated_at is null");
        }
        ps.setTimestamp(7, row.updatedAt);

        if (row.metadataJson == null || row.metadataJson.trim().length() == 0) {
            ps.setString(8, "{}");
        } else {
            ps.setString(8, row.metadataJson);
        }

        // ---- WHERE clause bind param ----
        ps.setString(9, row.reId);
    }

    private int[] executeBatch(PreparedStatement ps, int batchSize) throws BatchFailureException {
        try {
            int[] updateCounts = ps.executeBatch();
            ps.clearBatch();

            if (updateCounts != null) {
                for (int i = 0; i < updateCounts.length; i++) {
                    if (updateCounts[i] == Statement.EXECUTE_FAILED) {
                        throw new BatchFailureException("executeBatch returned failed row", batchSize);
                    }
                }
            }
            return updateCounts;
        } catch (BatchUpdateException e) {
            throw new BatchFailureException(rootCauseMessage(e), batchSize);
        } catch (SQLException e) {
            throw new BatchFailureException(rootCauseMessage(e), batchSize);
        }
    }

    /**
     * Splits the int[] returned by executeBatch() into [updatedCount, notFoundCount].
     * A per-statement count of 0 means the UPDATE's WHERE clause matched no row
     * (re_id existed in the app DB lookup but not, or no longer, in this target table).
     * SUCCESS_NO_INFO (-2) means the driver can't report an exact count; treated as updated.
     */
    private long[] tallyBatchCounts(int[] counts) {
        long updated = 0L;
        long notFound = 0L;
        if (counts != null) {
            for (int c : counts) {
                if (c == STATEMENT_SUCCESS_NO_INFO) {
                    updated++;
                } else if (c > 0) {
                    updated += c;
                } else {
                    notFound++;
                }
            }
        }
        return new long[] { updated, notFound };
    }

    private Integer parseIntegerValue(String value, String fieldName, String fileName) {
        String v = cleanValue(value);

        if (v == null) {
            throw new IllegalArgumentException(fieldName + " is null in file " + fileName);
        }

        try {
            if (v.contains(".")) {
                Double d = Double.valueOf(v);
                return Integer.valueOf((int) Math.round(d.doubleValue()));
            }

            return Integer.valueOf(v);

        } catch (Exception e) {
            throw new IllegalArgumentException(fieldName + " conversion failed in file " + fileName + " value=" + v);
        }
    }

    private boolean parseBooleanValue(String value, boolean defaultValue) {
        String v = cleanValue(value);
        if (v == null) {
            return defaultValue;
        }

        String lower = v.toLowerCase(Locale.ENGLISH);
        if ("true".equals(lower) || "t".equals(lower) || "1".equals(lower) || "yes".equals(lower) || "y".equals(lower) || "on".equals(lower)) {
            return true;
        }
        if ("false".equals(lower) || "f".equals(lower) || "0".equals(lower) || "no".equals(lower) || "n".equals(lower) || "off".equals(lower)) {
            return false;
        }

        return defaultValue;
    }

    private Timestamp parseTimestampValue(String value, boolean required, String fieldName, String fileName) {
        String v = cleanValue(value);
        if (v == null) {
            if (required) {
                throw new IllegalArgumentException(fieldName + " is null in file " + fileName);
            }
            return null;
        }

        List<DateTimeFormatter> dateTimeFormats = getDateTimeFormats();
        for (int i = 0; i < dateTimeFormats.size(); i++) {
            DateTimeFormatter formatter = dateTimeFormats.get(i);
            try {
                LocalDateTime ldt = LocalDateTime.parse(v, formatter);
                return Timestamp.valueOf(ldt);
            } catch (DateTimeParseException ignored) {
            }
        }

        try {
            OffsetDateTime odt = OffsetDateTime.parse(v);
            return Timestamp.from(odt.toInstant());
        } catch (DateTimeParseException ignored) {
        }

        try {
            ZonedDateTime zdt = ZonedDateTime.parse(v);
            return Timestamp.from(zdt.toInstant());
        } catch (DateTimeParseException ignored) {
        }

        try {
            LocalDate date = LocalDate.parse(v);
            return Timestamp.valueOf(date.atStartOfDay());
        } catch (DateTimeParseException ignored) {
        }

        throw new IllegalArgumentException(fieldName + " conversion failed in file " + fileName + " value=" + v);
    }

    private List<DateTimeFormatter> getDateTimeFormats() {
        List<DateTimeFormatter> list = new ArrayList<DateTimeFormatter>();
        list.add(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        list.add(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        list.add(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
        list.add(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"));
        list.add(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"));
        list.add(DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss.SSS"));
        list.add(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"));
        list.add(DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS"));
        list.add(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX"));
        list.add(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX"));
        return list;
    }

    private List<String> parsePipeLine(String line) {
        List<String> values = new ArrayList<String>();
        if (line == null) {
            return values;
        }

        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int length = line.length();

        for (int i = 0; i < length; i++) {
            char c = line.charAt(i);

            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < length && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(c);
                }
            } else {
                if (c == '|') {
                    values.add(current.toString());
                    current.setLength(0);
                } else if (c == '"') {
                    inQuotes = true;
                } else {
                    current.append(c);
                }
            }
        }

        values.add(current.toString());
        return values;
    }

    private void trimTrailingEmptyValues(List<String> values) {
        if (values == null) {
            return;
        }

        while (!values.isEmpty()) {
            String last = values.get(values.size() - 1);
            if (last == null || last.trim().length() == 0) {
                values.remove(values.size() - 1);
            } else {
                break;
            }
        }
    }

    private String firstNonEmptyValue(Map<String, String> rowMap, String... keys) {
        if (rowMap == null || keys == null) {
            return null;
        }

        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            if (key == null) {
                continue;
            }

            String value = rowMap.get(normalize(key));
            if (value != null && value.trim().length() > 0) {
                return value.trim();
            }
        }

        return null;
    }

    private String cleanValue(String value) {

        if (value == null) {
            return null;
        }

        String trimmed = value.trim();

        if (trimmed.length() == 0
                || "\\N".equals(trimmed)
                || "null".equalsIgnoreCase(trimmed)
                || "n/a".equalsIgnoreCase(trimmed)) {
            return null;
        }

        if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length() >= 2) {
            trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
        }

        if (trimmed.length() == 0) {
            return null;
        }

        return trimmed;
    }

    private String normalize(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ENGLISH);
    }

    private String stripBom(String line) {
        if (line == null || line.length() == 0) {
            return line;
        }
        if (line.charAt(0) == '\uFEFF') {
            return line.substring(1);
        }
        return line;
    }

    private String buildFileSummaryLine(FileProcessResult r) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("file=").append(r.fileName != null ? r.fileName : "NA");
        sb.append(" status=").append(r.status != null ? r.status : "NA");
        sb.append(" data_rows=").append(r.dataRows);
        sb.append(" updated=").append(r.inserted);
        sb.append(" not_found=").append(r.notFound);
        sb.append(" skipped=").append(r.skipped);
        sb.append(" db_failed=").append(r.dbFailed);
        sb.append(" row_errors=").append(r.rowErrors);
        sb.append(" batch_failures=").append(r.batchFailures);
        sb.append(" blank_lines=").append(r.blankLines);
        sb.append(" effective_batch_size=").append(r.effectiveBatchSize);
        sb.append(" moved=").append(r.moved);
        return sb.toString();
    }

    private String buildDirectorySummaryLine(DirectoryAggregate aggregate, int totalFiles) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("total_files=").append(totalFiles);
        sb.append(" completed_files=").append(aggregate.completedFiles);
        sb.append(" retained_files=").append(aggregate.retainedFiles);
        sb.append(" empty_files=").append(aggregate.emptyFiles);
        sb.append(" header_only_files=").append(aggregate.headerOnlyFiles);
        sb.append(" error_files=").append(aggregate.errorFiles);
        sb.append(" total_rows=").append(aggregate.totalRows);
        sb.append(" updated=").append(aggregate.inserted);
        sb.append(" not_found=").append(aggregate.notFound);
        sb.append(" skipped=").append(aggregate.skipped);
        sb.append(" db_failed=").append(aggregate.dbFailed);
        sb.append(" row_errors=").append(aggregate.rowErrors);
        sb.append(" batch_failures=").append(aggregate.batchFailures);
        sb.append(" blank_lines=").append(aggregate.blankLines);
        return sb.toString();
    }

    private void rollbackQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (Exception ignored) {
        }
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
        }
    }

    private String rootCauseMessage(Throwable t) {
        if (t == null) {
            return "NA";
        }

        Throwable current = t;
        Throwable next = current.getCause();

        while (next != null && next != current) {
            current = next;
            next = current.getCause();
        }

        String message = current.getMessage();
        if (message == null || message.trim().length() == 0) {
            message = current.getClass().getSimpleName();
        }
        return message;
    }

    private String toJson(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }

        StringBuilder sb = new StringBuilder(256);
        sb.append("{");

        boolean first = true;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;

            sb.append("\"").append(escapeJson(entry.getKey())).append("\"").append(":");
            String value = entry.getValue();
            if (value == null) {
                sb.append("null");
            } else {
                sb.append("\"").append(escapeJson(value)).append("\"");
            }
        }

        sb.append("}");
        return sb.toString();
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder(value.length() + 16);
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
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                    break;
            }
        }
        return sb.toString();
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

        logBuilder.append("Process_Version: ").append(processVersion).append(" ").append("Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA").append(" Description: ")
                .append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private String capitalize(String value) {
        if (value == null || value.trim().length() == 0) {
            return "NA";
        }

        String v = value.trim().toLowerCase(Locale.ENGLISH);
        if (v.length() == 1) {
            return v.toUpperCase(Locale.ENGLISH);
        }

        return Character.toUpperCase(v.charAt(0)) + v.substring(1);
    }

    private static class FileScanResult {
        public String headerLine;
		private long dataRows;
        private long blankLines;
        private boolean empty;
        private boolean headerOnly;
    }

    private static class HeaderInfo {
        private List<String> headers;
        private Map<String, Integer> indexMap;
    }

    private static class TargetRow {
        private String reId;
        private String sourceFiCode;
        private String institutionType;
        private int dailyLimit;
        private boolean isManual;
        private Timestamp firstDownloadAt;
        private Timestamp lastCalculatedAt;
        private Timestamp createdAt;
        private Timestamp updatedAt;
        private String metadataJson;
    }

    private static class FileProcessResult {
        private String fileName;
        private String status = "NA";
        private long dataRows;
        private long inserted;
        private long notFound;
        private long skipped;
        private long dbFailed;
        private long rowErrors;
        private long batchFailures;
        private long blankLines;
        private int effectiveBatchSize;
        private boolean moved;
        private String errorMessage;
    }

    private static class DirectoryAggregate {
        private long completedFiles;
        private long retainedFiles;
        private long emptyFiles;
        private long headerOnlyFiles;
        private long errorFiles;
        private long totalRows;
        private long inserted;
        private long notFound;
        private long skipped;
        private long dbFailed;
        private long rowErrors;
        private long batchFailures;
        private long blankLines;

        private void add(FileProcessResult r) {
            if (r == null) {
                return;
            }

            if ("EMPTY".equalsIgnoreCase(r.status)) {
                emptyFiles++;
                completedFiles++;
            } else if ("HEADER_ONLY".equalsIgnoreCase(r.status)) {
                headerOnlyFiles++;
                completedFiles++;
            } else if ("SUCCESS".equalsIgnoreCase(r.status)) {
                completedFiles++;
            } else {
                errorFiles++;
            }

            if (!r.moved) {
                retainedFiles++;
            }

            totalRows += r.dataRows;
            inserted += r.inserted;
            notFound += r.notFound;
            skipped += r.skipped;
            dbFailed += r.dbFailed;
            rowErrors += r.rowErrors;
            batchFailures += r.batchFailures;
            blankLines += r.blankLines;
        }
    }

    private static class BatchFailureException extends Exception {
        private static final long serialVersionUID = 1L;
        private final int batchSize;

        private BatchFailureException(String message, int batchSize) {
            super(message);
            this.batchSize = batchSize;
        }
    }
}