package ntp19_process_v2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ntp19_process_methods {

    private static final String PROCESS_NAME = "NTP19";
    private static final String PROCESS_VERSION = "1.0.0";
    private static final int MAX_PARALLEL_FILES = 5;
    private static final int DEFAULT_BATCH_SIZE = 1000;
    private static final Charset FILE_CHARSET = StandardCharsets.UTF_8;

    private final String jdbcUrl;
    private final String dbUser;
    private final String dbPassword;
    private final String delimiter;
    private final String processedDirPath;
    private final String serverIp;
    private final boolean debugEnabled = false;
    private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private volatile String reportingEntityCodeColumn;

    public ntp19_process_methods(String jdbcUrl, String dbUser, String dbPassword, String delimiter, String processedDirPath, String serverIp) {
        this.jdbcUrl = jdbcUrl;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        this.delimiter = (delimiter == null || delimiter.isEmpty()) ? "|" : delimiter;
        this.processedDirPath = processedDirPath;
        this.serverIp = serverIp;
    }

    public void process(File input) throws Exception {
        log("INFO", "Process started", serverIp, "PROCESS_START");

        List<File> files = collectFiles(input);
        if (files.isEmpty()) {
            log("INFO", "No CSV files found to process", serverIp, "DIR_SUMMARY");
            logDirectorySummary(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
            log("INFO", "Process completed", serverIp, "PROCESS_END");
            return;
        }

        File processedDir = processedDirPath == null || processedDirPath.trim().isEmpty()
                ? new File(input.isDirectory() ? input : input.getParentFile(), "processed")
                : new File(processedDirPath);
        ensureDirectory(processedDir);

        int poolSize = Math.min(MAX_PARALLEL_FILES, files.size());
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        List<Future<FileSummary>> futures = new ArrayList<Future<FileSummary>>();

        try {
            for (int i = 0; i < files.size(); i++) {
                final File file = files.get(i);
                futures.add(executor.submit(new Callable<FileSummary>() {
                    public FileSummary call() {
                        return processSingleFile(file, processedDir);
                    }
                }));
            }
        } finally {
            executor.shutdown();
        }

        long totalFiles = files.size();
        long completedFiles = 0L;
        long retainedFiles = 0L;
        long emptyFiles = 0L;
        long headerOnlyFiles = 0L;
        long errorFiles = 0L;
        long totalRows = 0L;
        long inserted = 0L;
        long skipped = 0L;
        long dbFailed = 0L;
        long rowErrors = 0L;
        long batchFailures = 0L;
        long blankLines = 0L;

        for (Future<FileSummary> future : futures) {
            FileSummary summary;
            try {
                summary = future.get();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw ie;
            } catch (ExecutionException ee) {
                Throwable root = rootCause(ee);
                log("ERROR", "Fatal error waiting for file result: " + (root == null ? ee.getMessage() : root.getMessage()), serverIp, "FATAL_ERROR");
                continue;
            }

            if (summary == null) {
                continue;
            }

            if (summary.moved) {
                completedFiles++;
            } else {
                retainedFiles++;
            }
            if ("EMPTY".equalsIgnoreCase(summary.status)) {
                emptyFiles++;
            }
            if ("HEADER_ONLY".equalsIgnoreCase(summary.status)) {
                headerOnlyFiles++;
            }
            if ("ERROR".equalsIgnoreCase(summary.status)) {
                errorFiles++;
            }

            totalRows += summary.dataRows;
            inserted += summary.inserted;
            skipped += summary.skipped;
            dbFailed += summary.dbFailed;
            rowErrors += summary.rowErrors;
            batchFailures += summary.batchFailures;
            blankLines += summary.blankLines;
        }

        logDirectorySummary(totalFiles, completedFiles, retainedFiles, emptyFiles, headerOnlyFiles, errorFiles,
                totalRows, inserted, skipped, dbFailed, rowErrors, batchFailures, blankLines);
        log("INFO", "Process completed", serverIp, "PROCESS_END");
    }

    private List<File> collectFiles(File input) {
        List<File> files = new ArrayList<File>();
        if (input == null) {
            return files;
        }
        if (input.isFile()) {
            if (isCsv(input)) {
                files.add(input);
            }
            return files;
        }

        File[] list = input.listFiles();
        if (list == null) {
            return files;
        }

        Arrays.sort(list, new Comparator<File>() {
            public int compare(File a, File b) {
                return a.getName().compareToIgnoreCase(b.getName());
            }
        });

        for (int i = 0; i < list.length; i++) {
            File f = list[i];
            if (f != null && f.isFile() && isCsv(f)) {
                files.add(f);
            }
        }
        return files;
    }

    private boolean isCsv(File f) {
        String name = f == null ? null : f.getName();
        return name != null && name.toLowerCase(Locale.ENGLISH).endsWith(".csv");
    }

    private void ensureDirectory(File dir) throws IOException {
        if (dir == null) {
            return;
        }
        if (!dir.exists()) {
            Files.createDirectories(dir.toPath());
        }
    }

    private FileSummary processSingleFile(File file, File processedDir) {
        FileSummary summary = new FileSummary();
        summary.fileName = file.getName();
        summary.effectiveBatchSize = DEFAULT_BATCH_SIZE;

        log("INFO", "File start: " + file.getName(), serverIp, "FILE_START");

        Connection conn = null;
        PreparedStatement ps = null;
        BufferedReader reader = null;
        boolean success = false;

        try {
            conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
            conn.setAutoCommit(false);

            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), FILE_CHARSET));
            String headerLine = readNextLogicalRecord(reader);
            if (headerLine == null) {
                summary.status = "EMPTY";
                summary.moved = moveToDirectory(file, processedDir);
                if (summary.moved) {
                    log("INFO", "File moved: " + file.getName(), serverIp, "FILE_MOVED");
                }
                logFileSummary(summary);
                success = true;
                return summary;
            }

            List<String> header = parseCsvLine(headerLine, delimiter.charAt(0));
            if (header.isEmpty()) {
                summary.status = "EMPTY";
                summary.moved = moveToDirectory(file, processedDir);
                if (summary.moved) {
                    log("INFO", "File moved: " + file.getName(), serverIp, "FILE_MOVED");
                }
                logFileSummary(summary);
                success = true;
                return summary;
            }

            Map<String, Integer> headerIndex = buildHeaderIndex(header);
            TableType tableType = detectTableType(headerIndex);
            if (tableType == TableType.UNKNOWN) {
                throw new IllegalArgumentException("Unsupported CSV header for file " + file.getName());
            }

            if (isHeaderOnlyFile(reader)) {
                summary.status = "HEADER_ONLY";
                summary.moved = moveToDirectory(file, processedDir);
                if (summary.moved) {
                    log("INFO", "File moved: " + file.getName(), serverIp, "FILE_MOVED");
                }
                logFileSummary(summary);
                success = true;
                return summary;
            }

            ps = conn.prepareStatement(tableType == TableType.HISTORY ? buildHistoryInsertSql() : buildCurrentInsertSql());

            List<RowData> batch = new ArrayList<RowData>(DEFAULT_BATCH_SIZE);
            String record;
            while ((record = readNextLogicalRecord(reader)) != null) {
                if (isBlankRecord(record)) {
                    summary.blankLines++;
                    continue;
                }

                summary.dataRows++;
                List<String> values = parseCsvLine(record, delimiter.charAt(0));
                RowData row = (tableType == TableType.HISTORY)
                        ? buildHistoryRow(conn, headerIndex, values)
                        : buildCurrentRow(conn, headerIndex, values);

                if (row.skipReason != null) {
                    summary.skipped++;
                    summary.rowErrors++;
                    continue;
                }

                batch.add(row);
                if (batch.size() >= summary.effectiveBatchSize) {
                    flushBatch(conn, ps, batch, summary);
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                flushBatch(conn, ps, batch, summary);
                batch.clear();
            }

            conn.commit();
            success = true;
            summary.status = "SUCCESS";
            summary.moved = moveToDirectory(file, processedDir);
            if (summary.moved) {
                log("INFO", "File moved: " + file.getName(), serverIp, "FILE_MOVED");
            }
            logFileSummary(summary);
        } catch (Exception ex) {
            summary.status = "ERROR";
            summary.moved = false;
            summary.errorMessage = rootCause(ex) == null ? ex.getMessage() : rootCause(ex).getMessage();
            log("ERROR", "Fatal error in file " + file.getName() + ": " + summary.errorMessage, serverIp, "FATAL_ERROR");
            rollbackQuietly(conn);
            logFileSummary(summary);
        } finally {
            closeQuietly(ps);
            closeQuietly(reader);
            closeQuietly(conn);
            if (!success && summary.status == null) {
                summary.status = "ERROR";
            }
            if (!success && summary.errorMessage == null && "ERROR".equalsIgnoreCase(summary.status)) {
                summary.errorMessage = "Unknown fatal error";
            }
        }

        return summary;
    }

    private TableType detectTableType(Map<String, Integer> headerIndex) {
        if (headerIndex == null) {
            return TableType.UNKNOWN;
        }
        if (headerIndex.containsKey(normalizeHeader("upcd_expiry_date"))
                || headerIndex.containsKey(normalizeHeader("upcd_fi_code"))
                || headerIndex.containsKey(normalizeHeader("upcd_certificate_name"))) {
            return TableType.HISTORY;
        }
        if (headerIndex.containsKey(normalizeHeader("file_name"))
                && headerIndex.containsKey(normalizeHeader("valid_till"))
                && headerIndex.containsKey(normalizeHeader("reporting_entity_id"))) {
            return TableType.CURRENT;
        }
        return TableType.UNKNOWN;
    }

    private boolean isHeaderOnlyFile(BufferedReader reader) throws IOException {
        if (reader == null) {
            return true;
        }
        reader.mark(1024 * 1024);
        String next;
        while ((next = readNextLogicalRecord(reader)) != null) {
            if (!isBlankRecord(next)) {
                reader.reset();
                return false;
            }
        }
        reader.reset();
        return true;
    }

    private String buildCurrentInsertSql() {
        return "INSERT INTO re_mgmt.re_ip_whitelisting_public_key "
                + "(public_key_id, file_name, valid_from, valid_till, reporting_entity_id, created_at, updated_at, created_by, updated_by, version, active, public_key_fingerprint) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    private String buildHistoryInsertSql() {
        return "INSERT INTO re_mgmt.re_ip_whitelisting_public_key_history "
                + "(public_key_id, valid_from, valid_till, reporting_entity_id, original_created_at, original_updated_at, original_created_by, original_updated_by, created_at, updated_at, created_by, updated_by, version, active, public_key_fingerprint) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    private void flushBatch(Connection conn, PreparedStatement ps, List<RowData> batch, FileSummary summary) throws Exception {
        if (batch == null || batch.isEmpty()) {
            return;
        }

        Savepoint sp = null;
        try {
            sp = conn.setSavepoint();
            for (int i = 0; i < batch.size(); i++) {
                bindRow(ps, batch.get(i));
                ps.addBatch();
            }
            int[] counts = ps.executeBatch();
            int insertedInBatch = 0;
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] >= 0 || counts[i] == Statement.SUCCESS_NO_INFO) {
                    insertedInBatch++;
                }
            }
            summary.inserted += insertedInBatch;
        } catch (BatchUpdateException bue) {
            summary.batchFailures++;
            log("ERROR", "Batch failure in file " + summary.fileName + ": " + rootCause(bue).getMessage(), serverIp, "BATCH_FAILURE");
            rollbackQuietly(conn, sp);
            fallbackBatchRowByRow(conn, ps, batch, summary);
        } catch (SQLException sqle) {
            summary.batchFailures++;
            log("ERROR", "Batch failure in file " + summary.fileName + ": " + rootCause(sqle).getMessage(), serverIp, "BATCH_FAILURE");
            rollbackQuietly(conn, sp);
            fallbackBatchRowByRow(conn, ps, batch, summary);
        } finally {
            try {
                ps.clearBatch();
            } catch (Exception ignore) {
            }
        }
    }

    private void fallbackBatchRowByRow(Connection conn, PreparedStatement ps, List<RowData> batch, FileSummary summary) throws Exception {
        if (batch == null || batch.isEmpty()) {
            return;
        }
        for (int i = 0; i < batch.size(); i++) {
            RowData row = batch.get(i);
            Savepoint rowSp = null;
            try {
                rowSp = conn.setSavepoint();
                bindRow(ps, row);
                ps.executeUpdate();
                summary.inserted++;
            } catch (SQLException sqle) {
                rollbackQuietly(conn, rowSp);
                String state = sqle.getSQLState();
                if (state != null && (state.startsWith("23") || state.startsWith("22") || state.startsWith("28") || state.startsWith("40"))) {
                    summary.dbFailed++;
                    summary.skipped++;
                } else {
                    throw sqle;
                }
            }
        }
    }

    private RowData buildCurrentRow(Connection conn, Map<String, Integer> headerIndex, List<String> values) throws Exception {
        RowData row = new RowData();

        row.fileName = getByHeader(headerIndex, values, "file_name");
        if (isNullLike(row.fileName)) {
            row.skipReason = "file_name missing";
            return row;
        }

        row.publicKeyId = row.fileName;
        row.validFrom = parseNullableTimestamp(getByHeader(headerIndex, values, "valid_from"));
        row.validTill = parseNullableTimestamp(getByHeader(headerIndex, values, "valid_till"));

        String reportingKey = getByHeader(headerIndex, values, "reporting_entity_id");
        row.reportingEntityId = resolveReportingEntityId(conn, reportingKey);
        if (row.reportingEntityId == null && !isNullLike(reportingKey) && looksLikeUuid(reportingKey)) {
            row.reportingEntityId = safeUuid(reportingKey);
        }

        row.createdAt = parseNullableTimestamp(getByHeader(headerIndex, values, "created_at"));
        if (row.createdAt == null) {
            row.createdAt = new Timestamp(System.currentTimeMillis());
        }

        row.updatedAt = parseNullableTimestamp(getByHeader(headerIndex, values, "updated_at"));
        if (row.updatedAt == null) {
            row.updatedAt = row.createdAt;
        }

        row.createdBy = getByHeader(headerIndex, values, "created_by");
        row.updatedBy = getByHeader(headerIndex, values, "updated_by");
        if (row.updatedBy == null) {
            row.updatedBy = row.createdBy;
        }

        String version = getByHeader(headerIndex, values, "version");
        if (!isNullLike(version)) {
            try {
                row.version = Long.valueOf(version);
            } catch (Exception ex) {
                row.skipReason = "invalid version";
                return row;
            }
        }

        String active = getByHeader(headerIndex, values, "active");
        row.active = isNullLike(active) ? Boolean.TRUE : parseBoolean(active);
        row.publicKeyFingerprint = getByHeader(headerIndex, values, "public_key_fingerprint");
        return row;
    }
    private Timestamp parseDateOnlyTimestamp(String value) {

        value = normalizeNullable(value);

        if (value == null) {
            return null;
        }

        if (value.length() >= 10) {
            value = value.substring(0, 10);
        }

        try {
            return Timestamp.valueOf(value + " 00:00:00");
        } catch (Exception e) {
            return null;
        }
    }

    private RowData buildHistoryRow(Connection conn, Map<String, Integer> headerIndex, List<String> values) throws Exception {
        RowData row = new RowData();
        row.isHistory = true;

        String certName = getByHeader(headerIndex, values, "upcd_certificate_name");
        if (isNullLike(certName)) {
            row.skipReason = "upcd_certificate_name missing";
            return row;
        }

        row.publicKeyId = null;

        row.validFrom = parseDateOnlyTimestamp(
                getByHeader(headerIndex, values, "upcd_uploaded_date"));

        row.validTill = parseDateOnlyTimestamp(
                getByHeader(headerIndex, values, "upcd_expiry_date"));
        String fiCode = getByHeader(headerIndex, values, "upcd_fi_code");
        row.reportingEntityId = resolveReportingEntityId(conn, fiCode);
        if (row.reportingEntityId == null && !isNullLike(fiCode) && looksLikeUuid(fiCode)) {
            row.reportingEntityId = safeUuid(fiCode);
        }

        row.originalCreatedAt = parseNullableTimestamp(getByHeader(headerIndex, values, "upcd_uploaded_date"));
        row.originalUpdatedAt = parseNullableTimestamp(getByHeader(headerIndex, values, "upcd_updated_time"));
        row.originalCreatedBy = getByHeader(headerIndex, values, "upcd_uploaded_userid");
        row.originalUpdatedBy = getByHeader(headerIndex, values, "upcd_removed_userid");

        row.createdAt = row.originalCreatedAt;
        if (row.createdAt == null) {
            row.createdAt = new Timestamp(System.currentTimeMillis());
        }

        Timestamp updatedAt = parseNullableTimestamp(getByHeader(headerIndex, values, "upcd_removed_date"));
        if (updatedAt == null) {
            updatedAt = row.originalUpdatedAt;
        }
        if (updatedAt == null) {
            updatedAt = row.createdAt;
        }
        row.updatedAt = updatedAt;

        row.createdBy = row.originalCreatedBy;
        if (row.createdBy == null) {
            row.createdBy = getByHeader(headerIndex, values, "upcd_uploaded_userid");
        }

        row.updatedBy = row.originalUpdatedBy;
        if (row.updatedBy == null) {
            row.updatedBy = row.createdBy;
        }

        String version = getByHeader(headerIndex, values, "version");
        if (!isNullLike(version)) {
            try {
                row.version = Long.valueOf(version);
            } catch (Exception ex) {
                row.skipReason = "invalid version";
                return row;
            }
        }

        String active = getByHeader(headerIndex, values, "active");
        row.active = isNullLike(active) ? Boolean.TRUE : parseBoolean(active);
        row.publicKeyFingerprint = getByHeader(headerIndex, values, "public_key_fingerprint");
        return row;
    }

    private void bindRow(PreparedStatement ps, RowData row) throws Exception {
        int idx = 1;
        if (row.isHistory) {
            ps.setString(idx++, row.publicKeyId);
            if (row.validFrom == null) {
                ps.setTimestamp(idx++, null);
            } else {
                ps.setTimestamp(idx++, row.validFrom);
            }
            if (row.validTill == null) {
                ps.setTimestamp(idx++, null);
            } else {
                ps.setTimestamp(idx++, row.validTill);
            }
            if (row.reportingEntityId == null) {
                ps.setObject(idx++, null);
            } else {
                ps.setObject(idx++, row.reportingEntityId);
            }
            ps.setTimestamp(idx++, row.originalCreatedAt);
            ps.setTimestamp(idx++, row.originalUpdatedAt);
            ps.setString(idx++, row.originalCreatedBy);
            ps.setString(idx++, row.originalUpdatedBy);
            ps.setTimestamp(idx++, row.createdAt);
            ps.setTimestamp(idx++, row.updatedAt);
            ps.setString(idx++, row.createdBy);
            ps.setString(idx++, row.updatedBy);
            if (row.version == null) {
                ps.setObject(idx++, null);
            } else {
                ps.setLong(idx++, row.version.longValue());
            }
            if (row.active == null) {
                ps.setObject(idx++, null);
            } else {
                ps.setBoolean(idx++, row.active.booleanValue());
            }
            ps.setString(idx++, row.publicKeyFingerprint);
            return;
        }

        ps.setString(idx++, row.publicKeyId);
        ps.setString(idx++, row.fileName);
        if (row.validFrom == null) {
            ps.setTimestamp(idx++, null);
        } else {
            ps.setTimestamp(idx++, row.validFrom);
        }
        if (row.validTill == null) {
            ps.setTimestamp(idx++, null);
        } else {
            ps.setTimestamp(idx++, row.validTill);
        }
        if (row.reportingEntityId == null) {
            ps.setObject(idx++, null);
        } else {
            ps.setObject(idx++, row.reportingEntityId);
        }
        ps.setTimestamp(idx++, row.createdAt);
        ps.setTimestamp(idx++, row.updatedAt);
        ps.setString(idx++, row.createdBy);
        ps.setString(idx++, row.updatedBy);
        if (row.version == null) {
            ps.setObject(idx++, null);
        } else {
            ps.setLong(idx++, row.version.longValue());
        }
        if (row.active == null) {
            ps.setObject(idx++, null);
        } else {
            ps.setBoolean(idx++, row.active.booleanValue());
        }
        ps.setString(idx++, row.publicKeyFingerprint);
    }

    private String getByHeader(Map<String, Integer> headerIndex, List<String> values, String headerName) {
        if (headerIndex == null || values == null || headerName == null) {
            return null;
        }
        Integer idx = headerIndex.get(normalizeHeader(headerName));
        if (idx == null || idx.intValue() < 0 || idx.intValue() >= values.size()) {
            return null;
        }
        return normalizeNullable(values.get(idx.intValue()));
    }

    private Map<String, Integer> buildHeaderIndex(List<String> header) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        if (header == null) {
            return map;
        }
        for (int i = 0; i < header.size(); i++) {
            String key = normalizeHeader(header.get(i));
            if (key != null && key.length() > 0 && !map.containsKey(key)) {
                map.put(key, Integer.valueOf(i));
            }
        }
        return map;
    }

    private String normalizeHeader(String value) {
        if (value == null) {
            return null;
        }
        return value.trim().toLowerCase(Locale.ENGLISH);
    }

    private String normalizeNullable(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.length() == 0) {
            return null;
        }
        if ("\\N".equals(trimmed) || "NULL".equalsIgnoreCase(trimmed)) {
            return null;
        }
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
            if (trimmed.length() == 0) {
                return null;
            }
            if ("\\N".equals(trimmed) || "NULL".equalsIgnoreCase(trimmed)) {
                return null;
            }
        }
        return trimmed;
    }

    private boolean isNullLike(String value) {
        return normalizeNullable(value) == null;
    }

    private boolean parseBoolean(String value) {
        String v = normalizeNullable(value);
        if (v == null) {
            return false;
        }
        return "1".equals(v) || "Y".equalsIgnoreCase(v) || "YES".equalsIgnoreCase(v) || "TRUE".equalsIgnoreCase(v) || "T".equalsIgnoreCase(v);
    }

    private UUID safeUuid(String value) {
        if (isNullLike(value)) {
            return null;
        }
        try {
            return UUID.fromString(value.trim());
        } catch (Exception ex) {
            return null;
        }
    }

    private boolean looksLikeUuid(String value) {
        if (isNullLike(value)) {
            return false;
        }
        String v = value.trim();
        return v.matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
    }

    private Timestamp parseNullableTimestamp(String value) {
        value = normalizeNullable(value);
        if (value == null) {
            return null;
        }

        List<String> patterns = Arrays.asList(
                "yyyy-MM-dd HH:mm:ss.SSS",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy/MM/dd HH:mm:ss",
                "dd-MM-yyyy HH:mm:ss",
                "dd/MM/yyyy HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss.SSS",
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ssXXX",
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
                "yyyy-MM-dd HH:mm:ssXXX",
                "yyyy-MM-dd"
        );

        for (int i = 0; i < patterns.size(); i++) {
            String pattern = patterns.get(i);
            try {
                if (pattern.contains("XXX")) {
                    DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern);
                    OffsetDateTime odt = OffsetDateTime.parse(value, fmt);
                    return Timestamp.from(odt.toInstant());
                }
                if (pattern.equals("yyyy-MM-dd")) {
                    DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern);
                    java.time.LocalDate ld = java.time.LocalDate.parse(value, fmt);
                    return Timestamp.valueOf(ld.atStartOfDay());
                }
                DateTimeFormatter fmt = DateTimeFormatter.ofPattern(pattern);
                LocalDateTime ldt = LocalDateTime.parse(value, fmt);
                return Timestamp.valueOf(ldt);
            } catch (DateTimeParseException ignore) {
            }
        }

        try {
            return Timestamp.valueOf(value.replace('T', ' '));
        } catch (Exception ex) {
            return null;
        }
    }

    private UUID resolveReportingEntityId(Connection conn, String fiCode) throws Exception {
        fiCode = normalizeNullable(fiCode);
        if (fiCode == null) {
            return null;
        }

        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(
                    "SELECT id FROM re_mgmt.reporting_entity WHERE fi_code = ? LIMIT 1");
            ps.setString(1, fiCode);
            rs = ps.executeQuery();

            if (rs.next()) {
                Object obj = rs.getObject(1);
                if (obj instanceof UUID) {
                    return (UUID) obj;
                }
                if (obj != null) {
                    return UUID.fromString(obj.toString());
                }
            }
            return null;
        } finally {
            closeQuietly(rs);
            closeQuietly(ps);
        }
    }
   

    private String readNextLogicalRecord(BufferedReader reader) throws IOException {
        if (reader == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder(1024);
        String line;
        boolean inQuotes = false;
        boolean sawAny = false;
        while ((line = reader.readLine()) != null) {
            sawAny = true;
            if (sb.length() > 0) {
                sb.append('\n');
            }
            sb.append(line);
            inQuotes = hasUnclosedQuotes(sb);
            if (!inQuotes) {
                break;
            }
        }
        if (!sawAny) {
            return null;
        }
        return sb.toString();
    }

    private boolean hasUnclosedQuotes(CharSequence cs) {
        if (cs == null) {
            return false;
        }
        boolean inQuotes = false;
        for (int i = 0; i < cs.length(); i++) {
            char c = cs.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < cs.length() && cs.charAt(i + 1) == '"') {
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            }
        }
        return inQuotes;
    }

    private boolean isBlankRecord(String record) {
        return record == null || record.trim().length() == 0;
    }

    private List<String> parseCsvLine(String line, char delimiter) {
        if (line == null) {
            return Collections.emptyList();
        }
        List<String> values = new ArrayList<String>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        cur.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    cur.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == delimiter) {
                    values.add(normalizeNullable(cur.toString()));
                    cur.setLength(0);
                } else {
                    cur.append(c);
                }
            }
        }
        values.add(normalizeNullable(cur.toString()));
        return values;
    }

    private boolean moveToDirectory(File source, File targetDir) {
        if (source == null || targetDir == null) {
            return false;
        }
        try {
            ensureDirectory(targetDir);
            Path sourcePath = source.toPath();
            Path targetPath = new File(targetDir, source.getName()).toPath();
            Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (Exception ex) {
            log("ERROR", "Failed to move file " + source.getName() + ": " + rootCause(ex).getMessage(), serverIp, "FILE_MOVE_ERROR");
            return false;
        }
    }

    private void logFileSummary(FileSummary summary) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("file=").append(summary.fileName).append(" status=").append(summary.status).append(" data_rows=").append(summary.dataRows)
                .append(" inserted=").append(summary.inserted).append(" skipped=").append(summary.skipped).append(" db_failed=").append(summary.dbFailed)
                .append(" row_errors=").append(summary.rowErrors).append(" batch_failures=").append(summary.batchFailures)
                .append(" blank_lines=").append(summary.blankLines).append(" effective_batch_size=").append(summary.effectiveBatchSize)
                .append(" moved=").append(summary.moved);
        log("INFO", sb.toString(), serverIp, "FILE_SUMMARY");
        if (summary.errorMessage != null && summary.status != null && "ERROR".equalsIgnoreCase(summary.status)) {
            log("ERROR", "File error detail for " + summary.fileName + ": " + summary.errorMessage, serverIp, "FILE_ERROR");
        }
    }

    private void logDirectorySummary(long totalFiles, long completedFiles, long retainedFiles, long emptyFiles, long headerOnlyFiles, long errorFiles,
                                     long totalRows, long inserted, long skipped, long dbFailed, long rowErrors, long batchFailures, long blankLines) {
        StringBuilder sb = new StringBuilder(256);
        sb.append("total_files=").append(totalFiles)
                .append(" completed_files=").append(completedFiles)
                .append(" retained_files=").append(retainedFiles)
                .append(" empty_files=").append(emptyFiles)
                .append(" header_only_files=").append(headerOnlyFiles)
                .append(" error_files=").append(errorFiles)
                .append(" total_rows=").append(totalRows)
                .append(" inserted=").append(inserted)
                .append(" skipped=").append(skipped)
                .append(" db_failed=").append(dbFailed)
                .append(" row_errors=").append(rowErrors)
                .append(" batch_failures=").append(batchFailures)
                .append(" blank_lines=").append(blankLines);
        log("INFO", sb.toString(), serverIp, "DIR_SUMMARY");
    }



   
    private void closeQuietly(AutoCloseable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (Exception ignore) {
        }
    }

    private void rollbackQuietly(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.rollback();
        } catch (Exception ignore) {
        }
    }

    private void rollbackQuietly(Connection conn, Savepoint sp) {
        if (conn == null || sp == null) {
            return;
        }
        try {
            conn.rollback(sp);
        } catch (Exception ignore) {
        }
    }

    private Throwable rootCause(Throwable t) {
        if (t == null) {
            return null;
        }
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }

    private String capitalize(String value) {
        if (value == null || value.length() == 0) {
            return "";
        }
        String lower = value.toLowerCase(Locale.ENGLISH);
        return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
    }

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
            return;
        }

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp).append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(PROCESS_NAME).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(PROCESS_VERSION).append(" ").append("Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA").append(" Description: ")
                .append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private static class RowData {
        boolean isHistory;
        String publicKeyId;
        String fileName;
        Timestamp validFrom;
        Timestamp validTill;
        UUID reportingEntityId;
        Timestamp createdAt;
        Timestamp updatedAt;
        String createdBy;
        String updatedBy;
        Long version;
        Boolean active;
        String publicKeyFingerprint;
        Timestamp originalCreatedAt;
        Timestamp originalUpdatedAt;
        String originalCreatedBy;
        String originalUpdatedBy;
        String skipReason;
    }

    private static class FileSummary {
        String fileName;
        String status;
        long dataRows;
        long inserted;
        long skipped;
        long dbFailed;
        long rowErrors;
        long batchFailures;
        long blankLines;
        long effectiveBatchSize;
        boolean moved;
        String errorMessage;
    }

    private static enum TableType {
        CURRENT,
        HISTORY,
        UNKNOWN
    }
}
