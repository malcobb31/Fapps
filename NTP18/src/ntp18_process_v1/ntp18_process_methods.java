package ntp18_process_v1;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

class ntp18_methods {

    private static final Logger LOGGER = Logger.getLogger(ntp18_methods.class.getName());

    private static final int MAX_PARALLEL_FILES = 5;
    private static final int BATCH_SIZE = 50000;
    private static final String DEFAULT_DELIMITER = "|";
    private static final String TARGET_SCHEMA = "re_mgmt";
    private static final String TARGET_TABLE = "re_ip_whitelisting_history";
    private static final String DEFAULT_STATUS = "ACTIVE";
    private static final String DEFAULT_SYSTEM_USER = "SYSTEM";
    private static final String DEFAULT_IP_WHITELISTED_FOR = "API";
    private static final String DEFAULT_IP_VERSION = "IPv4";
    private static final ZoneId IST_ZONE = ZoneId.of("Asia/Kolkata");
    private static final TimeZone IST_TIMEZONE = TimeZone.getTimeZone("Asia/Kolkata");

    private static final String REPORTING_ENTITY_LOOKUP_SQL =
            "SELECT id FROM re_mgmt.reporting_entity WHERE fi_code = ?";

    private static final String IP_WHITELISTING_LOOKUP_SQL =
            "SELECT id FROM re_mgmt.re_ip_whitelisting WHERE fi_code = ?";

    private final String processName = "ntp18_process_v1";
    private final String processVersion = "1.0.0";
    private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss", Locale.ENGLISH);
    private final boolean debugEnabled = false;
    private final Calendar istCalendar = Calendar.getInstance(IST_TIMEZONE);

    public void process(String inputPath, String processedDir, String jdbcUrl, String dbUser, String dbPass,
                        String ipWhitelistedForValueFromMain) {

        final String finalIpWhitelistedForValue = DEFAULT_IP_WHITELISTED_FOR;

        log("INFO", "process_start=input_path=" + inputPath, null, "START");

        List<File> files = resolveInputFiles(inputPath);
        if (files.isEmpty()) {
            logDirectorySummary(new Summary());
            return;
        }

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(MAX_PARALLEL_FILES, files.size()));
        List<Future<FileResult>> futures = new ArrayList<Future<FileResult>>(files.size());

        try {
            for (int i = 0; i < files.size(); i++) {
                final File file = files.get(i);

                futures.add(executor.submit(new Callable<FileResult>() {
                    @Override
                    public FileResult call() {
                        return processSingleFile(
                                file,
                                processedDir,
                                jdbcUrl,
                                dbUser,
                                dbPass,
                                finalIpWhitelistedForValue);
                    }
                }));
            }

            Summary summary = new Summary();
            for (int i = 0; i < futures.size(); i++) {
                Future<FileResult> future = futures.get(i);
                try {
                    FileResult result = future.get();
                    if (result != null) {
                        summary.add(result);
                    }
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    log("ERROR", "directory_processing_interrupted reason=" + rootCause(ex), null, "FATAL");
                } catch (ExecutionException ex) {
                    log("ERROR", "directory_task_failed reason=" + rootCause(ex), null, "FATAL");
                }
            }
            logDirectorySummary(summary);
        } finally {
            executor.shutdownNow();
        }
    }

    private FileResult processSingleFile(File file, String processedDir, String jdbcUrl, String dbUser, String dbPass,
                                         String ipWhitelistedForValue) {
        FileResult result = new FileResult();
        result.fileName = file.getName();
        result.status = "ERROR";
        result.effectiveBatchSize = 0;
        result.moved = false;

        log("INFO", "file_start=" + result.fileName, null, "FILE_START");

        if (!file.exists()) {
            result.reason = "file_not_found";
            logFileSummary(result);
            return result;
        }

        if (file.length() == 0L) {
            result.status = "EMPTY";
            log("INFO", "file=" + result.fileName + " status=EMPTY reason=empty_file", null, "FILE_COMPLETE");
            logFileSummary(result);
            return result;
        }

        BufferedReader reader = null;
        Connection conn = null;
        PreparedStatement insertPs = null;
        PreparedStatement reportingEntityLookupPs = null;
        PreparedStatement ipWhitelistingLookupPs = null;

        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8));

            String headerLine = nextNonBlankLine(reader, result);
            if (headerLine == null) {
                result.status = "EMPTY";
                log("INFO", "file=" + result.fileName + " status=EMPTY reason=empty_file", null, "FILE_COMPLETE");
                logFileSummary(result);
                return result;
            }

            List<String> headers = parseCsvLine(headerLine, DEFAULT_DELIMITER);
            if (headers.isEmpty()) {
                result.status = "EMPTY";
                log("INFO", "file=" + result.fileName + " status=EMPTY reason=no_header", null, "FILE_COMPLETE");
                logFileSummary(result);
                return result;
            }

            Map<String, Integer> headerIndex = buildHeaderIndex(headers);

            if (!hasHeader(headerIndex, "created_by")) {
                result.reason = "missing_required_source_header=created_by";
                logFileSummary(result);
                return result;
            }
            if (!hasHeader(headerIndex, "created_at")) {
                result.reason = "missing_required_source_header=created_at";
                logFileSummary(result);
                return result;
            }
            if (!hasHeader(headerIndex, "ip_address", "ipaddress")) {
                result.reason = "missing_required_source_header=ip_address";
                logFileSummary(result);
                return result;
            }
            if (!hasHeader(headerIndex, "reporting_entity_id", "ipd_fi_code")) {
                result.reason = "missing_required_source_header=reporting_entity_id";
                logFileSummary(result);
                return result;
            }

            List<String> targetColumns = buildTargetColumns();
            String insertSql = buildInsertSql(TARGET_SCHEMA, TARGET_TABLE, targetColumns);
            log("INFO", "file=" + result.fileName + " insert_sql=" + insertSql, null, "FILE_START");

            conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
            conn.setAutoCommit(false);

            reportingEntityLookupPs = conn.prepareStatement(REPORTING_ENTITY_LOOKUP_SQL);
            ipWhitelistingLookupPs = conn.prepareStatement(IP_WHITELISTING_LOOKUP_SQL);
            insertPs = conn.prepareStatement(insertSql);

            int pendingBatch = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().length() == 0 || isNullToken(line)) {
                    result.blankLines++;
                    continue;
                }

                List<String> values = parseCsvLine(line, DEFAULT_DELIMITER);
                if (values.size() > headers.size()) {
                    throw new FatalRowException("parse_error=column_count_mismatch expected=" + headers.size() + " actual=" + values.size());
                }
                while (values.size() < headers.size()) {
                    values.add("");
                }

                Map<String, String> sourceMap = buildSourceMap(headers, values);
                result.dataRows++;

                try {
                    Map<String, Object> targetValues = buildTargetValues(
                            sourceMap,
                            reportingEntityLookupPs,
                            ipWhitelistingLookupPs,
                            ipWhitelistedForValue
                    );

                    setInsertParameters(insertPs, targetColumns, targetValues);
                    insertPs.addBatch();
                    pendingBatch++;

                    if (pendingBatch >= BATCH_SIZE) {
                        executeBatch(insertPs, conn, result, pendingBatch);
                        pendingBatch = 0;
                    }
                } catch (RequirementSkipException ex) {
                    result.skipped++;

                    log(
                        "INFO",
                        "skip_reason=" + ex.getMessage(),
                        null,
                        "SKIP"
                    );
                }
            }

            if (pendingBatch > 0) {
                executeBatch(insertPs, conn, result, pendingBatch);
            }

            if (result.dataRows == 0) {
                result.status = "HEADER_ONLY";
                result.effectiveBatchSize = 0;
                conn.rollback();
                log("INFO", "file=" + result.fileName + " status=HEADER_ONLY reason=header_only_file", null, "FILE_COMPLETE");
                logFileSummary(result);
                return result;
            }

            conn.commit();
            result.status = "COMPLETED";
            result.effectiveBatchSize = result.dataRows < BATCH_SIZE ? result.dataRows : BATCH_SIZE;

            boolean moved = moveFile(file, processedDir);
            result.moved = moved;
            if (moved) {
                log("INFO", "file_moved=" + result.fileName, null, "FILE_MOVED");
            }
        } catch (FatalRowException ex) {
            result.status = "ERROR";
            result.reason = rootCause(ex);
            result.batchFailures++;
            result.dbFailed++;
            rollbackQuietly(conn);
            log("ERROR", "file_error=file=" + result.fileName + " reason=" + result.reason, null, "FILE_ERROR");
        } catch (SQLException ex) {
            result.status = "ERROR";
            result.reason = rootCause(ex);
            result.batchFailures++;
            result.dbFailed++;
            rollbackQuietly(conn);
            log("ERROR", "batch_error=file=" + result.fileName + " batch_size=" + BATCH_SIZE + " reason=" + result.reason, null, "BATCH_ERROR");
        } catch (IOException ex) {
            result.status = "ERROR";
            result.reason = rootCause(ex);
            rollbackQuietly(conn);
            log("ERROR", "file_error=file=" + result.fileName + " reason=" + result.reason, null, "FILE_ERROR");
        } finally {
            closeQuietly(insertPs);
            closeQuietly(reportingEntityLookupPs);
            closeQuietly(ipWhitelistingLookupPs);
            closeQuietly(conn);
            closeQuietly(reader);
        }

        logFileSummary(result);
        return result;
    }

    private Map<String, Object> buildTargetValues(Map<String, String> sourceMap,
                                                  PreparedStatement reportingEntityLookupPs,
                                                  PreparedStatement ipWhitelistingLookupPs,
                                                  String ipWhitelistedForValue)
            throws RequirementSkipException, FatalRowException {

        Map<String, Object> target = new LinkedHashMap<String, Object>();

        String createdBy = getValue(sourceMap, "created_by");
        String createdAtText = getValue(sourceMap, "created_at");
        String ipAddress = normalizeIpAddress(
                getValue(sourceMap, "ip_address", "ipaddress"));
        String reportingEntityCode = getValue(sourceMap, "reporting_entity_id", "ipd_fi_code");
        String originalCreatedBy = getValue(sourceMap, "original_created_by");
        String originalCreatedAtText = getValue(sourceMap, "original_created_at");
        String validFromText = getValue(sourceMap, "original_created_at");
        String validTillText = getValue(sourceMap, "created_at");

        if (createdAtText == null) {
            throw new RequirementSkipException("missing_required_value=created_at");
        }
        
        if (reportingEntityCode == null) {
            throw new RequirementSkipException("missing_required_value=reporting_entity_id");
        }

        Timestamp createdAt = parseTimestamp(createdAtText);
        Timestamp originalCreatedAt = parseNullableTimestamp(originalCreatedAtText);
        Timestamp validFrom = parseDateOnlyTimestamp(validFromText);
        Timestamp validTill = parseDateOnlyTimestamp(validTillText);

        UUID reportingEntityId = lookupUuid(reportingEntityLookupPs, reportingEntityCode);
        if (reportingEntityId == null) {
            throw new RequirementSkipException("lookup_failed=reporting_entity_id fi_code=" + reportingEntityCode);
        }

        UUID ipWhitelistingId = lookupUuid(ipWhitelistingLookupPs, reportingEntityCode);
        if (ipWhitelistingId == null) {
        	throw new RequirementSkipException(
        		    "lookup_failed=ip_whitelisting_id fi_code=" + reportingEntityCode);
        }

        target.put("active", Boolean.TRUE);
        target.put("created_at", createdAt);
        target.put("created_by", nullIfBlank(createdBy));
        target.put("ip_address", ipAddress);
        target.put("ip_version", DEFAULT_IP_VERSION);
        target.put("ip_whitelisted_for", ipWhitelistedForValue);
        target.put("ip_whitelisting_id", ipWhitelistingId);
        target.put("original_created_at", originalCreatedAt);
        target.put("original_created_by", nullIfBlank(originalCreatedBy));
        target.put("original_updated_at", null);
        target.put("original_updated_by", null);
        target.put("reason", null);
        target.put("reporting_entity_id", reportingEntityId);
        target.put("status", DEFAULT_STATUS);
        target.put("updated_at", new Timestamp(System.currentTimeMillis()));
        target.put("updated_by", DEFAULT_SYSTEM_USER);
        target.put("valid_from", validFrom);
        target.put("valid_till", validTill);
        target.put("version", Long.valueOf(0L));

        return target;
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

    private LocalDate extractLocalDate(String value) throws FatalRowException {
        if (isNullToken(value)) {
            return null;
        }

        String text = value.trim();

        List<String> patterns = new ArrayList<String>();
        patterns.add("yyyy-MM-dd HH:mm:ss.SSS Z");
        patterns.add("yyyy-MM-dd HH:mm:ss Z");
        patterns.add("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
        patterns.add("yyyy-MM-dd'T'HH:mm:ssZ");
        patterns.add("yyyy-MM-dd HH:mm:ss.SSS");
        patterns.add("yyyy-MM-dd HH:mm:ss");
        patterns.add("yyyy-MM-dd");
        patterns.add("dd-MM-yyyy HH:mm:ss.SSS Z");
        patterns.add("dd-MM-yyyy HH:mm:ss Z");
        patterns.add("dd-MM-yyyy HH:mm:ss.SSS");
        patterns.add("dd-MM-yyyy HH:mm:ss");
        patterns.add("dd-MM-yyyy");
        patterns.add("dd/MM/yyyy HH:mm:ss.SSS Z");
        patterns.add("dd/MM/yyyy HH:mm:ss Z");
        patterns.add("dd/MM/yyyy HH:mm:ss.SSS");
        patterns.add("dd/MM/yyyy HH:mm:ss");
        patterns.add("dd/MM/yyyy");

        for (int i = 0; i < patterns.size(); i++) {
            String pattern = patterns.get(i);
            SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.ENGLISH);
            sdf.setLenient(false);
            sdf.setTimeZone(IST_TIMEZONE);
            try {
                Date d = sdf.parse(text);
                return d.toInstant().atZone(IST_ZONE).toLocalDate();
            } catch (ParseException ex) {
                // next
            }
        }

        try {
            return Timestamp.valueOf(text).toInstant().atZone(IST_ZONE).toLocalDate();
        } catch (IllegalArgumentException ex) {
            throw new FatalRowException("date_parse_failed value=" + text);
        }
    }

    private String normalizeIpAddress(String ip) {
        if (isNullToken(ip)) {
            return null;
        }

        ip = ip.trim();

        int pipe = ip.indexOf('|');
        if (pipe >= 0) {
            ip = ip.substring(0, pipe).trim();
        }

        return ip.length() == 0 ? null : ip;
    }
    private List<String> buildTargetColumns() {
        List<String> columns = new ArrayList<String>();
        columns.add("active");
        columns.add("created_at");
        columns.add("created_by");
        columns.add("ip_address");
        columns.add("ip_version");
        columns.add("ip_whitelisted_for");
        columns.add("ip_whitelisting_id");
        columns.add("original_created_at");
        columns.add("original_created_by");
        columns.add("original_updated_at");
        columns.add("original_updated_by");
        columns.add("reason");
        columns.add("reporting_entity_id");
        columns.add("status");
        columns.add("updated_at");
        columns.add("updated_by");
        columns.add("valid_from");
        columns.add("valid_till");
        columns.add("version");
        return columns;
    }

    private String buildInsertSql(String schema, String table, List<String> columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(schema);
        sb.append(".");
        sb.append(table);
        sb.append(" (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(columns.get(i));
        }
        sb.append(") VALUES (");
        for (int i = 0; i < columns.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("?");
        }
        sb.append(")");
        return sb.toString();
    }

    private void setInsertParameters(PreparedStatement ps, List<String> columns, Map<String, Object> values) throws SQLException {
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            Object value = values.get(column);
            int index = i + 1;

            if (value == null) {
                ps.setObject(index, null);
            } else if (value instanceof Timestamp) {
                if ("valid_from".equalsIgnoreCase(column) || "valid_till".equalsIgnoreCase(column)) {
                    ps.setTimestamp(index, (Timestamp) value, istCalendar);
                } else {
                    ps.setTimestamp(index, (Timestamp) value);
                }
            } else if (value instanceof Boolean) {
                ps.setBoolean(index, ((Boolean) value).booleanValue());
            } else if (value instanceof Long) {
                ps.setLong(index, ((Long) value).longValue());
            } else if (value instanceof UUID) {
                ps.setObject(index, value);
            } else {
                ps.setObject(index, value);
            }
        }
    }

    private void executeBatch(PreparedStatement insertPs, Connection conn, FileResult result, int batchSize) throws SQLException {
        try {
            insertPs.executeBatch();
            insertPs.clearBatch();
            result.inserted += batchSize;
        } catch (SQLException ex) {
            result.batchFailures++;
            result.dbFailed++;
            rollbackQuietly(conn);
            throw ex;
        }
    }

    private UUID lookupUuid(PreparedStatement ps, String key) throws FatalRowException {
        if (key == null || isNullToken(key)) {
            return null;
        }

        ResultSet rs = null;
        try {
            ps.clearParameters();
            ps.setString(1, key.trim());
            rs = ps.executeQuery();
            if (rs.next()) {
                Object value = rs.getObject(1);
                if (value == null) {
                    return null;
                }
                if (value instanceof UUID) {
                    return (UUID) value;
                }
                return UUID.fromString(value.toString().trim());
            }
            return null;
        } catch (Exception ex) {
            throw new FatalRowException(rootCause(ex));
        } finally {
            closeQuietly(rs);
        }
    }

    private Timestamp parseTimestamp(String value) throws FatalRowException {
        Timestamp ts = parseNullableTimestamp(value);
        if (ts == null) {
            throw new FatalRowException("timestamp_parse_failed value=" + safeText(value));
        }
        return ts;
    }

    private Timestamp parseNullableTimestamp(String value) throws FatalRowException {
        if (isNullToken(value)) {
            return null;
        }

        String text = value.trim();

        List<String> patterns = new ArrayList<String>();
        patterns.add("yyyy-MM-dd HH:mm:ss");
        patterns.add("yyyy-MM-dd HH:mm:ss.SSS");
        patterns.add("yyyy-MM-dd'T'HH:mm:ss");
        patterns.add("yyyy-MM-dd'T'HH:mm:ss.SSS");
        patterns.add("dd-MM-yyyy HH:mm:ss");
        patterns.add("dd/MM/yyyy HH:mm:ss");
        patterns.add("yyyy-MM-dd");
        patterns.add("dd-MM-yyyy");
        patterns.add("dd/MM/yyyy");

        for (int i = 0; i < patterns.size(); i++) {
            String pattern = patterns.get(i);
            SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.ENGLISH);
            sdf.setLenient(false);
            sdf.setTimeZone(IST_TIMEZONE);
            try {
                Date d = sdf.parse(text);
                return new Timestamp(d.getTime());
            } catch (ParseException ex) {
                // next
            }
        }

        try {
            return Timestamp.valueOf(text);
        } catch (IllegalArgumentException ex) {
            throw new FatalRowException("timestamp_parse_failed value=" + text);
        }
    }

    private List<File> resolveInputFiles(String inputPath) {
        File input = new File(inputPath);
        List<File> files = new ArrayList<File>();

        if (input.isFile()) {
            if (input.getName().toLowerCase(Locale.ENGLISH).endsWith(".csv")) {
                files.add(input);
            }
            return files;
        }

        File[] children = input.listFiles();
        if (children == null) {
            return files;
        }

        for (int i = 0; i < children.length; i++) {
            File child = children[i];
            if (child != null && child.isFile() && child.getName().toLowerCase(Locale.ENGLISH).endsWith(".csv")) {
                files.add(child);
            }
        }
        return files;
    }

    private String nextNonBlankLine(BufferedReader reader, FileResult result) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().length() == 0 || isNullToken(line)) {
                result.blankLines++;
                continue;
            }
            return line;
        }
        return null;
    }

    private Map<String, Integer> buildHeaderIndex(List<String> headers) {
        Map<String, Integer> index = new LinkedHashMap<String, Integer>();
        for (int i = 0; i < headers.size(); i++) {
            String header = normalizeHeader(headers.get(i));
            if (header.length() > 0 && !index.containsKey(header)) {
                index.put(header, Integer.valueOf(i));
            }
        }
        return index;
    }

    private boolean hasHeader(Map<String, Integer> headerIndex, String... names) {
        if (headerIndex == null || names == null) {
            return false;
        }
        for (int i = 0; i < names.length; i++) {
            String n = normalizeHeader(names[i]);
            if (n.length() > 0 && headerIndex.containsKey(n)) {
                return true;
            }
        }
        return false;
    }

    private Map<String, String> buildSourceMap(List<String> headers, List<String> values) {
        Map<String, String> source = new LinkedHashMap<String, String>();
        for (int i = 0; i < headers.size(); i++) {
            String key = normalizeHeader(headers.get(i));
            if (key.length() == 0) {
                continue;
            }
            String value = i < values.size() ? values.get(i) : "";
            source.put(key, value);
        }
        return source;
    }

    private List<String> parseCsvLine(String line, String delimiter) throws FatalRowException {
        if (line == null) {
            return Collections.emptyList();
        }

        char delim = (delimiter == null || delimiter.length() == 0) ? '|' : delimiter.charAt(0);
        List<String> tokens = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        boolean quotedField = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        current.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    current.append(c);
                }
            } else {
                if (c == '"' && current.length() == 0) {
                    inQuotes = true;
                    quotedField = true;
                } else if (c == delim) {
                    tokens.add(quotedField ? current.toString() : current.toString().trim());
                    current.setLength(0);
                    quotedField = false;
                } else {
                    current.append(c);
                }
            }
        }

        if (inQuotes) {
            throw new FatalRowException("csv_parse_failed=unclosed_quotes");
        }

        tokens.add(quotedField ? current.toString() : current.toString().trim());
        return tokens;
    }

    private boolean moveFile(File file, String processedDir) {
        if (processedDir == null || processedDir.trim().length() == 0) {
            return true;
        }

        File destDir = new File(processedDir);
        if (!destDir.exists()) {
            boolean created = destDir.mkdirs();
            if (!created && !destDir.exists()) {
                return false;
            }
        }

        File destFile = new File(destDir, file.getName());
        if (destFile.exists()) {
            String name = file.getName();
            String base = name;
            String ext = "";
            int dot = name.lastIndexOf('.');
            if (dot >= 0) {
                base = name.substring(0, dot);
                ext = name.substring(dot);
            }
            destFile = new File(destDir, base + "_" + System.currentTimeMillis() + ext);
        }

        try {
            Files.move(file.toPath(), destFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (IOException ex) {
            log("ERROR", "file_move_failed=file=" + file.getName() + " reason=" + rootCause(ex), null, "FILE_ERROR");
            return false;
        }
    }

    private String nullIfBlank(String value) {
        if (isNullToken(value)) {
            return null;
        }
        return value.trim();
    }

    private String getValue(Map<String, String> sourceMap, String... keys) {
        if (sourceMap == null || keys == null) {
            return null;
        }
        for (int i = 0; i < keys.length; i++) {
            String key = normalizeHeader(keys[i]);
            if (key.length() == 0) {
                continue;
            }
            String value = sourceMap.get(key);
            if (!isNullToken(value)) {
                return value.trim();
            }
        }
        return null;
    }

    private boolean isNullToken(String value) {
        if (value == null) {
            return true;
        }
        String trimmed = value.trim();
        return trimmed.length() == 0 || "\\N".equalsIgnoreCase(trimmed) || "NULL".equalsIgnoreCase(trimmed);
    }

    private String normalizeHeader(String header) {
        if (header == null) {
            return "";
        }
        return header.trim().toLowerCase(Locale.ENGLISH);
    }

    private String safeText(String value) {
        if (value == null) {
            return "";
        }
        return value;
    }

    private void rollbackQuietly(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.rollback();
        } catch (SQLException ex) {
            log("ERROR", "rollback_failed reason=" + rootCause(ex), null, "FATAL");
        }
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ex) {
            // ignore
        }
    }

    private void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException ex) {
            // ignore
        }
    }

    private void logFileSummary(FileResult result) {
        log("INFO", "file=" + safe(result.fileName)
                + " status=" + safe(result.status)
                + " data_rows=" + result.dataRows
                + " inserted=" + result.inserted
                + " skipped=" + result.skipped
                + " db_failed=" + result.dbFailed
                + " row_errors=" + result.rowErrors
                + " batch_failures=" + result.batchFailures
                + " blank_lines=" + result.blankLines
                + " effective_batch_size=" + result.effectiveBatchSize
                + " moved=" + result.moved, null, "FILE_COMPLETE");
        if (result.reason != null && result.reason.length() > 0 && "ERROR".equals(result.status)) {
            log("ERROR", "file=" + safe(result.fileName) + " reason=" + result.reason, null, "FILE_ERROR");
        }
    }

    private void logDirectorySummary(Summary summary) {
        log("INFO", "total_files=" + summary.totalFiles
                + " completed_files=" + summary.completedFiles
                + " retained_files=" + summary.retainedFiles
                + " empty_files=" + summary.emptyFiles
                + " header_only_files=" + summary.headerOnlyFiles
                + " error_files=" + summary.errorFiles
                + " total_rows=" + summary.totalRows
                + " inserted=" + summary.inserted
                + " skipped=" + summary.skipped
                + " db_failed=" + summary.dbFailed
                + " row_errors=" + summary.rowErrors
                + " batch_failures=" + summary.batchFailures
                + " blank_lines=" + summary.blankLines, null, "DIR_SUMMARY");
    }

    private String safe(String value) {
        return value == null ? "" : value;
    }

    private String rootCause(Throwable t) {
        if (t == null) {
            return "unknown";
        }
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        String message = current.getMessage();
        if (message == null || message.trim().length() == 0) {
            return current.getClass().getSimpleName();
        }
        return current.getClass().getSimpleName() + ": " + message.trim();
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
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA").append(" Description: ")
                .append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private String capitalize(String value) {
        if (value == null || value.length() == 0) {
            return "INFO";
        }
        String lower = value.toLowerCase(Locale.ENGLISH);
        return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
    }

    private static class Summary {
        int totalFiles;
        int completedFiles;
        int retainedFiles;
        int emptyFiles;
        int headerOnlyFiles;
        int errorFiles;
        long totalRows;
        long inserted;
        long skipped;
        long dbFailed;
        long rowErrors;
        long batchFailures;
        long blankLines;

        void add(FileResult result) {
            if (result == null) {
                return;
            }
            totalFiles++;
            totalRows += result.dataRows;
            inserted += result.inserted;
            skipped += result.skipped;
            dbFailed += result.dbFailed;
            rowErrors += result.rowErrors;
            batchFailures += result.batchFailures;
            blankLines += result.blankLines;

            if ("COMPLETED".equals(result.status)) {
                completedFiles++;
                if (!result.moved) {
                    retainedFiles++;
                }
            } else if ("EMPTY".equals(result.status)) {
                emptyFiles++;
                retainedFiles++;
            } else if ("HEADER_ONLY".equals(result.status)) {
                headerOnlyFiles++;
                retainedFiles++;
            } else {
                errorFiles++;
                retainedFiles++;
            }
        }
    }

    private static class FileResult {
        String fileName;
        String status;
        String reason;
        int dataRows;
        long inserted;
        int skipped;
        int dbFailed;
        int rowErrors;
        int batchFailures;
        int blankLines;
        int effectiveBatchSize;
        boolean moved;
    }

    private static class RequirementSkipException extends Exception {
        private static final long serialVersionUID = 1L;
        RequirementSkipException(String message) {
            super(message);
        }
    }

    private static class FatalRowException extends Exception {
        private static final long serialVersionUID = 1L;
        FatalRowException(String message) {
            super(message);
        }
    }
}