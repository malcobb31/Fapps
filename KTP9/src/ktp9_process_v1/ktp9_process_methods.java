package ktp9_process_v1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ktp9_process_methods {

    private static final String PROCESS_NAME = "KTP9";
    private static final String PROCESS_VERSION = "1.0";
    private static final DateTimeFormatter LOG_FMT =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");
    private static boolean debugEnabled = false;

    private static final int MAX_PARALLEL_FILES = 5;
    private static final String TARGET_TABLE = "kyc.kyc_related_parties_v1";

    private static final String INSERT_SQL =
            "INSERT INTO " + TARGET_TABLE + " (" +
                    "related_party_id, " +
                    "ckyc_number, " +
                    "relation, " +
                    "guardian_ckyc_number, " +
                    "is_active, " +
                    "created_at, " +
                    "updated_at, " +
                    "created_by, " +
                    "updated_by, " +
                    "name, " +
                    "controlling_interest, " +
                    "fi_code, " +
                    "region_code, " +
                    "branch_code, " +
                    "rel_status, " +
                    "kyc_type, " +
                    "metadata" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS jsonb))";

    private static final Set<String> USED_SOURCE_HEADERS;
    static {
        Set<String> s = new HashSet<String>();
        s.add(normalizeHeader("krl_KYC_no"));
        s.add(normalizeHeader("krl_relation_code"));
        s.add(normalizeHeader("krl_relation_KYC_no"));
        s.add(normalizeHeader("krl_rel_status"));
        s.add(normalizeHeader("krl_created_date"));
        s.add(normalizeHeader("krl_updated_date"));
        s.add(normalizeHeader("krl_created_by"));
        s.add(normalizeHeader("krl_updated_by"));
        s.add(normalizeHeader("krl_rel_first_name"));
        s.add(normalizeHeader("krl_rel_middle_name"));
        s.add(normalizeHeader("krl_rel_last_name"));
        s.add(normalizeHeader("krl_fi_code"));
        s.add(normalizeHeader("krl_reg_code"));
        s.add(normalizeHeader("krl_br_code"));
        USED_SOURCE_HEADERS = Collections.unmodifiableSet(s);
    }

    public void process(File sourcePath, File processedDir, String jdbcUrl, String jdbcUser, String jdbcPassword, int batchSize, String serverIP) {
        log("INFO", "directory=" + sourcePath.getAbsolutePath() + " status=START", serverIP, "DIR_START");

        List<File> csvFiles = collectCsvFiles(sourcePath);
        DirectorySummary directorySummary = new DirectorySummary();

        if (csvFiles.isEmpty()) {
            logDirectorySummary(directorySummary, serverIP);
            return;
        }

        int poolSize = csvFiles.size() < MAX_PARALLEL_FILES ? csvFiles.size() : MAX_PARALLEL_FILES;
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);
        List<Future<FileResult>> futures = new ArrayList<Future<FileResult>>();

        for (final File csv : csvFiles) {
            final String serverIPForTask = serverIP;
            futures.add(executor.submit(new Callable<FileResult>() {
                public FileResult call() {
                    return processSingleFile(csv, processedDir, jdbcUrl, jdbcUser, jdbcPassword, batchSize, serverIPForTask);
                }
            }));
        }

        for (Future<FileResult> future : futures) {
            try {
                FileResult result = future.get();
                directorySummary.add(result);
            } catch (Exception e) {
                FileResult failed = new FileResult();
                failed.status = "ERROR";
                failed.errorFile = true;
                failed.retainFile = true;
                failed.reason = rootCauseMessage(e);
                directorySummary.add(failed);
                log("ERROR", "reason=" + failed.reason, serverIP, "FILE_ERROR");
            }
        }

        executor.shutdown();
        logDirectorySummary(directorySummary, serverIP);
    }

    private FileResult processSingleFile(File file, File processedDir, String jdbcUrl, String jdbcUser, String jdbcPassword, int configuredBatchSize, String serverIP) {
        FileResult result = new FileResult();
        result.fileName = file.getName();
        result.moved = false;
        result.status = "START";

        log("INFO", "file=" + result.fileName + " status=START", serverIP, "FILE_START");

        FileInspect inspect;
        try {
            inspect = inspectFile(file);
        } catch (Exception e) {
            result.status = "ERROR";
            result.errorFile = true;
            result.retainFile = true;
            result.reason = rootCauseMessage(e);
            log("ERROR", "file=" + result.fileName + " reason=" + result.reason, serverIP, "FILE_ERROR");
            logFileSummary(result, serverIP);
            return result;
        }

        if (inspect.emptyFile) {
            result.status = "EMPTY_FILE";
            result.emptyFile = true;
            result.retainFile = true;
            result.dataRows = 0;
            result.effectiveBatchSize = 0;
            logFileSummary(result, serverIP);
            return result;
        }

        if (inspect.headerOnly) {
            result.status = "HEADER_ONLY";
            result.headerOnlyFile = true;
            result.retainFile = true;
            result.dataRows = 0;
            result.effectiveBatchSize = 0;
            logFileSummary(result, serverIP);
            return result;
        }

        result.dataRows = inspect.dataRowCount;
        result.effectiveBatchSize = inspect.dataRowCount < configuredBatchSize ? inspect.dataRowCount : configuredBatchSize;

        Connection connection = null;
        PreparedStatement ps = null;
        BufferedReader reader = null;

        int insertedInTx = 0;
        int batchCount = 0;
        int currentBatchRows = 0;
        int blankLines = 0;
        int skipped = 0;
        int batchFailures = 0;
        int dbFailed = 0;
        int rowErrors = 0;
        boolean fatal = false;

        try {
            connection = DriverManager.getConnection(jdbcUrl, jdbcUser, jdbcPassword);
            connection.setAutoCommit(false);

            reader = new BufferedReader(new InputStreamReader(Files.newInputStream(file.toPath()), StandardCharsets.UTF_8));

            String headerLine = nextNonBlankLine(reader);
            if (headerLine == null) {
                result.status = "EMPTY_FILE";
                result.emptyFile = true;
                result.retainFile = true;
                result.dataRows = 0;
                result.effectiveBatchSize = 0;
                logFileSummary(result, serverIP);
                return result;
            }

            headerLine = stripBom(headerLine);
            List<String> headers = parseCsvLine(headerLine);
            if (headers == null || headers.size() == 0) {
                result.status = "EMPTY_FILE";
                result.emptyFile = true;
                result.retainFile = true;
                result.dataRows = 0;
                result.effectiveBatchSize = 0;
                logFileSummary(result, serverIP);
                return result;
            }

            Map<String, Integer> headerIndex = buildHeaderIndex(headers);

            log("DEBUG", "file=" + result.fileName + " insert_sql=" + INSERT_SQL, serverIP, "SQL_DEBUG");

            ps = connection.prepareStatement(INSERT_SQL);

            String line;
            int lineNumber = 1;

            while ((line = reader.readLine()) != null) {
                lineNumber++;

                if (line.trim().length() == 0) {
                    blankLines++;
                    continue;
                }

                List<String> values = parseCsvLine(line);
                if (isAllBlank(values)) {
                    blankLines++;
                    continue;
                }

                try {
                    MappedRow mapped = mapRow(headers, headerIndex, values);
                    if (mapped == null) {
                        skipped++;
                        continue;
                    }

                    bindRow(ps, mapped);
                    ps.addBatch();
                    insertedInTx++;
                    currentBatchRows++;

                    if (currentBatchRows >= result.effectiveBatchSize) {
                        executeBatch(ps, result.fileName);
                        batchCount++;
                        currentBatchRows = 0;
                    }
                } catch (RequirementSkipException skipEx) {
                    skipped++;
                } catch (IllegalArgumentException fatalEx) {
                    fatal = true;
                    rowErrors++;
                    result.reason = rootCauseMessage(fatalEx);
                    log("ERROR", "file=" + result.fileName + " reason=" + result.reason, serverIP, "ROW_ERROR");
                    rollbackQuietly(connection);
                    break;
                } catch (SQLException sqlEx) {
                    fatal = true;
                    batchFailures++;
                    dbFailed += currentBatchRows;
                    result.reason = rootCauseMessage(sqlEx);
                    log("ERROR", "file=" + result.fileName + " batch_size=" + result.effectiveBatchSize + " reason=" + result.reason, serverIP, "BATCH_ERROR");
                    rollbackQuietly(connection);
                    break;
                } catch (Exception ex) {
                    fatal = true;
                    rowErrors++;
                    result.reason = rootCauseMessage(ex);
                    log("ERROR", "file=" + result.fileName + " reason=" + result.reason, serverIP, "ROW_ERROR");
                    rollbackQuietly(connection);
                    break;
                }
            }

            if (!fatal) {
                if (currentBatchRows > 0) {
                    try {
                        executeBatch(ps, result.fileName);
                        batchCount++;
                        currentBatchRows = 0;
                    } catch (SQLException sqlEx) {
                        fatal = true;
                        batchFailures++;
                        dbFailed += currentBatchRows;
                        result.reason = rootCauseMessage(sqlEx);
                        log("ERROR", "file=" + result.fileName + " batch_size=" + result.effectiveBatchSize + " reason=" + result.reason, serverIP, "BATCH_ERROR");
                        rollbackQuietly(connection);
                    }
                }
            }

            if (!fatal) {
                connection.commit();

                File movedTo = moveFile(file, processedDir, serverIP);
                result.moved = movedTo != null && movedTo.exists();
                result.status = "COMPLETED";
                result.inserted = insertedInTx;
                result.skipped = skipped;
                result.dbFailed = 0;
                result.rowErrors = rowErrors;
                result.batchFailures = batchFailures;
                result.blankLines = blankLines;
                result.retainFile = !result.moved;

                if (result.moved) {
                    log("INFO", "file=" + result.fileName + " status=MOVED", serverIP, "FILE_MOVED");
                } else {
                    result.status = "MOVE_FAILED";
                    result.errorFile = true;
                    result.retainFile = true;
                    result.reason = "File committed but move failed";
                    log("ERROR", "file=" + result.fileName + " reason=" + result.reason, serverIP, "FILE_ERROR");
                }
            } else {
                result.status = "ERROR";
                result.errorFile = true;
                result.retainFile = true;
                result.inserted = 0;
                result.skipped = skipped;
                result.dbFailed = dbFailed;
                result.rowErrors = rowErrors;
                result.batchFailures = batchFailures;
                result.blankLines = blankLines;
            }

        } catch (Exception e) {
            fatal = true;
            result.status = "ERROR";
            result.errorFile = true;
            result.retainFile = true;
            result.reason = rootCauseMessage(e);
            result.inserted = 0;
            result.skipped = skipped;
            result.dbFailed = dbFailed;
            result.rowErrors = rowErrors;
            result.batchFailures = batchFailures;
            result.blankLines = blankLines;
            log("ERROR", "file=" + result.fileName + " reason=" + result.reason, serverIP, "FILE_ERROR");
            rollbackQuietly(connection);
        } finally {
            closeQuietly(ps);
            closeQuietly(reader);
            closeQuietly(connection);
        }

        if (!fatal && "COMPLETED".equalsIgnoreCase(result.status)) {
            result.blankLines = blankLines;
            result.skipped = skipped;
            result.dbFailed = 0;
            result.rowErrors = rowErrors;
            result.batchFailures = batchCount == 0 ? 0 : batchFailures;
        }

        logFileSummary(result, serverIP);
        return result;
    }

    private void bindRow(PreparedStatement ps, MappedRow row) throws SQLException {
        ps.setObject(1, java.util.UUID.fromString(row.relatedPartyId));
        ps.setString(2, row.ckycNumber);
        ps.setString(3, row.relation);
        setNullableString(ps, 4, row.guardianCkycNumber);
        setNullableBoolean(ps, 5, row.isActive);
        setNullableTimestamp(ps, 6, row.createdAt);
        setNullableTimestamp(ps, 7, row.updatedAt);
        setNullableString(ps, 8, row.createdBy);
        setNullableString(ps, 9, row.updatedBy);
        setNullableString(ps, 10, row.name);
        setNullableString(ps, 11, row.controllingInterest);
        setNullableString(ps, 12, row.fiCode);
        setNullableString(ps, 13, row.regionCode);
        setNullableString(ps, 14, row.branchCode);
        setNullableString(ps, 15, row.relStatus);
        setNullableString(ps, 16, row.kycType);
        setNullableString(ps, 17, row.metadataJson);
    }

    private void executeBatch(PreparedStatement ps, String fileName) throws SQLException {
        ps.executeBatch();
        ps.clearBatch();
    }

    private MappedRow mapRow(List<String> headers, Map<String, Integer> headerIndex, List<String> values) throws RequirementSkipException {
        String ckycNumber = getValue(headers, headerIndex, values, "krl_KYC_no");
        ckycNumber = safeTrim(ckycNumber);

        if (ckycNumber == null) {
            throw new RequirementSkipException("Missing ckyc number");
        }

        char firstChar = ckycNumber.charAt(0);
        if (!Character.isDigit(firstChar) || firstChar == '0') {
            throw new RequirementSkipException("Invalid ckyc prefix");
        }

        if (firstChar < '1' || firstChar > '6') {
            throw new RequirementSkipException("Invalid ckyc prefix");
        }

        String ckycType = (firstChar >= '1' && firstChar <= '6') ? "I" : "L";

        String relationCode = safeTrim(getValue(headers, headerIndex, values, "krl_relation_code"));
        if (relationCode == null) {
            throw new RequirementSkipException("Missing relation code");
        }

        String relation = mapRelation(relationCode);
        if (relation == null || relation.length() == 0) {
            throw new RequirementSkipException("Invalid relation code");
        }

        MappedRow row = new MappedRow();
        row.relatedPartyId = UUID.randomUUID().toString();
        row.ckycNumber = ckycNumber;
        row.relation = relation;
        row.guardianCkycNumber = safeTrim(getValue(headers, headerIndex, values, "krl_relation_KYC_no"));
        row.isActive = parseBooleanLoose(getValue(headers, headerIndex, values, "krl_rel_status"));
        row.createdAt = parseTimestampNullable(getValue(headers, headerIndex, values, "krl_created_date"));
        row.updatedAt = parseTimestampNullable(getValue(headers, headerIndex, values, "krl_updated_date"));
        row.createdBy = safeTrim(getValue(headers, headerIndex, values, "krl_created_by"));
        row.updatedBy = safeTrim(getValue(headers, headerIndex, values, "krl_updated_by"));
        row.name = buildName(
                getValue(headers, headerIndex, values, "krl_rel_first_name"),
                getValue(headers, headerIndex, values, "krl_rel_middle_name"),
                getValue(headers, headerIndex, values, "krl_rel_last_name")
        );
        row.controllingInterest = null;
        row.fiCode = safeTrim(getValue(headers, headerIndex, values, "krl_fi_code"));
        row.regionCode = safeTrim(getValue(headers, headerIndex, values, "krl_reg_code"));
        row.branchCode = safeTrim(getValue(headers, headerIndex, values, "krl_br_code"));
        row.relStatus = booleanToString(parseBooleanLoose(getValue(headers, headerIndex, values, "krl_rel_status")));
        row.kycType = ckycType;
        row.metadataJson = buildMetadataJson(headers, headerIndex, values);

        return row;
    }

    private FileInspect inspectFile(File file) throws IOException {
        FileInspect inspect = new FileInspect();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(Files.newInputStream(file.toPath()), StandardCharsets.UTF_8));
            String headerLine = nextNonBlankLine(br);
            if (headerLine == null) {
                inspect.emptyFile = true;
                inspect.headerOnly = false;
                inspect.dataRowCount = 0;
                return inspect;
            }

            headerLine = stripBom(headerLine);
            List<String> headers = parseCsvLine(headerLine);
            if (headers == null || headers.size() == 0) {
                inspect.emptyFile = true;
                inspect.headerOnly = false;
                inspect.dataRowCount = 0;
                return inspect;
            }

            int count = 0;
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().length() == 0) {
                    continue;
                }
                List<String> row = parseCsvLine(line);
                if (isAllBlank(row)) {
                    continue;
                }
                count++;
            }

            inspect.headers = headers;
            inspect.headerOnly = (count == 0);
            inspect.emptyFile = false;
            inspect.dataRowCount = count;
            return inspect;
        } finally {
            closeQuietly(br);
        }
    }

    private List<File> collectCsvFiles(File sourcePath) {
        if (sourcePath.isFile()) {
            if (sourcePath.getName().toLowerCase(Locale.ENGLISH).endsWith(".csv")) {
                List<File> single = new ArrayList<File>();
                single.add(sourcePath);
                return single;
            }
            return new ArrayList<File>();
        }

        File[] files = sourcePath.listFiles();
        if (files == null || files.length == 0) {
            return new ArrayList<File>();
        }

        List<File> csvs = new ArrayList<File>();
        for (File file : files) {
            if (file != null && file.isFile() && file.getName().toLowerCase(Locale.ENGLISH).endsWith(".csv")) {
                csvs.add(file);
            }
        }
        return csvs;
    }

    private File moveFile(File sourceFile, File processedDir, String serverIP) {
        try {
            Path destDir = processedDir.toPath();
            Files.createDirectories(destDir);

            Path dest = destDir.resolve(sourceFile.getName());
            try {
                return Files.move(sourceFile.toPath(), dest, StandardCopyOption.REPLACE_EXISTING).toFile();
            } catch (AtomicMoveNotSupportedException e) {
                return Files.move(sourceFile.toPath(), dest, StandardCopyOption.REPLACE_EXISTING).toFile();
            }
        } catch (Exception e) {
            log("ERROR", "file=" + sourceFile.getName() + " reason=" + rootCauseMessage(e), serverIP, "FILE_ERROR");
            return null;
        }
    }

    private Map<String, Integer> buildHeaderIndex(List<String> headers) {
        Map<String, Integer> index = new LinkedHashMap<String, Integer>();
        for (int i = 0; i < headers.size(); i++) {
            String key = normalizeHeader(headers.get(i));
            if (key != null && key.length() > 0 && !index.containsKey(key)) {
                index.put(key, Integer.valueOf(i));
            }
        }
        return index;
    }

    private String getValue(List<String> headers, Map<String, Integer> headerIndex, List<String> values, String headerName) {
        Integer idx = headerIndex.get(normalizeHeader(headerName));
        if (idx == null) {
            return null;
        }
        int i = idx.intValue();
        if (values == null || i < 0 || i >= values.size()) {
            return null;
        }
        return values.get(i);
    }

    private String buildMetadataJson(List<String> headers, Map<String, Integer> headerIndex, List<String> values) {
        Map<String, String> metadata = new LinkedHashMap<String, String>();
        for (int i = 0; i < headers.size(); i++) {
            String originalHeader = headers.get(i);
            String normalized = normalizeHeader(originalHeader);
            if (normalized == null || normalized.length() == 0) {
                continue;
            }
            if (USED_SOURCE_HEADERS.contains(normalized)) {
                continue;
            }
            String value = (values != null && i < values.size()) ? values.get(i) : null;
            value = safeTrim(value);
            if (value != null) {
                metadata.put(normalized, value);
            }
        }

        if (metadata.isEmpty()) {
            return null;
        }
        return toJson(metadata);
    }

    private String toJson(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;
            sb.append("\"").append(escapeJson(entry.getKey())).append("\":");
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

    private String buildName(String first, String middle, String last) {
        StringBuilder sb = new StringBuilder();
        appendPart(sb, first);
        appendPart(sb, middle);
        appendPart(sb, last);
        String name = sb.toString().trim().replaceAll("\\s+", " ");
        if (name.length() == 0) {
            return null;
        }
        return name;
    }

    private void appendPart(StringBuilder sb, String value) {
        String t = safeTrim(value);
        if (t == null) {
            return;
        }
        if (sb.length() > 0) {
            sb.append(' ');
        }
        sb.append(t);
    }

    private String mapRelation(String code) {
        if (code == null) {
            return null;
        }
        String c = code.trim();
        if (c.length() == 0) {
            return null;
        }
        if ("01".equals(c) || "1".equals(c)) {
            return "GUARDIAN";
        }
        if ("02".equals(c) || "2".equals(c)) {
            return "ASSIGNEE";
        }
        if ("03".equals(c) || "3".equals(c)) {
            return "AUTHORIZED_REPRESENTATIVE";
        }
        return c;
    }

    private Boolean parseBooleanLoose(String value) {
        String t = safeTrim(value);
        if (t == null) {
            return Boolean.FALSE;
        }
        String n = t.toLowerCase(Locale.ENGLISH);
        if ("1".equals(n) || "01".equals(n) || "true".equals(n) || "t".equals(n) || "y".equals(n) || "yes".equals(n)) {
            return Boolean.TRUE;
        }
        return Boolean.FALSE;
    }

    private String booleanToString(Boolean value) {
        if (value == null) {
            return null;
        }
        return value.booleanValue() ? "true" : "false";
    }

    private Timestamp parseTimestampNullable(String value) {
        String t = safeTrim(value);
        if (t == null) {
            return null;
        }

        List<DateTimeFormatter> dateTimeFormats = Arrays.asList(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss.SSS")
        );

        for (DateTimeFormatter fmt : dateTimeFormats) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(t, fmt);
                return Timestamp.valueOf(ldt);
            } catch (DateTimeParseException ignore) {
            }
        }

        List<DateTimeFormatter> dateFormats = Arrays.asList(
                DateTimeFormatter.ISO_LOCAL_DATE,
                DateTimeFormatter.ofPattern("dd/MM/yyyy"),
                DateTimeFormatter.ofPattern("yyyy/MM/dd"),
                DateTimeFormatter.ofPattern("dd-MM-yyyy"),
                DateTimeFormatter.ofPattern("yyyyMMdd")
        );

        for (DateTimeFormatter fmt : dateFormats) {
            try {
                LocalDate ld = LocalDate.parse(t, fmt);
                return Timestamp.valueOf(ld.atStartOfDay());
            } catch (DateTimeParseException ignore) {
            }
        }

        try {
            OffsetDateTime odt = OffsetDateTime.parse(t, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
            return Timestamp.from(odt.toInstant());
        } catch (DateTimeParseException ignore) {
        }

        try {
            ZonedDateTime zdt = ZonedDateTime.parse(t, DateTimeFormatter.ISO_ZONED_DATE_TIME);
            return Timestamp.from(zdt.toInstant());
        } catch (DateTimeParseException ignore) {
        }

        throw new IllegalArgumentException("Invalid timestamp: " + t);
    }

    private void setNullableString(PreparedStatement ps, int index, String value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value);
        }
    }

    private void setNullableBoolean(PreparedStatement ps, int index, Boolean value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.BOOLEAN);
        } else {
            ps.setBoolean(index, value.booleanValue());
        }
    }

    private void setNullableTimestamp(PreparedStatement ps, int index, Timestamp value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.TIMESTAMP);
        } else {
            ps.setTimestamp(index, value);
        }
    }

    private List<String> parseCsvLine(String line) {
        List<String> result = new ArrayList<String>();
        if (line == null) {
            return result;
        }

        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

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
                if (c == '|') {
                    result.add(current.toString());
                    current.setLength(0);
                } else if (c == '"') {
                    inQuotes = true;
                } else {
                    current.append(c);
                }
            }
        }

        result.add(current.toString());
        return result;
    }

    private boolean isAllBlank(List<String> values) {
        if (values == null || values.isEmpty()) {
            return true;
        }
        for (String v : values) {
            if (v != null && v.trim().length() > 0) {
                return false;
            }
        }
        return true;
    }

    private String nextNonBlankLine(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().length() > 0) {
                return line;
            }
        }
        return null;
    }

    private String stripBom(String text) {
        if (text == null) {
            return null;
        }
        if (text.startsWith("\uFEFF")) {
            return text.substring(1);
        }
        return text;
    }

    private static String normalizeHeader(String header) {
        if (header == null) {
            return null;
        }
        String t = header.trim();
        if (t.length() == 0) {
            return null;
        }
        return t.toLowerCase(Locale.ENGLISH);
    }

    private String safeTrim(String value)
    {
        if (value == null) {
            return null;
        }

        String t = value.trim();

        if (t.length() == 0) {
            return null;
        }

        if ("\\N".equals(t)) {
            return null;
        }

        if ("NULL".equalsIgnoreCase(t)) {
            return null;
        }

        return t;
    }

    private void rollbackQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.rollback();
        } catch (Exception ignore) {
        }
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignore) {
        }
    }

    private void logFileSummary(FileResult r, String serverIP) {
        StringBuilder sb = new StringBuilder();
        sb.append("file=").append(r.fileName == null ? "unknown" : r.fileName);
        sb.append(" status=").append(r.status == null ? "UNKNOWN" : r.status);
        sb.append(" data_rows=").append(r.dataRows);
        sb.append(" inserted=").append(r.inserted);
        sb.append(" skipped=").append(r.skipped);
        sb.append(" db_failed=").append(r.dbFailed);
        sb.append(" row_errors=").append(r.rowErrors);
        sb.append(" batch_failures=").append(r.batchFailures);
        sb.append(" blank_lines=").append(r.blankLines);
        sb.append(" effective_batch_size=").append(r.effectiveBatchSize);
        sb.append(" moved=").append(r.moved);
        log("INFO", sb.toString(), serverIP, "FILE_SUMMARY");
    }

    private void logDirectorySummary(DirectorySummary s, String serverIP) {
        StringBuilder sb = new StringBuilder();
        sb.append("total_files=").append(s.totalFiles);
        sb.append(" completed_files=").append(s.completedFiles);
        sb.append(" retained_files=").append(s.retainedFiles);
        sb.append(" empty_files=").append(s.emptyFiles);
        sb.append(" header_only_files=").append(s.headerOnlyFiles);
        sb.append(" error_files=").append(s.errorFiles);
        sb.append(" total_rows=").append(s.totalRows);
        sb.append(" inserted=").append(s.inserted);
        sb.append(" skipped=").append(s.skipped);
        sb.append(" db_failed=").append(s.dbFailed);
        sb.append(" row_errors=").append(s.rowErrors);
        sb.append(" batch_failures=").append(s.batchFailures);
        sb.append(" blank_lines=").append(s.blankLines);
        log("INFO", sb.toString(), serverIP, "DIR_SUMMARY");
    }



    private String rootCauseMessage(Throwable t) {
        if (t == null) {
            return "unknown";
        }
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        String msg = current.getMessage();
        if (msg == null || msg.trim().length() == 0) {
            return current.getClass().getSimpleName();
        }
        return current.getClass().getSimpleName() + ": " + msg;
    }

    private static class RequirementSkipException extends Exception {
        private static final long serialVersionUID = 1L;

        RequirementSkipException(String message) {
            super(message);
        }
    }

    private static class FileInspect {
        boolean emptyFile;
        boolean headerOnly;
        int dataRowCount;
        List<String> headers;
    }

    private static class MappedRow {
        String relatedPartyId;
        String ckycNumber;
        String relation;
        String guardianCkycNumber;
        Boolean isActive;
        Timestamp createdAt;
        Timestamp updatedAt;
        String createdBy;
        String updatedBy;
        String name;
        String controllingInterest;
        String fiCode;
        String regionCode;
        String branchCode;
        String relStatus;
        String kycType;
        String metadataJson;
    }

    private static class FileResult {
        String fileName;
        String status;
        int dataRows;
        int inserted;
        int skipped;
        int dbFailed;
        int rowErrors;
        int batchFailures;
        int blankLines;
        int effectiveBatchSize;
        boolean moved;
        boolean emptyFile;
        boolean headerOnlyFile;
        boolean errorFile;
        boolean retainFile;
        String reason;
    }

    private static class DirectorySummary {
        int totalFiles;
        int completedFiles;
        int retainedFiles;
        int emptyFiles;
        int headerOnlyFiles;
        int errorFiles;
        int totalRows;
        int inserted;
        int skipped;
        int dbFailed;
        int rowErrors;
        int batchFailures;
        int blankLines;

        void add(FileResult r) {
            if (r == null) {
                return;
            }
            totalFiles++;
            totalRows += r.dataRows;
            skipped += r.skipped;
            dbFailed += r.dbFailed;
            rowErrors += r.rowErrors;
            batchFailures += r.batchFailures;
            blankLines += r.blankLines;

            if (r.emptyFile) {
                emptyFiles++;
                retainedFiles++;
                return;
            }

            if (r.headerOnlyFile) {
                headerOnlyFiles++;
                retainedFiles++;
                return;
            }

            if ("COMPLETED".equalsIgnoreCase(r.status)) {
                completedFiles++;
                inserted += r.inserted;
                if (!r.moved) {
                    retainedFiles++;
                }
                return;
            }

            if ("MOVE_FAILED".equalsIgnoreCase(r.status)) {
                errorFiles++;
                retainedFiles++;
                inserted += r.inserted;
                return;
            }

            if (r.errorFile || "ERROR".equalsIgnoreCase(r.status)) {
                errorFiles++;
                retainedFiles++;
            }
        }
    }

    private static void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled)
            return;
        String timestamp = LocalDateTime.now().format(LOG_FMT);
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

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }
}