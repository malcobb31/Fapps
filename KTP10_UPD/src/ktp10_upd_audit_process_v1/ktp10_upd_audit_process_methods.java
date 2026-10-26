package ktp10_upd_audit_process_v1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * ktp10_audit_process_methods
 *
 * Reads already-transformed kyc_linked_re-shaped CSV files (pipe delimited,
 * metadata as quoted JSON) and inserts one audit row per CSV row into
 * either:
 *
 *     kyc.kyc_individual_audit_log   (ckyc_no first digit 1-6)
 *     kyc.kyc_legal_audit_log        (ckyc_no first digit 7-9)
 *
 * Both audit tables share an identical schema/mapping for this parser, so a
 * single mapping routine is used and only the target table name changes.
 *
 * This parser is NEW and independent from the kyc_linked_re insert/update
 * parsers. It does not touch kyc.kyc_linked_re at all.
 *
 * Two separate JDBC connections are used:
 *
 *   APP DB  - read-only lookup of re_mgmt.reporting_entity (loaded once)
 *   KYC DB  - one transaction per CSV file, audit inserts via PreparedStatement
 *
 * The CSV is parsed with a custom pipe-delimited parser (not \COPY, not a
 * naive split) because the metadata column contains quoted JSON that may
 * itself contain pipes and escaped/doubled quotes.
 */
public class ktp10_upd_audit_process_methods {

    // ==================================================================
    // LOGGING
    // ==================================================================

    private static final DateTimeFormatter LOG_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String PROCESS_NAME = "KYC_AUDIT_INSERT";
    private static final String PROCESS_VERSION = "1.0.0";

    private final boolean debugEnabled = false;

    private String serverIP;

    private void log(String level, String evtType, String message) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;
        String ts = LocalDateTime.now().format(LOG_FORMAT);
        System.out.println("Time:" + ts + " [" + capitalize(level) + "] "
                + "Process_Name: " + PROCESS_NAME + " Server_IP: " + serverIP
                + " Process_Version: " + PROCESS_VERSION + " Log_Level: EVT_LOG"
                + " EVT_Type: " + (evtType != null ? evtType : "NA")
                + " Description: " + (message != null ? message : ""));
    }

    private String capitalize(String level) {
        if (level == null || level.isEmpty()) return "";
        return level.substring(0, 1).toUpperCase() + level.substring(1).toLowerCase();
    }

    // ==================================================================
    // CONFIGURATION CONSTANTS
    // ==================================================================

    private static final String DEFAULT_DELIMITER = "|";

    /** Target audit tables. Never constructed from CSV data. */
    private static final String INDIVIDUAL_AUDIT_TABLE = "kyc.kyc_individual_audit_log";
    private static final String LEGAL_AUDIT_TABLE = "kyc.kyc_legal_audit_log";

    /** APP DB lookup source. */
    private static final String REPORTING_ENTITY_QUERY =
            "SELECT id, fi_code, name_of_institution FROM re_mgmt.reporting_entity";

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    // ------------------------------------------------------------------
    // Expected / required CSV headers (transformed kyc_linked_re shape)
    // ------------------------------------------------------------------

    private static final Set<String> EXPECTED_CSV_HEADERS = Collections.unmodifiableSet(
            new HashSet<String>(Arrays.asList(
                    "ckyc_no",
                    "entity_type",
                    "reporting_entity_id",
                    "user_id",
                    "title",
                    "first_name",
                    "middle_name",
                    "last_name",
                    "designation",
                    "email",
                    "citizenship",
                    "country_code",
                    "mobile",
                    "dob",
                    "gender",
                    "employee_id",
                    "keycloak_user_id",
                    "user_type",
                    "operational_status",
                    "region",
                    "region_code",
                    "relation_type",
                    "relation_active",
                    "created_by",
                    "updated_by",
                    "created_date",
                    "updated_date",
                    "closed_at",
                    "closed_by",
                    "closure_remarks",
                    "closure_date",
                    "approver_user_id",
                    "approver_re_id",
                    "workflow_id",
                    "fi_code",
                    "re_name",
                    "branch_code",
                    "metadata",
                    "ckyc_reference_id"
            )));

    private static final String[] REQUIRED_CSV_HEADERS = {
            "ckyc_no",
            "fi_code",
            "metadata",
            "ckyc_reference_id"
    };

    // ------------------------------------------------------------------
    // Fields consumed by the audit mapping - excluded from new_data.
    // Kept explicit/visible so it's obvious what is (and isn't) preserved.
    // ------------------------------------------------------------------

    /**
     * Top-level CSV columns that are directly consumed by an explicit audit
     * column mapping (ckyc_number, ckyc_reference_id) or used purely to
     * drive the reporting_entity lookup / table routing (fi_code, ckyc_no).
     * Everything else at the top level is preserved in new_data.
     */
    private static final Set<String> AUDIT_CONSUMED_CSV_FIELDS =
            Collections.unmodifiableSet(
                    new HashSet<String>(Arrays.asList(
                            "ckyc_no",
                            "ckyc_reference_id",
                            "user_id",
                            "created_by",
                            "fi_code"
                    )));

    /**
     * metadata JSON keys that are directly consumed by an explicit audit
     * column mapping. Everything else inside metadata is preserved (nested)
     * in new_data.metadata.
     */
    private static final Set<String> AUDIT_CONSUMED_METADATA_FIELDS =
            Collections.unmodifiableSet(
                    new HashSet<String>(Arrays.asList(
                            "ukm_reason",
                            "ukm_status",
                            "ukm_batch_mode",
                            "ukm_created_date"
                    )));
    private final Comparator<Path> naturalFileComparator =
            (p1, p2) -> compareNaturally(p1.getFileName().toString(), p2.getFileName().toString());

    // ==================================================================
    // CONFIGURATION HOLDERS
    // ==================================================================

    private static class DatabaseConfig {
        final String host, port, database, user, password, url;

        DatabaseConfig(String host, String port, String database, String user, String password) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.user = user;
            this.password = password;
            this.url = String.format(
                    "jdbc:postgresql://%s:%s/%s?socketTimeout=0&tcpKeepAlive=true", host, port, database);
        }
    }

    private static class ProcessConfig {
        final String dirPath;
        final String processedDirPath;
        final boolean hasHeader;
        final String delimiter;
        final DatabaseConfig appDb;
        final DatabaseConfig kycDb;

        ProcessConfig(String dirPath, String processedDirPath, String headerFlag, String delimiter,
                      DatabaseConfig appDb, DatabaseConfig kycDb) {
            this.dirPath = dirPath;
            this.processedDirPath = processedDirPath;
            this.hasHeader = "yes".equalsIgnoreCase(headerFlag) || "true".equalsIgnoreCase(headerFlag);
            this.delimiter = (delimiter == null || delimiter.isEmpty()) ? DEFAULT_DELIMITER : delimiter;
            this.appDb = appDb;
            this.kycDb = kycDb;
        }
    }

    private static class FileProcessResult {
        final Path file;
        final boolean success;
        final String errorMessage;

        FileProcessResult(Path file, boolean success, String errorMessage) {
            this.file = file;
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }

    /** reporting_entity lookup row. */
    private static class ReportingEntityLookup {
        final String id;
        final String nameOfInstitution;

        ReportingEntityLookup(String id, String nameOfInstitution) {
            this.id = id;
            this.nameOfInstitution = nameOfInstitution;
        }
    }

    /** Per-file row counters, logged in FILE_SUMMARY_SUCCESS / FILE_SUMMARY_ERROR. */
    private static class RowCounters {
        long totalRows = 0;
        long individualRows = 0;
        long legalRows = 0;
        long insertedRows = 0;
        long skippedRows = 0;
        long invalidPrefixRows = 0;
        long lookupFailures = 0;
        long metadataFailures = 0;
    }

    // ==================================================================
    // ENTRY POINT
    // ==================================================================

    public void processAllInDirectory(String dirPath, String processedDirPath, String headerFlag, String delimiter,
                                       String appDbHost, String appDbPort, String appDbName, String appDbUser,
                                       String appDbPassword, String kycDbHost, String kycDbPort, String kycDbName,
                                       String kycDbUser, String kycDbPassword, String serverIP) {

        this.serverIP = (serverIP == null || serverIP.trim().isEmpty()) ? resolveServerIP() : serverIP;

        log("INFO", "PROCESS_START", "KYC audit-log insert processing started");

        DatabaseConfig appDb = new DatabaseConfig(appDbHost, appDbPort, appDbName, appDbUser, appDbPassword);
        DatabaseConfig kycDb = new DatabaseConfig(kycDbHost, kycDbPort, kycDbName, kycDbUser, kycDbPassword);

        ProcessConfig config = new ProcessConfig(dirPath, processedDirPath, headerFlag, delimiter, appDb, kycDb);

        try {

            Map<String, ReportingEntityLookup> reLookup = loadReportingEntityLookup(config.appDb);

            executeProcessing(config, reLookup);

            log("INFO", "PROCESS_END", "KYC audit-log insert processing completed successfully");

        } catch (Exception e) {
            log("ERROR", "PROCESS_ERROR", "Processing failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private String resolveServerIP() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "NA";
        }
    }

    // ==================================================================
    // APP DB - reporting_entity LOOKUP (loaded once)
    // ==================================================================

    private Map<String, ReportingEntityLookup> loadReportingEntityLookup(DatabaseConfig appDb) throws SQLException {

        log("INFO", "LOOKUP_START", "Loading re_mgmt.reporting_entity lookup from APP DB");

        Map<String, ReportingEntityLookup> lookup = new HashMap<String, ReportingEntityLookup>();

        try (Connection conn = DriverManager.getConnection(appDb.url, appDb.user, appDb.password);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(REPORTING_ENTITY_QUERY)) {

            while (rs.next()) {
                String id = rs.getString("id");
                String fiCode = rs.getString("fi_code");
                String nameOfInstitution = rs.getString("name_of_institution");

                if (fiCode == null || fiCode.trim().isEmpty()) {
                    continue;
                }

                lookup.put(fiCode.trim().toUpperCase(Locale.ROOT),
                        new ReportingEntityLookup(id, nameOfInstitution));
            }
        }

        log("INFO", "LOOKUP_SUCCESS", "reporting_entity lookup loaded - Entries:" + lookup.size());

        return lookup;
    }

    // ==================================================================
    // DIRECTORY / FILE PROCESSING
    // ==================================================================

    private void executeProcessing(ProcessConfig config, Map<String, ReportingEntityLookup> reLookup) {

        if (!validateDirectories(config.dirPath, config.processedDirPath)) {
            log("ERROR", "DIR_VALIDATION_FAILED", "Directory validation failed");
            return;
        }

        List<Path> csvFiles = discoverCsvFiles(config.dirPath);
        if (csvFiles.isEmpty()) {
            log("INFO", "NO_FILES_FOUND", "No CSV files found in directory: " + config.dirPath);
            return;
        }

        List<FileProcessResult> results = new ArrayList<FileProcessResult>();

        for (Path file : csvFiles) {
            results.add(processSingleFile(config, file, reLookup));
        }

        long successCount = 0;
        for (FileProcessResult r : results) {
            if (r.success) successCount++;
        }

        log("INFO", "BATCH_SUMMARY",
                String.format("Files_Processed:%d Success:%d Failed:%d",
                        results.size(), successCount, results.size() - successCount));

        for (FileProcessResult r : results) {
            if (!r.success) {
                log("ERROR", "FILE_FAILED", "File:" + r.file.getFileName() + " Error:" + r.errorMessage);
            }
        }
    }

    private boolean validateDirectories(String dirPath, String processedDirPath) {
        try {
            Path sourceDir = Paths.get(dirPath);

            if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
                log("ERROR", "DIR_NOT_FOUND", "Source directory does not exist: " + dirPath);
                return false;
            }

            Files.createDirectories(Paths.get(processedDirPath));
            log("INFO", "DIR_VALIDATED", String.format("Source:%s Processed:%s", dirPath, processedDirPath));
            return true;
        } catch (Exception e) {
            log("ERROR", "DIR_VALIDATION_ERROR", "Directory validation failed: " + e.getMessage());
            return false;
        }
    }

    private List<Path> discoverCsvFiles(String dirPath) {
        try (Stream<Path> stream = Files.list(Paths.get(dirPath))) {
            List<Path> files = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".csv"))
                    .sorted(naturalFileComparator)
                    .collect(Collectors.toList());

            log("INFO", "FILE_DISCOVERY", String.format("Files_Found:%d Directory:%s", files.size(), dirPath));
            return files;
        } catch (IOException e) {
            log("ERROR", "FILE_DISCOVERY_ERROR", "Failed to discover CSV files: " + e.getMessage());
            return Collections.emptyList();
        }
    }

    private int compareNaturally(String str1, String str2) {
        int len1 = str1.length();
        int len2 = str2.length();
        int i = 0, j = 0;

        while (i < len1 && j < len2) {
            char c1 = str1.charAt(i);
            char c2 = str2.charAt(j);

            if (Character.isDigit(c1) && Character.isDigit(c2)) {
                StringBuilder num1 = new StringBuilder();
                StringBuilder num2 = new StringBuilder();

                while (i < len1 && Character.isDigit(str1.charAt(i))) num1.append(str1.charAt(i++));
                while (j < len2 && Character.isDigit(str2.charAt(j))) num2.append(str2.charAt(j++));

                int numCompare = Integer.compare(Integer.parseInt(num1.toString()), Integer.parseInt(num2.toString()));
                if (numCompare != 0) return numCompare;
            } else {
                if (c1 != c2) return Character.compare(c1, c2);
                i++;
                j++;
            }
        }

        return Integer.compare(len1, len2);
    }

    // ==================================================================
    // PER FILE
    // ==================================================================

    private FileProcessResult processSingleFile(ProcessConfig config, Path file,
                                                 Map<String, ReportingEntityLookup> reLookup) {

        String csvFileName = file.getFileName().toString();
        LocalDateTime fileStart = LocalDateTime.now();

        log("INFO", "FILE_START", "Starting file processing - File:" + csvFileName);

        try {

            if (isEmptyOrHeaderlessCsv(file)) {
                log("WARNING", "CSV_EMPTY_SKIP",
                        "CSV file is empty or header is missing - skipping file:" + csvFileName);
                return new FileProcessResult(file, true, null);
            }

            RowCounters counters = importAndInsertAuditRows(config, file, reLookup);

            Path moved = moveFileToProcessed(file, Paths.get(config.processedDirPath));

            if (moved == null) {
                return new FileProcessResult(file, false,
                        "File processing succeeded but moving to processed directory failed");
            }

            long durationMs = java.time.Duration.between(fileStart, LocalDateTime.now()).toMillis();

            log("INFO", "FILE_SUMMARY_SUCCESS",
                    "File:" + csvFileName
                            + " Time_Taken_MS:" + durationMs
                            + " Total_Rows:" + counters.totalRows
                            + " Individual_Rows:" + counters.individualRows
                            + " Legal_Rows:" + counters.legalRows
                            + " Inserted_Rows:" + counters.insertedRows
                            + " Skipped_Rows:" + counters.skippedRows
                            + " Invalid_Prefix_Rows:" + counters.invalidPrefixRows
                            + " Lookup_Failures:" + counters.lookupFailures
                            + " Metadata_Failures:" + counters.metadataFailures);

            return new FileProcessResult(file, true, null);

        } catch (Exception e) {

            long durationSec = java.time.Duration.between(fileStart, LocalDateTime.now()).getSeconds();

            log("ERROR", "FILE_SUMMARY_ERROR",
                    "File:" + csvFileName + " Duration:" + durationSec + "s Error:" + e.getMessage());

            // Failed files remain in the input directory.
            return new FileProcessResult(file, false, e.getMessage());
        }
    }

    private boolean isEmptyOrHeaderlessCsv(Path file) throws IOException {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.ISO_8859_1))) {
            String firstLine = br.readLine();
            return firstLine == null || firstLine.trim().isEmpty();
        }
    }

    private Path moveFileToProcessed(Path sourceFile, Path processedDir) {
        try {
            Files.createDirectories(processedDir);
            Path targetFile = processedDir.resolve(sourceFile.getFileName());

            if (Files.exists(targetFile)) {
                String fileName = sourceFile.getFileName().toString();
                int dot = fileName.lastIndexOf('.');
                String baseName = dot >= 0 ? fileName.substring(0, dot) : fileName;
                String extension = dot >= 0 ? fileName.substring(dot) : ".csv";
                String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
                targetFile = processedDir.resolve(baseName + "_" + timestamp + extension);
            }

            Files.move(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);

            log("INFO", "FILE_MOVED", String.format("Source:%s Target:%s", sourceFile, targetFile));

            return targetFile;
        } catch (IOException e) {
            log("ERROR", "FILE_MOVE_ERROR",
                    String.format("Failed to move file - Source:%s Error:%s", sourceFile, e.getMessage()));
            return null;
        }
    }

    // ==================================================================
    // CSV HEADERS
    // ==================================================================

    private String[] readCsvHeaders(Path file, String delimiter) throws IOException {
        try (BufferedReader br = new BufferedReader(
                new InputStreamReader(Files.newInputStream(file), StandardCharsets.ISO_8859_1))) {

            String headerLine = br.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                return new String[0];
            }

            return Arrays.stream(headerLine.split(Pattern.quote(delimiter), -1))
                    .map(this::normalizeCsvHeader)
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new);
        }
    }

    private String normalizeCsvHeader(String header) {
        if (header == null) return "";
        String h = header.trim();

        if (!h.isEmpty() && h.charAt(0) == '\ufeff') {
            h = h.substring(1);
        }

        if (h.startsWith("\"") && h.endsWith("\"") && h.length() >= 2) {
            h = h.substring(1, h.length() - 1);
        }

        return h.trim();
    }

    private void validateCsvHeaders(String[] headers, String fileName) {

        if (headers == null || headers.length == 0) {
            throw new RuntimeException("CSV header is empty: " + fileName);
        }

        Set<String> actualHeaders = new HashSet<String>();
        for (String header : headers) {
            String normalized = normalizeCsvHeader(header).toLowerCase(Locale.ROOT);
            if (!normalized.isEmpty()) {
                actualHeaders.add(normalized);
            }
        }

        List<String> missing = new ArrayList<String>();
        for (String required : REQUIRED_CSV_HEADERS) {
            if (!actualHeaders.contains(required)) {
                missing.add(required);
            }
        }

        if (!missing.isEmpty()) {
            throw new RuntimeException(
                    "Missing required CSV columns in file " + fileName + ": " + String.join(", ", missing));
        }

        List<String> unexpected = new ArrayList<String>();
        for (String actual : actualHeaders) {
            if (!EXPECTED_CSV_HEADERS.contains(actual)) {
                unexpected.add(actual);
            }
        }

        if (!unexpected.isEmpty()) {
            throw new RuntimeException(
                    "Unexpected CSV columns in file " + fileName + ": " + String.join(", ", unexpected));
        }

        Set<String> seen = new HashSet<String>();
        for (String header : headers) {
            String normalized = normalizeCsvHeader(header).toLowerCase(Locale.ROOT);
            if (normalized.isEmpty()) continue;
            if (!seen.add(normalized)) {
                throw new RuntimeException("Duplicate CSV column '" + normalized + "' in file: " + fileName);
            }
        }

        log("INFO", "CSV_HEADERS_VALID",
                "CSV header validation successful - File:" + fileName + " Columns:" + actualHeaders.size());
    }

    // ==================================================================
    // COMPLETE FILE IMPORT - ONE TRANSACTION PER FILE
    // ==================================================================

    private RowCounters importAndInsertAuditRows(ProcessConfig config, Path file,
                                                  Map<String, ReportingEntityLookup> reLookup) throws Exception {

        String csvFileName = file.getFileName().toString();

        RowCounters counters = new RowCounters();

        String[] csvHeaders = readCsvHeaders(file, config.delimiter);
        if (csvHeaders.length == 0) {
            throw new RuntimeException("CSV header is empty: " + csvFileName);
        }

        log("INFO", "CSV_HEADERS", "CSV Headers:" + Arrays.toString(csvHeaders) + " File:" + csvFileName);

        validateCsvHeaders(csvHeaders, csvFileName);

        // Build normalized header -> column index, used to pull values by name
        // regardless of the exact header order in this particular file.
        Map<String, Integer> headerIndex = new HashMap<String, Integer>();
        for (int i = 0; i < csvHeaders.length; i++) {
            headerIndex.put(normalizeCsvHeader(csvHeaders[i]).toLowerCase(Locale.ROOT), i);
        }

        String individualInsertSql = buildAuditInsertSql(INDIVIDUAL_AUDIT_TABLE);
        String legalInsertSql = buildAuditInsertSql(LEGAL_AUDIT_TABLE);

        try (Connection kycConn = DriverManager.getConnection(
                     config.kycDb.url, config.kycDb.user, config.kycDb.password);
             PreparedStatement individualPs = kycConn.prepareStatement(individualInsertSql);
             PreparedStatement legalPs = kycConn.prepareStatement(legalInsertSql);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(Files.newInputStream(file), StandardCharsets.UTF_8))) {

            kycConn.setAutoCommit(false);

            try {

                String headerLine = reader.readLine();
                if (headerLine == null) {
                    throw new RuntimeException("CSV contains no rows: " + csvFileName);
                }

                String line;
                while ((line = reader.readLine()) != null) {

                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    counters.totalRows++;

                    List<String> parsedValues = parseCsvLine(line);

                    if (parsedValues.size() != csvHeaders.length) {
                        throw new RuntimeException(
                                "CSV column count mismatch at row " + (counters.totalRows + 1)
                                        + " Expected:" + csvHeaders.length
                                        + " Actual:" + parsedValues.size());
                    }

                    processRow(csvFileName, counters, headerIndex, parsedValues, reLookup,
                            individualPs, legalPs);
                }

                individualPs.executeBatch();
                legalPs.executeBatch();

                kycConn.commit();

                log("INFO", "TARGET_INSERT_SUCCESS",
                        "File:" + csvFileName
                                + " Total_Rows:" + counters.totalRows
                                + " Inserted_Rows:" + counters.insertedRows);

            } catch (Exception e) {

                try {
                    kycConn.rollback();
                } catch (SQLException rollbackEx) {
                    log("ERROR", "ROLLBACK_ERROR",
                            "Rollback failed for File:" + csvFileName + " Error:" + rollbackEx.getMessage());
                }

                throw e;
            }

        } catch (Exception e) {

            log("ERROR", "CSV_IMPORT_ERROR",
                    "File:" + csvFileName
                            + " Rows_Processed:" + counters.totalRows
                            + " Rows_Inserted:" + counters.insertedRows
                            + " Error:" + e.getMessage());

            throw e;
        }

        return counters;
    }

    /**
     * Processes a single CSV row: determines target audit table from the
     * ckyc_no prefix, builds the audit record, and adds it to the
     * appropriate PreparedStatement batch.
     */
    private void processRow(String csvFileName, RowCounters counters, Map<String, Integer> headerIndex,
                             List<String> values, Map<String, ReportingEntityLookup> reLookup,
                             PreparedStatement individualPs, PreparedStatement legalPs) throws Exception {

        String ckycNo = getValue(values, headerIndex, "ckyc_no");

        String targetTable = routeByCkycPrefix(ckycNo);

        if (targetTable == null) {
            counters.invalidPrefixRows++;
            counters.skippedRows++;
            log("WARNING", "ROW_ROUTING",
                    "Row skipped - invalid/absent CKYC prefix - File:" + csvFileName
                            + " ckyc_no:" + ckycNo);
            return;
        }

        if (INDIVIDUAL_AUDIT_TABLE.equals(targetTable)) {
            counters.individualRows++;
        } else {
            counters.legalRows++;
        }

        log("DEBUG", "ROW_ROUTING", "File:" + csvFileName + " ckyc_no:" + ckycNo + " Target:" + targetTable);

        String fiCode = getValue(values, headerIndex, "fi_code");

        ReportingEntityLookup re = null;
        if (fiCode != null) {
            re = reLookup.get(fiCode.trim().toUpperCase(Locale.ROOT));
        }

        if (re == null) {
            counters.lookupFailures++;
            counters.skippedRows++;
            log("WARNING", "LOOKUP_FAILURE",
                    "Row skipped - no reporting_entity match for fi_code - File:" + csvFileName
                            + " fi_code:" + fiCode + " ckyc_no:" + ckycNo);
            return;
        }

        String metadataRaw = getValue(values, headerIndex, "metadata");

        ObjectNode metadataNode;
        try {
            metadataNode = parseMetadata(metadataRaw);
        } catch (Exception metaEx) {
            counters.metadataFailures++;
            // Metadata is malformed: fail the whole file per the error policy
            // (rollback happens at the caller level).
            throw new RuntimeException(
                    "Malformed metadata JSON at row (ckyc_no=" + ckycNo + "): " + metaEx.getMessage(), metaEx);
        }

        AuditRecord record = buildAuditRecord(csvFileName, headerIndex, values, ckycNo, fiCode, re, metadataNode);

        PreparedStatement ps = INDIVIDUAL_AUDIT_TABLE.equals(targetTable) ? individualPs : legalPs;

        boolean legalTable =
                LEGAL_AUDIT_TABLE.equals(targetTable);

        bindAuditParameters(
                ps,
                record,
                legalTable
        );
        ps.addBatch();

        counters.insertedRows++;

        log("DEBUG", "AUDIT_INSERT",
                "File:" + csvFileName + " ckyc_no:" + ckycNo + " Target:" + targetTable);
    }

    /**
     * Routes to the correct audit table based on the FIRST character of
     * ckyc_no. Does not cast the whole value to an integer.
     *
     * '1'-'6' -> kyc.kyc_individual_audit_log
     * '7'-'9' -> kyc.kyc_legal_audit_log
     * anything else (null/empty/non-digit) -> null (row rejected)
     */
    private String routeByCkycPrefix(String ckycNo) {
        if (ckycNo == null || ckycNo.isEmpty()) {
            return null;
        }

        char first = ckycNo.charAt(0);

        if (first >= '1' && first <= '6') {
            return INDIVIDUAL_AUDIT_TABLE;
        }

        if (first >= '7' && first <= '9') {
            return LEGAL_AUDIT_TABLE;
        }

        return null;
    }

    private String getValue(List<String> values, Map<String, Integer> headerIndex, String columnName) {
        Integer idx = headerIndex.get(columnName);
        if (idx == null || idx >= values.size()) {
            return null;
        }
        return values.get(idx);
    }

    // ==================================================================
    // METADATA PARSING
    // ==================================================================

    private ObjectNode parseMetadata(String metadataRaw) throws IOException {
        if (metadataRaw == null || metadataRaw.trim().isEmpty()) {
            return JSON_MAPPER.createObjectNode();
        }

        JsonNode node = JSON_MAPPER.readTree(metadataRaw);

        if (node == null || node.isNull()) {
            return JSON_MAPPER.createObjectNode();
        }

        if (!node.isObject()) {
            throw new IOException("metadata is not a JSON object");
        }

        return (ObjectNode) node;
    }

    private String metadataText(ObjectNode metadata, String key) {
        if (metadata == null) return null;
        JsonNode v = metadata.get(key);
        if (v == null || v.isNull()) return null;
        String text = v.asText();
        return (text == null || text.trim().isEmpty()) ? null : text.trim();
    }

    // ==================================================================
    // AUDIT RECORD (intermediate holder before binding to PreparedStatement)
    // ==================================================================

    private static class AuditRecord {
    	String ckycNumber;
        String ckycReferenceId;
        String action;
        // original_data always null
        String newDataJson;
        String reName;
        String reUserId;
        // re_ckyc_number always null
        // re_branch always null
        String institutionName;
        String institutionCode;
        String changeReason;
        String status;
        String environment;
        String modeOfOperation;
        Timestamp submittedOn;
        String submittedBy;
        String level;
        boolean isFinalApproval;
        Timestamp createdAt;
        String reId;
        String fiCode;
    }

    private AuditRecord buildAuditRecord(String csvFileName, Map<String, Integer> headerIndex, List<String> values,
                                          String ckycNo, String fiCode, ReportingEntityLookup re,
                                          ObjectNode metadata) {

        AuditRecord rec = new AuditRecord();

        rec.ckycNumber = ckycNo;
        rec.ckycReferenceId = getValue(values, headerIndex, "ckyc_reference_id");

        rec.action = "UPDATE";

        rec.reName = re.nameOfInstitution;
        rec.reUserId = getValue(values, headerIndex, "user_id");
        rec.institutionName = re.nameOfInstitution;
        rec.institutionCode = fiCode;

        rec.changeReason = metadataText(metadata, "ukm_reason");

        String ukmStatus = metadataText(metadata, "ukm_status");
        rec.status = "KU".equalsIgnoreCase(ukmStatus) ? "SUCCESS" : null;

        rec.environment = "PROD";

        String batchMode = metadataText(metadata, "ukm_batch_mode");
        rec.modeOfOperation = mapModeOfOperation(batchMode);

        String submittedOnRaw = metadataText(metadata, "ukm_created_date");
        rec.submittedOn = parseTimestamp(submittedOnRaw, csvFileName, "ukm_created_date");

        rec.submittedBy = getValue(values, headerIndex, "created_by");

        rec.level = "UPDATE";
        rec.isFinalApproval = true;

        rec.createdAt = parseTimestamp(submittedOnRaw, csvFileName, "ukm_created_date");

        rec.reId = re.id;
        rec.fiCode = fiCode;

        rec.newDataJson = buildNewData(headerIndex, values, metadata).toString();

        return rec;
    }

    /**
     * B -> BULK, S -> WEB, anything else (including null/empty) -> API
     */
    private String mapModeOfOperation(String batchMode) {
        if (batchMode == null || batchMode.trim().isEmpty()) {
            return "API";
        }

        String v = batchMode.trim().toUpperCase(Locale.ROOT);

        if ("B".equals(v)) return "BULK";
        if ("S".equals(v)) return "WEB";
        return "API";
    }

    private Timestamp parseTimestamp(String raw, String csvFileName, String fieldName) {
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }

        String v = raw.trim();

        // Common formats seen in metadata: "yyyy-MM-dd HH:mm:ss" or ISO "yyyy-MM-dd'T'HH:mm:ss"
        try {
            return Timestamp.valueOf(v.replace('T', ' '));
        } catch (IllegalArgumentException e) {
            log("WARNING", "TIMESTAMP_PARSE_WARNING",
                    "Could not parse timestamp field '" + fieldName + "' value '" + v
                            + "' in file " + csvFileName + " - storing NULL");
            return null;
        }
    }

    // ==================================================================
    // NEW_DATA CONSTRUCTION (preservation bucket)
    // ==================================================================

    /**
     * Builds the new_data JSON object:
     *
     *  1. every top-level CSV column NOT in AUDIT_CONSUMED_CSV_FIELDS is
     *     preserved as-is (string value, possibly null)
     *  2. metadata is parsed, keys in AUDIT_CONSUMED_METADATA_FIELDS are
     *     excluded, every other key/value pair is preserved nested under
     *     "metadata" as a real JSON object (not an escaped string)
     */
    private ObjectNode buildNewData(Map<String, Integer> headerIndex, List<String> values, ObjectNode metadata) {

        ObjectNode newData = JSON_MAPPER.createObjectNode();

        // Preserve unused top-level CSV columns, in header order for stable output.
        List<Map.Entry<String, Integer>> orderedHeaders = new ArrayList<Map.Entry<String, Integer>>(headerIndex.entrySet());
        orderedHeaders.sort((a, b) -> Integer.compare(a.getValue(), b.getValue()));

        for (Map.Entry<String, Integer> entry : orderedHeaders) {
            String columnName = entry.getKey();

            if (AUDIT_CONSUMED_CSV_FIELDS.contains(columnName)) {
                continue;
            }

            // "metadata" itself is handled separately below (nested object, not raw text).
            if ("metadata".equals(columnName)) {
                continue;
            }

            int idx = entry.getValue();
            String value = idx < values.size() ? values.get(idx) : null;

            if (value == null) {
                newData.putNull(columnName);
            } else {
                newData.put(columnName, value);
            }
        }

        // Preserve unused metadata keys, nested as a real JSON object.
        ObjectNode preservedMetadata = JSON_MAPPER.createObjectNode();

        java.util.Iterator<Map.Entry<String, JsonNode>> fields = metadata.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String key = field.getKey();

            if (AUDIT_CONSUMED_METADATA_FIELDS.contains(key)) {
                continue;
            }

            preservedMetadata.set(key, field.getValue());
        }

        newData.set("metadata", preservedMetadata);

        return newData;
    }

    // ==================================================================
    // AUDIT INSERT SQL
    // ==================================================================

    private String buildAuditInsertSql(String targetTable) {
        return "INSERT INTO " + targetTable + " ("
                + "ckyc_number,"
                + "ckyc_reference_id,"
                + "action,"
                + "original_data,"
                + "new_data,"
                + "re_name,"
                + "re_user_id,"
                + "re_ckyc_number,"
                + "re_branch,"
                + "institution_name,"
                + "institution_code,"
                + "change_reason,"
                + "status,"
                + "environment,"
                + "mode_of_operation,"
                + "submitted_on,"
                + "submitted_by,"
                + "level,"
                + "is_final_approval,"
                + "created_at,"
                + "re_id,"
                + "fi_code"
                + ") VALUES (?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    private void bindAuditParameters(
            PreparedStatement ps,
            AuditRecord rec,
            boolean legalTable) throws SQLException {

        int i = 1;

        // ckyc_number:
        // Individual audit table -> VARCHAR
        // Legal audit table      -> BIGINT
        if (legalTable) {

            if (rec.ckycNumber == null || rec.ckycNumber.trim().isEmpty()) {

                ps.setNull(i++, Types.BIGINT);

            } else {

                try {
                    ps.setLong(
                            i++,
                            Long.parseLong(rec.ckycNumber.trim())
                    );

                } catch (NumberFormatException e) {

                    throw new SQLException(
                            "Invalid numeric ckyc_number for legal audit table: "
                                    + rec.ckycNumber,
                            e
                    );
                }
            }

        } else {

            setStringOrNull(
                    ps,
                    i++,
                    rec.ckycNumber
            );
        }

        setStringOrNull(ps, i++, rec.ckycReferenceId);
        setStringOrNull(ps, i++, rec.action);

        // original_data
        ps.setNull(i++, Types.OTHER);

        // new_data -> ?::jsonb
        setStringOrNull(ps, i++, rec.newDataJson);

        setStringOrNull(ps, i++, rec.reName);
        setStringOrNull(ps, i++, rec.reUserId);

        // re_ckyc_number
        ps.setNull(i++, Types.VARCHAR);

        // re_branch
        ps.setNull(i++, Types.VARCHAR);

        setStringOrNull(ps, i++, rec.institutionName);
        setStringOrNull(ps, i++, rec.institutionCode);
        setStringOrNull(ps, i++, rec.changeReason);
        setStringOrNull(ps, i++, rec.status);
        setStringOrNull(ps, i++, rec.environment);
        setStringOrNull(ps, i++, rec.modeOfOperation);

        // submitted_on -> timestamp without time zone
        if (rec.submittedOn == null) {
            ps.setNull(i++, Types.TIMESTAMP);
        } else {
            ps.setTimestamp(i++, rec.submittedOn);
        }

        setStringOrNull(ps, i++, rec.submittedBy);
        setStringOrNull(ps, i++, rec.level);

        // is_final_approval
        ps.setBoolean(i++, rec.isFinalApproval);

        // created_at -> timestamp with time zone
        if (rec.createdAt == null) {
            ps.setNull(i++, Types.TIMESTAMP_WITH_TIMEZONE);
        } else {
            ps.setTimestamp(i++, rec.createdAt);
        }

        setStringOrNull(ps, i++, rec.reId);
        setStringOrNull(ps, i++, rec.fiCode);
    }

    private void setStringOrNull(PreparedStatement ps, int index, String value) throws SQLException {
        if (value == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, value);
        }
    }

    // ==================================================================
    // CUSTOM CSV PARSER
    // ==================================================================

    /**
     * Custom pipe-delimited CSV parser. Handles:
     *
     * - pipe delimiter
     * - quoted fields (pipes inside quotes are preserved)
     * - "" escaped quotes
     * - \" escaped quotes
     * - JSON content inside a quoted metadata field
     * - \N and empty-string normalization to null
     */
    private List<String> parseCsvLine(String line) throws Exception {

        List<String> values = new ArrayList<String>();

        if (line == null) {
            return values;
        }

        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int length = line.length();

        for (int i = 0; i < length; i++) {

            char ch = line.charAt(i);

            // Opening quote
            if (!inQuotes && ch == '"') {
                inQuotes = true;
                continue;
            }

            // Inside quoted field
            if (inQuotes && ch == '"') {

                // CSV escaped quote: ""
                if (i + 1 < length && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                    continue;
                }

                // Closing quote
                inQuotes = false;
                continue;
            }

            // Pipe delimiter outside quoted field
            if (!inQuotes && ch == '|') {
                values.add(normalizeCsvValue(current.toString()));
                current.setLength(0);
                continue;
            }

            current.append(ch);
        }

        if (inQuotes) {
            throw new RuntimeException("unclosed_quotes");
        }

        values.add(normalizeCsvValue(current.toString()));

        return values;
    }

    private String normalizeCsvValue(String value) {
        if (value == null) return null;

        String trimmed = value.trim();

        if (trimmed.isEmpty()) return null;
        if ("\\N".equalsIgnoreCase(trimmed)) return null;
        if ("NULL".equalsIgnoreCase(trimmed)) return null;

        return trimmed;
    }
}