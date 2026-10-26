package ktp10_upd_process_v1;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * CSV-to-CSV ETL for the UKM ("KU"-status) update feed, targeting the same
 * logical shape as kyc.kyc_linked_re.
 *
 * This is a SEPARATE parser modeled on ktp10_update_methods (ktp10_update_v1)
 * and ktp10_process_methods (ktp10_process_v2). Neither of those classes is
 * modified by this file.
 *
 * Differences from ktp10_update_methods:
 *   - Source columns are UKM-prefixed (ukm_*), not KLI-prefixed (kli_*).
 *   - Only rows where normalize(ukm_status) == "KU" are processed; every
 *     other row is skipped BEFORE mapping/enrichment.
 *   - relation_type default is "UPDATE" (not "DOWNLOAD").
 *   - user_id / re_user enrichment key is ukm_user_updated (not kli_created_by).
 *   - created_date is intentionally sourced from ukm_updated_date (see mapping doc).
 *   - ckyc_reference_no and id remain unresolved/omitted from the output CSV;
 *     see loadCkycReferenceMap() and OUTPUT_HEADERS comments below.
 *
 * Unresolved dependencies (documented, not invented):
 *   - kyc_alphanumeric_id_table.kat_alphanumeric_refno lookup for
 *     ckyc_reference_no: schema/query not confirmed. Stubbed out below.
 *   - re_user.poi / re_user.poi_number: not present in the existing re_user
 *     lookup query in either reference parser. Omitted from OUTPUT_HEADERS
 *     until confirmed present in the real schema.
 */
public class ktp10_upd_process_methods {

    private static final Logger LOGGER = Logger.getLogger(ktp10_upd_process_methods.class.getName());

    private static final String PROCESS_NAME = "UKM_KU_STATUS_UPDATE_CSV";
    private static final String PROCESS_VERSION = "1.0.0";
    private static final boolean debugEnabled = false;
    private static final DateTimeFormatter LOG_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final int MAX_PARALLEL_FILES = 1;
    private static final int DEFAULT_FLUSH_SIZE = 50000;
    private static final int MAX_FLUSH_SIZE = 50000;
    private static final SimpleDateFormat OUT_FILE_NAME_FMT =
            new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss_SSS");

    private static final String OUT_DELIM = "|";
    private static final SimpleDateFormat OUT_TS_FMT_TEMPLATE = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat OUT_DATE_FMT_TEMPLATE = new SimpleDateFormat("yyyy-MM-dd");

    private static final String STATUS_FILTER_VALUE = "KU";

    // Source columns that are explicitly mapped and must NOT fall into metadata.
    // ukm_status is included here too: it is a filter/control field, never metadata.
    private static final String[] MAPPED_HEADERS = new String[] {
            "ukm_ckyc_no",
            "ukm_lastipvupdated_fi",
            "ukm_region_code",
            "ukm_branch_code",
            "ukm_user_created",
            "ukm_user_updated",
            "ukm_updated_date",
            "ukm_upd_checker_approved",
            "ukm_status"
    };

    // NOTE: id and ckyc_reference_no are intentionally omitted.
    // - id: this is a CSV-to-CSV parser, not a DB insert; no surrogate key is
    //   generated at this stage (see class-level doc comment and HLD).
    // - ckyc_reference_no: kat_alphanumeric_refno lookup is an unresolved
    //   schema dependency (see loadCkycReferenceMap()); once confirmed, add
    //   the column back here and to toOutputRow()/RowData.
    // NOTE: poi / poi_number are omitted pending confirmation that these
    // columns exist on re_mgmt.re_user (see SQL_RE_USER_ALL / ReUserData).
    private static final String[] OUTPUT_HEADERS = new String[] {
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
            "metadata"
    };

    private static final String SQL_REPORTING_ENTITY_ALL =
            "SELECT fi_code, id FROM re_mgmt.reporting_entity";

    private static final String SQL_RE_USER_ALL =
            "SELECT user_id, title, first_name, middle_name, last_name, designation, email, "
                    + "citizenship, country_code, mobile, dob, gender, "
                    + "employee_id, key_cloak_user_id, user_type "
                    + "FROM re_mgmt.re_user";

    // UNRESOLVED DEPENDENCY — see class doc comment / Ambiguity A.
    // Proposed shape only; NOT executed until the schema is confirmed.
    // private static final String SQL_CKYC_REFERENCE_ALL =
    //         "SELECT kat_ckyc_no, kat_alphanumeric_refno FROM kyc.kyc_alphanumeric_id_table";

    public static void process(File inputPath, File archiveDir, File outputDir,
            String appJdbcUrl, String appDbUser, String appDbPass,
            String kycJdbcUrl, String kycDbUser, String kycDbPass,
            String serverIP, int flushSize) {

        DirectoryResult dirResult = new DirectoryResult();

        log(
                "INFO",
                "process_start input_path=" + inputPath.getAbsolutePath()
                        + " archive_dir=" + archiveDir.getAbsolutePath()
                        + " output_dir=" + outputDir.getAbsolutePath(),
                serverIP,
                "PROCESS_START");

        Map<String, String> reportingEntityMap;
        Map<String, ReUserData> reUserMap;

        try {
            AppLookups lookups = loadAppLookups(appJdbcUrl, appDbUser, appDbPass, serverIP);
            reportingEntityMap = lookups.reportingEntityMap;
            reUserMap = lookups.reUserMap;
        } catch (SQLException sqle) {
            log(
                    "ERROR",
                    "fatal_error=app_db_lookup_load_failed reason=" + rootCause(sqle),
                    serverIP,
                    "FATAL_ERROR");
            return;
        }

        // KYC DB credentials are accepted and connection-tested here per requirement,
        // even though no query currently depends on them (see Ambiguity A /
        // loadCkycReferenceMap). This keeps the credential plumbing ready without
        // inventing the kat_alphanumeric_refno lookup.
        if (kycJdbcUrl != null && kycJdbcUrl.trim().length() > 0) {
            try {
                loadCkycReferenceMap(kycJdbcUrl, kycDbUser, kycDbPass, serverIP);
            } catch (SQLException sqle) {
                log(
                        "WARN",
                        "kyc_db_lookup_unavailable reason=" + rootCause(sqle)
                                + " ckyc_reference_no will remain unresolved (\\N) for this run",
                        serverIP,
                        "LOOKUP");
            }
        }

        log(
                "INFO",
                "app_lookups_loaded reporting_entity=" + reportingEntityMap.size()
                        + " re_user=" + reUserMap.size(),
                serverIP,
                "LOOKUP");

        List<File> files = collectCsvFiles(inputPath);
        dirResult.totalFiles = files.size();

        log(
                "INFO",
                "directory_start input_path=" + inputPath.getAbsolutePath()
                        + " archive_dir=" + archiveDir.getAbsolutePath()
                        + " output_dir=" + outputDir.getAbsolutePath()
                        + " total_files=" + files.size()
                        + " flush_size=" + flushSize,
                serverIP,
                "DIRECTORY_START");

        if (files.isEmpty()) {
            logDirectorySummary(dirResult, serverIP);
            return;
        }

        int poolSize = getWorkerPoolSize(files.size());
        ExecutorService executor = Executors.newFixedThreadPool(poolSize);

        final Map<String, String> fReportingEntityMap = reportingEntityMap;
        final Map<String, ReUserData> fReUserMap = reUserMap;

        List<Future<FileResult>> futures = new ArrayList<Future<FileResult>>();
        for (int i = 0; i < files.size(); i++) {
            final File file = files.get(i);
            futures.add(executor.submit(new Callable<FileResult>() {
                public FileResult call() {
                    return processSingleFile(file, archiveDir, outputDir,
                            fReportingEntityMap, fReUserMap,
                            serverIP, flushSize);
                }
            }));
        }

        executor.shutdown();

        for (int i = 0; i < futures.size(); i++) {
            try {
                FileResult result = futures.get(i).get();

                dirResult.totalRows += result.dataRows;
                dirResult.kuRows += result.kuRows;
                dirResult.skippedNonKu += result.skippedNonKu;
                dirResult.written += result.written;
                dirResult.skipped += result.skipped;
                dirResult.unmatchedFiCode += result.unmatchedFiCode;
                dirResult.unmatchedUser += result.unmatchedUser;
                dirResult.blankLines += result.blankLines;

                if ("SUCCESS".equals(result.status)) {
                    dirResult.completedFiles++;
                } else {
                    dirResult.retainedFiles++;
                }

                if ("EMPTY".equals(result.status)) {
                    dirResult.emptyFiles++;
                } else if ("HEADER_ONLY".equals(result.status)) {
                    dirResult.headerOnlyFiles++;
                } else if ("ERROR".equals(result.status)) {
                    dirResult.errorFiles++;
                }

            } catch (Exception ex) {
                dirResult.errorFiles++;
                dirResult.retainedFiles++;
                log(
                        "ERROR",
                        "fatal_error=directory_worker_failure reason=" + rootCause(ex),
                        serverIP,
                        "FATAL_ERROR");
            }
        }

        logDirectorySummary(dirResult, serverIP);
    }
    private static String buildOutputFileName(File sourceFile) {
        String sourceName = sourceFile.getName();

        String baseName = sourceName;
        if (baseName.toLowerCase(Locale.ENGLISH).endsWith(".csv")) {
            baseName = baseName.substring(0, baseName.length() - 4);
        }

        // Expected source format:
        // raw_NA_NA_NA_7_2021_08_20_14_10_03_230
        //
        // Index:
        // 0 = raw
        // 1 = NA
        // 2 = NA
        // 3 = NA
        // 4 = SANO
        // 5 = yyyy
        // 6 = MM
        // 7 = dd
        // 8 = HH
        // 9 = mm
        // 10 = ss
        // 11 = millis

        String[] parts = baseName.split("_");

        if (parts.length < 5) {
            throw new IllegalArgumentException(
                    "Invalid source filename format: " + sourceName);
        }

        String sano = parts[4];

        String timestamp;
        synchronized (OUT_FILE_NAME_FMT) {
            timestamp = OUT_FILE_NAME_FMT.format(new java.util.Date());
        }

        return "raw_NA_NA_NA_"
                + sano
                + "_"
                + timestamp
                + ".csv";
    }
    private static int getWorkerPoolSize(int fileCount) {
        if (fileCount <= 0) {
            return 1;
        }
        int cpu = Runtime.getRuntime().availableProcessors();
        int preferred = cpu / 2;
        if (preferred < 1) {
            preferred = 1;
        }
        if (preferred > MAX_PARALLEL_FILES) {
            preferred = MAX_PARALLEL_FILES;
        }
        if (fileCount < preferred) {
            return fileCount;
        }
        return preferred;
    }

    private static FileResult processSingleFile(File file, File archiveDir, File outputDir,
            Map<String, String> reportingEntityMap, Map<String, ReUserData> reUserMap,
            String serverIP, int configuredFlushSize) {

        FileResult result = new FileResult(file.getName());

        log("INFO", "file_start file=" + file.getName(), serverIP, "FILE_START");

        BufferedReader reader = null;
        BufferedWriter writer = null;

        boolean headerCaptured = false;
        boolean writeStarted = false;
        boolean writeCompleted = false;

        String[] header = null;
        int flushSize = prepareFlushSize(configuredFlushSize);
        result.effectiveFlushSize = flushSize;

        Set<String> loggedFiCodeMisses = new HashSet<String>();
        Set<String> loggedUserMisses = new HashSet<String>();
        boolean nonKuSkipLogged = false;

        File outputFile = new File(outputDir, buildOutputFileName(file));

        try {
            if (!outputDir.exists()) {
                Files.createDirectories(outputDir.toPath());
            }

            reader = new BufferedReader(
                    new InputStreamReader(
                            new FileInputStream(file),
                            "UTF-8"));

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().length() == 0) {
                    result.blankLines++;
                    continue;
                }

                List<String> parsed = parseCsvLine(line);
                String[] row = toArray(parsed);

                if (!headerCaptured) {
                    if (row.length == 0) {
                        continue;
                    }
                    row[0] = removeBom(row[0]);
                    header = row;
                    headerCaptured = true;
                    continue;
                }

                result.dataRows++;

                // KU filter happens first, before any mapping/enrichment work,
                // and before the writer is even created for this row.
                String ukmStatus = extractUkmStatus(header, row);
                if (!STATUS_FILTER_VALUE.equals(ukmStatus)) {
                    result.skippedNonKu++;
                    if (!nonKuSkipLogged) {
                        // Logged once per file (not per row) to avoid log flooding.
                        log(
                                "INFO",
                                "file=" + file.getName() + " non_ku_rows_present filter=ukm_status!=KU",
                                serverIP,
                                "ROW_SKIPPED");
                        nonKuSkipLogged = true;
                    }
                    continue;
                }
                result.kuRows++;

                if (!writeStarted) {
                    writer = new BufferedWriter(new OutputStreamWriter(new java.io.FileOutputStream(outputFile), "UTF-8"));
                    writeOutputRow(writer, OUTPUT_HEADERS);
                    writeStarted = true;
                }

                RowData mapped = mapRow(header, row);
                if (mapped == null) {
                    result.skipped++;
                    continue;
                }

                if (!enrichFromMaps(mapped, reportingEntityMap, reUserMap,
                        loggedFiCodeMisses, loggedUserMisses,
                        result, file.getName(), serverIP)) {
                    result.skipped++;
                    continue;
                }

                String[] outRow = toOutputRow(mapped);
                writeOutputRow(writer, outRow);
                result.written++;

                if (result.written % flushSize == 0) {
                    writer.flush();
                    log(
                            "INFO",
                            "file=" + file.getName() + " rows_written_so_far=" + result.written,
                            serverIP,
                            "WRITE_FLUSH");
                }
            }

            if (!headerCaptured) {
                result.status = "EMPTY";
                result.moved = false;
                logFileCompletion(result, serverIP);
                return result;
            }

            if (result.dataRows == 0) {
                result.status = "HEADER_ONLY";
                result.moved = false;
                logFileCompletion(result, serverIP);
                return result;
            }

            // Rows existed but none were KU: this is a successful run with a
            // header-only output CSV, not an input error. Write the header now
            // if it was never written (i.e. writer was never opened).
            if (!writeStarted) {
                writer = new BufferedWriter(new OutputStreamWriter(new java.io.FileOutputStream(outputFile), "UTF-8"));
                writeOutputRow(writer, OUTPUT_HEADERS);
                writeStarted = true;
            }

            if (writer != null) {
                writer.flush();
            }
            writeCompleted = true;
            result.status = "SUCCESS";

            try {
                moveFile(file, archiveDir);
                result.moved = true;
                log("INFO", "file_moved file=" + file.getName(), serverIP, "FILE_MOVED");
            } catch (IOException ioe) {
                result.status = "ERROR";
                result.moved = false;
                log(
                        "ERROR",
                        "file=" + file.getName() + "\nreason=" + rootCause(ioe),
                        serverIP,
                        "FILE_ERROR");
            }

        } catch (FatalProcessingException fpe) {
            result.status = "ERROR";
            result.moved = false;
            log(
                    "ERROR",
                    "file=" + file.getName() + "\nreason=" + rootCause(fpe),
                    serverIP,
                    "FATAL_ERROR");
        } catch (RuntimeException re) {
            result.status = "ERROR";
            result.moved = false;
            log(
                    "ERROR",
                    "file=" + file.getName() + "\nreason=" + rootCause(re),
                    serverIP,
                    "FATAL_ERROR");
        } catch (Exception ex) {
            result.status = "ERROR";
            result.moved = false;
            log(
                    "ERROR",
                    "file=" + file.getName() + "\nreason=" + rootCause(ex),
                    serverIP,
                    "FATAL_ERROR");
        } finally {
            closeQuietly(writer);
            closeQuietly(reader);

            if (!writeCompleted && "SUCCESS".equals(result.status)) {
                result.status = "ERROR";
            }

            if (!writeCompleted && outputFile.exists()) {
                try {
                    Files.deleteIfExists(outputFile.toPath());
                } catch (IOException ignore) {
                    // best effort cleanup
                }
            }
        }

        logFileCompletion(result, serverIP);
        return result;
    }

    private static int prepareFlushSize(int configuredFlushSize) {
        int flushSize = configuredFlushSize;
        if (flushSize <= 0) {
            flushSize = DEFAULT_FLUSH_SIZE;
        }
        if (flushSize > MAX_FLUSH_SIZE) {
            flushSize = MAX_FLUSH_SIZE;
        }
        return flushSize;
    }

    private static List<File> collectCsvFiles(File inputPath) {
        if (inputPath == null || !inputPath.exists()) {
            return Collections.emptyList();
        }

        List<File> files = new ArrayList<File>();

        if (inputPath.isFile()) {
            if (isCsvFile(inputPath)) {
                files.add(inputPath);
            }
            return files;
        }

        File[] listed = inputPath.listFiles();
        if (listed == null || listed.length == 0) {
            return files;
        }

        for (int i = 0; i < listed.length; i++) {
            File f = listed[i];
            if (f != null && f.isFile() && isCsvFile(f)) {
                files.add(f);
            }
        }

        Collections.sort(files, new Comparator<File>() {
            public int compare(File a, File b) {
                return a.getName().compareToIgnoreCase(b.getName());
            }
        });

        return files;
    }

    private static boolean isCsvFile(File file) {
        if (file == null) {
            return false;
        }
        String name = file.getName();
        if (name == null) {
            return false;
        }
        return name.toLowerCase(Locale.ENGLISH).endsWith(".csv");
    }

    private static List<String> parseCsvLine(String line) throws FatalProcessingException {
        List<String> values = new ArrayList<String>();

        if (line == null) {
            return values;
        }

        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        int length = line.length();

        for (int i = 0; i < length; i++) {
            char ch = line.charAt(i);

            /*
             * A quoted field starts with:
             *
             * |"
             *
             * or at the beginning of the line:
             *
             * "
             */
            if (!inQuotes && ch == '"') {
                inQuotes = true;
                continue;
            }

            /*
             * Inside a quoted field:
             *
             * The field ends ONLY when we see:
             *
             * "|
             *
             * or a final quote at end-of-line.
             *
             * A quote preceded by '\' is data and NOT a closing quote.
             */
            if (inQuotes && ch == '"') {

                // Backslash escaped quote: \"
                if (i > 0 && line.charAt(i - 1) == '\\') {
                    current.append(ch);
                    continue;
                }

                // Standard CSV escaped quote: ""
                if (i + 1 < length && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                    continue;
                }

                // Quote followed by pipe = end of quoted column
                if (i + 1 < length && line.charAt(i + 1) == '|') {
                    inQuotes = false;
                    continue;
                }

                // Quote at the very end of the record = end of field
                if (i + 1 == length) {
                    inQuotes = false;
                    continue;
                }

                /*
                 * Any other quote inside a quoted field is data.
                 * DO NOT toggle quote state.
                 */
                current.append(ch);
                continue;
            }

            /*
             * Pipe outside a quoted field is a column delimiter.
             */
            if (ch == '|' && !inQuotes) {
                values.add(current.toString());
                current.setLength(0);
                continue;
            }

            current.append(ch);
        }

        if (inQuotes) {
            throw new FatalProcessingException("unclosed_quotes");
        }

        values.add(current.toString());

        return values;
    }

    private static String[] toArray(List<String> values) {
        String[] arr = new String[values.size()];
        for (int i = 0; i < values.size(); i++) {
            arr[i] = values.get(i);
        }
        return arr;
    }

    /**
     * Extracts and normalizes ukm_status without building the full source map,
     * so the KU filter can run cheaply before mapping/enrichment.
     */
    private static String extractUkmStatus(String[] header, String[] row) {
        if (header == null) {
            return null;
        }
        for (int i = 0; i < header.length; i++) {
            String h = cleanHeader(header[i]);
            if (h == null) {
                continue;
            }
            if ("ukm_status".equalsIgnoreCase(h)) {
                if (i < row.length) {
                    return normalize(row[i]);
                }
                return null;
            }
        }
        return null;
    }

    private static RowData mapRow(String[] header, String[] row) throws FatalProcessingException {
        if (header == null || header.length == 0) {
            throw new FatalProcessingException("missing_header");
        }

        if (row == null) {
            throw new FatalProcessingException("unexpected_null_row");
        }

        if (row.length > header.length) {
            for (int i = header.length; i < row.length; i++) {
                String extra = normalize(row[i]);
                if (extra != null) {
                    throw new FatalProcessingException("malformed_csv_row");
                }
            }
        }

        Map<String, String> sourceMap = new LinkedHashMap<String, String>();
        for (int i = 0; i < header.length; i++) {
            String h = cleanHeader(header[i]);
            if (h == null || h.length() == 0) {
                continue;
            }

            String value = null;
            if (i < row.length) {
                value = normalize(row[i]);
            }
            sourceMap.put(h.toLowerCase(Locale.ENGLISH), value);
        }

        String ckycNoText = normalize(sourceMap.get("ukm_ckyc_no"));
        if (ckycNoText == null) {
            return null;
        }

        Long ckycNo;
        try {
            ckycNo = Long.valueOf(ckycNoText);
        } catch (NumberFormatException ex) {
            throw new FatalProcessingException("invalid_ckyc_no=" + ckycNoText);
        }

        String entityType = deriveEntityType(ckycNo);
        if (entityType == null) {
            return null;
        }

        Map<String, String> metadata = new LinkedHashMap<String, String>();
        for (int i = 0; i < header.length; i++) {
            String h = cleanHeader(header[i]);
            if (h == null || h.length() == 0) {
                continue;
            }

            String lower = h.toLowerCase(Locale.ENGLISH);
            if (isMappedHeader(lower)) {
                continue;
            }

            String value = null;
            if (i < row.length) {
                value = normalize(row[i]);
            }

            if (value != null) {
                metadata.put(lower, value);
            }
        }

        RowData data = new RowData();
        data.ckycNo = ckycNo;
        data.entityType = entityType;
        data.userId = sourceMap.get("ukm_user_updated");
        data.operationalStatus = "ACTIVE";
        data.region = null;
        data.regionCode = sourceMap.get("ukm_region_code");
        data.relationType = "UPDATE";
        data.relationActive = Boolean.TRUE;
        data.createdBy = sourceMap.get("ukm_user_created");
        data.updatedBy = sourceMap.get("ukm_user_updated");
        // Intentional: created_date is sourced from ukm_updated_date per mapping spec.
        data.createdDate = parseTimestamp(sourceMap.get("ukm_updated_date"));
        data.updatedDate = parseTimestamp(sourceMap.get("ukm_updated_date"));
        data.closedAt = null;
        data.closedBy = null;
        data.closureRemarks = null;
        data.closureDate = null;
        data.approverUserId = sourceMap.get("ukm_upd_checker_approved");
        data.approverReId = null;
        data.workflowId = null;
        data.fiCode = sourceMap.get("ukm_lastipvupdated_fi");
        data.branchCode = sourceMap.get("ukm_branch_code");
        data.metadata = toJson(metadata);

        data.reportingEntityId = null;
        data.title = null;
        data.firstName = null;
        data.middleName = null;
        data.lastName = null;
        data.designation = null;
        data.email = null;
        data.citizenship = null;
        data.countryCode = null;
        data.mobile = null;
        data.dob = null;
        data.gender = null;
        data.employeeId = null;
        data.keycloakUserId = null;
        data.userType = null;
        data.reName = null;

        return data;
    }

    private static boolean isMappedHeader(String lowerHeader) {
        if (lowerHeader == null) {
            return false;
        }

        for (int i = 0; i < MAPPED_HEADERS.length; i++) {
            if (lowerHeader.equals(MAPPED_HEADERS[i])) {
                return true;
            }
        }
        return false;
    }

    private static String deriveEntityType(Long ckycNo) {
        if (ckycNo == null) {
            return null;
        }

        char first = String.valueOf(ckycNo).charAt(0);
        if (first >= '1' && first <= '6') {
            return "I";
        }
        if (first >= '7' && first <= '9') {
            return "L";
        }
        return null;
    }

    private static Timestamp parseTimestamp(String value) throws FatalProcessingException {
        value = normalize(value);
        if (value == null) {
            return null;
        }

        String[] patterns = new String[] {
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss.SSS",
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss.SSS",
                "yyyy-MM-dd HH:mm:ssXXX",
                "yyyy-MM-dd HH:mm:ss.SSSXXX",
                "yyyy-MM-dd'T'HH:mm:ssXXX",
                "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        };

        for (int i = 0; i < patterns.length; i++) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(patterns[i]);
                sdf.setLenient(false);
                java.util.Date d = sdf.parse(value);
                return new Timestamp(d.getTime());
            } catch (Exception ex) {
                // try next format
            }
        }

        throw new FatalProcessingException("invalid_timestamp=" + value);
    }

    @SuppressWarnings("unused")
    private static Date parseDate(String value) throws FatalProcessingException {
        value = normalize(value);
        if (value == null) {
            return null;
        }

        String[] patterns = new String[] {
                "yyyy-MM-dd",
                "yyyy/MM/dd",
                "dd-MM-yyyy",
                "dd/MM/yyyy"
        };

        for (int i = 0; i < patterns.length; i++) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(patterns[i]);
                sdf.setLenient(false);
                java.util.Date d = sdf.parse(value);
                return new Date(d.getTime());
            } catch (Exception ex) {
                // try next format
            }
        }

        throw new FatalProcessingException("invalid_date=" + value);
    }

    private static boolean enrichFromMaps(RowData data,
            Map<String, String> reportingEntityMap, Map<String, ReUserData> reUserMap,
            Set<String> loggedFiCodeMisses, Set<String> loggedUserMisses,
            FileResult result, String fileName, String serverIP) {

        if (data.fiCode != null) {
            String reportingEntityId = reportingEntityMap.get(data.fiCode);
            if (reportingEntityId != null) {
                data.reportingEntityId = reportingEntityId;
            } else {
                result.unmatchedFiCode++;
                if (loggedFiCodeMisses.add(data.fiCode)) {
                    log(
                            "WARN",
                            "file=" + fileName + " no_reporting_entity_match fi_code=" + data.fiCode,
                            serverIP,
                            "ROW_SKIPPED");
                }
                return false;
            }
        } else {
            result.unmatchedFiCode++;
            if (loggedFiCodeMisses.add("NULL_FI_CODE")) {
                log(
                        "WARN",
                        "file=" + fileName + " no_reporting_entity_match fi_code=NULL",
                        serverIP,
                        "ROW_SKIPPED");
            }
            return false;
        }

        if (data.userId != null) {
            ReUserData reUser = reUserMap.get(data.userId);
            if (reUser != null) {
                data.title = reUser.title;
                data.firstName = reUser.firstName;
                data.middleName = reUser.middleName;
                data.lastName = reUser.lastName;
                data.designation = reUser.designation;
                data.email = reUser.email;
                data.citizenship = reUser.citizenship;
                data.countryCode = reUser.countryCode;
                data.mobile = reUser.mobile;

                data.dob = reUser.dob;
                data.gender = reUser.gender;
                data.employeeId = reUser.employeeId;

                data.keycloakUserId = reUser.keycloakUserId;
                data.userType = reUser.userType;

                data.reName = buildFullName(
                        reUser.firstName,
                        reUser.middleName,
                        reUser.lastName);
            } else {
                result.unmatchedUser++;
                if (loggedUserMisses.add(data.userId)) {
                    log(
                            "WARN",
                            "file=" + fileName + " no_re_user_match user_id=" + data.userId,
                            serverIP,
                            "ROW_SKIPPED");
                }
            }
        }

        return true;
    }

    private static String buildFullName(String firstName, String middleName, String lastName) {
        StringBuilder sb = new StringBuilder();
        appendNamePart(sb, firstName);
        appendNamePart(sb, middleName);
        appendNamePart(sb, lastName);
        if (sb.length() == 0) {
            return null;
        }
        return sb.toString();
    }

    private static void appendNamePart(StringBuilder sb, String part) {
        String cleaned = normalize(part);
        if (cleaned == null) {
            return;
        }
        if (sb.length() > 0) {
            sb.append(" ");
        }
        sb.append(cleaned);
    }

    private static String[] toOutputRow(RowData row) {
        List<String> out = new ArrayList<String>();

        out.add(row.ckycNo == null ? null : row.ckycNo.toString());
        out.add(row.entityType);
        out.add(row.reportingEntityId);
        out.add(row.userId);
        out.add(row.title);
        out.add(row.firstName);
        out.add(row.middleName);
        out.add(row.lastName);
        out.add(row.designation);
        out.add(row.email);
        out.add(row.citizenship);
        out.add(row.countryCode);
        out.add(row.mobile);
        out.add(formatDate(row.dob));
        out.add(row.gender);
        out.add(row.employeeId);
        out.add(row.keycloakUserId);
        out.add(row.userType);
        out.add(row.operationalStatus);
        out.add(row.region);
        out.add(row.regionCode);
        out.add(row.relationType);
        out.add(row.relationActive == null ? null : row.relationActive.toString());
        out.add(row.createdBy);
        out.add(row.updatedBy);
        out.add(formatTimestamp(row.createdDate));
        out.add(formatTimestamp(row.updatedDate));
        out.add(formatTimestamp(row.closedAt));
        out.add(row.closedBy);
        out.add(row.closureRemarks);
        out.add(formatDate(row.closureDate));
        out.add(row.approverUserId);
        out.add(row.approverReId);
        out.add(row.workflowId);
        out.add(row.fiCode);
        out.add(row.reName);
        out.add(row.branchCode);
        out.add(row.metadata);

        return out.toArray(new String[0]);
    }

    private static String formatTimestamp(Timestamp ts) {
        if (ts == null) {
            return null;
        }
        synchronized (OUT_TS_FMT_TEMPLATE) {
            return OUT_TS_FMT_TEMPLATE.format(ts);
        }
    }

    private static String formatDate(Date d) {
        if (d == null) {
            return null;
        }
        synchronized (OUT_DATE_FMT_TEMPLATE) {
            return OUT_DATE_FMT_TEMPLATE.format(d);
        }
    }

    private static void writeOutputRow(BufferedWriter bw, String[] values) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(OUT_DELIM);
            }
            if (values[i] == null) {
                sb.append("\\N");
            } else {
                sb.append("\"").append(escapeQuotes(values[i])).append("\"");
            }
        }
        bw.write(sb.toString());
        bw.newLine();
    }

    private static String escapeQuotes(String val) {
        return val.replace("\"", "\"\"");
    }

    private static void moveFile(File sourceFile, File archiveDir) throws IOException {
        if (sourceFile == null || archiveDir == null) {
            return;
        }
        if (!archiveDir.exists()) {
            Files.createDirectories(archiveDir.toPath());
        }

        File targetFile = new File(archiveDir, sourceFile.getName());
        Files.move(sourceFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    private static String toJson(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return "{}";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("{");

        boolean first = true;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            if (!first) {
                sb.append(",");
            }
            first = false;

            sb.append("\"");
            sb.append(escapeJson(entry.getKey()));
            sb.append("\":\"");
            sb.append(escapeJson(entry.getValue()));
            sb.append("\"");
        }

        sb.append("}");
        return sb.toString();
    }

    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
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
                    if (ch < 32) {
                        String hex = Integer.toHexString(ch);
                        sb.append("\\u");
                        for (int j = hex.length(); j < 4; j++) {
                            sb.append('0');
                        }
                        sb.append(hex);
                    } else {
                        sb.append(ch);
                    }
                    break;
            }
        }
        return sb.toString();
    }

    private static String normalize(String value) {
        if (value == null) {
            return null;
        }

        String t = value.trim();
        if (t.length() == 0) {
            return null;
        }

        if ("\\N".equalsIgnoreCase(t)) {
            return null;
        }

        if ("NULL".equalsIgnoreCase(t)) {
            return null;
        }

        return t;
    }

    private static String cleanHeader(String header) {
        header = normalize(header);
        if (header == null) {
            return null;
        }
        return removeBom(header);
    }

    private static String removeBom(String value) {
        if (value == null) {
            return null;
        }
        if (value.length() > 0 && value.charAt(0) == '\ufeff') {
            return value.substring(1);
        }
        return value;
    }

    private static void closeQuietly(Closeable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ex) {
            // ignore
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ex) {
            // ignore
        }
    }

    private static String rootCause(Throwable t) {
        if (t == null) {
            return "unknown";
        }

        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }

        String msg = current.getMessage();
        if (msg == null || msg.trim().length() == 0) {
            msg = current.getClass().getSimpleName();
        }
        return msg;
    }

    private static void logFileCompletion(FileResult result, String serverIP) {
        log(
                "INFO",
                "file=" + result.fileName
                        + "\nstatus=" + result.status
                        + "\ndata_rows=" + result.dataRows
                        + "\nku_rows=" + result.kuRows
                        + "\nskipped_non_ku=" + result.skippedNonKu
                        + "\nwritten=" + result.written
                        + "\nskipped=" + result.skipped
                        + "\nunmatched_fi_code=" + result.unmatchedFiCode
                        + "\nunmatched_user=" + result.unmatchedUser
                        + "\nblank_lines=" + result.blankLines
                        + "\neffective_flush_size=" + result.effectiveFlushSize
                        + "\nmoved=" + result.moved,
                serverIP,
                "FILE_SUMMARY");
    }

    private static void logDirectorySummary(DirectoryResult result, String serverIP) {
        log(
                "INFO",
                "total_files=" + result.totalFiles
                        + "\ncompleted_files=" + result.completedFiles
                        + "\nretained_files=" + result.retainedFiles
                        + "\nempty_files=" + result.emptyFiles
                        + "\nheader_only_files=" + result.headerOnlyFiles
                        + "\nerror_files=" + result.errorFiles
                        + "\ntotal_rows=" + result.totalRows
                        + "\nku_rows=" + result.kuRows
                        + "\nskipped_non_ku=" + result.skippedNonKu
                        + "\nwritten=" + result.written
                        + "\nskipped=" + result.skipped
                        + "\nunmatched_fi_code=" + result.unmatchedFiCode
                        + "\nunmatched_user=" + result.unmatchedUser
                        + "\nblank_lines=" + result.blankLines,
                serverIP,
                "DIRECTORY_SUMMARY");
    }

    public static void log(String level, String message, String serverIP, String evtType) {
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

    private static String capitalize(String level) {
        if (level == null || level.length() == 0) {
            return "Info";
        }
        String lower = level.toLowerCase(Locale.ENGLISH);
        if ("debug".equals(lower)) {
            return "Debug";
        }
        if ("error".equals(lower)) {
            return "Error";
        }
        if ("warn".equals(lower) || "warning".equals(lower)) {
            return "Warn";
        }
        if ("info".equals(lower)) {
            return "Info";
        }
        return level.substring(0, 1).toUpperCase(Locale.ENGLISH) + level.substring(1).toLowerCase(Locale.ENGLISH);
    }

    private static AppLookups loadAppLookups(String appJdbcUrl, String appDbUser, String appDbPass, String serverIP)
            throws SQLException {

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(appJdbcUrl, appDbUser, appDbPass);

            Map<String, String> reportingEntityMap = loadReportingEntityMap(conn, serverIP);
            Map<String, ReUserData> reUserMap = loadReUserMap(conn, serverIP);

            AppLookups lookups = new AppLookups();
            lookups.reportingEntityMap = reportingEntityMap;
            lookups.reUserMap = reUserMap;
            return lookups;
        } finally {
            closeQuietly(conn);
        }
    }

    private static Map<String, String> loadReportingEntityMap(Connection conn, String serverIP) throws SQLException {
        Map<String, String> map = new HashMap<String, String>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            log("INFO", "sql=" + SQL_REPORTING_ENTITY_ALL, serverIP, "LOOKUP");
            ps = conn.prepareStatement(SQL_REPORTING_ENTITY_ALL);
            rs = ps.executeQuery();

            int dup = 0;
            while (rs.next()) {
                String fiCode = rs.getString("fi_code");
                String id = rs.getString("id");
                if (fiCode == null) {
                    continue;
                }
                if (map.put(fiCode, id) != null) {
                    dup++;
                }
            }
            if (dup > 0) {
                log("WARN", "duplicate_fi_code_count=" + dup + " (last-wins)", serverIP, "LOOKUP");
            }
        } finally {
            closeQuietly(rs);
            closeQuietly(ps);
        }
        return map;
    }

    private static Map<String, ReUserData> loadReUserMap(Connection conn, String serverIP) throws SQLException {
        Map<String, ReUserData> map = new HashMap<String, ReUserData>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            log("INFO", "sql=" + SQL_RE_USER_ALL, serverIP, "LOOKUP");
            ps = conn.prepareStatement(SQL_RE_USER_ALL);
            rs = ps.executeQuery();

            int dup = 0;
            while (rs.next()) {
                String userId = rs.getString("user_id");
                if (userId == null) {
                    continue;
                }

                ReUserData data = new ReUserData();
                data.title = rs.getString("title");
                data.firstName = rs.getString("first_name");
                data.middleName = rs.getString("middle_name");
                data.lastName = rs.getString("last_name");
                data.designation = rs.getString("designation");
                data.email = rs.getString("email");
                data.citizenship = rs.getString("citizenship");
                data.countryCode = rs.getString("country_code");
                data.mobile = rs.getString("mobile");
                data.dob = rs.getDate("dob");
                data.gender = rs.getString("gender");
                data.employeeId = rs.getString("employee_id");
                data.keycloakUserId = rs.getString("key_cloak_user_id");
                data.userType = rs.getString("user_type");

                if (map.put(userId, data) != null) {
                    dup++;
                }
            }
            if (dup > 0) {
                log("WARN", "duplicate_user_id_count=" + dup + " (last-wins)", serverIP, "LOOKUP");
            }
        } finally {
            closeQuietly(rs);
            closeQuietly(ps);
        }
        return map;
    }

    /**
     * UNRESOLVED DEPENDENCY (see class doc comment / Ambiguity A).
     *
     * kyc_alphanumeric_id_table.kat_alphanumeric_refno needs to be looked up
     * by ukm_ckyc_no to populate ckyc_reference_no, but neither reference
     * parser queries this table and its schema has not been confirmed.
     *
     * This method currently only validates that the KYC DB connection is
     * reachable (per the requirement to accept KYC DB credentials
     * separately in main) and returns an empty map. It intentionally does
     * NOT invent the kat_alphanumeric_refno query. Once the schema is
     * confirmed, uncomment SQL_CKYC_REFERENCE_ALL above, load it the same
     * way as loadReportingEntityMap(), wire the resulting map into
     * mapRow()/RowData/OUTPUT_HEADERS/toOutputRow(), and remove this notice.
     */
    private static Map<String, String> loadCkycReferenceMap(String kycJdbcUrl, String kycDbUser, String kycDbPass,
            String serverIP) throws SQLException {
        Map<String, String> map = new HashMap<String, String>();
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(kycJdbcUrl, kycDbUser, kycDbPass);
            log(
                    "INFO",
                    "kyc_db_connected ckyc_reference_no lookup not yet implemented (unresolved dependency)",
                    serverIP,
                    "LOOKUP");
            // Intentionally not executed: schema for kyc_alphanumeric_id_table
            // is unconfirmed. See class doc comment for proposed SQL.
        } finally {
            closeQuietly(conn);
        }
        return map;
    }

    private static class AppLookups {
        Map<String, String> reportingEntityMap;
        Map<String, ReUserData> reUserMap;
    }

    private static class FileResult {
        String fileName;
        String status = "PENDING";
        int dataRows = 0;
        int kuRows = 0;
        int skippedNonKu = 0;
        int written = 0;
        int skipped = 0;
        int unmatchedFiCode = 0;
        int unmatchedUser = 0;
        int blankLines = 0;
        int effectiveFlushSize = 0;
        boolean moved = false;

        FileResult(String fileName) {
            this.fileName = fileName;
        }
    }

    private static class DirectoryResult {
        int totalFiles = 0;
        int completedFiles = 0;
        int retainedFiles = 0;
        int emptyFiles = 0;
        int headerOnlyFiles = 0;
        int errorFiles = 0;
        int totalRows = 0;
        int kuRows = 0;
        int skippedNonKu = 0;
        int written = 0;
        int skipped = 0;
        int unmatchedFiCode = 0;
        int unmatchedUser = 0;
        int blankLines = 0;
    }

    private static class RowData {
        Long ckycNo;
        String entityType;
        String reportingEntityId;
        String userId;
        String title;
        String firstName;
        String middleName;
        String lastName;
        String designation;
        String email;
        String citizenship;
        String countryCode;
        String mobile;
        Date dob;
        String gender;
        String employeeId;
        String keycloakUserId;
        String userType;
        String operationalStatus;
        String region;
        String regionCode;
        String relationType;
        Boolean relationActive;
        String createdBy;
        String updatedBy;
        Timestamp createdDate;
        Timestamp updatedDate;
        Timestamp closedAt;
        String closedBy;
        String closureRemarks;
        Date closureDate;
        String approverUserId;
        String approverReId;
        String workflowId;
        String fiCode;
        String reName;
        String branchCode;
        String metadata;
    }

    private static class ReUserData {
        String title;
        String firstName;
        String middleName;
        String lastName;
        String designation;
        String email;
        String citizenship;
        String countryCode;
        String mobile;
        Date dob;
        String gender;
        String employeeId;
        String keycloakUserId;
        String userType;
    }

    private static class FatalProcessingException extends Exception {
        FatalProcessingException(String message) {
            super(message);
        }
    }
}