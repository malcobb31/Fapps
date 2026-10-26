package ktp10_insert_process_v1;

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
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * kyc_linked_re_insert_methods
 *
 * Insert-only parser for loading already-transformed CSV data into
 * kyc.kyc_linked_re_test using the existing staging table.
 *
 * Flow:
 *
 *   CSV
 *     |
 *     | Java CSV parser
 *     |
 *     | ckyc_reference_id -> ckyc_reference_no
 *     v
 * kyc.kyc_linked_re_staging
 *     |
 *     v
 * kyc.kyc_linked_re_test
 *
 * IMPORTANT:
 *
 * 1. The CSV is already transformed upstream.
 * 2. No business-field enrichment is performed here.
 * 3. The CSV is parsed in Java because metadata contains quoted JSON.
 * 4. The existing staging table is NOT dropped or recreated.
 * 5. One complete file is processed in one DB transaction.
 */
public class ktp10_insert_process_methods {

    // ==================================================================
    // LOGGING
    // ==================================================================

    private static final DateTimeFormatter LOG_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String PROCESS_NAME =
            "KYC_LINKED_RE_INSERT";

    private static final String PROCESS_VERSION =
            "1.0.0";

    private final boolean debugEnabled = false;

    private String serverIP;


    // ==================================================================
    // CONFIGURATION
    // ==================================================================

    /**
     * Existing staging table.
     *
     * DO NOT DROP / CREATE this table from the parser.
     */
    private static final String STAGING_TABLE =
            "kyc.kyc_linked_re_staging";

    /**
     * Current test target.
     *
     * For production change this to:
     *
     * kyc.kyc_linked_re
     */
    private static final String TARGET_TABLE =
            "kyc.kyc_linked_re";

    private static final String DEFAULT_DELIMITER =
            "|";

    /**
     * JDBC batch size.
     *
     * This controls executeBatch(), NOT transaction commits.
     */
    private static final int DEFAULT_BATCH_SIZE =
            5000;


    // ==================================================================
    // DESTINATION COLUMNS
    // ==================================================================

    /**
     * All destination columns except the generated identity column "id".
     *
     * CSV:
     *     ckyc_reference_id
     *
     * DB:
     *     ckyc_reference_no
     *
     * That transformation is handled explicitly before staging insert.
     */
    private static final List<String> TARGET_COLUMNS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            "ckyc_no",
                            "ckyc_reference_no",
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
                            "poi",
                            "poi_number",
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
                    )
            );


    // ==================================================================
    // EXPECTED CSV HEADERS
    // ==================================================================

    private static final Set<String> EXPECTED_CSV_HEADERS =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
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
                            )
                    )
            );


    private static final String[] REQUIRED_CSV_HEADERS = {
            "ckyc_no",
            "entity_type",
            "reporting_entity_id",
            "user_id",
            "operational_status",
            "relation_type",
            "relation_active",
            "fi_code",
            "metadata",
            "ckyc_reference_id"
    };


    private final Comparator<Path> naturalFileComparator =
            (p1, p2) ->
                    compareNaturally(
                            p1.getFileName().toString(),
                            p2.getFileName().toString()
                    );


    // ==================================================================
    // LOGGING
    // ==================================================================

    private void log(
            String level,
            String evtType,
            String message
    ) {
        if ("DEBUG".equalsIgnoreCase(level) &&
                !debugEnabled) {
            return;
        }

        String ts =
                LocalDateTime.now()
                        .format(LOG_FORMAT);

        System.out.println(
                "Time:" + ts + " [" +
                        capitalize(level) + "] " +
                        "Process_Name: " +
                        PROCESS_NAME +
                        " Server_IP: " +
                        serverIP +
                        " Process_Version: " +
                        PROCESS_VERSION +
                        " Log_Level: EVT_LOG" +
                        " EVT_Type: " +
                        (evtType != null
                                ? evtType
                                : "NA") +
                        " Description: " +
                        (message != null
                                ? message
                                : "")
        );
    }


    private String capitalize(String level) {

        if (level == null ||
                level.isEmpty()) {
            return "";
        }

        return level.substring(0, 1).toUpperCase()
                + level.substring(1).toLowerCase();
    }


    // ==================================================================
    // CONFIGURATION HOLDERS
    // ==================================================================

    private static class DatabaseConfig {

        final String host;
        final String port;
        final String database;
        final String user;
        final String password;
        final String url;

        DatabaseConfig(
                String host,
                String port,
                String database,
                String user,
                String password
        ) {

            this.host = host;
            this.port = port;
            this.database = database;
            this.user = user;
            this.password = password;

            this.url =
                    String.format(
                            "jdbc:postgresql://%s:%s/%s" +
                                    "?socketTimeout=0&tcpKeepAlive=true",
                            host,
                            port,
                            database
                    );
        }
    }


    private static class ProcessConfig {

        final String dirPath;
        final String processedDirPath;
        final boolean hasHeader;
        final String delimiter;
        final DatabaseConfig targetDb;

        ProcessConfig(
                String dirPath,
                String processedDirPath,
                String headerFlag,
                String delimiter,
                DatabaseConfig targetDb
        ) {

            this.dirPath =
                    dirPath;

            this.processedDirPath =
                    processedDirPath;

            this.hasHeader =
                    "yes".equalsIgnoreCase(headerFlag)
                            || "true".equalsIgnoreCase(headerFlag);

            this.delimiter =
                    delimiter == null ||
                            delimiter.isEmpty()
                            ? DEFAULT_DELIMITER
                            : delimiter;

            this.targetDb =
                    targetDb;
        }
    }


    private static class FileProcessResult {

        final Path file;
        final boolean success;
        final String errorMessage;

        FileProcessResult(
                Path file,
                boolean success,
                String errorMessage
        ) {

            this.file = file;
            this.success = success;
            this.errorMessage = errorMessage;
        }
    }


    // ==================================================================
    // ENTRY POINT
    // ==================================================================

    public void processAllInDirectory(
            String dirPath,
            String processedDirPath,
            String headerFlag,
            String delimiter,
            String pdbHost,
            String pdbPort,
            String pdbName,
            String pdbUser,
            String pdbPass,
            String serverIP
    ) {

        this.serverIP =
                (serverIP == null ||
                        serverIP.trim().isEmpty())
                        ? resolveServerIP()
                        : serverIP;

        log(
                "INFO",
                "PROCESS_START",
                "kyc_linked_re insert processing started"
        );

        DatabaseConfig targetDb =
                new DatabaseConfig(
                        pdbHost,
                        pdbPort,
                        pdbName,
                        pdbUser,
                        pdbPass
                );

        ProcessConfig config =
                new ProcessConfig(
                        dirPath,
                        processedDirPath,
                        headerFlag,
                        delimiter,
                        targetDb
                );

        try {

            executeProcessing(config);

            log(
                    "INFO",
                    "PROCESS_END",
                    "kyc_linked_re insert processing completed successfully"
            );

        } catch (Exception e) {

            log(
                    "ERROR",
                    "PROCESS_ERROR",
                    "Processing failed: " +
                            e.getMessage()
            );

            e.printStackTrace();
        }
    }


    private String resolveServerIP() {

        try {
            return InetAddress
                    .getLocalHost()
                    .getHostAddress();

        } catch (Exception e) {
            return "NA";
        }
    }


    // ==================================================================
    // DIRECTORY PROCESSING
    // ==================================================================

    private void executeProcessing(
            ProcessConfig config
    ) {

        if (!validateDirectories(
                config.dirPath,
                config.processedDirPath
        )) {

            log(
                    "ERROR",
                    "DIR_VALIDATION_FAILED",
                    "Directory validation failed"
            );

            return;
        }

        List<Path> csvFiles =
                discoverCsvFiles(
                        config.dirPath
                );

        if (csvFiles.isEmpty()) {

            log(
                    "INFO",
                    "NO_FILES_FOUND",
                    "No CSV files found in directory: "
                            + config.dirPath
            );

            return;
        }

        List<FileProcessResult> results =
                new ArrayList<>();

        for (Path file : csvFiles) {

            results.add(
                    processSingleFile(
                            config,
                            file
                    )
            );
        }

        long successCount =
                results.stream()
                        .filter(r -> r.success)
                        .count();

        log(
                "INFO",
                "BATCH_SUMMARY",
                String.format(
                        "Files_Processed:%d Success:%d Failed:%d",
                        results.size(),
                        successCount,
                        results.size() - successCount
                )
        );

        results.stream()
                .filter(r -> !r.success)
                .forEach(
                        r ->
                                log(
                                        "ERROR",
                                        "FILE_FAILED",
                                        "File:" +
                                                r.file.getFileName() +
                                                " Error:" +
                                                r.errorMessage
                                )
                );
    }


    private boolean validateDirectories(
            String dirPath,
            String processedDirPath
    ) {

        try {

            Path sourceDir =
                    Paths.get(dirPath);

            if (!Files.exists(sourceDir) ||
                    !Files.isDirectory(sourceDir)) {

                log(
                        "ERROR",
                        "DIR_NOT_FOUND",
                        "Source directory does not exist: "
                                + dirPath
                );

                return false;
            }

            Files.createDirectories(
                    Paths.get(processedDirPath)
            );

            log(
                    "INFO",
                    "DIR_VALIDATED",
                    String.format(
                            "Source:%s Processed:%s",
                            dirPath,
                            processedDirPath
                    )
            );

            return true;

        } catch (Exception e) {

            log(
                    "ERROR",
                    "DIR_VALIDATION_ERROR",
                    "Directory validation failed: "
                            + e.getMessage()
            );

            return false;
        }
    }


    private List<Path> discoverCsvFiles(
            String dirPath
    ) {

        try (
                Stream<Path> stream =
                        Files.list(
                                Paths.get(dirPath)
                        )
        ) {

            List<Path> files =
                    stream
                            .filter(Files::isRegularFile)
                            .filter(
                                    p ->
                                            p.getFileName()
                                                    .toString()
                                                    .toLowerCase()
                                                    .endsWith(".csv")
                            )
                            .sorted(naturalFileComparator)
                            .collect(
                                    Collectors.toList()
                            );

            log(
                    "INFO",
                    "FILE_DISCOVERY",
                    String.format(
                            "Files_Found:%d Directory:%s",
                            files.size(),
                            dirPath
                    )
            );

            return files;

        } catch (IOException e) {

            log(
                    "ERROR",
                    "FILE_DISCOVERY_ERROR",
                    "Failed to discover CSV files: "
                            + e.getMessage()
            );

            return Collections.emptyList();
        }
    }


    private int compareNaturally(
            String str1,
            String str2
    ) {

        int len1 = str1.length();
        int len2 = str2.length();

        int i = 0;
        int j = 0;

        while (i < len1 &&
                j < len2) {

            char c1 = str1.charAt(i);
            char c2 = str2.charAt(j);

            if (Character.isDigit(c1) &&
                    Character.isDigit(c2)) {

                StringBuilder num1 =
                        new StringBuilder();

                StringBuilder num2 =
                        new StringBuilder();

                while (i < len1 &&
                        Character.isDigit(
                                str1.charAt(i)
                        )) {

                    num1.append(
                            str1.charAt(i++)
                    );
                }

                while (j < len2 &&
                        Character.isDigit(
                                str2.charAt(j)
                        )) {

                    num2.append(
                            str2.charAt(j++)
                    );
                }

                int numCompare =
                        Integer.compare(
                                Integer.parseInt(
                                        num1.toString()
                                ),
                                Integer.parseInt(
                                        num2.toString()
                                )
                        );

                if (numCompare != 0) {
                    return numCompare;
                }

            } else {

                if (c1 != c2) {
                    return Character.compare(
                            c1,
                            c2
                    );
                }

                i++;
                j++;
            }
        }

        return Integer.compare(
                len1,
                len2
        );
    }


    // ==================================================================
    // PER FILE
    // ==================================================================

    private FileProcessResult processSingleFile(
            ProcessConfig config,
            Path file
    ) {

        String csvFileName =
                file.getFileName().toString();

        LocalDateTime fileStart =
                LocalDateTime.now();

        try {

            if (isEmptyOrHeaderlessCsv(file)) {

                log(
                        "WARNING",
                        "CSV_EMPTY_SKIP",
                        "CSV file is empty or header is missing - "
                                + "skipping file:"
                                + csvFileName
                );

                return new FileProcessResult(
                        file,
                        true,
                        null
                );
            }

            importAndInsert(
                    config,
                    file
            );

            Path moved =
                    moveFileToProcessed(
                            file,
                            Paths.get(
                                    config.processedDirPath
                            )
                    );

            if (moved == null) {

                return new FileProcessResult(
                        file,
                        false,
                        "File processing succeeded but moving to processed directory failed"
                );
            }

            long durationMs =
                    java.time.Duration
                            .between(
                                    fileStart,
                                    LocalDateTime.now()
                            )
                            .toMillis();

            log(
                    "INFO",
                    "FILE_SUMMARY_SUCCESS",
                    "File:" +
                            csvFileName +
                            " Time_Taken_MS:" +
                            durationMs
            );

            return new FileProcessResult(
                    file,
                    true,
                    null
            );

        } catch (Exception e) {

            long durationSec =
                    java.time.Duration
                            .between(
                                    fileStart,
                                    LocalDateTime.now()
                            )
                            .getSeconds();

            log(
                    "ERROR",
                    "FILE_SUMMARY_ERROR",
                    "File:" +
                            csvFileName +
                            " Duration:" +
                            durationSec +
                            "s Error:" +
                            e.getMessage()
            );

            /*
             * Failed files stay in input directory.
             */
            return new FileProcessResult(
                    file,
                    false,
                    e.getMessage()
            );
        }
    }


    private boolean isEmptyOrHeaderlessCsv(
            Path file
    ) throws IOException {

        try (
                BufferedReader br =
                        new BufferedReader(
                                new InputStreamReader(
                                        Files.newInputStream(file),
                                        StandardCharsets.ISO_8859_1
                                )
                        )
        ) {

            String firstLine =
                    br.readLine();

            return firstLine == null ||
                    firstLine.trim().isEmpty();
        }
    }


    private Path moveFileToProcessed(
            Path sourceFile,
            Path processedDir
    ) {

        try {

            Files.createDirectories(
                    processedDir
            );

            Path targetFile =
                    processedDir.resolve(
                            sourceFile.getFileName()
                    );

            if (Files.exists(targetFile)) {

                String fileName =
                        sourceFile.getFileName().toString();

                int dot =
                        fileName.lastIndexOf('.');

                String baseName =
                        dot >= 0
                                ? fileName.substring(
                                        0,
                                        dot
                                )
                                : fileName;

                String extension =
                        dot >= 0
                                ? fileName.substring(dot)
                                : ".csv";

                String timestamp =
                        LocalDateTime.now()
                                .format(
                                        DateTimeFormatter.ofPattern(
                                                "yyyyMMdd_HHmmss"
                                        )
                                );

                targetFile =
                        processedDir.resolve(
                                baseName +
                                        "_" +
                                        timestamp +
                                        extension
                        );
            }

            Files.move(
                    sourceFile,
                    targetFile,
                    StandardCopyOption.REPLACE_EXISTING
            );

            log(
                    "INFO",
                    "FILE_MOVED",
                    String.format(
                            "Source:%s Target:%s",
                            sourceFile,
                            targetFile
                    )
            );

            return targetFile;

        } catch (IOException e) {

            log(
                    "ERROR",
                    "FILE_MOVE_ERROR",
                    String.format(
                            "Failed to move file - Source:%s Error:%s",
                            sourceFile,
                            e.getMessage()
                    )
            );

            return null;
        }
    }


    // ==================================================================
    // CSV HEADERS
    // ==================================================================

    private String[] readCsvHeaders(
            Path file,
            String delimiter
    ) throws IOException {

        try (
                BufferedReader br =
                        new BufferedReader(
                                new InputStreamReader(
                                        Files.newInputStream(file),
                                        StandardCharsets.ISO_8859_1
                                )
                        )
        ) {

            String headerLine =
                    br.readLine();

            if (headerLine == null ||
                    headerLine.trim().isEmpty()) {

                return new String[0];
            }

            return Arrays.stream(
                            headerLine.split(
                                    java.util.regex.Pattern.quote(
                                            delimiter
                                    ),
                                    -1
                            )
                    )
                    .map(this::normalizeCsvHeader)
                    .filter(
                            s ->
                                    !s.isEmpty()
                    )
                    .toArray(
                            String[]::new
                    );
        }
    }


    private String normalizeCsvHeader(
            String header
    ) {

        if (header == null) {
            return "";
        }

        String h =
                header.trim();

        /*
         * Remove BOM from first column if present.
         */
        if (!h.isEmpty() &&
                h.charAt(0) == '\ufeff') {

            h = h.substring(1);
        }

        if (h.startsWith("\"") &&
                h.endsWith("\"") &&
                h.length() >= 2) {

            h =
                    h.substring(
                            1,
                            h.length() - 1
                    );
        }

        return h.trim();
    }


    // ==================================================================
    // CSV -> DB COLUMN MAPPING
    // ==================================================================

    /**
     * CSV:
     *
     *     ckyc_reference_id
     *
     * becomes:
     *
     *     ckyc_reference_no
     *
     * All other columns map directly.
     */
    private String resolveStagingColumnForCsvHeader(
            String csvHeader
    ) {

        if ("ckyc_reference_id".equalsIgnoreCase(
                csvHeader
        )) {

            return "ckyc_reference_no";
        }

        return csvHeader
                .toLowerCase(
                        Locale.ROOT
                );
    }


    private String quotePgIdent(
            String ident
    ) {

        return "\"" +
                ident.replace(
                        "\"",
                        "\"\""
                ) +
                "\"";
    }


    // ==================================================================
    // HEADER VALIDATION
    // ==================================================================

    private void validateCsvHeaders(
            String[] headers,
            String fileName
    ) {

        if (headers == null ||
                headers.length == 0) {

            throw new RuntimeException(
                    "CSV header is empty: "
                            + fileName
            );
        }

        Set<String> actualHeaders =
                new HashSet<>();

        for (String header : headers) {

            String normalized =
                    normalizeCsvHeader(header)
                            .toLowerCase(
                                    Locale.ROOT
                            );

            if (!normalized.isEmpty()) {
                actualHeaders.add(
                        normalized
                );
            }
        }

        List<String> missing =
                new ArrayList<>();

        for (String required :
                REQUIRED_CSV_HEADERS) {

            if (!actualHeaders.contains(
                    required
            )) {

                missing.add(
                        required
                );
            }
        }

        if (!missing.isEmpty()) {

            throw new RuntimeException(
                    "Missing required CSV columns in file "
                            + fileName
                            + ": "
                            + String.join(
                                    ", ",
                                    missing
                            )
            );
        }

        List<String> unexpected =
                new ArrayList<>();

        for (String actual :
                actualHeaders) {

            if (!EXPECTED_CSV_HEADERS.contains(
                    actual
            )) {

                unexpected.add(
                        actual
                );
            }
        }

        if (!unexpected.isEmpty()) {

            throw new RuntimeException(
                    "Unexpected CSV columns in file "
                            + fileName
                            + ": "
                            + String.join(
                                    ", ",
                                    unexpected
                            )
            );
        }

        /*
         * Duplicate header check.
         */
        Set<String> seen =
                new HashSet<>();

        for (String header : headers) {

            String normalized =
                    normalizeCsvHeader(header)
                            .toLowerCase(
                                    Locale.ROOT
                            );

            if (normalized.isEmpty()) {
                continue;
            }

            if (!seen.add(normalized)) {

                throw new RuntimeException(
                        "Duplicate CSV column '"
                                + normalized
                                + "' in file: "
                                + fileName
                );
            }
        }

        log(
                "INFO",
                "CSV_HEADERS_VALID",
                "CSV header validation successful - "
                        + "File:"
                        + fileName
                        + " Columns:"
                        + actualHeaders.size()
        );
    }


    // ==================================================================
    // COMPLETE FILE IMPORT
    // ==================================================================

    private void importAndInsert(
            ProcessConfig config,
            Path file
    ) throws Exception {

        String csvFileName =
                file.getFileName().toString();

        log(
                "INFO",
                "CSV_IMPORT_START",
                "Starting CSV import - "
                        + "File:"
                        + csvFileName
                        + " Staging_Table:"
                        + STAGING_TABLE
                        + " Target_Table:"
                        + TARGET_TABLE
        );

        String[] csvHeaders =
                readCsvHeaders(
                        file,
                        config.delimiter
                );

        if (csvHeaders.length == 0) {

            throw new RuntimeException(
                    "CSV header is empty: "
                            + csvFileName
            );
        }

        log(
                "INFO",
                "CSV_HEADERS",
                "CSV Headers:"
                        + Arrays.toString(
                                csvHeaders
                        )
                        + " File:"
                        + csvFileName
        );

        /*
         * IMPORTANT:
         *
         * Validate the ORIGINAL CSV headers.
         *
         * This must happen BEFORE:
         *
         * ckyc_reference_id
         *       
         * ckyc_reference_no
         */
        validateCsvHeaders(
                csvHeaders,
                csvFileName
        );

        /*
         * Convert CSV headers into staging headers.
         */
        String[] mappedHeaders =
                new String[
                        csvHeaders.length
                ];

        for (int i = 0;
             i < csvHeaders.length;
             i++) {

            mappedHeaders[i] =
                    resolveStagingColumnForCsvHeader(
                            csvHeaders[i]
                    );
        }

        /*
         * Verify that the CSV mapping doesn't
         * create duplicate staging columns.
         */
        Set<String> mappedHeaderSet =
                new HashSet<>();

        for (String mappedHeader :
                mappedHeaders) {

            if (!mappedHeaderSet.add(
                    mappedHeader.toLowerCase(
                            Locale.ROOT
                    )
            )) {

                throw new RuntimeException(
                        "Duplicate staging column after mapping: "
                                + mappedHeader
                                + " File:"
                                + csvFileName
                );
            }
        }

        String stagingInsertSql =
                buildStagingInsertSql(
                        mappedHeaders
                );

        long totalRows = 0;
        long stagedRows = 0;
        long targetRows = 0;

        try (
                Connection conn =
                        DriverManager.getConnection(
                                config.targetDb.url,
                                config.targetDb.user,
                                config.targetDb.password
                        );

                PreparedStatement stagingPs =
                        conn.prepareStatement(
                                stagingInsertSql
                        );

                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(
                                        Files.newInputStream(file),
                                        StandardCharsets.UTF_8
                                )
                        )
        ) {

            conn.setAutoCommit(false);

            try {

                /*
                 * EXISTING staging table.
                 *
                 * We only clear its old data.
                 *
                 * We DO NOT DROP or CREATE it.
                 */
                try (
                        Statement st =
                                conn.createStatement()
                ) {

                    st.executeUpdate(
                            "TRUNCATE TABLE "
                                    + STAGING_TABLE
                    );
                }


                /*
                 * Skip CSV header.
                 */
                String headerLine =
                        reader.readLine();

                if (headerLine == null) {

                    throw new RuntimeException(
                            "CSV contains no rows: "
                                    + csvFileName
                    );
                }


                int batchSize = 0;

                String line;

                while ((line =
                        reader.readLine()) != null) {

                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    /*
                     * IMPORTANT:
                     *
                     * Use the same custom parser that
                     * successfully reads your source CSV.
                     *
                     * This preserves metadata JSON.
                     */
                    List<String> parsedValues =
                            parseCsvLine(line);

                    if (parsedValues.size() !=
                            csvHeaders.length) {

                        throw new RuntimeException(
                                "CSV column count mismatch at row "
                                        + (totalRows + 2)
                                        + " Expected:"
                                        + csvHeaders.length
                                        + " Actual:"
                                        + parsedValues.size()
                        );
                    }

                    totalRows++;

                    setStagingParameters(
                            stagingPs,
                            parsedValues,
                            mappedHeaders
                    );

                    stagingPs.addBatch();

                    batchSize++;

                    if (batchSize >=
                            DEFAULT_BATCH_SIZE) {

                        int[] results =
                                stagingPs.executeBatch();

                        stagedRows +=
                                countBatchResults(
                                        results
                                );

                        /*
                         * IMPORTANT:
                         *
                         * NO COMMIT HERE.
                         *
                         * The whole file remains
                         * one transaction.
                         */
                        batchSize = 0;

                        log(
                                "INFO",
                                "STAGING_BATCH",
                                "File:"
                                        + csvFileName
                                        + " Rows_Processed:"
                                        + totalRows
                        );
                    }
                }


                /*
                 * Remaining batch.
                 */
                if (batchSize > 0) {

                    int[] results =
                            stagingPs.executeBatch();

                    stagedRows +=
                            countBatchResults(
                                    results
                            );
                }


                log(
                        "INFO",
                        "STAGING_IMPORT_SUCCESS",
                        "File:"
                                + csvFileName
                                + " Rows_Read:"
                                + totalRows
                                + " Rows_Staged:"
                                + stagedRows
                );


                /*
                 * Staging -> target.
                 */
                targetRows =
                        insertFromStaging(
                                conn,
                                csvFileName
                        );


                /*
                 * SINGLE COMMIT FOR THE FILE.
                 */
                conn.commit();


                log(
                        "INFO",
                        "TARGET_INSERT_SUCCESS",
                        "File:"
                                + csvFileName
                                + " Rows_Read:"
                                + totalRows
                                + " Rows_Staged:"
                                + stagedRows
                                + " Rows_Inserted:"
                                + targetRows
                );

            } catch (Exception e) {

                /*
                 * Roll back:
                 *
                 * - staging truncate
                 * - staging inserts
                 * - final target insert
                 */
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {

                    log(
                            "ERROR",
                            "ROLLBACK_ERROR",
                            "Rollback failed for File:"
                                    + csvFileName
                                    + " Error:"
                                    + rollbackEx.getMessage()
                    );
                }

                throw e;
            }

        } catch (Exception e) {

            log(
                    "ERROR",
                    "CSV_IMPORT_ERROR",
                    "File:"
                            + csvFileName
                            + " Rows_Processed:"
                            + totalRows
                            + " Rows_Staged:"
                            + stagedRows
                            + " Rows_Inserted:"
                            + targetRows
                            + " Error:"
                            + e.getMessage()
            );

            throw e;
        }
    }


    // ==================================================================
    // CUSTOM CSV PARSER
    // ==================================================================

    /**
     * Same parsing behavior as the working
     * ktp10_upd_process_methods parser.
     *
     * Handles:
     *
     * - pipe delimiter
     * - quoted fields
     * - "" escaped quotes
     * - \" escaped quotes
     * - JSON inside quoted metadata
     */
    
    private List<String> parseCsvLine(
            String line
    ) throws Exception {

        List<String> values =
                new ArrayList<>();

        StringBuilder current =
                new StringBuilder();

        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {

            char ch = line.charAt(i);

            /*
             * Outside quoted field.
             */
            if (!inQuotes) {

                if (ch == '"') {

                    inQuotes = true;

                    continue;
                }

                if (ch == '|') {

                    values.add(
                            normalizeCsvValue(
                                    current.toString()
                            )
                    );

                    current.setLength(0);

                    continue;
                }

                current.append(ch);

                continue;
            }


            /*
             * Inside quoted field.
             */
            if (ch == '"') {

                /*
                 * CSV escaped quote.
                 *
                 * ""
                 */
                if (i + 1 < line.length()
                        && line.charAt(i + 1) == '"') {

                    /*
                     * IMPORTANT:
                     *
                     * If this is """ followed by |,
                     * the first two quotes are the literal
                     * quote inside the value and the third
                     * quote is the CSV closing quote.
                     *
                     * We therefore consume only the first
                     * two quotes here.
                     */
                    current.append('"');

                    i++;

                    /*
                     * Check whether the next character
                     * is the actual CSV delimiter.
                     */
                    if (i + 1 < line.length()
                            && line.charAt(i + 1) == '|') {

                        inQuotes = false;

                        i++;

                        values.add(
                                normalizeCsvValue(
                                        current.toString()
                                )
                        );

                        current.setLength(0);

                    }

                    continue;
                }


                /*
                 * Normal closing quote.
                 *
                 * A closing quote is followed either by:
                 *
                 * |   -> next CSV field
                 *
                 * end -> end of row
                 */
                if (i + 1 == line.length()) {

                    inQuotes = false;

                    continue;
                }

                if (line.charAt(i + 1) == '|') {

                    inQuotes = false;

                    continue;
                }


                /*
                 * Any other quote inside the quoted
                 * field is actual data.
                 */
                current.append('"');

                continue;
            }


            /*
             * Normal character inside quoted field.
             */
            current.append(ch);
        }


        if (inQuotes) {

            throw new RuntimeException(
                    "unclosed_quotes"
            );
        }


        /*
         * Add final field if it wasn't already added
         * by the quote+delimiter handling above.
         */
        if (current.length() > 0 || line.endsWith("|")) {

            values.add(
                    normalizeCsvValue(
                            current.toString()
                    )
            );
        }

        return values;
    }
    
//    private List<String> parseCsvLine(
//            String line
//    ) throws Exception {
//
//        List<String> values =
//                new ArrayList<>();
//
//        if (line == null) {
//            return values;
//        }
//
//        StringBuilder current =
//                new StringBuilder();
//
//        boolean inQuotes = false;
//
//        int length = line.length();
//
//        for (int i = 0;
//             i < length;
//             i++) {
//
//            char ch =
//                    line.charAt(i);
//
//
//            /*
//             * Opening quote.
//             */
//            if (!inQuotes &&
//                    ch == '"') {
//
//                inQuotes = true;
//                continue;
//            }
//
//
//            /*
//             * Inside quoted value.
//             */
//            if (inQuotes &&
//                    ch == '"') {
//
//                /*
//                 * Backslash escaped quote:
//                 *
//                 * \"
//                 */
//                if (i > 0 &&
//                        line.charAt(i - 1) == '\\') {
//
//                    current.append(ch);
//                    continue;
//                }
//
//
//                /*
//                 * Standard CSV escaped quote:
//                 *
//                 * ""
//                 */
//                if (i + 1 < length &&
//                        line.charAt(i + 1) == '"') {
//
//                    current.append('"');
//
//                    i++;
//
//                    continue;
//                }
//
//
//                /*
//                 * Quote followed by pipe
//                 * = closing quote.
//                 */
//                if (i + 1 < length &&
//                        line.charAt(i + 1) == '|') {
//
//                    inQuotes = false;
//
//                    continue;
//                }
//
//
//                /*
//                 * Quote at end of line.
//                 */
//                if (i + 1 == length) {
//
//                    inQuotes = false;
//
//                    continue;
//                }
//
//
//                /*
//                 * Any other quote inside
//                 * quoted data is data.
//                 */
//                current.append(ch);
//
//                continue;
//            }
//
//
//            /*
//             * Pipe outside quoted field.
//             */
//            if (ch == '|' &&
//                    !inQuotes) {
//
//                values.add(
//                        normalizeCsvValue(
//                                current.toString()
//                        )
//                );
//
//                current.setLength(0);
//
//                continue;
//            }
//
//            current.append(ch);
//        }
//
//
//        if (inQuotes) {
//
//            throw new RuntimeException(
//                    "unclosed_quotes"
//            );
//        }
//
//
//        values.add(
//                normalizeCsvValue(
//                        current.toString()
//                )
//        );
//
//        return values;
//    }


    private String normalizeCsvValue(
            String value
    ) {

        if (value == null) {
            return null;
        }

        String trimmed =
                value.trim();

        if (trimmed.isEmpty()) {
            return null;
        }

        if ("\\N".equalsIgnoreCase(
                trimmed
        )) {

            return null;
        }

        if ("NULL".equalsIgnoreCase(
                trimmed
        )) {

            return null;
        }

        return trimmed;
    }


    // ==================================================================
    // STAGING INSERT
    // ==================================================================

    private String buildStagingInsertSql(
            String[] mappedHeaders
    ) {

        StringBuilder columns =
                new StringBuilder();

        StringBuilder values =
                new StringBuilder();

        for (int i = 0;
             i < mappedHeaders.length;
             i++) {

            if (i > 0) {
                columns.append(",");
                values.append(",");
            }

            columns.append(
                    quotePgIdent(
                            mappedHeaders[i]
                    )
            );

            values.append("?");
        }

        return "INSERT INTO "
                + STAGING_TABLE
                + " ("
                + columns
                + ") VALUES ("
                + values
                + ")";
    }


    private void setStagingParameters(
            PreparedStatement ps,
            List<String> values,
            String[] mappedHeaders
    ) throws SQLException {

        for (int i = 0;
             i < mappedHeaders.length;
             i++) {

            String value =
                    values.get(i);

            if (value == null) {

                ps.setNull(
                        i + 1,
                        Types.VARCHAR
                );

            } else {

                ps.setString(
                        i + 1,
                        value
                );
            }
        }
    }


    // ==================================================================
    // STAGING -> TARGET
    // ==================================================================

    private long insertFromStaging(
            Connection conn,
            String csvFileName
    ) throws SQLException {

        StringBuilder insertCols =
                new StringBuilder();

        StringBuilder selectCols =
                new StringBuilder();


        for (String dbCol :
                TARGET_COLUMNS) {

            /*
             * poi and poi_number are not
             * present in the CSV mapping.
             *
             * Insert NULL.
             */
            if ("poi".equalsIgnoreCase(dbCol) ||
                    "poi_number".equalsIgnoreCase(dbCol)) {

                insertCols
                        .append(
                                quotePgIdent(dbCol)
                        )
                        .append(",");

                selectCols
                        .append("NULL,");

                continue;
            }


            insertCols
                    .append(
                            quotePgIdent(dbCol)
                    )
                    .append(",");


            String quoted =
                    quotePgIdent(
                            dbCol
                    );


            /*
             * metadata:
             *
             * staging TEXT
             *       
             * target JSONB
             */
            if ("metadata".equalsIgnoreCase(
                    dbCol
            )) {

                selectCols.append(
                        "CASE WHEN "
                                + quoted
                                + " IS NULL OR "
                                + quoted
                                + " = '' "
                                + "THEN NULL ELSE "
                                + quoted
                                + "::jsonb END,"
                );

            } else {

                selectCols.append(
                        "CASE WHEN "
                                + quoted
                                + " IS NULL OR "
                                + quoted
                                + " = '' "
                                + "THEN NULL ELSE "
                                + quoted
                                + "::"
                                + pgTypeForColumn(
                                        dbCol
                                )
                                + " END,"
                );
            }
        }


        insertCols.setLength(
                insertCols.length() - 1
        );

        selectCols.setLength(
                selectCols.length() - 1
        );


        String sql =
                "INSERT INTO "
                        + TARGET_TABLE
                        + " ("
                        + insertCols
                        + ") SELECT "
                        + selectCols
                        + " FROM "
                        + STAGING_TABLE;


        log(
                "INFO",
                "TARGET_INSERT_START",
                "File:"
                        + csvFileName
                        + " Staging -> Target started"
        );


        try (
                Statement st =
                        conn.createStatement()
        ) {

            int count =
                    st.executeUpdate(sql);

            return count;
        }
    }


    // ==================================================================
    // TYPE MAPPING
    // ==================================================================

    private String pgTypeForColumn(
            String dbCol
    ) {

        switch (dbCol) {

            case "ckyc_no":
                return "bigint";

            case "keycloak_user_id":
                return "uuid";

            case "dob":
            case "closure_date":
                return "date";

            case "created_date":
            case "updated_date":
                return "timestamptz";

            case "closed_at":
                return "timestamp";

            case "relation_active":
                return "boolean";

            default:
                return "text";
        }
    }


    // ==================================================================
    // BATCH COUNT
    // ==================================================================

    private long countBatchResults(
            int[] results
    ) {

        if (results == null) {
            return 0;
        }

        long count = 0;

        for (int result : results) {

            if (result >= 0) {

                count += result;

            } else if (
                    result ==
                            Statement.SUCCESS_NO_INFO
            ) {

                count++;
            }
        }

        return count;
    }
}