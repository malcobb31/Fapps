package ktp10_update_process_v1;

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
 * kyc_linked_re_update_methods
 *
 * UPDATE ONLY parser for kyc.kyc_linked_re.
 *
 * Flow:
 *
 *   CSV
 *     |
 *     | Java CSV parser
 *     |
 *     | Match using:
 *     | ckyc_no + fi_code
 *     v
 *   kyc.kyc_linked_re
 *     |
 *     v
 *   UPDATE existing row only
 *
 * IMPORTANT:
 *
 * 1. CSV is already transformed upstream.
 * 2. No INSERT is performed.
 * 3. ckyc_no and fi_code are used as the update key.
 * 4. ckyc_no and fi_code are NOT updated.
 * 5. Rows which do not exist in target table are skipped.
 * 6. One complete file is processed in one DB transaction.
 */
public class ktp10_update_process_methods {

    // ==================================================================
    // LOGGING
    // ==================================================================

    private static final DateTimeFormatter LOG_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String PROCESS_NAME =
            "KYC_LINKED_RE_UPDATE";

    private static final String PROCESS_VERSION =
            "1.0.0";

    private final boolean debugEnabled = false;

    private String serverIP;


    // ==================================================================
    // CONFIGURATION
    // ==================================================================

    private static final String TARGET_TABLE =
            "kyc.kyc_linked_re";

    private static final String DEFAULT_DELIMITER =
            "|";

    private static final int DEFAULT_BATCH_SIZE =
            5000;


    // ==================================================================
    // ALLOWED CSV HEADERS
    // ==================================================================

    /**
     * CSV columns that are allowed for update.
     *
     * ckyc_no and fi_code are present because they are
     * used as the WHERE condition.
     *
     * They are NOT included in SET clause.
     */
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
            "fi_code"
    };


    /**
     * Columns which can NEVER be modified.
     *
     * They are used only for matching.
     */
    private static final Set<String> MATCH_COLUMNS =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "ckyc_no",
                                    "fi_code"
                            )
                    )
            );


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

        if ("DEBUG".equalsIgnoreCase(level)
                && !debugEnabled) {
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


    private String resolveDbColumnForCsvHeader(
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
    private String capitalize(
            String level
    ) {

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
                            "jdbc:postgresql://%s:%s/%s"
                                    + "?socketTimeout=0&tcpKeepAlive=true",
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
                    delimiter == null
                            || delimiter.isEmpty()
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
                "kyc_linked_re update processing started"
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
                    "kyc_linked_re update processing completed successfully"
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
                                        "File:"
                                                + r.file.getFileName()
                                                + " Error:"
                                                + r.errorMessage
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

            if (!Files.exists(sourceDir)
                    || !Files.isDirectory(sourceDir)) {

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

        while (i < len1 && j < len2) {

            char c1 = str1.charAt(i);
            char c2 = str2.charAt(j);

            if (Character.isDigit(c1)
                    && Character.isDigit(c2)) {

                StringBuilder num1 =
                        new StringBuilder();

                StringBuilder num2 =
                        new StringBuilder();

                while (i < len1 &&
                        Character.isDigit(
                                str1.charAt(i))) {

                    num1.append(
                            str1.charAt(i++)
                    );
                }

                while (j < len2 &&
                        Character.isDigit(
                                str2.charAt(j))) {

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

            updateFromCsv(
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
                    "File:"
                            + csvFileName
                            + " Time_Taken_MS:"
                            + durationMs
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
                    "File:"
                            + csvFileName
                            + " Duration:"
                            + durationSec
                            + "s Error:"
                            + e.getMessage()
            );

            /*
             * Failed files remain in the input directory.
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

            return firstLine == null
                    || firstLine.trim().isEmpty();
        }
    }


    // ==================================================================
    // MOVE FILE
    // ==================================================================

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
                                baseName
                                        + "_"
                                        + timestamp
                                        + extension
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
         * Remove BOM.
         */
        if (!h.isEmpty() &&
                h.charAt(0) == '\ufeff') {

            h = h.substring(1);
        }

        /*
         * Remove surrounding quotes.
         */
        if (h.startsWith("\"")
                && h.endsWith("\"")
                && h.length() >= 2) {

            h =
                    h.substring(
                            1,
                            h.length() - 1
                    );
        }

        return h.trim();
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

        /*
         * Required columns.
         */
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

        /*
         * Unexpected columns.
         */
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

        for (String header :
                headers) {

            String normalized =
                    normalizeCsvHeader(header)
                            .toLowerCase(
                                    Locale.ROOT
                            );

            if (normalized.isEmpty()) {
                continue;
            }

            if (!seen.add(
                    normalized
            )) {

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
    // UPDATE PROCESSING
    // ==================================================================

    private void updateFromCsv(
            ProcessConfig config,
            Path file
    ) throws Exception {

        String csvFileName =
                file.getFileName().toString();

        log(
                "INFO",
                "CSV_UPDATE_START",
                "Starting CSV update - "
                        + "File:"
                        + csvFileName
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

        validateCsvHeaders(
                csvHeaders,
                csvFileName
        );

        /*
         * Build UPDATE SQL dynamically from CSV headers.
         *
         * ckyc_no and fi_code are excluded from SET.
         *
         * Matching:
         *
         * WHERE ckyc_no = ?
         *   AND fi_code = ?
         */
        String updateSql =
                buildUpdateSql(
                        csvHeaders
                );

        log(
                "DEBUG",
                "UPDATE_SQL",
                "Generated SQL:"
                        + updateSql
        );

        long rowsRead = 0;
        long rowsUpdated = 0;
        long rowsNotFound = 0;

        try (
                Connection conn =
                        DriverManager.getConnection(
                                config.targetDb.url,
                                config.targetDb.user,
                                config.targetDb.password
                        );

                PreparedStatement updatePs =
                        conn.prepareStatement(
                                updateSql
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

                String headerLine =
                        reader.readLine();

                if (headerLine == null) {

                    throw new RuntimeException(
                            "CSV contains no header: "
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

                    List<String> parsedValues =
                            parseCsvLine(line);

                    if (parsedValues.size()
                            != csvHeaders.length) {

                        throw new RuntimeException(
                                "CSV column count mismatch at row "
                                        + (rowsRead + 2)
                                        + " Expected:"
                                        + csvHeaders.length
                                        + " Actual:"
                                        + parsedValues.size()
                        );
                    }

                    rowsRead++;

                    String ckycNo =
                            getCsvValue(
                                    parsedValues,
                                    csvHeaders,
                                    "ckyc_no"
                            );

                    String fiCode =
                            getCsvValue(
                                    parsedValues,
                                    csvHeaders,
                                    "fi_code"
                            );

                    if (ckycNo == null ||
                            fiCode == null) {

                        log(
                                "WARNING",
                                "UPDATE_KEY_MISSING",
                                "Row:"
                                        + rowsRead
                                        + " missing ckyc_no or fi_code. Skipping."
                        );

                        continue;
                    }

                    setUpdateParameters(
                            updatePs,
                            parsedValues,
                            csvHeaders
                    );

                    int updateCount =
                            updatePs.executeUpdate();

                    if (updateCount > 0) {

                        rowsUpdated +=
                                updateCount;

                        log(
                                "DEBUG",
                                "ROW_UPDATED",
                                "Row:"
                                        + rowsRead
                                        + " ckyc_no:"
                                        + ckycNo
                                        + " fi_code:"
                                        + fiCode
                        );

                    } else {

                        rowsNotFound++;

                        log(
                                "WARNING",
                                "ROW_NOT_FOUND",
                                "No matching row found for "
                                        + "ckyc_no:"
                                        + ckycNo
                                        + " fi_code:"
                                        + fiCode
                        );
                    }

                    /*
                     * Batch log only.
                     *
                     * Actual UPDATE is executed immediately.
                     *
                     * This keeps ROW_NOT_FOUND detection simple.
                     */
                    batchSize++;

                    if (batchSize >=
                            DEFAULT_BATCH_SIZE) {

                        log(
                                "INFO",
                                "UPDATE_PROGRESS",
                                "File:"
                                        + csvFileName
                                        + " Rows_Read:"
                                        + rowsRead
                                        + " Rows_Updated:"
                                        + rowsUpdated
                                        + " Rows_Not_Found:"
                                        + rowsNotFound
                        );

                        batchSize = 0;
                    }
                }

                /*
                 * Single commit for the entire file.
                 */
                conn.commit();

                log(
                        "INFO",
                        "CSV_UPDATE_SUCCESS",
                        "File:"
                                + csvFileName
                                + " Rows_Read:"
                                + rowsRead
                                + " Rows_Updated:"
                                + rowsUpdated
                                + " Rows_Not_Found:"
                                + rowsNotFound
                );

            } catch (Exception e) {

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
                    "CSV_UPDATE_ERROR",
                    "File:"
                            + csvFileName
                            + " Rows_Read:"
                            + rowsRead
                            + " Rows_Updated:"
                            + rowsUpdated
                            + " Rows_Not_Found:"
                            + rowsNotFound
                            + " Error:"
                            + e.getMessage()
            );

            throw e;
        }
    }


    // ==================================================================
    // BUILD UPDATE SQL
    // ==================================================================

    private String buildUpdateSql(
            String[] csvHeaders
    ) {

        StringBuilder setClause =
                new StringBuilder();

        for (String header :
                csvHeaders) {

        	String column =
        	        resolveDbColumnForCsvHeader(
        	                normalizeCsvHeader(header)
        	        );

            /*
             * These are matching fields only.
             */
            if (MATCH_COLUMNS.contains(
                    column
            )) {
                continue;
            }

            if (setClause.length() > 0) {
                setClause.append(", ");
            }

            setClause
                    .append(
                            quotePgIdent(
                                    column
                            )
                    )
                    .append(" = ?");

        }

        if (setClause.length() == 0) {

            throw new RuntimeException(
                    "No columns available for update"
            );
        }

        return "UPDATE "
                + TARGET_TABLE
                + " SET "
                + setClause
                + " WHERE "
                + quotePgIdent("ckyc_no")
                + " = ?"
                + " AND "
                + quotePgIdent("fi_code")
                + " = ?";
    }


    // ==================================================================
    // SET UPDATE PARAMETERS
    // ==================================================================

    private void setUpdateParameters(
            PreparedStatement ps,
            List<String> values,
            String[] csvHeaders
    ) throws SQLException {

        int parameterIndex = 1;

        String ckycNo = null;
        String fiCode = null;

        /*
         * Set all update columns.
         *
         * CSV -> DB mapping:
         *
         * ckyc_reference_id -> ckyc_reference_no
         *
         * ckyc_no and fi_code are NOT updated.
         * They are used only in WHERE.
         */
        for (int i = 0;
             i < csvHeaders.length;
             i++) {

            String csvColumn =
                    normalizeCsvHeader(
                            csvHeaders[i]
                    ).toLowerCase(
                            Locale.ROOT
                    );

            String dbColumn =
                    resolveDbColumnForCsvHeader(
                            csvColumn
                    );

            String value =
                    values.get(i);

            /*
             * Capture matching fields.
             */
            if ("ckyc_no".equalsIgnoreCase(
                    dbColumn
            )) {

                ckycNo = value;
                continue;
            }

            if ("fi_code".equalsIgnoreCase(
                    dbColumn
            )) {

                fiCode = value;
                continue;
            }

            /*
             * Set update value using DB column name.
             */
            setTypedParameter(
                    ps,
                    parameterIndex++,
                    dbColumn,
                    value
            );
        }

        /*
         * WHERE ckyc_no = ?
         */
        setTypedParameter(
                ps,
                parameterIndex++,
                "ckyc_no",
                ckycNo
        );

        /*
         * WHERE fi_code = ?
         */
        setTypedParameter(
                ps,
                parameterIndex,
                "fi_code",
                fiCode
        );
    }


    // ==================================================================
    // TYPED JDBC VALUES
    // ==================================================================

    private void setTypedParameter(
            PreparedStatement ps,
            int index,
            String column,
            String value
    ) throws SQLException {

        if (value == null) {

            ps.setNull(
                    index,
                    jdbcSqlTypeForColumn(
                            column
                    )
            );

            return;
        }

        switch (column) {

            case "ckyc_no":

                ps.setLong(
                        index,
                        Long.parseLong(
                                value
                        )
                );

                break;

            case "reporting_entity_id":

                try {

                    ps.setObject(
                            index,
                            java.util.UUID.fromString(
                                    value
                            )
                    );

                } catch (Exception e) {

                    ps.setString(
                            index,
                            value
                    );
                }

                break;

            case "keycloak_user_id":

                try {

                    ps.setObject(
                            index,
                            java.util.UUID.fromString(
                                    value
                            )
                    );

                } catch (Exception e) {

                    ps.setString(
                            index,
                            value
                    );
                }

                break;

            case "dob":
            case "closure_date":

                ps.setObject(
                        index,
                        java.sql.Date.valueOf(
                                value
                        )
                );

                break;

            case "created_date":
            case "updated_date":

                ps.setObject(
                        index,
                        java.sql.Timestamp.valueOf(
                                normalizeTimestamp(
                                        value
                                )
                        )
                );

                break;

            case "closed_at":

                ps.setObject(
                        index,
                        java.sql.Timestamp.valueOf(
                                normalizeTimestamp(
                                        value
                                )
                        )
                );

                break;

            case "relation_active":

                ps.setBoolean(
                        index,
                        Boolean.parseBoolean(
                                value
                        )
                );

                break;

            case "metadata":

                /*
                 * JSONB is handled through setObject
                 * with PostgreSQL OTHER type.
                 */
                ps.setObject(
                        index,
                        value,
                        Types.OTHER
                );

                break;

            default:

                ps.setString(
                        index,
                        value
                );

                break;
        }
    }


    private int jdbcSqlTypeForColumn(
            String column
    ) {

        switch (column) {

            case "ckyc_no":
                return Types.BIGINT;

            case "reporting_entity_id":
            case "keycloak_user_id":
                return Types.OTHER;

            case "dob":
            case "closure_date":
                return Types.DATE;

            case "created_date":
            case "updated_date":
            case "closed_at":
                return Types.TIMESTAMP;

            case "relation_active":
                return Types.BOOLEAN;

            case "metadata":
                return Types.OTHER;

            default:
                return Types.VARCHAR;
        }
    }


    private String normalizeTimestamp(
            String value
    ) {

        /*
         * Handles:
         *
         * 2017-05-05 18:51:14
         *
         * and:
         *
         * 2017-05-05 18:51:14.123
         */
        return value.trim();
    }


    // ==================================================================
    // GET CSV VALUE
    // ==================================================================

    private String getCsvValue(
            List<String> values,
            String[] headers,
            String requestedColumn
    ) {

        for (int i = 0;
             i < headers.length;
             i++) {

            String header =
                    normalizeCsvHeader(
                            headers[i]
                    )
                    .toLowerCase(
                            Locale.ROOT
                    );

            if (requestedColumn.equalsIgnoreCase(
                    header
            )) {

                return values.get(i);
            }
        }

        return null;
    }


    // ==================================================================
    // QUOTE IDENTIFIER
    // ==================================================================

    private String quotePgIdent(
            String ident
    ) {

        return "\""
                + ident.replace(
                        "\"",
                        "\"\""
                )
                + "\"";
    }


    // ==================================================================
    // CUSTOM CSV PARSER
    // ==================================================================

    /**
     * Handles:
     *
     * - pipe delimiter
     * - quoted values
     * - JSON inside quoted metadata
     * - "" escaped quotes
     * - \" escaped quotes
     */
    private List<String> parseCsvLine(
            String line
    ) throws Exception {

        List<String> values =
                new ArrayList<>();

        if (line == null) {
            return values;
        }

        StringBuilder current =
                new StringBuilder();

        boolean inQuotes = false;

        int length = line.length();

        for (int i = 0;
             i < length;
             i++) {

            char ch =
                    line.charAt(i);

            /*
             * Opening quote.
             */
            if (!inQuotes
                    && ch == '"') {

                inQuotes = true;
                continue;
            }

            /*
             * Inside quoted field.
             */
            if (inQuotes
                    && ch == '"') {

                /*
                 * Backslash escaped quote:
                 *
                 * \"
                 */
                if (i > 0
                        && line.charAt(i - 1) == '\\') {

                    current.append(ch);
                    continue;
                }

                /*
                 * CSV escaped quote:
                 *
                 * ""
                 */
                if (i + 1 < length
                        && line.charAt(i + 1) == '"') {

                    current.append('"');

                    i++;

                    continue;
                }

                /*
                 * Quote followed by pipe:
                 * closing quote.
                 */
                if (i + 1 < length
                        && line.charAt(i + 1) == '|') {

                    inQuotes = false;

                    continue;
                }

                /*
                 * Quote at end of line.
                 */
                if (i + 1 == length) {

                    inQuotes = false;

                    continue;
                }

                /*
                 * Other quote:
                 * preserve as data.
                 */
                current.append(ch);

                continue;
            }

            /*
             * Pipe outside quotes.
             */
            if (ch == '|'
                    && !inQuotes) {

                values.add(
                        normalizeCsvValue(
                                current.toString()
                        )
                );

                current.setLength(0);

                continue;
            }

            current.append(ch);
        }

        if (inQuotes) {

            throw new RuntimeException(
                    "unclosed_quotes"
            );
        }

        values.add(
                normalizeCsvValue(
                        current.toString()
                )
        );

        return values;
    }


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
}