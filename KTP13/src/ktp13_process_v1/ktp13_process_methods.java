package ktp13_process_v1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

class ktp13_process_methods {

    private final String processName = "KTP13";
    private final String processVersion = "1.0.0";
    private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final boolean debugEnabled = false;

    private static final int MAX_PARALLEL_FILES = 5;
    private static final String DEFAULT_DELIMITER = "|";

    public void execute(String sourcePath, String destinationDir, String jdbcUrl, String dbUser, String dbPassword,
                        String targetTable, int configuredBatchSize, String serverIP) {

        log("INFO", "Process started with sourcePath=" + sourcePath + ", destinationDir=" + destinationDir
                + ", targetTable=" + targetTable + ", batchSize=" + configuredBatchSize, serverIP, "PROCESS_START");

        if (!isValidIdentifier(targetTable)) {
            log("ERROR", "Invalid target table name: " + targetTable, serverIP, "FATAL_ERROR");
            return;
        }

        File source = new File(sourcePath);
        File destination = new File(destinationDir);

        List<File> files = collectCsvFiles(source);
        if (files.isEmpty()) {
            log("INFO", "No CSV files found in source path: " + sourcePath, serverIP, "DIRECTORY_SUMMARY");
            logDirectorySummary(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, serverIP);
            return;
        }

        log("INFO", "Directory start: " + sourcePath + " file_count=" + files.size(), serverIP, "DIRECTORY_START");

        int threadCount = Math.min(MAX_PARALLEL_FILES, files.size());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CompletionService<FileProcessResult> completionService = new ExecutorCompletionService<FileProcessResult>(executor);

        for (File file : files) {
            completionService.submit(new FileProcessor(file, destination, jdbcUrl, dbUser, dbPassword, targetTable,
                    configuredBatchSize, serverIP));
        }

        int totalFiles = files.size();
        int completedFiles = 0;
        int retainedFiles = 0;
        int emptyFiles = 0;
        int headerOnlyFiles = 0;
        int errorFiles = 0;
        long totalRows = 0;
        long inserted = 0;
        long skipped = 0;
        long dbFailed = 0;
        long rowErrors = 0;
        long batchFailures = 0;
        long blankLines = 0;
        int movedFiles = 0;

        for (int i = 0; i < totalFiles; i++) {
            try {
                Future<FileProcessResult> future = completionService.take();
                FileProcessResult result = future.get();

                if (result != null) {
                    totalRows += result.dataRows;
                    inserted += result.inserted;
                    skipped += result.skipped;
                    dbFailed += result.dbFailed;
                    rowErrors += result.rowErrors;
                    batchFailures += result.batchFailures;
                    blankLines += result.blankLines;

                    if (result.emptyFile) {
                        emptyFiles++;
                    }
                    if (result.headerOnlyFile) {
                        headerOnlyFiles++;
                    }
                    if (result.success) {
                        completedFiles++;
                    } else if (result.fatalError) {
                        errorFiles++;
                    }

                    if (result.moved) {
                        movedFiles++;
                    }
                    if (!result.moved) {
                        retainedFiles++;
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                log("ERROR", "Directory processing interrupted: " + rootCauseMessage(ie), serverIP, "FATAL_ERROR");
                break;
            } catch (ExecutionException ee) {
                log("ERROR", "Directory worker failure: " + rootCauseMessage(ee), serverIP, "FATAL_ERROR");
            }
        }

        executor.shutdown();

        retainedFiles = totalFiles - movedFiles;

        logDirectorySummary(totalFiles, completedFiles, retainedFiles, emptyFiles, headerOnlyFiles, errorFiles,
                totalRows, inserted, skipped, dbFailed, rowErrors, batchFailures, blankLines, movedFiles, serverIP);

        log("INFO", "Process completed for sourcePath=" + sourcePath, serverIP, "PROCESS_END");
    }

    private final class FileProcessor implements Callable<FileProcessResult> {

        private final File file;
        private final File destinationDir;
        private final String jdbcUrl;
        private final String dbUser;
        private final String dbPassword;
        private final String targetTable;
        private final int configuredBatchSize;
        private final String serverIP;

        private FileProcessor(File file, File destinationDir, String jdbcUrl, String dbUser, String dbPassword,
                              String targetTable, int configuredBatchSize, String serverIP) {
            this.file = file;
            this.destinationDir = destinationDir;
            this.jdbcUrl = jdbcUrl;
            this.dbUser = dbUser;
            this.dbPassword = dbPassword;
            this.targetTable = targetTable;
            this.configuredBatchSize = configuredBatchSize;
            this.serverIP = serverIP;
        }

        public FileProcessResult call() {
            return processSingleFile(file, destinationDir, jdbcUrl, dbUser, dbPassword, targetTable,
                    configuredBatchSize, serverIP);
        }
    }

    private FileProcessResult processSingleFile(File file, File destinationDir, String jdbcUrl, String dbUser,
                                                String dbPassword, String targetTable, int configuredBatchSize,
                                                String serverIP) {

        FileProcessResult result = new FileProcessResult();
        result.fileName = file.getName();
        result.requestedBatchSize = configuredBatchSize;

        log("INFO", "File start: " + file.getName(), serverIP, "FILE_START");

        Connection connection = null;
        PreparedStatement ps = null;
        BufferedReader reader = null;

        try {
            connection = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
            connection.setAutoCommit(false);

            reader = new BufferedReader(new FileReader(file));

            String headerLine = readFirstNonNullLine(reader, result);
            if (headerLine == null) {
                result.emptyFile = true;
                result.status = "EMPTY";
                logFileSummary(result, serverIP, false);
                return result;
            }

            String[] headers = parseDelimitedLine(headerLine, DEFAULT_DELIMITER.charAt(0));
            if (headers == null || headers.length == 0) {
                result.emptyFile = true;
                result.status = "EMPTY";
                logFileSummary(result, serverIP, false);
                return result;
            }

            Map<String, Integer> headerIndex = buildHeaderIndexMap(headers);
            if (headerIndex.isEmpty()) {
                result.emptyFile = true;
                result.status = "EMPTY";
                logFileSummary(result, serverIP, false);
                return result;
            }

            List<String> insertColumns = buildInsertColumns();
            String insertSql = buildInsertSql(targetTable, insertColumns);

            log("INFO", "Insert SQL: " + insertSql, serverIP, "SQL_PREPARED");

            ps = connection.prepareStatement(insertSql);

            List<RowPayload> batch = new ArrayList<RowPayload>();

            String firstDataLine = readFirstNonNullLine(reader, result);
            if (firstDataLine == null) {
                result.headerOnlyFile = true;
                result.status = "HEADER_ONLY";
                logFileSummary(result, serverIP, false);
                return result;
            }

            result.dataRows++;
            RowPayload firstPayload = buildRowPayload(firstDataLine,serverIP, headers, headerIndex);
            if (firstPayload.skipBusiness) {
                result.skipped++;
            } else if (firstPayload.fatalError != null) {
                result.rowErrors++;
                throw firstPayload.fatalError;
            } else {
                batch.add(firstPayload);
                if (batch.size() >= configuredBatchSize) {
                    executeBatch(connection, ps, batch, result, serverIP, file.getName());
                    batch.clear();
                }
            }

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().length() == 0) {
                    result.blankLines++;
                    continue;
                }

                result.dataRows++;

                RowPayload payload = buildRowPayload(line,serverIP, headers, headerIndex);
                if (payload.skipBusiness) {
                    result.skipped++;
                    continue;
                }

                if (payload.fatalError != null) {
                    result.rowErrors++;
                    throw payload.fatalError;
                }

                batch.add(payload);

                if (batch.size() >= configuredBatchSize) {
                    executeBatch(connection, ps, batch, result, serverIP, file.getName());
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                executeBatch(connection, ps, batch, result, serverIP, file.getName());
                batch.clear();
            }

            connection.commit();

            result.success = true;
            result.inserted = result.insertedAttempted;

            boolean moved = moveFile(file, destinationDir, serverIP);
            result.moved = moved;

            result.status = "COMPLETED";
            logFileSummary(result, serverIP, true);

            if (moved) {
                log("INFO", "File moved: " + file.getName() + " -> " + destinationDir.getAbsolutePath(), serverIP,
                        "FILE_MOVED");
            } else {
                log("ERROR", "File move failed: " + file.getName(), serverIP, "FILE_MOVED");
            }

        } catch (BatchExceptionWrapper bew) {
            result.batchFailures++;
            result.dbFailed += bew.failedRowCount;
            result.status = "ERROR";
            safeRollback(connection);
            log("ERROR", "Batch error file=" + file.getName() + " batch_size=" + bew.failedRowCount + " reason="
                    + rootCauseMessage(bew.getCause()), serverIP, "BATCH_FAILURE");
            logFileSummary(result, serverIP, false);
            result.fatalError = true;
        } catch (FatalProcessException fpe) {
            result.status = "ERROR";
            safeRollback(connection);
            log("ERROR", "File error file=" + file.getName() + " reason=" + rootCauseMessage(fpe), serverIP,
                    "FATAL_ERROR");
            logFileSummary(result, serverIP, false);
            result.fatalError = true;
        } catch (SQLException sqle) {
            result.status = "ERROR";
            safeRollback(connection);
            log("ERROR", "File error file=" + file.getName() + " reason=" + rootCauseMessage(sqle), serverIP,
                    "FATAL_ERROR");
            logFileSummary(result, serverIP, false);
            result.fatalError = true;
        } catch (IOException ioe) {
            result.status = "ERROR";
            safeRollback(connection);
            log("ERROR", "File error file=" + file.getName() + " reason=" + rootCauseMessage(ioe), serverIP,
                    "FATAL_ERROR");
            logFileSummary(result, serverIP, false);
            result.fatalError = true;
        } catch (Exception ex) {
            result.status = "ERROR";
            safeRollback(connection);
            log("ERROR", "File error file=" + file.getName() + " reason=" + rootCauseMessage(ex), serverIP,
                    "FATAL_ERROR");
            logFileSummary(result, serverIP, false);
            result.fatalError = true;
        } finally {
            closeQuietly(ps);
            closeQuietly(reader);
            closeQuietly(connection);
        }

        return result;
    }

    private void executeBatch(Connection connection, PreparedStatement ps, List<RowPayload> batch,
                              FileProcessResult result, String serverIP, String fileName)
            throws SQLException, BatchExceptionWrapper {

        try {
            for (int i = 0; i < batch.size(); i++) {
                RowPayload payload = batch.get(i);
                bindPreparedStatement(ps, payload.values);
                ps.addBatch();
            }

            int[] counts = ps.executeBatch();
            int successCount = 0;

            if (counts != null) {
                for (int i = 0; i < counts.length; i++) {
                    if (counts[i] >= 0 || counts[i] == PreparedStatement.SUCCESS_NO_INFO) {
                        successCount++;
                    }
                }
            } else {
                successCount = batch.size();
            }

            result.insertedAttempted += successCount;
            ps.clearBatch();

        } catch (BatchUpdateException bue) {
            ps.clearBatch();
            throw new BatchExceptionWrapper(bue, batch.size());
        }
    }

    private RowPayload buildRowPayload(String line, String serverIP, String[] headers,
            Map<String, Integer> headerIndex) throws FatalProcessException {

        String[] values = parseDelimitedLine(line, DEFAULT_DELIMITER.charAt(0));
        if (values == null) {
            throw new FatalProcessException("Unable to parse CSV line");
        }

        if (values.length != headers.length) {
            throw new FatalProcessException(
                    "CSV column count mismatch expected="
                            + headers.length
                            + " actual="
                            + values.length
                            + " linePrefix="
                            + (line.length() > 250
                                    ? line.substring(0, 250)
                                    : line));
        }

        if (values == null) {
            throw new FatalProcessException("Unable to parse CSV line");
        }

        if (values.length != headers.length) {
            throw new FatalProcessException(
                    "CSV column count mismatch expected=" + headers.length
                            + " actual=" + values.length);
        }

        Map<String, String> sourceMap = new LinkedHashMap<String, String>();

        for (int i = 0; i < headers.length; i++) {
            String header = normalize(headers[i]);
            String value = normalizeValue(values[i]);

            sourceMap.put(header, value);
        }

        if (!sourceMap.containsKey("kmh_id")) {
            throw new FatalProcessException("kmh_id header not found");
        }

        if (!sourceMap.containsKey("km_ckyc_no")) {
            throw new FatalProcessException("km_ckyc_no header not found");
        }

        String historyId = sourceMap.get("kmh_id");
        String ckycNumber = sourceMap.get("km_ckyc_no");

        /*
         * These are not business skips.
         *
         * history_id is the target primary key.
         * ckyc_number is NOT NULL.
         *
         * If either source value is missing, the row is invalid
         * and the file must fail/rollback.
         */
        if (isBlank(historyId)) {
            throw new FatalProcessException(
                    "Required source field kmh_id is missing");
        }

        if (isBlank(ckycNumber)) {
            throw new FatalProcessException(
                    "Required source field km_ckyc_no is missing");
        }

        String fatherFname =
                trimToNull(sourceMap.get("km_ident_father_fname"));

        String fatherMname =
                trimToNull(sourceMap.get("km_ident_father_mname"));

        String fatherLname =
                trimToNull(sourceMap.get("km_ident_father_lname"));

        String spouseFlag =
                trimToNull(sourceMap.get("km_ident_father_spse_flag"));

        boolean isSpouseRecord =
                spouseFlag != null
                        && "spouse".equalsIgnoreCase(spouseFlag);

        List<Object> rowValues = new ArrayList<Object>();

        /*
         * history_id
         */
        rowValues.add(resolveHistoryId(historyId));

        /*
         * ckyc_number
         */
        rowValues.add(trimToNull(ckycNumber));

        /*
         * title
         */
        rowValues.add(
                trimToNull(sourceMap.get("km_ident_prefix")));

        /*
         * first_name
         */
        rowValues.add(
                trimToNull(sourceMap.get("km_ident_fname")));

        /*
         * middle_name
         */
        rowValues.add(
                trimToNull(sourceMap.get("km_ident_mname")));

        /*
         * last_name
         */
        rowValues.add(
                trimToNull(sourceMap.get("km_ident_lname")));

        /*
         * Father / spouse mapping.
         *
         * When km_ident_father_spse_flag = spouse:
         *
         * father_* = NULL
         * spouse_* = km_ident_father_*
         *
         * Otherwise:
         *
         * father_* = km_ident_father_*
         * spouse_* = NULL
         */
        if (isSpouseRecord) {

            rowValues.add(null);
            rowValues.add(null);
            rowValues.add(null);

        } else {

            rowValues.add(fatherFname);
            rowValues.add(fatherMname);
            rowValues.add(fatherLname);
        }

        /*
         * Mother
         */
        rowValues.add(
                trimToNull(sourceMap.get("km_ident_mother_fname")));

        rowValues.add(
                trimToNull(sourceMap.get("km_ident_mother_mname")));

        rowValues.add(
                trimToNull(sourceMap.get("km_ident_mother_lname")));

        /*
         * Spouse
         */
        if (isSpouseRecord) {

            rowValues.add(fatherFname);
            rowValues.add(fatherMname);
            rowValues.add(fatherLname);

        } else {

            rowValues.add(null);
            rowValues.add(null);
            rowValues.add(null);
        }

        /*
         * date_of_birth
         */
        rowValues.add(
                parseDateOnly(
                        sourceMap.get("km_ident_dob")));

        /*
         * gender
         *
         * M -> Male
         * F -> Female
         * T -> Transgender
         */
        rowValues.add(
                mapGender(
                        sourceMap.get("km_ident_gender")));

        /*
         * kyc_status
         */
        rowValues.add(
                trimToNull(
                        sourceMap.get("km_status")));

        /*
         * history_timestamp
         */
        rowValues.add(
                parseTimestamp(
                        sourceMap.get("km_updated_date")));

        /*
         * history_user
         */
        rowValues.add(
                trimToNull(
                        sourceMap.get("km_user_updated")));

        /*
         * history_reason
         */
        rowValues.add(
                trimToNull(
                        sourceMap.get("km_status_remarks")));

        return RowPayload.row(rowValues);
    }
    private void bindPreparedStatement(PreparedStatement ps, List<Object> values) throws SQLException {
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            int index = i + 1;

            if (value == null) {
                ps.setObject(index, null);
                continue;
            }

            if (value instanceof Long) {
                ps.setLong(index, ((Long) value).longValue());
            } else if (value instanceof Date) {
                ps.setDate(index, (Date) value);
            } else if (value instanceof Timestamp) {
                ps.setTimestamp(index, (Timestamp) value);
            } else {
                ps.setString(index, String.valueOf(value));
            }
        }
    }

    private List<String> buildInsertColumns() {
        List<String> columns = new ArrayList<String>();
        columns.add("history_id");
        columns.add("ckyc_number");
        columns.add("title");
        columns.add("first_name");
        columns.add("middle_name");
        columns.add("last_name");
        columns.add("father_first_name");
        columns.add("father_middle_name");
        columns.add("father_last_name");
        columns.add("mother_first_name");
        columns.add("mother_middle_name");
        columns.add("mother_last_name");
        columns.add("spouse_first_name");
        columns.add("spouse_middle_name");
        columns.add("spouse_last_name");
        columns.add("date_of_birth");
        columns.add("gender");
        columns.add("kyc_status");
        columns.add("history_timestamp");
        columns.add("history_user");
        columns.add("history_reason");
        return columns;
    }

    private String buildInsertSql(String targetTable, List<String> columns) {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("INSERT INTO ").append(targetTable).append(" (");

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

        sb.append(") ON CONFLICT DO NOTHING");
        return sb.toString();
    }

    private Map<String, Integer> buildHeaderIndexMap(String[] headers) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < headers.length; i++) {
            String key = normalize(headers[i]);
            if (key != null && !map.containsKey(key)) {
                map.put(key, Integer.valueOf(i));
            }
        }
        return map;
    }

    private List<File> collectCsvFiles(File source) {
        List<File> files = new ArrayList<File>();

        if (source.isFile()) {
            if (isCsvFile(source)) {
                files.add(source);
            }
            return files;
        }

        File[] arr = source.listFiles(new FileFilter() {
            public boolean accept(File pathname) {
                return pathname != null && pathname.isFile() && isCsvFile(pathname);
            }
        });

        if (arr != null) {
            Arrays.sort(arr, new Comparator<File>() {
                public int compare(File f1, File f2) {
                    return f1.getName().compareToIgnoreCase(f2.getName());
                }
            });
            files.addAll(Arrays.asList(arr));
        }

        return files;
    }

    private boolean isCsvFile(File file) {
        if (file == null) {
            return false;
        }
        String name = file.getName();
        if (name == null) {
            return false;
        }
        return name.toLowerCase(Locale.ENGLISH).endsWith(".csv");
    }

    private String readFirstNonNullLine(BufferedReader reader, FileProcessResult result) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.trim().length() == 0) {
                result.blankLines++;
                continue;
            }
            return line;
        }
        return null;
    }

    private boolean moveFile(File sourceFile, File destinationDir, String serverIP) {
        try {
            File target = new File(destinationDir, sourceFile.getName());
            Files.move(sourceFile.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING);
            return true;
        } catch (Exception ex) {
            log("ERROR", "File move failed file=" + sourceFile.getName() + " reason=" + rootCauseMessage(ex), serverIP,
                    "FILE_MOVED");
            return false;
        }
    }

    private void safeRollback(Connection connection) {
        if (connection != null) {
            try {
                connection.rollback();
            } catch (Exception ex) {
                // Ignore rollback failures.
            }
        }
    }

    private void logFileSummary(FileProcessResult result, String serverIP, boolean moved) {
        if (result == null) {
            return;
        }

        long effectiveBatchSize = result.dataRows < result.requestedBatchSize ? result.dataRows : result.requestedBatchSize;
        StringBuilder sb = new StringBuilder(512);
        sb.append("file=").append(result.fileName != null ? result.fileName : "");
        sb.append(" status=").append(result.status != null ? result.status : "");
        sb.append(" data_rows=").append(result.dataRows);
        sb.append(" inserted=").append(result.inserted);
        sb.append(" skipped=").append(result.skipped);
        sb.append(" db_failed=").append(result.dbFailed);
        sb.append(" row_errors=").append(result.rowErrors);
        sb.append(" batch_failures=").append(result.batchFailures);
        sb.append(" blank_lines=").append(result.blankLines);
        sb.append(" effective_batch_size=").append(effectiveBatchSize);
        sb.append(" moved=").append(moved);

        log("INFO", sb.toString(), serverIP, "FILE_COMPLETE");
    }

    private void logDirectorySummary(int totalFiles, int completedFiles, int retainedFiles, int emptyFiles,
                                     int headerOnlyFiles, int errorFiles, long totalRows, long inserted,
                                     long skipped, long dbFailed, long rowErrors, long batchFailures,
                                     long blankLines, int movedFiles, String serverIP) {

        StringBuilder sb = new StringBuilder(512);
        sb.append("total_files=").append(totalFiles);
        sb.append(" completed_files=").append(completedFiles);
        sb.append(" retained_files=").append(retainedFiles);
        sb.append(" empty_files=").append(emptyFiles);
        sb.append(" header_only_files=").append(headerOnlyFiles);
        sb.append(" error_files=").append(errorFiles);
        sb.append(" total_rows=").append(totalRows);
        sb.append(" inserted=").append(inserted);
        sb.append(" skipped=").append(skipped);
        sb.append(" db_failed=").append(dbFailed);
        sb.append(" row_errors=").append(rowErrors);
        sb.append(" batch_failures=").append(batchFailures);
        sb.append(" blank_lines=").append(blankLines);
        sb.append(" moved_files=").append(movedFiles);

        log("INFO", sb.toString(), serverIP, "DIRECTORY_SUMMARY");
    }

    private Object resolveHistoryId(String value) throws FatalProcessException {
        String v = trimToNull(value);
        if (v == null) {
            throw new FatalProcessException("history_id is required");
        }

        try {
            return Long.valueOf(v);
        } catch (Exception ex) {
            throw new FatalProcessException("Invalid history_id value: " + v);
        }
    }

    private Date parseDateOnly(String value) throws FatalProcessException {
        String v = trimToNull(value);
        if (v == null) {
            return null;
        }

        String[] patterns = new String[] {
                "yyyy-MM-dd",
                "dd-MM-yyyy",
                "dd/MM/yyyy",
                "yyyy/MM/dd",
                "yyyy-MM-dd HH:mm:ss",
                "dd-MM-yyyy HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss.SSS"
        };

        for (int i = 0; i < patterns.length; i++) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(patterns[i]);
                sdf.setLenient(false);
                java.util.Date d = sdf.parse(v);
                return new Date(d.getTime());
            } catch (ParseException pe) {
                // try next pattern
            }
        }

        throw new FatalProcessException("Invalid date_of_birth value: " + v);
    }

    private Timestamp parseTimestamp(String value) throws FatalProcessException {
        String v = trimToNull(value);
        if (v == null) {
            return null;
        }

        String[] patterns = new String[] {
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd HH:mm:ss.SSS",
                "dd-MM-yyyy HH:mm:ss",
                "dd/MM/yyyy HH:mm:ss",
                "yyyy-MM-dd'T'HH:mm:ss",
                "yyyy-MM-dd"
        };

        for (int i = 0; i < patterns.length; i++) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(patterns[i]);
                sdf.setLenient(false);
                java.util.Date d = sdf.parse(v);
                return new Timestamp(d.getTime());
            } catch (ParseException pe) {
                // try next pattern
            }
        }

        throw new FatalProcessException("Invalid timestamp value: " + v);
    }

    private String mapGender(String value) {
        String v = trimToNull(value);
        if (v == null) {
            return null;
        }

        if ("M".equalsIgnoreCase(v)) {
            return "Male";
        }
        if ("F".equalsIgnoreCase(v)) {
            return "Female";
        }
        if ("T".equalsIgnoreCase(v)) {
            return "Transgender";
        }

        return v;
    }

    private String normalize(String value) {
        if (value == null) {
            return null;
        }
        String v = value.trim();
        if (v.length() == 0) {
            return null;
        }
        return v.toLowerCase(Locale.ENGLISH);
    }

    private String normalizeValue(String value) {
        if (value == null) {
            return null;
        }

        String v = value.trim();
        if (v.length() == 0) {
            return null;
        }

        if ("\\N".equals(v)) {
            return null;
        }

        if (v.length() >= 2) {
            char first = v.charAt(0);
            char last = v.charAt(v.length() - 1);
            if (first == '"' && last == '"') {
                v = v.substring(1, v.length() - 1);
            }
        }

        v = v.trim();
        if (v.length() == 0) {
            return null;
        }

        if ("\\N".equals(v)) {
            return null;
        }

        return v;
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String v = value.trim();
        if (v.length() == 0) {
            return null;
        }
        if ("\\N".equals(v)) {
            return null;
        }
        return v;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }

    private boolean isValidIdentifier(String value) {
        if (value == null) {
            return false;
        }
        String v = value.trim();
        if (v.length() == 0) {
            return false;
        }
        return v.matches("^[A-Za-z0-9_]+(\\.[A-Za-z0-9_]+)?$");
    }

    private String[] parseDelimitedLine(String line, char delimiter) {

        if (line == null) {
            return null;
        }

        List<String> fields = new ArrayList<String>();
        StringBuilder current = new StringBuilder();

        int length = line.length();
        int i = 0;

        while (i < length) {

            boolean quoted = false;

            /*
             * Quoted field.
             */
            if (line.charAt(i) == '"') {
                quoted = true;
                i++;
            }

            if (quoted) {

                boolean closed = false;

                while (i < length) {

                    char c = line.charAt(i);

                    /*
                     * Backslash escape.
                     *
                     * \" -> literal "
                     * \\ -> literal \
                     */
                    if (c == '\\' && i + 1 < length) {

                        char next = line.charAt(i + 1);

                        if (next == '"' || next == '\\') {

                            current.append(next);
                            i += 2;

                            continue;
                        }

                        /*
                         * Unknown escape.
                         * Preserve the backslash.
                         */
                        current.append(c);
                        i++;

                        continue;
                    }

                    /*
                     * Quote.
                     */
                    if (c == '"') {

                        /*
                         * Standard CSV escaped quote:
                         *
                         * ""
                         */
                        if (i + 1 < length
                                && line.charAt(i + 1) == '"') {

                            current.append('"');
                            i += 2;

                            continue;
                        }

                        /*
                         * Closing quote.
                         */
                        i++;
                        closed = true;

                        break;
                    }

                    current.append(c);
                    i++;
                }

                /*
                 * Quoted field was never closed.
                 */
                if (!closed) {
                    return null;
                }

            } else {

                /*
                 * Unquoted field.
                 */
                while (i < length
                        && line.charAt(i) != delimiter) {

                    current.append(line.charAt(i));
                    i++;
                }
            }

            /*
             * Store field.
             */
            fields.add(current.toString());
            current.setLength(0);

            /*
             * Consume delimiter.
             */
            if (i < length && line.charAt(i) == delimiter) {
                i++;
            }
        }

        /*
         * Preserve final empty field.
         */
        if (length > 0
                && line.charAt(length - 1) == delimiter) {

            fields.add("");
        }

        String[] result = new String[fields.size()];

        for (int j = 0; j < fields.size(); j++) {
            result[j] = fields.get(j);
        }

        return result;
    }

    private void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ex) {
                // Ignore close failures.
            }
        }
    }

    private String rootCauseMessage(Throwable t) {
        if (t == null) {
            return "";
        }

        Throwable current = t;
        while (current.getCause() != null) {
            current = current.getCause();
        }

        String msg = current.getMessage();
        if (msg == null || msg.trim().length() == 0) {
            return current.getClass().getSimpleName();
        }

        return current.getClass().getSimpleName() + ": " + msg.trim();
    }

    private void log(String level, String message, String serverIP, String evtType) {

        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled)
            return;

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
        if (value == null || value.length() == 0) {
            return "";
        }
        String lower = value.toLowerCase(Locale.ENGLISH);
        return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
    }

    private static final class FileProcessResult {
        private String fileName;
        private String status;
        private boolean success;
        private boolean fatalError;
        private boolean moved;
        private boolean emptyFile;
        private boolean headerOnlyFile;
        private long dataRows;
        private long inserted;
        private long insertedAttempted;
        private long skipped;
        private long dbFailed;
        private long rowErrors;
        private long batchFailures;
        private long blankLines;
        private int requestedBatchSize = 50000;
    }

    private static final class RowPayload {
        private final List<Object> values;
        private final boolean skipBusiness;
        private final FatalProcessException fatalError;

        private RowPayload(List<Object> values, boolean skipBusiness, FatalProcessException fatalError) {
            this.values = values;
            this.skipBusiness = skipBusiness;
            this.fatalError = fatalError;
        }

        private static RowPayload row(List<Object> values) {
            return new RowPayload(values, false, null);
        }

        private static RowPayload skip() {
            return new RowPayload(null, true, null);
        }
    }

    private static final class FatalProcessException extends Exception {
        private static final long serialVersionUID = 1L;

        private FatalProcessException(String message) {
            super(message);
        }
    }

    private static final class BatchExceptionWrapper extends Exception {
        private static final long serialVersionUID = 1L;
        private final int failedRowCount;

        private BatchExceptionWrapper(Throwable cause, int failedRowCount) {
            super(cause);
            this.failedRowCount = failedRowCount;
        }
    }
}

