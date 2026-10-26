package binlog_reader_v2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class binlog_methods {

    private String mysqlbinlogPath;
    private String binlogFile;
    private String startDatetime;
    private String stopDatetime;

    private String dbUrl;
    private String dbUser;
    private String dbPass;

    private String processName = "BINLOG_READER";
    private String processVersion = "1.1";

    private boolean debugEnabled = true;
    private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // EVT TYPES
    private static final String EVT_START = "START";
    private static final String EVT_CONFIG = "CONFIG_LOAD";
    private static final String EVT_BINLOG_CMD = "BINLOG_CMD";
    private static final String EVT_BINLOG_STDERR = "BINLOG_STDERR";
    private static final String EVT_METADATA = "METADATA_LOAD";
    private static final String EVT_DB_WRITE = "DB_WRITE";
    private static final String EVT_WORKER_START = "WORKER_START";
    private static final String EVT_WORKER_END = "WORKER_END";
    private static final String EVT_SUMMARY = "EXECUTION_SUMMARY";
    private static final String EVT_VALIDATION = "VALIDATION";
    private static final String EVT_ERROR = "ERROR";
    private static final String EVT_END = "END";

    private String currentOperation;
    private String currentTable;

    private boolean readingWhere = false;
    private boolean readingSet = false;

    private final Map<String, TableMetadata> metadataCache = new ConcurrentHashMap<>();
    private final Map<String, String> whereValues = new LinkedHashMap<>();
    private final Map<String, String> setValues = new LinkedHashMap<>();

    private static final Pattern COLUMN_PATTERN = Pattern.compile("@(\\d+)\\s*=\\s*(.*)");

    private static final int BATCH_SIZE = 500;
    private static final int WORKER_THREADS = 4;
    private static final int QUEUE_CAPACITY = 10000;

    private final BlockingQueue<TriggerRow> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private ExecutorService workerPool;

    private final AtomicReference<Exception> fatalError = new AtomicReference<>();
    private final AtomicLong totalLinesRead = new AtomicLong(0);
    private final AtomicLong totalEventsDetected = new AtomicLong(0);
    private final AtomicLong totalInsertEvents = new AtomicLong(0);
    private final AtomicLong totalUpdateEvents = new AtomicLong(0);
    private final AtomicLong totalDeleteEvents = new AtomicLong(0);
    private final AtomicLong totalMetadataLoads = new AtomicLong(0);
    private final AtomicLong totalQueueInsertions = new AtomicLong(0);
    private final AtomicLong totalBatchExecutions = new AtomicLong(0);
    private final AtomicLong totalDbRowsInserted = new AtomicLong(0);
    private final AtomicLong totalParseWarnings = new AtomicLong(0);
    private final AtomicLong totalColumnMappingWarnings = new AtomicLong(0);

    private volatile boolean foundRowEvents = false;

    // ================= LOG FUNCTION =================
    private void log(String level, String message, String serverIP, String evtType) {

        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
            return;
        }

        String safeLevel = capitalize(level);
        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp).append(" [").append(safeLevel).append("] ")
                .append("Process_Name: ").append(processName).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(processVersion).append(" ")
                .append("Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                .append(" Description: ").append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private String capitalize(String level) {
        if (level == null || level.trim().isEmpty()) {
            return "NA";
        }
        String v = level.trim().toLowerCase(Locale.ROOT);
        return Character.toUpperCase(v.charAt(0)) + v.substring(1);
    }

    private String safePreview(String value) {
        if (value == null) {
            return "null";
        }
        String trimmed = value.trim();
        if (trimmed.length() <= 240) {
            return trimmed;
        }
        return trimmed.substring(0, 240) + "...";
    }

    private void setFatalError(Exception e) {
        if (e != null) {
            fatalError.compareAndSet(null, e);
        }
    }

    // ================= MAIN =================
    public void startBinlogReader(
            String mysqlbinlogPath,
            String binlogFile,
            String startDatetime,
            String stopDatetime,
            String dbUrl,
            String dbUser,
            String dbPass
    ) throws Exception {

        long processStartNs = System.nanoTime();
        Exception failure = null;

        this.mysqlbinlogPath = mysqlbinlogPath;
        this.binlogFile = binlogFile;
        this.startDatetime = startDatetime;
        this.stopDatetime = stopDatetime;
        this.dbUrl = dbUrl;
        this.dbUser = dbUser;
        this.dbPass = dbPass;

        log("INFO", "Binlog reader started", dbUrl, EVT_START);
        log("INFO",
                "Configuration loaded -> mysqlbinlogPath=" + mysqlbinlogPath +
                        ", binlogFile=" + binlogFile +
                        ", startDatetime=" + startDatetime +
                        ", stopDatetime=" + stopDatetime,
                dbUrl,
                EVT_CONFIG);

        workerPool = Executors.newFixedThreadPool(WORKER_THREADS, new ThreadFactory() {
            private final AtomicLong seq = new AtomicLong(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "binlog-db-worker-" + seq.incrementAndGet());
                t.setDaemon(false);
                return t;
            }
        });

        try {
            for (int i = 0; i < WORKER_THREADS; i++) {
                workerPool.submit(new DBWorker(i + 1));
            }

            readBinlog();

            flushPreviousEvent();

            if (fatalError.get() != null) {
                throw fatalError.get();
            }

        } catch (Exception e) {
            failure = e;
        } finally {
            log("INFO", "Initiating worker shutdown", dbUrl, EVT_END);

            if (workerPool != null) {
                workerPool.shutdown();

                try {
                    if (!workerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                        log("WARN",
                                "Worker pool did not terminate gracefully. Forcing shutdown.",
                                dbUrl,
                                EVT_ERROR);
                        workerPool.shutdownNow();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log("ERROR",
                            "Worker shutdown interrupted: " + ie.getMessage(),
                            dbUrl,
                            EVT_ERROR);
                    workerPool.shutdownNow();
                }
            }

            printSummary(processStartNs);

            if (failure == null) {
                log("INFO", "Binlog reading completed successfully", dbUrl, EVT_END);
            } else {
                log("ERROR", "Binlog reading ended with error: " + failure.getMessage(), dbUrl, EVT_ERROR);
            }
        }

        if (failure != null) {
            throw failure;
        }
    }

    // ================= BINLOG READ =================
    private void readBinlog() throws Exception {

        log("INFO",
                "Executing mysqlbinlog command with row decoding for file: " + binlogFile,
                dbUrl,
                EVT_BINLOG_CMD);

        ProcessBuilder pb = new ProcessBuilder(
                mysqlbinlogPath,
                "--base64-output=DECODE-ROWS",
                "-v",
                "--start-datetime=" + startDatetime,
                "--stop-datetime=" + stopDatetime,
                binlogFile
        );

        Process process = pb.start();

        List<String> stderrLines = Collections.synchronizedList(new ArrayList<>());
        Thread stderrThread = new Thread(
                () -> consumeErrorStream(process.getErrorStream(), stderrLines),
                "mysqlbinlog-stderr-reader"
        );
        stderrThread.setDaemon(true);
        stderrThread.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                totalLinesRead.incrementAndGet();
                processLine(line);

                if (fatalError.get() != null) {
                    break;
                }
            }
        } catch (Exception e) {
            setFatalError(e);
            log("ERROR", "Error while reading binlog stream: " + e.getMessage(), dbUrl, EVT_ERROR);
        }

        int exitCode = process.waitFor();

        try {
            stderrThread.join(5000L);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        if (!foundRowEvents) {
            log("WARN",
                    "No row-based INSERT/UPDATE/DELETE events were detected. Ensure the file was generated using mysqlbinlog with --base64-output=DECODE-ROWS -v and contains row events.",
                    dbUrl,
                    EVT_VALIDATION);
        }

        if (fatalError.get() != null) {
            throw fatalError.get();
        }

        if (exitCode != 0) {
            String stderrText;
            synchronized (stderrLines) {
                stderrText = String.join(" | ", new ArrayList<>(stderrLines));
            }

            throw new IOException("mysqlbinlog exited with code " + exitCode +
                    (stderrText.isEmpty() ? "" : ". stderr: " + stderrText));
        }
    }

    private void consumeErrorStream(InputStream inputStream, List<String> stderrLines) {
        try (BufferedReader errReader = new BufferedReader(new InputStreamReader(inputStream))) {
            String errLine;
            while ((errLine = errReader.readLine()) != null) {
                stderrLines.add(errLine);
                log("WARN", "mysqlbinlog stderr: " + errLine, dbUrl, EVT_BINLOG_STDERR);
            }
        } catch (Exception e) {
            setFatalError(e instanceof Exception ? (Exception) e : new Exception(e));
            log("ERROR", "Failed to read mysqlbinlog stderr stream: " + e.getMessage(), dbUrl, EVT_ERROR);
        }
    }

    // ================= PARSE =================
    private void processLine(String line) {

        try {
            if (line == null) {
                return;
            }

            line = line.trim();
            if (line.isEmpty()) {
                return;
            }

            if (line.startsWith("### INSERT INTO")) {
                flushPreviousEvent();
                currentOperation = "INSERT";
                currentTable = extractTable(line);
                foundRowEvents = true;
                totalEventsDetected.incrementAndGet();
                totalInsertEvents.incrementAndGet();
                resetFlags();
            } else if (line.startsWith("### UPDATE")) {
                flushPreviousEvent();
                currentOperation = "UPDATE";
                currentTable = extractTable(line);
                foundRowEvents = true;
                totalEventsDetected.incrementAndGet();
                totalUpdateEvents.incrementAndGet();
                resetFlags();
            } else if (line.startsWith("### DELETE FROM")) {
                flushPreviousEvent();
                currentOperation = "DELETE";
                currentTable = extractTable(line);
                foundRowEvents = true;
                totalEventsDetected.incrementAndGet();
                totalDeleteEvents.incrementAndGet();
                resetFlags();
            } else if (line.startsWith("### WHERE")) {
                readingWhere = true;
                readingSet = false;
            } else if (line.startsWith("### SET")) {
                readingWhere = false;
                readingSet = true;
            } else if (line.startsWith("###   @")) {
                parseColumnLine(line);
            }
        } catch (Exception e) {
            setFatalError(e);
            log("ERROR",
                    "Error in processLine: " + e.getMessage() + " | line=" + safePreview(line),
                    null,
                    EVT_ERROR);
        }
    }

    private void resetFlags() {
        readingWhere = false;
        readingSet = false;
        whereValues.clear();
        setValues.clear();
    }

    private void parseColumnLine(String line) throws Exception {

        if (currentTable == null) {
            return;
        }

        TableMetadata metadata = getMetadata(currentTable);

        Matcher matcher = COLUMN_PATTERN.matcher(line.replace("###", "").trim());
        if (!matcher.find()) {
            totalParseWarnings.incrementAndGet();
            return;
        }

        int columnIndex = Integer.parseInt(matcher.group(1));
        String value = cleanValue(matcher.group(2));

        String columnName = metadata.columnIndexMap.get(columnIndex);
        if (columnName == null) {
            totalColumnMappingWarnings.incrementAndGet();
            return;
        }

        if (readingWhere) {
            whereValues.put(columnName, value);
        }
        if (readingSet) {
            setValues.put(columnName, value);
        }
    }

    // ================= FLUSH =================
    private void flushPreviousEvent() throws Exception {

        if (currentOperation == null || currentTable == null) {
            return;
        }

        TableMetadata metadata = getMetadata(currentTable);

        Map<String, String> source = "INSERT".equals(currentOperation) ? setValues : whereValues;

        List<String> pkNames = new ArrayList<>();
        List<String> pkValues = new ArrayList<>();

        for (String pk : metadata.primaryKeys) {
            if (source.containsKey(pk)) {
                pkNames.add(pk);
                pkValues.add(source.get(pk));
            }
        }

        if (pkNames.isEmpty()) {
            totalParseWarnings.incrementAndGet();
            pkNames.add("UNKNOWN_PK");
            pkValues.add("UNKNOWN");
        }

        queue.put(new TriggerRow(
                currentTable,
                String.join(",", pkNames),
                String.join(",", pkValues),
                currentOperation
        ));

        totalQueueInsertions.incrementAndGet();

        currentOperation = null;
        currentTable = null;
    }

    // ================= METADATA =================
    private TableMetadata getMetadata(String table) throws Exception {

        TableMetadata cached = metadataCache.get(table);
        if (cached != null) {
            return cached;
        }

        log("INFO", "Loading metadata for table: " + table, null, EVT_METADATA);

        TableMetadata metadata = loadTableMetadata(table);
        metadataCache.put(table, metadata);

        totalMetadataLoads.incrementAndGet();

        log("INFO",
                "Metadata loaded for table: " + table +
                        ", columns=" + metadata.columnIndexMap.size() +
                        ", primaryKeys=" + metadata.primaryKeys,
                null,
                EVT_METADATA);

        return metadata;
    }

    private TableMetadata loadTableMetadata(String tableFullName) throws Exception {

        TableMetadata metadata = new TableMetadata();

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {

            String[] parts = tableFullName.split("\\.");
            String schema = parts.length > 1 ? parts[0] : conn.getCatalog();
            String table = parts.length > 1 ? parts[1] : parts[0];

            String columnSql =
                    "SELECT column_name, ordinal_position " +
                    "FROM information_schema.columns " +
                    "WHERE table_name=? AND table_schema=? " +
                    "ORDER BY ordinal_position";

            try (PreparedStatement ps = conn.prepareStatement(columnSql)) {
                ps.setString(1, table);
                ps.setString(2, schema);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        metadata.columnIndexMap.put(
                                rs.getInt("ordinal_position"),
                                rs.getString("column_name")
                        );
                    }
                }
            }

            String pkSql =
                    "SELECT column_name " +
                    "FROM information_schema.key_column_usage " +
                    "WHERE table_name=? AND table_schema=? AND constraint_name='PRIMARY'";

            try (PreparedStatement ps = conn.prepareStatement(pkSql)) {
                ps.setString(1, table);
                ps.setString(2, schema);

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        metadata.primaryKeys.add(rs.getString("column_name"));
                    }
                }
            }
        }

        return metadata;
    }

    private String extractTable(String line) {

        int firstTick = line.indexOf("`");
        int lastTick = line.lastIndexOf("`");

        if (firstTick != -1 && lastTick != -1 && lastTick > firstTick) {
            return line.substring(firstTick + 1, lastTick).replace("`.`", ".");
        }

        return null;
    }

    private String cleanValue(String value) {

        if (value == null) {
            return null;
        }

        value = value.trim();

        int commentIndex = value.indexOf("/*");
        if (commentIndex >= 0) {
            value = value.substring(0, commentIndex).trim();
        }

        if ("NULL".equals(value)) {
            return null;
        }

        if (value.startsWith("'") && value.endsWith("'") && value.length() >= 2) {
            return value.substring(1, value.length() - 1);
        }

        return value;
    }

    private void printSummary(long processStartNs) {

        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - processStartNs);

        log("INFO",
                "Execution summary -> " +
                        "linesRead=" + totalLinesRead.get() +
                        ", eventsDetected=" + totalEventsDetected.get() +
                        ", insertEvents=" + totalInsertEvents.get() +
                        ", updateEvents=" + totalUpdateEvents.get() +
                        ", deleteEvents=" + totalDeleteEvents.get() +
                        ", metadataLoads=" + totalMetadataLoads.get() +
                        ", queueInsertions=" + totalQueueInsertions.get() +
                        ", batchExecutions=" + totalBatchExecutions.get() +
                        ", dbRowsInserted=" + totalDbRowsInserted.get() +
                        ", parseWarnings=" + totalParseWarnings.get() +
                        ", columnMappingWarnings=" + totalColumnMappingWarnings.get() +
                        ", elapsedMs=" + elapsedMs,
                dbUrl,
                EVT_SUMMARY);
    }

    // ================= INNER CLASSES =================
    static class TableMetadata {
        Map<Integer, String> columnIndexMap = new HashMap<>();
        Set<String> primaryKeys = new LinkedHashSet<>();
    }

    static class TriggerRow {
        String table, pkName, pkValue, operation;

        TriggerRow(String t, String pkn, String pkv, String op) {
            table = t;
            pkName = pkn;
            pkValue = pkv;
            operation = op;
        }
    }

    class DBWorker implements Runnable {

        private final int workerId;

        DBWorker(int workerId) {
            this.workerId = workerId;
        }

        public void run() {

            String workerName = "DBWorker-" + workerId;
            List<TriggerRow> batch = new ArrayList<>(BATCH_SIZE);

            log("INFO", workerName + " started", null, EVT_WORKER_START);

            try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
                 PreparedStatement ps = conn.prepareStatement(
                         "INSERT INTO data_insertion_trigger " +
                                 "(record_time, table_name, primary_columns_name, primary_columns_value, type_of_operation) " +
                                 "VALUES (?, ?, ?, ?, ?)")) {

                conn.setAutoCommit(false);

                while (true) {

                    if (fatalError.get() != null) {
                        break;
                    }

                    TriggerRow row = queue.poll(5, TimeUnit.SECONDS);

                    if (row == null) {
                        if (workerPool.isShutdown() && queue.isEmpty()) {
                            break;
                        }
                        continue;
                    }

                    batch.add(row);

                    if (batch.size() >= BATCH_SIZE) {
                        executeBatch(workerName, batch, ps, conn);
                        batch.clear();
                    }
                }

                if (!batch.isEmpty()) {
                    executeBatch(workerName, batch, ps, conn);
                    batch.clear();
                }

                log("INFO", workerName + " stopped cleanly", null, EVT_WORKER_END);

            } catch (Exception e) {
                setFatalError(e);
                log("ERROR", workerName + " error: " + e.getMessage(), null, EVT_ERROR);
            }
        }

        private void executeBatch(String workerName, List<TriggerRow> batch, PreparedStatement ps, Connection conn) throws Exception {

            if (batch == null || batch.isEmpty()) {
                return;
            }

            long batchStartNs = System.nanoTime();

            for (TriggerRow r : batch) {
                ps.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
                ps.setString(2, r.table);
                ps.setString(3, r.pkName);
                ps.setString(4, r.pkValue);
                ps.setString(5, r.operation);
                ps.addBatch();
            }

            try {
                ps.executeBatch();
                conn.commit();

                totalBatchExecutions.incrementAndGet();
                totalDbRowsInserted.addAndGet(batch.size());

                long batchElapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - batchStartNs);

                log("INFO",
                        workerName + " batch committed successfully -> batchSize=" + batch.size() +
                                ", totalInserted=" + totalDbRowsInserted.get() +
                                ", totalBatches=" + totalBatchExecutions.get() +
                                ", durationMs=" + batchElapsedMs,
                        null,
                        EVT_DB_WRITE);

            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (Exception rollbackEx) {
                    log("ERROR",
                            workerName + " rollback failed: " + rollbackEx.getMessage(),
                            null,
                            EVT_ERROR);
                }

                setFatalError(e);
                log("ERROR",
                        workerName + " batch failed and rolled back -> " + e.getMessage(),
                        null,
                        EVT_ERROR);

                throw e;
            }
        }
    }
}