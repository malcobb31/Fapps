package postgre_ip_v3;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ProcessData_v3 {

    private static final String PROCESS_NAME = "CSV_TO_PSQL";
    private static final String PROCESS_VERSION = "1.2.0";
    private static final int BATCH_SIZE = 5000;

    private static final DateTimeFormatter LOG_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter FILE_TS_FORMAT = DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss");

    private static final ConcurrentLinkedQueue<LogRecord> LOG_QUEUE = new ConcurrentLinkedQueue<>();
    private static final int DEFAULT_THREAD_COUNT = Math.max(1, Runtime.getRuntime().availableProcessors());

    private static class LogRecord {
        final String timestamp;
        final String processName;
        final String processVersion;
        final String logLevel;
        final String evtType;
        final String description;

        LogRecord(String timestamp, String processName, String processVersion, String logLevel, String evtType, String description) {
            this.timestamp = timestamp;
            this.processName = processName;
            this.processVersion = processVersion;
            this.logLevel = logLevel;
            this.evtType = evtType;
            this.description = description;
        }
    }

    public static void processAllInDirectory(String dirPath, String processedDirPath, String headerFlag, String delimiter,
                                             String dbUrl, String dbUser, String dbPass,
                                             String pdbHost, String pdbPort, String pdbName, String pdbUser, String pdbPass,
                                             String numTablesToPickStr, String config_table, String MAX_CPU_UTILIZATION,
                                             String MAX_MEMORY_UTILIZATION, String bulkNumberStr) {

        // ---- Parse bulk threshold ----
        int tmpBulk;
        try {
            tmpBulk = Integer.parseInt(bulkNumberStr);
        } catch (NumberFormatException e) {
            enqueueLog("CRITICAL", "INVALID_BULK_NUMBER",
                    "Invalid bulk number format: " + bulkNumberStr + ". Using default 10000");
            tmpBulk = 10000;
        }
        final int bulkThreshold = tmpBulk; // effectively final

        // ---- Parse CPU/memory thresholds ----
        final double maxCpuThreshold = parseDoubleOrDefault(MAX_CPU_UTILIZATION, 80.0,
                "INVALID_CPU_THRESHOLD", "CPU");
        final double maxMemoryThreshold = parseDoubleOrDefault(MAX_MEMORY_UTILIZATION, 80.0,
                "INVALID_MEMORY_THRESHOLD", "Memory");

        // ---- Resource check ----
        if (!checkSystemResources(maxCpuThreshold, maxMemoryThreshold)) {
            enqueueLog("CRITICAL", "RESOURCE_LIMIT_EXCEEDED",
                    "System resources exceeded threshold. CPU or Memory utilization too high.");
            flushLogsToStdOut();
            return;
        }

        int numTablesToPick;
        try {
            numTablesToPick = Integer.parseInt(numTablesToPickStr);
        } catch (NumberFormatException e) {
            enqueueLog("CRITICAL", "INVALID_TABLE_COUNT",
                    "Invalid number format for tables to pick: " + numTablesToPickStr);
            flushLogsToStdOut();
            return;
        }

        try {
            // ---- Fetch tables ----
            List<String[]> readyTables = new ArrayList<>();
            try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
                 PreparedStatement ps = conn.prepareStatement("SELECT sno, table_name FROM " + config_table +
                         " WHERE sync_status = 'ready_to_import' LIMIT ?")) {
                enqueueLog("INFO", "Query_Ran", "Query:" + ps);
                ps.setInt(1, numTablesToPick);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        readyTables.add(new String[]{rs.getString("sno"), rs.getString("table_name")});
                    }
                }
            }

            if (readyTables.isEmpty()) {
                enqueueLog("INFO", "NO_READY_TABLES", "No tables found with sync_status=ready_to_import");
                return;
            }

            // ---- Load files ----
            List<Path> allFiles;
            try (java.util.stream.Stream<Path> s = Files.list(Paths.get(dirPath))) {
                allFiles = s.filter(p -> {
                            String fname = p.getFileName().toString();
                            return (fname.startsWith("raw_") || fname.startsWith("process_")) && fname.endsWith(".csv");
                        })
                        .collect(Collectors.toList());
            }

            if (allFiles.isEmpty()) {
                enqueueLog("INFO", "NO_FILES", "No files to process in directory: " + dirPath);
                return;
            }

            // ---- Group files per table ----
            Map<String, List<Path>> tableFilesMap = new HashMap<>();
            for (String[] tableEntry : readyTables) {
                String sno = tableEntry[0];
                List<Path> matchedFiles = allFiles.stream()
                        .filter(f -> {
                            String[] parts = f.getFileName().toString().split("_");
                            return parts.length > 4 && parts[4].equals(sno);
                        })
                        .sorted(Comparator.comparing(p -> {
                            String fname = p.getFileName().toString();
                            try {
                                String[] parts = fname.split("_");
                                if (parts.length >= 7) {
                                    String tsStr = String.join("_", Arrays.copyOfRange(parts, parts.length - 6, parts.length))
                                            .replace(".csv", "");
                                    return LocalDateTime.parse(tsStr, FILE_TS_FORMAT);
                                }
                            } catch (Exception e) {
                                enqueueLog("MAJOR", "FILENAME_TS_PARSE_FAIL",
                                        "File: " + fname + " Error: " + safeMessage(e));
                            }
                            return LocalDateTime.now();
                        }))
                        .collect(Collectors.toList());
                tableFilesMap.put(sno, matchedFiles);
            }

            // ---- Process in parallel ----
            ExecutorService executor = Executors.newFixedThreadPool(Math.min(numTablesToPick, DEFAULT_THREAD_COUNT));
            List<Future<?>> futures = new ArrayList<>();

            for (String[] tableEntry : readyTables) {
                String sno = tableEntry[0];
                String tableName = tableEntry[1];
                List<Path> tableFiles = tableFilesMap.getOrDefault(sno, Collections.emptyList());

                futures.add(executor.submit(() -> {
                    updateTableStatus(sno, "in_process", dbUrl, dbUser, dbPass, config_table);

                    if (tableFiles.isEmpty()) {
                        enqueueLog("INFO", "TABLE_NO_FILES",
                                "Table: " + tableName + " (SNO: " + sno + ") - No files found for processing");
                        updateTableStatus(sno, "init", dbUrl, dbUser, dbPass, config_table);
                        return;
                    }

                    enqueueLog("INFO", "TABLE_PROCESS_START",
                            "Table: " + tableName + " (SNO: " + sno + ") - Starting processing with " + tableFiles.size() + " files");

                    boolean allSuccess = true;
                    int processedCount = 0;

                    for (Path file : tableFiles) {
                        enqueueLog("INFO", "FILE_PROCESS_START",
                                "Table: " + tableName + " - Processing file " + (processedCount + 1) + "/" + tableFiles.size()
                                        + ": " + file.getFileName());

                        boolean success = process(file.toString(), headerFlag, delimiter,
                                dbUrl, dbUser, dbPass,
                                pdbHost, pdbPort, pdbName, pdbUser, pdbPass,
                                config_table, bulkThreshold);

                        if (success) {
                            processedCount++;
                            moveFile(file, Paths.get(processedDirPath), true);
                            enqueueLog("INFO", "FILE_PROCESS_SUCCESS",
                                    "Table: " + tableName + " - Successfully processed file: " + file.getFileName()
                                            + " (" + processedCount + "/" + tableFiles.size() + ")");
                        } else {
                            allSuccess = false;
                            enqueueLog("MAJOR", "TABLE_PARSE_ERROR",
                                    "Table: " + tableName + " failed on file: " + file.getFileName()
                                            + " (Processed: " + processedCount + "/" + tableFiles.size() + ")");
                            break;
                        }
                    }

                    String finalStatus = allSuccess ? "init" : "error";
                    updateTableStatus(sno, finalStatus, dbUrl, dbUser, dbPass, config_table);

                    if (allSuccess) {
                        enqueueLog("INFO", "TABLE_PROCESS_COMPLETE",
                                "Table: " + tableName + " (SNO: " + sno + ") - Successfully processed all "
                                        + processedCount + " files -> Status: " + finalStatus);
                    } else {
                        enqueueLog("MAJOR", "TABLE_PROCESS_FAILED",
                                "Table: " + tableName + " (SNO: " + sno + ") - Failed after processing "
                                        + processedCount + "/" + tableFiles.size() + " files -> Status: " + finalStatus);
                    }
                }));
            }

            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (Exception e) {
                    enqueueLog("CRITICAL", "THREAD_EXEC_ERROR", e.getMessage());
                }
            }

            executor.shutdown();

        } catch (Exception e) {
            enqueueLog("CRITICAL", "DIRECTORY_PROCESS_ERROR", e.getMessage());
            e.printStackTrace();
        }
    }

    
    // ... (other existing methods remain the same until process method)
    
    private static double parseDoubleOrDefault(String raw, double def, String evtType, String label) {
        try {
            return Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            enqueueLog("CRITICAL", evtType,
                    "Invalid " + label + " threshold format: " + raw + ". Using default " + def);
            return def;
        }
    }


    public static boolean process(String csvFile, String headerFlag, String delimiter,String dbUrl,String dbUser,String dbPass,
                                  String pdbHost, String pdbPort, String pdbName, String pdbUser, String pdbPass,String config_table, int bulkNumber) {

        boolean header = headerFlag.equalsIgnoreCase("yes");
        String resolvedTable = resolveTableFromCsvName(csvFile,dbUrl,dbUser,dbPass,config_table);

        if (resolvedTable == null || resolvedTable.isEmpty()) {
            enqueueLog("CRITICAL", "TABLE_RESOLVE_FAIL", "CSV_File:" + csvFile + "# Status:InvalidNaming or Schedule Mapping Missing");
            return false;
        }

        enqueueLog("INFO", "P_START", "CSV_File:" + csvFile + "# ResolvedTable:" + resolvedTable + "# Host:" + pdbHost);

        if (!Files.exists(Paths.get(csvFile))) {
            enqueueLog("CRITICAL", "CSV_NOT_FOUND", "CSV_File:" + csvFile + "# Status:NotFound");
            enqueueLog("INFO", "P_END", "CSV_File:" + csvFile + "# Status:Terminated");
            return false;
        }

        try {
            // Count records in the file
            long recordCount = countRecordsInFile(csvFile, header);
            enqueueLog("INFO", "RECORD_COUNT", "CSV_File:" + csvFile + "# RecordCount:" + recordCount + "# BulkThreshold:" + bulkNumber);

            if (!tableExists(pdbHost, pdbPort, pdbName, pdbUser, pdbPass, resolvedTable)) {
                enqueueLog("MAJOR", "TABLE_CREATION", "Table:" + resolvedTable + "# Action:Auto-Create");
                createTableFromCSV(resolvedTable, csvFile, delimiter, pdbHost, pdbPort, pdbName, pdbUser, pdbPass);
                enqueueLog("MAJOR", "TABLE_CREATED", "Table:" + resolvedTable + "# Status:Created");
            }

            // Process based on record count
            if (recordCount > bulkNumber) {
                enqueueLog("INFO", "BATCH_PROCESSING_START", "CSV_File:" + csvFile + "# Table:" + resolvedTable + "# RecordCount:" + recordCount + "# BatchSize:" + BATCH_SIZE);
                processCsvInBatches(csvFile, delimiter, header, resolvedTable, pdbHost, pdbPort, pdbName, pdbUser, pdbPass, recordCount);
                enqueueLog("INFO", "BATCH_PROCESSING_END", "CSV_File:" + csvFile + "# BatchProcessing:Success");
            } else {
                enqueueLog("INFO", "DIRECT_IMPORT_START", "CSV_File:" + csvFile + "# Table:" + resolvedTable + "# Header:" + header);
                importCSVUsingPsql(csvFile, delimiter, header, resolvedTable, pdbHost, pdbPort, pdbName, pdbUser, pdbPass);
                enqueueLog("INFO", "DIRECT_IMPORT_END", "CSV_File:" + csvFile + "# DirectImport:Success");
            }

        } catch (Exception e) {
            enqueueLog("CRITICAL", "ERR_IMPORT_FAILED", "Exception:" + safeMessage(e));
            e.printStackTrace();
            return false;
        }

        enqueueLog("INFO", "P_END", "Process:Complete# File:" + csvFile);
        return true;
    }

    private static long countRecordsInFile(String csvFile, boolean hasHeader) throws IOException {
        long count = Files.lines(Paths.get(csvFile)).count();
        return hasHeader ? count - 1 : count; // Subtract 1 if header exists
    }

    private static void processCsvInBatches(String csvFile, String delimiter, boolean header, String tableName,
                                          String host, String port, String dbName, String user, String pass, long totalRecords) 
                                          throws IOException, InterruptedException {
        
        Path originalFile = Paths.get(csvFile);
        String tempDir = originalFile.getParent().toString() + File.separator + "temp_batches_" + System.currentTimeMillis();
        Files.createDirectories(Paths.get(tempDir));
        
        try {
            List<String> allLines = Files.readAllLines(originalFile);
            String headerLine = header ? allLines.get(0) : null;
            List<String> dataLines = header ? allLines.subList(1, allLines.size()) : allLines;
            
            int totalBatches = (int) Math.ceil((double) dataLines.size() / BATCH_SIZE);
            enqueueLog("INFO", "BATCH_INFO", "TotalRecords:" + totalRecords + "# TotalBatches:" + totalBatches + "# BatchSize:" + BATCH_SIZE);
            
            for (int batchNum = 0; batchNum < totalBatches; batchNum++) {
                int startIndex = batchNum * BATCH_SIZE;
                int endIndex = Math.min(startIndex + BATCH_SIZE, dataLines.size());
                
                String batchFileName = tempDir + File.separator + "batch_" + (batchNum + 1) + ".csv";
                
                try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(batchFileName))) {
                    // Write header if it exists
                    if (header && headerLine != null) {
                        writer.write(headerLine);
                        writer.newLine();
                    }
                    
                    // Write batch data
                    for (int i = startIndex; i < endIndex; i++) {
                        writer.write(dataLines.get(i));
                        writer.newLine();
                    }
                }
                
                enqueueLog("INFO", "BATCH_IMPORT_START", "Batch:" + (batchNum + 1) + "/" + totalBatches + "# Records:" + (endIndex - startIndex) + "# File:" + batchFileName);
                
                // Import this batch
                importCSVUsingPsql(batchFileName, delimiter, header, tableName, host, port, dbName, user, pass);
                
                enqueueLog("INFO", "BATCH_IMPORT_SUCCESS", "Batch:" + (batchNum + 1) + "/" + totalBatches + "# ImportStatus:Success");
                
                // Clean up batch file
                Files.deleteIfExists(Paths.get(batchFileName));
            }
            
        } finally {
            // Clean up temp directory
            try {
                Files.deleteIfExists(Paths.get(tempDir));
            } catch (IOException e) {
                enqueueLog("MINOR", "TEMP_CLEANUP_FAIL", "Failed to cleanup temp directory: " + tempDir + "# Error:" + safeMessage(e));
            }
        }
    }

    // ... (rest of the existing methods remain the same)

    private static boolean checkSystemResources(double maxCpuThreshold, double maxMemoryThreshold) {
        try {
            // Prefer com.sun.management bean if available
            com.sun.management.OperatingSystemMXBean sunOsBean =
                    ManagementFactory.getPlatformMXBean(com.sun.management.OperatingSystemMXBean.class);

            double cpuUsage = -1.0;
            if (sunOsBean != null) {
                // Returns [0.0,1.0] or negative if not available
                cpuUsage = sunOsBean.getSystemCpuLoad() * 100.0;
                if (cpuUsage < 0 && sunOsBean.getProcessCpuLoad() >= 0) {
                    cpuUsage = sunOsBean.getProcessCpuLoad() * 100.0;
                }
            }

            if (cpuUsage < 0) {
                // Fallback: approximate using load average / cores
                double load = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();
                int cores = Runtime.getRuntime().availableProcessors();
                if (load >= 0 && cores > 0) {
                    cpuUsage = Math.min(100.0, (load / cores) * 100.0);
                } else {
                    cpuUsage = 0.0; // Unknown => assume fine
                }
            }

            // Heap memory utilization (JVM)
            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            long usedMemory = memoryBean.getHeapMemoryUsage().getUsed();
            long maxMemory = memoryBean.getHeapMemoryUsage().getMax();
            double memoryUsage = maxMemory > 0 ? (double) usedMemory / maxMemory * 100.0 : 0.0;

            enqueueLog("INFO", "RESOURCE_CHECK",
                    String.format("Current CPU: %.2f%%, Memory: %.2f%% (Thresholds: CPU=%.1f%%, Memory=%.1f%%)",
                            cpuUsage, memoryUsage, maxCpuThreshold, maxMemoryThreshold));

            if (cpuUsage >= maxCpuThreshold) {
                enqueueLog("CRITICAL", "CPU_THRESHOLD_EXCEEDED",
                        String.format("CPU utilization %.2f%% exceeds threshold %.1f%%", cpuUsage, maxCpuThreshold));
                return false;
            }

            if (memoryUsage >= maxMemoryThreshold) {
                enqueueLog("CRITICAL", "MEMORY_THRESHOLD_EXCEEDED",
                        String.format("Memory utilization %.2f%% exceeds threshold %.1f%%", memoryUsage, maxMemoryThreshold));
                return false;
            }

            return true;

        } catch (Exception e) {
            enqueueLog("MAJOR", "RESOURCE_CHECK_ERROR", "Failed to check system resources: " + safeMessage(e));
            // If we can't reliably check resources, proceed.
            return true;
        }
    }

    private static void updateTableStatus(String sno, String status, String dbUrl, String dbUser, String dbPass, String config_table) {
        String Query="UPDATE " + config_table + " SET sync_status = ? WHERE sno = ?";
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
             PreparedStatement ps = conn.prepareStatement(Query)) {
        	
            ps.setString(1, status);
            ps.setString(2, sno);
            enqueueLog("INFO","Updation_Query","Query:"+ps);
            ps.executeUpdate();
        } catch (Exception e) {
            enqueueLog("MAJOR", "STATUS_UPDATE_FAIL", "SNO: " + sno + " Status: " + status + " Error: " + safeMessage(e));
        }
    }

    private static void moveFile(Path file, Path processedDir, boolean success) {
        try {
            if (!success) return;
            Files.createDirectories(processedDir);
            Files.move(file, processedDir.resolve(file.getFileName()), StandardCopyOption.REPLACE_EXISTING);
            enqueueLog("INFO", "FILE_MOVED", file.getFileName() + " -> " + processedDir);
        } catch (IOException e) {
            enqueueLog("MAJOR", "FILE_MOVE_ERROR", safeMessage(e));
        }
    }

    private static String resolveTableFromCsvName(String csvFile,String dbUrl,String dbUser,String dbPass,String config_table) {
        try (Connection conn = DriverManager.getConnection(dbUrl,dbUser,dbPass)) {
            String fileName = Paths.get(csvFile).getFileName().toString();
            String[] parts = fileName.split("_");
            if (parts.length < 5) return null;
            String id = parts[4];
            String sql = "SELECT table_name FROM " + config_table + " WHERE sno = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, id);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
        } catch (Exception e) {
            enqueueLog("MAJOR", "SCHEDULE_DB_ERROR", "Failed to resolve table name from schedule DB: " + safeMessage(e));
        }
        return null;
    }

    private static boolean tableExists(String host, String port, String dbName, String user, String pass, String tableName) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("psql", "-h", host, "-p", port, "-U", user, "-d", dbName,
                "-tAc", "SELECT 1 FROM information_schema.tables WHERE table_name = '" + tableName + "'");
        pb.environment().put("PGPASSWORD", pass);
        Process process = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = reader.readLine();
        process.waitFor();
        return line != null && line.trim().equals("1");
    }

    private static void createTableFromCSV(String tableName, String csvFile, String delimiter, String host, String port, String dbName, String user, String pass) throws IOException, InterruptedException {
        String headerLine = Files.lines(Paths.get(csvFile)).findFirst().orElseThrow(() -> new IOException("Empty CSV file: " + csvFile));
        String[] columns = headerLine.split(Pattern.quote(delimiter), -1);
        StringBuilder sql = new StringBuilder("CREATE TABLE \"" + tableName + "\" (");
        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i].trim()
                    .replaceAll("[^a-zA-Z0-9_]", "_")
                    .replaceAll("^_+", "")
                    .replaceAll("_+$", "");
            if (columnName.isEmpty()) columnName = "col_" + i;
            sql.append("\"").append(columnName).append("\" text");
            if (i != columns.length - 1) sql.append(", ");
        }
        sql.append(");");

        ProcessBuilder pb = new ProcessBuilder("psql", "-h", host, "-p", port, "-U", user, "-d", dbName, "-c", sql.toString());
        pb.environment().put("PGPASSWORD", pass);
        pb.inheritIO();
        Process p = pb.start();
        p.waitFor();
    }

    private static void importCSVUsingPsql(String csvFile, String delimiter, boolean header, String tableName,
                                           String host, String port, String dbName, String user, String pass) throws IOException, InterruptedException {
        String copyCmd = String.format(
                "COPY \"%s\" FROM '%s' WITH (FORMAT csv, DELIMITER '%s'%s);",
                tableName, csvFile.replace("\\", "\\\\"), delimiter, header ? ", HEADER" : "");

        ProcessBuilder pb = new ProcessBuilder("psql", "-h", host, "-p", port, "-U", user, "-d", dbName, "-c", copyCmd);
        pb.environment().put("PGPASSWORD", pass);
        pb.inheritIO();
        Process p = pb.start();
        int code = p.waitFor();
        if (code != 0) {
            throw new IOException("psql COPY command failed with exit code: " + code);
        }
    }

    private static void enqueueLog(String logLevel, String evtType, String description) {
        String timestamp = LocalDateTime.now().format(LOG_FORMAT);
        LogRecord lr = new LogRecord(timestamp, PROCESS_NAME, PROCESS_VERSION, logLevel, evtType, description);
        LOG_QUEUE.add(lr);
        System.out.println(String.format("Time:%s [Debug] Process_Name: %s Server_IP:127.0.0.1 Process_Version: %s Log_Level: %s EVT_Log EVT_Type:%s Description: %s",
                timestamp, PROCESS_NAME, PROCESS_VERSION, logLevel, evtType, description));
    }

    private static void flushLogsToStdOut() {
        for (LogRecord r : LOG_QUEUE) {
            System.out.println(String.format("QUEUED_LOG: %s | %s | %s | %s | %s | %s", r.timestamp,r.processName, r.logLevel, r.evtType, r.processVersion, r.description));
        }
    }

    private static String safeMessage(Throwable t) {
        if (t == null) return "";
        String m = t.getMessage();
        if (m == null) m = t.toString();
        return m.replace("\n", " ").replace("\r", " ");
    }
}
