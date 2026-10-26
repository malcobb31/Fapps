package postgre_ip_v4;

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

public class ProcessData_v4 {
    private static final String PROCESS_NAME = "CSV_TO_DB";
    private static final String PROCESS_VERSION = "1.3.0";
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

    public static void processAllInDirectory(String dirPath, String processedDirPath, String headerFlag, 
            String delimiter, String dbUrl, String dbUser, String dbPass, String pdbHost, String pdbPort, 
            String pdbName, String pdbUser, String pdbPass, String numTablesToPickStr, String config_table, 
            String MAX_CPU_UTILIZATION, String MAX_MEMORY_UTILIZATION, String bulkNumberStr, String dbTypeStr) {
        
        // Parse database type using if-else
        String dbType;
        if (dbTypeStr.equalsIgnoreCase("psql")) {
            dbType = "psql";
        } else if (dbTypeStr.equalsIgnoreCase("sql")) {
            dbType = "sql";
        } else {
            enqueueLog("CRITICAL", "INVALID_DB_TYPE", "Invalid database type: " + dbTypeStr + ". Must be 'psql' or 'sql'. Using default 'psql'");
            dbType = "psql";
        }
        
        enqueueLog("INFO", "DB_TYPE_SELECTED", "Database type: " + dbType);

        // Parse bulk threshold
        final int bulkThreshold;
        int parsedBulk;
        try {
            parsedBulk = Integer.parseInt(bulkNumberStr);
        } catch (NumberFormatException e) {
            enqueueLog("CRITICAL", "INVALID_BULK_NUMBER", "Invalid bulk number format: " + bulkNumberStr + ". Using default 10000");
            parsedBulk = 10000;
        }
        bulkThreshold = parsedBulk; // assigned exactly once


        // Parse CPU/memory thresholds
        double maxCpuThreshold;
        try {
            maxCpuThreshold = Double.parseDouble(MAX_CPU_UTILIZATION);
        } catch (NumberFormatException e) {
            enqueueLog("CRITICAL", "INVALID_CPU_THRESHOLD", "Invalid CPU threshold format: " + MAX_CPU_UTILIZATION + ". Using default 80.0");
            maxCpuThreshold = 80.0;
        }

        double maxMemoryThreshold;
        try {
            maxMemoryThreshold = Double.parseDouble(MAX_MEMORY_UTILIZATION);
        } catch (NumberFormatException e) {
            enqueueLog("CRITICAL", "INVALID_MEMORY_THRESHOLD", "Invalid Memory threshold format: " + MAX_MEMORY_UTILIZATION + ". Using default 80.0");
            maxMemoryThreshold = 80.0;
        }

        // Resource check
        if (!checkSystemResources(maxCpuThreshold, maxMemoryThreshold)) {
            enqueueLog("CRITICAL", "RESOURCE_LIMIT_EXCEEDED", "System resources exceeded threshold. CPU or Memory utilization too high.");
            flushLogsToStdOut();
            return;
        }

        int numTablesToPick;
        try {
            numTablesToPick = Integer.parseInt(numTablesToPickStr);
        } catch (NumberFormatException e) {
            enqueueLog("CRITICAL", "INVALID_TABLE_COUNT", "Invalid number format for tables to pick: " + numTablesToPickStr);
            flushLogsToStdOut();
            return;
        }

        try {
            // Fetch tables
            List<String[]> readyTables = new ArrayList<>();
            try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
                 PreparedStatement ps = conn.prepareStatement("SELECT sno, table_name FROM " + config_table + " WHERE sync_status = 'ready_to_import' LIMIT ?")) {
                
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

            // Load files
            List<Path> allFiles;
            try (java.util.stream.Stream<Path> s = Files.list(Paths.get(dirPath))) {
                allFiles = s.filter(p -> {
                    String fname = p.getFileName().toString();
                    return (fname.startsWith("raw_") || fname.startsWith("process_")) && fname.endsWith(".csv");
                }).collect(Collectors.toList());
            }

            if (allFiles.isEmpty()) {
                enqueueLog("INFO", "NO_FILES", "No files to process in directory: " + dirPath);
                return;
            }

            // Group files per table
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
                            enqueueLog("MAJOR", "FILENAME_TS_PARSE_FAIL", "File: " + fname + " Error: " + safeMessage(e));
                        }
                        return LocalDateTime.now();
                    }))
                    .collect(Collectors.toList());
                tableFilesMap.put(sno, matchedFiles);
            }

            // Process in parallel
            ExecutorService executor = Executors.newFixedThreadPool(Math.min(numTablesToPick, DEFAULT_THREAD_COUNT));
            List<Future<?>> futures = new ArrayList<>();

            for (String[] tableEntry : readyTables) {
                String sno = tableEntry[0];
                String tableName = tableEntry[1];
                List<Path> tableFiles = tableFilesMap.getOrDefault(sno, Collections.emptyList());

                futures.add(executor.submit(() -> {
                    updateTableStatus(sno, "in_process", dbUrl, dbUser, dbPass, config_table);

                    if (tableFiles.isEmpty()) {
                        enqueueLog("INFO", "TABLE_NO_FILES", "Table: " + tableName + " (SNO: " + sno + ") - No files found for processing");
                        updateTableStatus(sno, "init", dbUrl, dbUser, dbPass, config_table);
                        return;
                    }

                    enqueueLog("INFO", "TABLE_PROCESS_START", "Table: " + tableName + " (SNO: " + sno + ") - Starting processing with " + tableFiles.size() + " files");

                    boolean allSuccess = true;
                    int processedCount = 0;

                    for (Path file : tableFiles) {
                        enqueueLog("INFO", "FILE_PROCESS_START", "Table: " + tableName + " - Processing file " + (processedCount + 1) + "/" + tableFiles.size() + ": " + file.getFileName());
                        
                        boolean success = process(file.toString(), headerFlag, delimiter, dbUrl, dbUser, dbPass, 
                            pdbHost, pdbPort, pdbName, pdbUser, pdbPass, config_table, bulkThreshold, dbType);

                        if (success) {
                            processedCount++;
                            moveFile(file, Paths.get(processedDirPath), true);
                            enqueueLog("INFO", "FILE_PROCESS_SUCCESS", "Table: " + tableName + " - Successfully processed file: " + file.getFileName() + " (" + processedCount + "/" + tableFiles.size() + ")");
                        } else {
                            allSuccess = false;
                            enqueueLog("MAJOR", "TABLE_PARSE_ERROR", "Table: " + tableName + " failed on file: " + file.getFileName() + " (Processed: " + processedCount + "/" + tableFiles.size() + ")");
                            break;
                        }
                    }

                    String finalStatus;
                    if (allSuccess) {
                        finalStatus = "init";
                    } else {
                        finalStatus = "error";
                    }

                    updateTableStatus(sno, finalStatus, dbUrl, dbUser, dbPass, config_table);

                    if (allSuccess) {
                        enqueueLog("INFO", "TABLE_PROCESS_COMPLETE", "Table: " + tableName + " (SNO: " + sno + ") - Successfully processed all " + processedCount + " files -> Status: " + finalStatus);
                    } else {
                        enqueueLog("MAJOR", "TABLE_PROCESS_FAILED", "Table: " + tableName + " (SNO: " + sno + ") - Failed after processing " + processedCount + "/" + tableFiles.size() + " files -> Status: " + finalStatus);
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
        } finally {
            flushLogsToStdOut();
        }
    }

    public static boolean process(String csvFile, String headerFlag, String delimiter, String dbUrl, 
            String dbUser, String dbPass, String pdbHost, String pdbPort, String pdbName, 
            String pdbUser, String pdbPass, String config_table, int bulkNumber, String dbType) {
        
        boolean header = headerFlag.equalsIgnoreCase("yes");

        String resolvedTable = resolveTableFromCsvName(csvFile, dbUrl, dbUser, dbPass, config_table);
        if (resolvedTable == null || resolvedTable.isEmpty()) {
            enqueueLog("CRITICAL", "TABLE_RESOLVE_FAIL", "CSV_File:" + csvFile + "# Status:InvalidNaming or Schedule Mapping Missing");
            return false;
        }

        enqueueLog("INFO", "P_START", "CSV_File:" + csvFile + "# ResolvedTable:" + resolvedTable + "# Host:" + pdbHost + "# DBType:" + dbType);

        if (!Files.exists(Paths.get(csvFile))) {
            enqueueLog("CRITICAL", "CSV_NOT_FOUND", "CSV_File:" + csvFile + "# Status:NotFound");
            enqueueLog("INFO", "P_END", "CSV_File:" + csvFile + "# Status:Terminated");
            return false;
        }

        try {
            // Count records in the file
            long recordCount = countRecordsInFile(csvFile, header);
            enqueueLog("INFO", "RECORD_COUNT", "CSV_File:" + csvFile + "# RecordCount:" + recordCount + "# BulkThreshold:" + bulkNumber);

            // Check if table exists based on database type
            if (!tableExists(pdbHost, pdbPort, pdbName, pdbUser, pdbPass, resolvedTable, dbType)) {
                enqueueLog("MAJOR", "TABLE_CREATION", "Table:" + resolvedTable + "# Action:Auto-Create");
                createTableFromCSV(resolvedTable, csvFile, delimiter, pdbHost, pdbPort, pdbName, pdbUser, pdbPass, dbType);
                enqueueLog("MAJOR", "TABLE_CREATED", "Table:" + resolvedTable + "# Status:Created");
            }

            // Process based on record count
            if (recordCount > bulkNumber) {
                enqueueLog("INFO", "BATCH_PROCESSING_START", "CSV_File:" + csvFile + "# Table:" + resolvedTable + "# RecordCount:" + recordCount + "# BatchSize:" + BATCH_SIZE);
                processCsvInBatches(csvFile, delimiter, header, resolvedTable, pdbHost, pdbPort, pdbName, pdbUser, pdbPass, recordCount, dbType);
                enqueueLog("INFO", "BATCH_PROCESSING_END", "CSV_File:" + csvFile + "# BatchProcessing:Success");
            } else {
                enqueueLog("INFO", "DIRECT_IMPORT_START", "CSV_File:" + csvFile + "# Table:" + resolvedTable + "# Header:" + header);
                importCSV(csvFile, delimiter, header, resolvedTable, pdbHost, pdbPort, pdbName, pdbUser, pdbPass, dbType);
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
        if (hasHeader) {
            return count - 1;
        } else {
            return count;
        }
    }

    private static void processCsvInBatches(String csvFile, String delimiter, boolean header, String tableName, 
            String host, String port, String dbName, String user, String pass, long totalRecords, String dbType) 
            throws IOException, InterruptedException, SQLException {
        
        Path originalFile = Paths.get(csvFile);
        String tempDir = originalFile.getParent().toString() + File.separator + "temp_batches_" + System.currentTimeMillis();
        Files.createDirectories(Paths.get(tempDir));

        try {
            List<String> allLines = Files.readAllLines(originalFile);
            String headerLine = null;
            List<String> dataLines;
            
            if (header) {
                headerLine = allLines.get(0);
                dataLines = allLines.subList(1, allLines.size());
            } else {
                dataLines = allLines;
            }

            int totalBatches = (int) Math.ceil((double) dataLines.size() / BATCH_SIZE);
            enqueueLog("INFO", "BATCH_INFO", "TotalRecords:" + totalRecords + "# TotalBatches:" + totalBatches + "# BatchSize:" + BATCH_SIZE);

            for (int batchNum = 0; batchNum < totalBatches; batchNum++) {
                int startIndex = batchNum * BATCH_SIZE;
                int endIndex = Math.min(startIndex + BATCH_SIZE, dataLines.size());
                String batchFileName = tempDir + File.separator + "batch_" + (batchNum + 1) + ".csv";

                try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(batchFileName))) {
                    if (header && headerLine != null) {
                        writer.write(headerLine);
                        writer.newLine();
                    }

                    for (int i = startIndex; i < endIndex; i++) {
                        writer.write(dataLines.get(i));
                        writer.newLine();
                    }
                }

                enqueueLog("INFO", "BATCH_IMPORT_START", "Batch:" + (batchNum + 1) + "/" + totalBatches + "# Records:" + (endIndex - startIndex) + "# File:" + batchFileName);
                importCSV(batchFileName, delimiter, header, tableName, host, port, dbName, user, pass, dbType);
                enqueueLog("INFO", "BATCH_IMPORT_SUCCESS", "Batch:" + (batchNum + 1) + "/" + totalBatches + "# ImportStatus:Success");

                Files.deleteIfExists(Paths.get(batchFileName));
            }
        } finally {
            try {
                Files.deleteIfExists(Paths.get(tempDir));
            } catch (IOException e) {
                enqueueLog("MINOR", "TEMP_CLEANUP_FAIL", "Failed to cleanup temp directory: " + tempDir + "# Error:" + safeMessage(e));
            }
        }
    }

    private static void importCSV(String csvFile, String delimiter, boolean header, String tableName, 
            String host, String port, String dbName, String user, String pass, String dbType) 
            throws IOException, InterruptedException, SQLException {
        
        if (dbType.equals("psql")) {
            importCSVUsingPsql(csvFile, delimiter, header, tableName, host, port, dbName, user, pass);
        } else if (dbType.equals("sql")) {
            importCSVUsingSql(csvFile, delimiter, header, tableName, host, port, dbName, user, pass);
        } else {
            throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
    }

    private static void importCSVUsingPsql(String csvFile, String delimiter, boolean header, String tableName, 
            String host, String port, String dbName, String user, String pass) throws IOException, InterruptedException {
        
        String copyCommand;
        if (header) {
            copyCommand = String.format("\\COPY %s FROM '%s' WITH (FORMAT csv, DELIMITER '%s', HEADER true)", 
                tableName, csvFile, delimiter);
        } else {
            copyCommand = String.format("\\COPY %s FROM '%s' WITH (FORMAT csv, DELIMITER '%s')", 
                tableName, csvFile, delimiter);
        }

        ProcessBuilder pb = new ProcessBuilder("psql", "-h", host, "-p", port, "-U", user, "-d", dbName, "-c", copyCommand);
        pb.environment().put("PGPASSWORD", pass);
        pb.redirectErrorStream(true);

        Process process = pb.start();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            StringBuilder output = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("PSQL import failed with exit code " + exitCode + ": " + output.toString());
            }
        }
    }

    private static void importCSVUsingSql(String csvFile, String delimiter, boolean header, String tableName, 
            String host, String port, String dbName, String user, String pass) throws IOException, SQLException {
        
        String jdbcUrl = String.format("jdbc:sqlserver://%s:%s;databaseName=%s", host, port, dbName);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, pass)) {
            List<String> lines = Files.readAllLines(Paths.get(csvFile));
            if (lines.isEmpty()) {
                return;
            }

            int startIndex;
            if (header) {
                startIndex = 1;
            } else {
                startIndex = 0;
            }

            if (startIndex >= lines.size()) {
                return;
            }

            // Get column count from first data row
            String[] firstRow = lines.get(startIndex).split(Pattern.quote(delimiter));
            int columnCount = firstRow.length;

            // Create prepared statement with placeholders
            StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" VALUES (");
            for (int i = 0; i < columnCount; i++) {
                if (i > 0) sql.append(", ");
                sql.append("?");
            }
            sql.append(")");

            try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                conn.setAutoCommit(false);

                for (int i = startIndex; i < lines.size(); i++) {
                    String[] values = lines.get(i).split(Pattern.quote(delimiter), -1);

                    for (int j = 0; j < Math.min(values.length, columnCount); j++) {
                        String value = values[j].trim();
                        if (value.isEmpty() || value.equalsIgnoreCase("null")) {
                            pstmt.setNull(j + 1, Types.VARCHAR);
                        } else {
                            pstmt.setString(j + 1, value);
                        }
                    }

                    // Fill remaining columns with null if row has fewer columns
                    for (int j = values.length; j < columnCount; j++) {
                        pstmt.setNull(j + 1, Types.VARCHAR);
                    }

                    pstmt.addBatch();

                    if ((i - startIndex + 1) % 1000 == 0) {
                        pstmt.executeBatch();
                        conn.commit();
                    }
                }

                pstmt.executeBatch();
                conn.commit();
            }
        }
    }

    private static boolean tableExists(String host, String port, String dbName, String user, String pass, 
            String tableName, String dbType) throws IOException, InterruptedException, SQLException {
        
        if (dbType.equals("psql")) {
            return tableExistsPostgres(host, port, dbName, user, pass, tableName);
        } else if (dbType.equals("sql")) {
            return tableExistsSqlServer(host, port, dbName, user, pass, tableName);
        } else {
            throw new IllegalArgumentException("Unsupported database type: " + dbType);
        }
    }

    private static boolean tableExistsPostgres(String host, String port, String dbName, String user, String pass, String tableName) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("psql", "-h", host, "-p", port, "-U", user, "-d", dbName, 
            "-c", "SELECT to_regclass('" + tableName + "');");
        pb.environment().put("PGPASSWORD", pass);
        pb.redirectErrorStream(true);

        Process process = pb.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().equals(tableName)) {
                    return true;
                }
            }
        }
        process.waitFor();
        return false;
    }

    private static boolean tableExistsSqlServer(String host, String port, String dbName, String user, String pass, String tableName) throws SQLException {
        String jdbcUrl = String.format("jdbc:sqlserver://%s:%s;databaseName=%s", host, port, dbName);
        
        try (Connection conn = DriverManager.getConnection(jdbcUrl, user, pass);
             PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?")) {
            
            pstmt.setString(1, tableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        }
        return false;
    }

   

  
private static String resolveTableFromCsvName(String csvFile, String dbUrl, String dbUser, String dbPass, String configTable) {
    try {
        // Extract filename from path
        String fileName = Paths.get(csvFile).getFileName().toString();
        
        // Parse filename to get SNO (assuming format: prefix_something_something_something_SNO_timestamp.csv)
        String[] parts = fileName.split("_");
        if (parts.length < 5) {
            enqueueLog("MAJOR", "FILENAME_PARSE_ERROR", "Invalid filename format: " + fileName);
            return null;
        }
        
        String sno = parts[4]; // Assuming SNO is at index 4
        
        // Query database to get table name
        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
             PreparedStatement ps = conn.prepareStatement("SELECT table_name FROM " + configTable + " WHERE sno = ?")) {
            
            ps.setString(1, sno);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    String tableName = rs.getString("table_name");
                    enqueueLog("INFO", "TABLE_RESOLVED", "SNO: " + sno + " -> Table: " + tableName);
                    return tableName;
                } else {
                    enqueueLog("MAJOR", "TABLE_NOT_FOUND", "No table found for SNO: " + sno);
                    return null;
                }
            }
        }
    } catch (Exception e) {
        enqueueLog("CRITICAL", "TABLE_RESOLVE_ERROR", "Error resolving table: " + safeMessage(e));
        return null;
    }
}

private static void updateTableStatus(String sno, String status, String dbUrl, String dbUser, String dbPass, String configTable) {
    try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
         PreparedStatement ps = conn.prepareStatement("UPDATE " + configTable + " SET sync_status = ? WHERE sno = ?")) {
        
        ps.setString(1, status);
        ps.setString(2, sno);
        int rowsUpdated = ps.executeUpdate();
        
        if (rowsUpdated > 0) {
            enqueueLog("INFO", "STATUS_UPDATED", "SNO: " + sno + " -> Status: " + status);
        } else {
            enqueueLog("MAJOR", "STATUS_UPDATE_FAILED", "No rows updated for SNO: " + sno);
        }
        
    } catch (Exception e) {
        enqueueLog("CRITICAL", "STATUS_UPDATE_ERROR", "Error updating status for SNO: " + sno + " Error: " + safeMessage(e));
    }
}

private static void moveFile(Path source, Path targetDir, boolean success) {
    try {
        // Create target directory if it doesn't exist
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }
        
        String fileName = source.getFileName().toString();
        Path targetPath;
        
        if (success) {
            // Move to processed directory
            targetPath = targetDir.resolve(fileName);
        } else {
            // Move to error directory (create error subdirectory)
            Path errorDir = targetDir.resolve("error");
            if (!Files.exists(errorDir)) {
                Files.createDirectories(errorDir);
            }
            targetPath = errorDir.resolve(fileName);
        }
        
        // If file already exists, add timestamp
        if (Files.exists(targetPath)) {
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String nameWithoutExt = fileName.substring(0, fileName.lastIndexOf('.'));
            String extension = fileName.substring(fileName.lastIndexOf('.'));
            String newFileName = nameWithoutExt + "_" + timestamp + extension;
            targetPath = targetPath.getParent().resolve(newFileName);
        }
        
        Files.move(source, targetPath, StandardCopyOption.REPLACE_EXISTING);
        
        if (success) {
            enqueueLog("INFO", "FILE_MOVED", "File moved to processed: " + targetPath);
        } else {
            enqueueLog("INFO", "FILE_MOVED_ERROR", "File moved to error: " + targetPath);
        }
        
    } catch (Exception e) {
        enqueueLog("CRITICAL", "FILE_MOVE_ERROR", "Failed to move file: " + source + " Error: " + safeMessage(e));
    }
}

private static void createTableFromCSV(String tableName, String csvFile, String delimiter, 
        String host, String port, String dbName, String user, String pass, String dbType) {
    
    try {
        // Read first few lines to determine column structure
        List<String> lines = Files.readAllLines(Paths.get(csvFile));
        if (lines.isEmpty()) {
            enqueueLog("CRITICAL", "EMPTY_CSV", "CSV file is empty: " + csvFile);
            return;
        }
        
        // Assume first line is header or data
        String[] columns = lines.get(0).split(Pattern.quote(delimiter));
        
        if (dbType.equals("psql")) {
            createTablePostgres(tableName, columns, host, port, dbName, user, pass);
        } else if (dbType.equals("sql")) {
            createTableSqlServer(tableName, columns, host, port, dbName, user, pass);
        } else {
            enqueueLog("CRITICAL", "UNSUPPORTED_DB_TYPE", "Unsupported database type for table creation: " + dbType);
        }
        
    } catch (Exception e) {
        enqueueLog("CRITICAL", "TABLE_CREATE_ERROR", "Error creating table: " + tableName + " Error: " + safeMessage(e));
    }
}

private static void createTablePostgres(String tableName, String[] columns, String host, String port, 
        String dbName, String user, String pass) throws IOException, InterruptedException {
    
    StringBuilder createTableSQL = new StringBuilder();
    createTableSQL.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
    
    for (int i = 0; i < columns.length; i++) {
        if (i > 0) {
            createTableSQL.append(", ");
        }
        
        String columnName = sanitizeColumnName(columns[i]);
        createTableSQL.append(columnName).append(" TEXT"); // Default to TEXT for simplicity
    }
    
    createTableSQL.append(");");
    
    ProcessBuilder pb = new ProcessBuilder("psql", "-h", host, "-p", port, "-U", user, "-d", dbName, 
        "-c", createTableSQL.toString());
    pb.environment().put("PGPASSWORD", pass);
    pb.redirectErrorStream(true);
    
    Process process = pb.start();
    
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
        String line;
        StringBuilder output = new StringBuilder();
        while ((line = reader.readLine()) != null) {
            output.append(line).append("\n");
        }
        
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("PostgreSQL table creation failed with exit code " + exitCode + ": " + output.toString());
        } else {
            enqueueLog("INFO", "POSTGRES_TABLE_CREATED", "Table created successfully: " + tableName);
        }
    }
}

private static void createTableSqlServer(String tableName, String[] columns, String host, String port, 
        String dbName, String user, String pass) throws SQLException {
    
    String jdbcUrl = String.format("jdbc:sqlserver://%s:%s;databaseName=%s", host, port, dbName);
    
    StringBuilder createTableSQL = new StringBuilder();
    createTableSQL.append("IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='").append(tableName)
                 .append("' AND xtype='U') CREATE TABLE ").append(tableName).append(" (");
    
    for (int i = 0; i < columns.length; i++) {
        if (i > 0) {
            createTableSQL.append(", ");
        }
        
        String columnName = sanitizeColumnName(columns[i]);
        createTableSQL.append("[").append(columnName).append("] NVARCHAR(MAX)"); // Default to NVARCHAR(MAX) for simplicity
    }
    
    createTableSQL.append(");");
    
    try (Connection conn = DriverManager.getConnection(jdbcUrl, user, pass);
         Statement stmt = conn.createStatement()) {
        
        stmt.execute(createTableSQL.toString());
        enqueueLog("INFO", "SQLSERVER_TABLE_CREATED", "Table created successfully: " + tableName);
        
    }
}

private static String sanitizeColumnName(String columnName) {
    // Remove special characters and spaces, replace with underscores
    String sanitized = columnName.trim()
                               .replaceAll("[^a-zA-Z0-9_]", "_")
                               .replaceAll("_{2,}", "_") // Replace multiple underscores with single
                               .replaceAll("^_+|_+$", ""); // Remove leading/trailing underscores
    
    // Ensure it doesn't start with a number
    if (sanitized.matches("^[0-9].*")) {
        sanitized = "col_" + sanitized;
    }
    
    // If empty after sanitization, provide default
    if (sanitized.isEmpty()) {
        sanitized = "column_" + System.currentTimeMillis();
    }
    
    return sanitized;
}

private static boolean checkSystemResources(double maxCpuThreshold, double maxMemoryThreshold) {
    try {
        // Check memory usage
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        long usedMemory = memoryBean.getHeapMemoryUsage().getUsed();
        long maxMemory = memoryBean.getHeapMemoryUsage().getMax();
        
        double memoryUsagePercent = 0.0;
        if (maxMemory > 0) {
            memoryUsagePercent = (double) usedMemory / maxMemory * 100.0;
        }
        
        // Simple CPU check (this is a basic implementation)
        // For more accurate CPU monitoring, you might need additional libraries
        Runtime runtime = Runtime.getRuntime();
        int availableProcessors = runtime.availableProcessors();
        long freeMemory = runtime.freeMemory();
        long totalMemory = runtime.totalMemory();
        
        double systemMemoryUsage = (double) (totalMemory - freeMemory) / totalMemory * 100.0;
        
        // Log current resource usage
        enqueueLog("INFO", "RESOURCE_CHECK", 
            "Memory Usage: " + String.format("%.2f", memoryUsagePercent) + "% " +
            "System Memory Usage: " + String.format("%.2f", systemMemoryUsage) + "% " +
            "Available Processors: " + availableProcessors);
        
        // Check if resources are within limits
        if (memoryUsagePercent > maxMemoryThreshold) {
            enqueueLog("MAJOR", "MEMORY_THRESHOLD_EXCEEDED", 
                "Memory usage " + String.format("%.2f", memoryUsagePercent) + "% exceeds threshold " + maxMemoryThreshold + "%");
            return false;
        }
        
        if (systemMemoryUsage > maxMemoryThreshold) {
            enqueueLog("MAJOR", "SYSTEM_MEMORY_THRESHOLD_EXCEEDED", 
                "System memory usage " + String.format("%.2f", systemMemoryUsage) + "% exceeds threshold " + maxMemoryThreshold + "%");
            return false;
        }
        
        return true;
        
    } catch (Exception e) {
        enqueueLog("MAJOR", "RESOURCE_CHECK_ERROR", "Error checking system resources: " + safeMessage(e));
        return true; // Assume resources are OK if we can't check
    }
}

// Enhanced logging method with better formatting
private static void enqueueLog(String level, String type, String message) {
    String timestamp = LocalDateTime.now().format(LOG_FORMAT);
    LogRecord logRecord = new LogRecord(timestamp, PROCESS_NAME, PROCESS_VERSION, level, type, message);
    LOG_QUEUE.offer(logRecord);
    
    // Also print immediately for debugging (optional)
    if (level.equals("CRITICAL") || level.equals("MAJOR")) {
        System.err.println(String.format("[%s] %s - %s: %s", 
            timestamp, level, type, message));
    }
}

private static void flushLogsToStdOut() {
    LogRecord log;
    while ((log = LOG_QUEUE.poll()) != null) {
        String logLine = String.format("[%s] [%s] [%s v%s] %s - %s: %s", 
            log.timestamp, 
            log.logLevel, 
            log.processName, 
            log.processVersion,
            log.evtType, 
            log.logLevel,
            log.description);
        
        if (log.logLevel.equals("CRITICAL") || log.logLevel.equals("MAJOR")) {
            System.err.println(logLine);
        } else {
            System.out.println(logLine);
        }
    }
}

private static String safeMessage(Exception e) {
    if (e == null) {
        return "Unknown error";
    }
    
    String message = e.getMessage();
    if (message != null && !message.trim().isEmpty()) {
        return message.trim();
    } else {
        return e.getClass().getSimpleName();
    }
}

// Main method for testing
public static void main(String[] args) {
}}
