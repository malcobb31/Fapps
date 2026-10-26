package postgre_ip_v6;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.nio.file.*;
import java.sql.*;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.opencsv.CSVReader;

import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;

public class PostgreV6Methods {

	// Instance variables instead of static
	private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private final int defaultThreadCount = Math.max(2, Runtime.getRuntime().availableProcessors());
	private final String processName = "IP";
	private final String processVersion = "1.1.0";
	private final boolean debugEnabled = false;

	// Natural sorting comparator for files
	private final Comparator<Path> naturalFileComparator = (path1, path2) -> {
		String name1 = path1.getFileName().toString();
		String name2 = path2.getFileName().toString();
		return compareNaturally(name1, name2);
	};

	// Natural string comparison for proper file ordering
	private int compareNaturally(String str1, String str2) {
		int len1 = str1.length();
		int len2 = str2.length();
		int i = 0, j = 0;

		while (i < len1 && j < len2) {
			char c1 = str1.charAt(i);
			char c2 = str2.charAt(j);

			if (Character.isDigit(c1) && Character.isDigit(c2)) {
				// Extract numbers and compare numerically
				StringBuilder num1 = new StringBuilder();
				StringBuilder num2 = new StringBuilder();

				while (i < len1 && Character.isDigit(str1.charAt(i))) {
					num1.append(str1.charAt(i++));
				}
				while (j < len2 && Character.isDigit(str2.charAt(j))) {
					num2.append(str2.charAt(j++));
				}

				int numCompare = Integer.compare(Integer.parseInt(num1.toString()), Integer.parseInt(num2.toString()));
				if (numCompare != 0) {
					return numCompare;
				}
			} else {
				if (c1 != c2) {
					return Character.compare(c1, c2);
				}
				i++;
				j++;
			}
		}

		return Integer.compare(len1, len2);
	}

	// Instance-based logging
	private void log(String level, String message, String serverIP, String evtType) {
		if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled)
			return;

		String timestamp = LocalDateTime.now().format(logFormat);
		StringBuilder logBuilder = new StringBuilder(512);
		logBuilder.append("ALERT - ").append(timestamp).append(" --> ").append("Process_Name:").append(processName)
				.append(" ");

		if (serverIP != null && !serverIP.isEmpty()) {
			logBuilder.append("Server_IP: ").append(serverIP).append(" ");
		}

		logBuilder.append("Process_Version:").append(processVersion).append(" ").append("Log_Level:")
				.append(level.toUpperCase()).append(" ").append("EVT_Type:").append(evtType != null ? evtType : "NA")
				.append(" ").append("Description:").append(message != null ? message : "");

		String finalLog = logBuilder.toString().replaceAll("\\s+$", "");
		System.out.println(finalLog);
	}

	public class MemoryAndCPUUsages {

	    public String[] getMemoryAndCPUUsages() {

	        String[] result = null;

	        try {

	            String[] memCmd = { "/bin/sh", "-c", "free | awk 'FNR == 2 {print ($3/$2)*100}'" };
	            Process memProcess = Runtime.getRuntime().exec(memCmd);

	            BufferedReader memBr =
	                    new BufferedReader(new InputStreamReader(memProcess.getInputStream()));

	            String str;
	            double currentMemory = 0;

	            while ((str = memBr.readLine()) != null) {
	                currentMemory = Double.parseDouble(str.trim());
	            }

	            memProcess.waitFor();
	            memBr.close();
	            memProcess.destroy();

	            
	            BufferedReader loadReader =
	                    new BufferedReader(new FileReader("/proc/loadavg"));

	            String loadLine = loadReader.readLine();
	            loadReader.close();

	            double firstLoadAvg = 0;

	            if (loadLine != null && !loadLine.isEmpty()) {
	                
	                String[] parts = loadLine.split("\\s+");
	                firstLoadAvg = Double.parseDouble(parts[0]);
	            }

	            result = new String[] {
	                    String.valueOf(firstLoadAvg),   
	                    String.valueOf(currentMemory)  
	            };

	        } catch (Exception e) {
	            e.printStackTrace();
	        }

	        return result;
	    }
	}

	// Resource monitoring with proper resource cleanup
	private void logResourceUsage(int[] usagesThreshold, String serverIP) {
		try {
			// Get current CPU and memory usages
			String[] currUsages = new MemoryAndCPUUsages().getMemoryAndCPUUsages();

			if (currUsages != null) {
				double cpuLoad = Double.parseDouble(currUsages[0]); // cpu %
				double memoryUsage = Double.parseDouble(currUsages[1]); // memory
																		// %

				// Check against thresholds
				if (memoryUsage <= usagesThreshold[0] && cpuLoad <= usagesThreshold[1]) {
					log("INFO",
							String.format("Memory: %.2f%% CPU Load: %.2f%% within thresholds", memoryUsage, cpuLoad),
							serverIP, "RESOURCE_MONITOR");
				} else {
					log("WARNING",
							String.format("Memory: %.2f%% CPU Load: %.2f%% exceeded thresholds", memoryUsage, cpuLoad),
							serverIP, "RESOURCE_MONITOR");
				}
			} else {
				log("WARNING", "Failed to get current memory/CPU usages", serverIP, "RESOURCE_MONITOR");
			}
		} catch (Exception e) {
			log("WARNING", "Failed to check resource usage: " + e.getMessage(), serverIP, "RESOURCE_ERROR");
		}
	}

	// Configuration classes remain the same but now used with instance
	public class ProcessConfig {
		public final String dirPath, processedDirPath, delimiter;
		public final boolean hasHeader;
		public final int numTablesToPick, bulkSize, threadCount;
		public final double maxCpuThreshold, maxMemoryThreshold;
		public final DatabaseConfig sourceDb, targetDb;
		public final String configTable, dbType;

		public ProcessConfig(String dirPath, String processedDirPath, String headerFlag, String delimiter,
				String sqldburl, String sdbuser, String sdbpass, String serverIP, String pdbHost, String pdbPort,
				String pdbName, String pdbUser, String pdbPass, String destUrl, String destUser, String destPass,
				String numTablesToPickStr, String configTable, String maxCpuUtil, String maxMemUtil,
				String bulkNumberStr, String dbTypeStr, PostgreV6Methods instance) {

			this.dirPath = dirPath;
			this.processedDirPath = processedDirPath;
			this.hasHeader = "yes".equalsIgnoreCase(headerFlag) || "true".equalsIgnoreCase(headerFlag);
			this.delimiter = (delimiter == null || delimiter.isEmpty()) ? "," : delimiter;
			this.numTablesToPick = parseIntOrDefault(numTablesToPickStr, 10);
			this.bulkSize = parseIntOrDefault(bulkNumberStr, 10000);
			this.threadCount = Math.min(numTablesToPick, instance.defaultThreadCount);
			this.maxCpuThreshold = parseDoubleOrDefault(maxCpuUtil, 80.0);
			this.maxMemoryThreshold = parseDoubleOrDefault(maxMemUtil, 80.0);
			this.configTable = configTable;
			this.dbType = (dbTypeStr == null) ? "psql" : dbTypeStr.trim().toLowerCase();

			instance.log("INFO",
					String.format(
							"Configuration initialized - DB_Type:%s Tables_To_Process:%d Bulk_Size:%d Thread_Count:%d CPU_Threshold:%.1f%% Memory_Threshold:%.1f%%",
							this.dbType, this.numTablesToPick, this.bulkSize, this.threadCount, this.maxCpuThreshold,
							this.maxMemoryThreshold),
					serverIP, "CONFIG_INIT");

			this.sourceDb = new DatabaseConfig(sqldburl, sdbuser, sdbpass);

			if ("psql".equalsIgnoreCase(this.dbType)) {
				String pgUrl = String.format("jdbc:postgresql://%s:%s/%s", pdbHost, pdbPort, pdbName);
				this.targetDb = new DatabaseConfig(pgUrl, pdbUser, pdbPass, pdbHost, pdbPort, pdbName);
				instance.log("INFO", String.format("PostgreSQL target configured - Host:%s Port:%s Database:%s",
						pdbHost, pdbPort, pdbName), serverIP, "DB_CONFIG");
			} else {
				this.targetDb = new DatabaseConfig(destUrl, destUser, destPass);
				instance.log("INFO", "MySQL target configured", serverIP, "DB_CONFIG");
			}
		}

		private int parseIntOrDefault(String value, int defaultValue) {
			try {
				return (value != null && !value.trim().isEmpty()) ? Integer.parseInt(value.trim()) : defaultValue;
			} catch (NumberFormatException e) {
				return defaultValue;
			}
		}

		private double parseDoubleOrDefault(String value, double defaultValue) {
			try {
				return (value != null && !value.trim().isEmpty()) ? Double.parseDouble(value.trim()) : defaultValue;
			} catch (NumberFormatException e) {
				return defaultValue;
			}
		}
	}

	public class DatabaseConfig {
		public final String url, user, password, host, port, database;

		public DatabaseConfig(String url, String user, String password) {
			this(url, user, password, null, null, null);
		}

		public DatabaseConfig(String url, String user, String password, String host, String port, String database) {
			this.url = url;
			this.user = user;
			this.password = password;
			this.host = host;
			this.port = port;
			this.database = database;
		}
	}

	public class TableEntry {
		public final String sno, tableName;

		public TableEntry(String sno, String tableName) {
			this.sno = sno;
			this.tableName = tableName;
		}

		public String toString() {
			return String.format("Table[sno=%s, name=%s]", sno, tableName);
		}
	}

	public class TableProcessResult {
		public final TableEntry table;
		public final boolean success;
		public final int filesProcessed;
		public final String errorMessage;

		public TableProcessResult(TableEntry table, boolean success, int filesProcessed, String errorMessage) {
			this.table = table;
			this.success = success;
			this.filesProcessed = filesProcessed;
			this.errorMessage = errorMessage;
		}
	}

	// Main processing method - now instance-based
	public void processAllInDirectory(String dirPath, String processedDirPath, String headerFlag, String delimiter,
			String sqldburl, String sdbuser, String sdbpass, String serverIP, String pdbHost, String pdbPort,
			String pdbName, String pdbUser, String pdbPass, String destUrl, String destUser, String destPass,
			String numTablesToPickStr, String configTable, String maxCpuUtil, String maxMemUtil, String bulkNumberStr,
			String dbTypeStr) {
		int bulkSize = Integer.parseInt(bulkNumberStr);

		log("INFO", "CSV processing started", serverIP, "PROCESS_START");
		int[] usagesThreshold = new int[2];
		usagesThreshold[0] = Integer.parseInt(maxMemUtil); // memory threshold
															// 80%
		usagesThreshold[1] = Integer.parseInt(maxCpuUtil); // CPU threshold 70%

		logResourceUsage(usagesThreshold, serverIP);

		ProcessConfig config = new ProcessConfig(dirPath, processedDirPath, headerFlag, delimiter, sqldburl, sdbuser,
				sdbpass, serverIP, pdbHost, pdbPort, pdbName, pdbUser, pdbPass, destUrl, destUser, destPass,
				numTablesToPickStr, configTable, maxCpuUtil, maxMemUtil, bulkNumberStr, dbTypeStr, this);

		try {

			executeProcessing(config, serverIP, pdbHost, bulkSize);

			logResourceUsage(usagesThreshold, serverIP);
			log("INFO", "CSV processing completed successfully", serverIP, "PROCESS_END");
		} catch (Exception e) {
			log("ERROR", "Processing failed: " + e.getMessage(), serverIP, "PROCESS_ERROR");
			e.printStackTrace();
		}
	}

	private void executeProcessing(ProcessConfig config, String serverIP, String pdbHost, int bulkSize) {

		if (!validateDirectories(config.dirPath, config.processedDirPath, serverIP)) {
			log("ERROR", "Directory validation failed", serverIP, "DIR_VALIDATION_FAILED");
			return;
		}

		List<Path> csvFiles = discoverCsvFiles(config.dirPath, serverIP);
		if (csvFiles.isEmpty()) {
			log("INFO", "No CSV files found in directory: " + config.dirPath, serverIP, "NO_FILES_FOUND");
			return;
		}

		List<TableEntry> readyTables = fetchReadyTables(config, serverIP);
		if (readyTables.isEmpty()) {
			log("INFO", "No tables ready for import", serverIP, "NO_TABLES_READY");
			return;
		}

		Map<String, List<Path>> tableFilesMap = groupFilesByTable(csvFiles, readyTables, serverIP);
		// log("INFO", String.format("File grouping completed -
		// Tables_With_Files:%d Total_CSV_Files:%d",
		// tableFilesMap.size(), csvFiles.size()), pdbHost, "FILE_GROUPING");
		processTablesInParallel(config, readyTables, tableFilesMap, serverIP, bulkSize);

	}

	private boolean validateDirectories(String dirPath, String processedDirPath, String serverIP) {
		try {
			Path sourceDir = Paths.get(dirPath);
			Path processedDir = Paths.get(processedDirPath);

			if (!Files.exists(sourceDir) || !Files.isDirectory(sourceDir)) {
				log("ERROR", "Source directory does not exist: " + dirPath, serverIP, "DIR_NOT_FOUND");
				return false;
			}

			Files.createDirectories(processedDir);
			log("INFO", String.format("Directory validation successful - Source:%s Processed:%s", dirPath,
					processedDirPath), serverIP, "DIR_VALIDATED");
			return true;
		} catch (Exception e) {
			log("ERROR", "Directory validation failed: " + e.getMessage(), serverIP, "DIR_VALIDATION_ERROR");
			return false;
		}
	}

	// Fixed file discovery with proper natural sorting
	private List<Path> discoverCsvFiles(String dirPath, String serverIP) {
		try (Stream<Path> stream = Files.list(Paths.get(dirPath))) {
			List<Path> files = stream.filter(Files::isRegularFile).filter(p -> {
				String fileName = p.getFileName().toString().toLowerCase();
				return (fileName.startsWith("raw_") || fileName.startsWith("process_")) && fileName.endsWith(".csv");
			}).sorted(naturalFileComparator) // Use natural sorting
					.collect(Collectors.toList());

			log("INFO",
					String.format("CSV file discovery completed - Files_Found:%d Directory:%s", files.size(), dirPath),
					serverIP, "FILE_DISCOVERY");
			return files;
		} catch (IOException e) {
			log("ERROR", "Failed to discover CSV files: " + e.getMessage(), serverIP, "FILE_DISCOVERY_ERROR");
			return Collections.emptyList();
		}
	}

	// Fixed resource management - proper connection closing
	private List<TableEntry> fetchReadyTables(ProcessConfig config, String serverIP) {
		List<TableEntry> tables = new ArrayList<>();
		String query = "SELECT sno, table_name FROM " + config.configTable
				+ " WHERE sync_status = 'ready_to_import' LIMIT ?";

		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			conn = DriverManager.getConnection(config.sourceDb.url, config.sourceDb.user, config.sourceDb.password);
			ps = conn.prepareStatement(query);
			ps.setInt(1, config.numTablesToPick);
			rs = ps.executeQuery();

			while (rs.next()) {
				TableEntry table = new TableEntry(rs.getString("sno"), rs.getString("table_name"));
				tables.add(table);
			}

			log("INFO",
					String.format("Ready tables fetched - Count:%d Config_Table:%s", tables.size(), config.configTable),
					serverIP, "TABLE_FETCH");
		} catch (SQLException e) {
			log("ERROR", "Failed to fetch ready tables: " + e.getMessage(), serverIP, "TABLE_FETCH_ERROR");
		} finally {
			// Proper resource cleanup
			closeQuietly(rs);
			closeQuietly(ps);
			closeQuietly(conn);
		}

		return tables;
	}

	// Utility method for safe resource closing
	private void closeQuietly(AutoCloseable resource) {
		if (resource != null) {
			try {
				resource.close();
			} catch (Exception e) {
				// Log but don't throw - this is cleanup code
				System.err.println("Warning: Failed to close resource: " + e.getMessage());
			}
		}
	}

	private Map<String, List<Path>> groupFilesByTable(List<Path> csvFiles, List<TableEntry> readyTables,
			String serverIP) {
		Map<String, List<Path>> tableFilesMap = new HashMap<>();
		Set<String> readySnoSet = readyTables.stream().map(t -> t.sno).collect(Collectors.toSet());

		for (Path file : csvFiles) {
			String fileName = file.getFileName().toString();
			String[] parts = fileName.split("_");
			if (parts.length > 4) {
				String sno = parts[4];
				if (readySnoSet.contains(sno)) {
					tableFilesMap.computeIfAbsent(sno, k -> new ArrayList<>()).add(file);
				}
			}
		}

		log("INFO", String.format("File grouping completed - Tables_With_Files:%d Total_Files:%d", tableFilesMap.size(),
				csvFiles.size()), serverIP, "FILE_GROUPING");
		return tableFilesMap;
	}

	private void processTablesInParallel(ProcessConfig config, List<TableEntry> readyTables,
			Map<String, List<Path>> tableFilesMap, String serverIP, int bulkSize) {

		log("INFO", String.format("Parallel processing started - Thread_Count:%d Tables_To_Process:%d",
				config.threadCount, readyTables.size()), serverIP, "PARALLEL_START");

		ExecutorService executor = Executors.newFixedThreadPool(config.threadCount);
		List<Future<TableProcessResult>> futures = new ArrayList<>();

		for (TableEntry table : readyTables) {
			List<Path> files = tableFilesMap.getOrDefault(table.sno, Collections.emptyList());
			futures.add(executor.submit(() -> processTable(config, table, files, serverIP, bulkSize)));
		}

		List<TableProcessResult> results = new ArrayList<>();
		for (Future<TableProcessResult> future : futures) {
			try {
				TableProcessResult result = future.get();
				results.add(result);
			} catch (Exception e) {
				log("ERROR", "Thread execution failed: " + e.getMessage(), serverIP, "THREAD_ERROR");
			}
		}

		executor.shutdown();
		try {
			if (!executor.awaitTermination(30, TimeUnit.MINUTES)) {
				log("WARNING", "Thread pool timeout - forcing shutdown", serverIP, "THREAD_TIMEOUT");
				executor.shutdownNow();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			executor.shutdownNow();
		}

		// Log summary
		long successful = results.stream().mapToLong(r -> r.success ? 1 : 0).sum();
		int totalFiles = results.stream().mapToInt(r -> r.filesProcessed).sum();

		log("INFO", String.format("Parallel processing completed - Successful_Tables:%d/%d Total_Files_Processed:%d",
				successful, results.size(), totalFiles), serverIP, "PARALLEL_END");

		// Log failed tables
		results.stream().filter(r -> !r.success)
				.forEach(r -> log("ERROR", String.format("Table processing failed - tableName:%s SNO:%s Error:%s",
						r.table.tableName, r.table.sno, r.errorMessage), serverIP, "TABLE_FAILED"));
	}

	private TableProcessResult processTable(ProcessConfig config, TableEntry table, List<Path> files, String serverIP,
			int bulkSize) {
		log("INFO", String.format("Table processing started - tableName:%s SNO:%s Files_Count:%d", table.tableName,
				table.sno, files.size()), serverIP, "TABLE_START");

		try {
			updateTableStatus(table.sno, table.tableName, "in_process", config, serverIP);

			if (files.isEmpty()) {
				updateTableStatus(table.sno, table.tableName, "init", config, serverIP);
				log("INFO", String.format("Table processing completed - tableName:%s Files_Processed:0 Status:No_Files",
						table.tableName), serverIP, "TABLE_END");
				return new TableProcessResult(table, true, 0, null);
			}

			int processedCount = 0;
			for (Path file : files) {
				log("INFO", String.format("File processing started - file:%s tableName:%s",
						file.getFileName().toString(), table.tableName), serverIP, "FILE_START");

				try {
					processFile(config, table, file, serverIP, bulkSize);
					processedCount++;
					log("INFO", String.format("File processing completed - File:%s tableName:%s",
							file.getFileName().toString(), table.tableName), serverIP, "FILE_END");
				} catch (Exception e) {
					log("ERROR",
							String.format("File processing failed - File:%s tableName:%s Error:%s",
									file.getFileName().toString(), table.tableName, e.getMessage()),
							serverIP, "FILE_ERROR");
					e.printStackTrace();
					throw e;
				}
			}

			updateTableStatus(table.sno, table.tableName, "init", config, serverIP);
			log("INFO",
					String.format("Table processing completed - tableName:%s SNO:%s Files_Processed:%d Status:Success",
							table.tableName, table.sno, processedCount),
					serverIP, "TABLE_END");
			return new TableProcessResult(table, true, processedCount, null);

		} catch (Exception e) {
			log("ERROR", String.format("Table processing failed - tableName:%s SNO:%s Error:%s", table.tableName,
					table.sno, e.getMessage()), serverIP, "TABLE_ERROR");
			updateTableStatus(table.sno, table.tableName, "error", config, serverIP);
			return new TableProcessResult(table, false, 0, e.getMessage());
		}
	}

	private void processFile(ProcessConfig config, TableEntry table, Path file, String serverIP, int bulkSize)
			throws Exception {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(config.targetDb.url, config.targetDb.user, config.targetDb.password);
			boolean tableExists = tableExists(conn, table.tableName);

			if (tableExists) {
				log("INFO", String.format("Table found - tableName:%s", table.tableName), serverIP, "TABLE_FOUND");
			} else {
				log("INFO", String.format("Table not found, will be created - tableName:%s", table.tableName), serverIP,
						"TABLE_NOT_FOUND");
			}

			// Process based on database type
			if ("psql".equalsIgnoreCase(config.dbType)) {
				processFilePostgreSQL(config, table, file, serverIP, bulkSize);
			} else if ("sql".equals(config.dbType) || "mysql".equals(config.dbType)) {
				processFileMySQL(config, table, file, serverIP, bulkSize);
			} else {
				throw new IllegalArgumentException("Unsupported database type: " + config.dbType);
			}

			// Move file to processed directory
			moveFileToProcessed(file, Paths.get(config.processedDirPath), serverIP);

		} finally {
			closeQuietly(conn);
		}
	}

	private void processFilePostgreSQL(ProcessConfig config, TableEntry table, Path file, String serverIP, int bulkSize)
			throws Exception {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(config.targetDb.url, config.targetDb.user, config.targetDb.password);

			if (!tableExists(conn, table.tableName)) {
				createTableUsingPsql(table.tableName, file.toString(), config.delimiter, config.targetDb, serverIP);
				log("INFO", String.format("Table created - tableName:%s Database:PostgreSQL", table.tableName),
						serverIP, "TABLE_CREATED");
			}
		} finally {
			closeQuietly(conn);
		}

		// importCSVUsingPsql(file.toString(), config.delimiter,
		// config.hasHeader, table.tableName, config.targetDb,
		// pdbHost, bulkSize);

		importCSVUsingPsql(file.toString(), table.tableName, config.targetDb, serverIP, bulkSize);

	}

	private void processFileMySQL(ProcessConfig config, TableEntry table, Path file, String serverIP, int bulkSize)
			throws Exception {
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(config.targetDb.url, config.targetDb.user, config.targetDb.password);
			conn.setAutoCommit(false);

			if (!tableExists(conn, table.tableName)) {
				createTableFromCSV(table.tableName, file.toString(), config.delimiter, conn, serverIP);
				log("INFO", String.format("Table created - tableName:%s Database:MySQL", table.tableName), serverIP,
						"TABLE_CREATED");
			}

			// importCSVToMySQL now does column check (exact vs partial)
			importCSVToMySQL(file.toString(), config.delimiter, config.hasHeader, table.tableName, conn, serverIP,
					bulkSize);

			conn.commit();
		} catch (Exception e) {
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException rollbackEx) {
					log("ERROR", "Failed to rollback transaction: " + rollbackEx.getMessage(), serverIP,
							"ROLLBACK_ERROR");
				}
			}
			throw e;
		} finally {
			if (conn != null) {
				try {
					conn.setAutoCommit(true);
				} catch (SQLException autoCommitEx) {
					log("WARNING", "Failed to reset auto-commit: " + autoCommitEx.getMessage(), serverIP,
							"AUTOCOMMIT_WARNING");
				}
				try {
					conn.close(); // explicit close
				} catch (SQLException closeEx) {
					log("WARNING", "Failed to close connection: " + closeEx.getMessage(), serverIP,
							"CONNECTION_CLOSE_WARNING");
				}
			}
		}
	}

	// Updated status update method with proper resource management
	private void updateTableStatus(String sno, String tableName, String status, ProcessConfig config, String serverIP) {
		String query = "UPDATE " + config.configTable + " SET sync_status = ? WHERE sno = ?";
		Connection conn = null;
		PreparedStatement ps = null;

		try {
			conn = DriverManager.getConnection(config.sourceDb.url, config.sourceDb.user, config.sourceDb.password);
			ps = conn.prepareStatement(query);
			ps.setString(1, status);
			ps.setString(2, sno);

			int rowsUpdated = ps.executeUpdate();
			if (rowsUpdated > 0) {
				log("INFO",
						String.format("Table status updated - SNO:%s tableName:%s Status:%s", sno, tableName, status),
						serverIP, "STATUS_UPDATE");
			} else {
				log("WARNING", String.format("No rows updated for SNO:%s Status:%s", sno, status), serverIP,
						"STATUS_UPDATE_WARNING");
			}
		} catch (SQLException e) {
			log("ERROR", String.format("Failed to update table status - SNO:%s Status:%s Error:%s", sno, status,
					e.getMessage()), serverIP, "STATUS_UPDATE_ERROR");
		} finally {
			closeQuietly(ps);
			closeQuietly(conn);
		}
	}

	// File moving with proper resource management
	// File moving with proper resource management
	private void moveFileToProcessed(Path sourceFile, Path processedDir, String serverIP) {
		try {
			Files.createDirectories(processedDir);
			Path targetFile = processedDir.resolve(sourceFile.getFileName());
			// indexing
			// batching
			// CPU util
			// Handle duplicate file names by adding timestamp
			if (Files.exists(targetFile)) {
				String fileName = sourceFile.getFileName().toString();
				String baseName = fileName.substring(0, fileName.lastIndexOf('.'));
				String extension = fileName.substring(fileName.lastIndexOf('.'));
				String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
				targetFile = processedDir.resolve(baseName + "_" + timestamp + extension);
			}

			Files.move(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);
			log("INFO", String.format("File moved to processed directory - Source:%s Target:%s", sourceFile.toString(),
					targetFile.toString()), serverIP, "FILE_MOVED");
		} catch (IOException e) {
			log("ERROR",
					String.format("Failed to move file - Source:%s Error:%s", sourceFile.toString(), e.getMessage()),
					serverIP, "FILE_MOVE_ERROR");
		}
	}

	// Table existence check with proper resource management
	private boolean tableExists(Connection conn, String tableName) {

		PreparedStatement ps = null;
		ResultSet rs = null;

		try {

			// If table name is schema.table_name
			if (tableName != null && tableName.contains(".")) {

				String[] parts = tableName.split("\\.", 2);
				String schema = parts[0].trim();
				String table = parts[1].trim();

				String query = "SELECT 1 FROM information_schema.tables "
						+ "WHERE table_schema = ? AND table_name = ? LIMIT 1";

				ps = conn.prepareStatement(query);
				ps.setString(1, schema);
				ps.setString(2, table);

			} else {

				// If table name is only table_name
				String query = "SELECT 1 FROM information_schema.tables WHERE table_name = ? LIMIT 1";

				ps = conn.prepareStatement(query);
				ps.setString(1, tableName);
			}

			rs = ps.executeQuery();
			return rs.next();

		} catch (SQLException e) {
			log("ERROR", "Failed to check table existence: " + e.getMessage(), null, "TABLE_CHECK_ERROR");
			return false;
		} finally {
			closeQuietly(rs);
			closeQuietly(ps);
		}
	}

	// PostgreSQL table creation with proper resource management
	private void createTableUsingPsql(String tableName, String csvFilePath, String delimiter, DatabaseConfig dbConfig,
			String serverIP) throws Exception {

		// First, analyze CSV structure
		List<String> columnDefinitions = analyzeCSVStructure(csvFilePath, delimiter, serverIP);

		if (columnDefinitions.isEmpty()) {
			throw new Exception("Could not determine CSV structure for table creation");
		}

		StringBuilder createTableSQL = new StringBuilder();
		createTableSQL.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
		createTableSQL.append(String.join(", ", columnDefinitions));
		createTableSQL.append(")");

		Connection conn = null;
		Statement stmt = null;

		try {
			conn = DriverManager.getConnection(dbConfig.url, dbConfig.user, dbConfig.password);
			stmt = conn.createStatement();
			stmt.executeUpdate(createTableSQL.toString());

			log("INFO", String.format("PostgreSQL table created - tableName:%s Columns:%d", tableName,
					columnDefinitions.size()), serverIP, "PSQL_TABLE_CREATED");
		} finally {
			closeQuietly(stmt);
			closeQuietly(conn);
		}
	}

	// MySQL table creation with proper resource management
	private void createTableFromCSV(String tableName, String csvFilePath, String delimiter, Connection conn,
			String serverIP) throws Exception {

		List<String> columnDefinitions = analyzeCSVStructure(csvFilePath, delimiter, serverIP);

		if (columnDefinitions.isEmpty()) {
			throw new Exception("Could not determine CSV structure for table creation");
		}

		StringBuilder createTableSQL = new StringBuilder();
		createTableSQL.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");
		createTableSQL.append(String.join(", ", columnDefinitions));
		createTableSQL.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");

		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(createTableSQL.toString());

			log("INFO",
					String.format("MySQL table created - tableName:%s Columns:%d", tableName, columnDefinitions.size()),
					serverIP, "MYSQL_TABLE_CREATED");
		} finally {
			closeQuietly(stmt);
		}
	}

	// CSV structure analysis with proper resource management
	private List<String> analyzeCSVStructure(String csvFilePath, String delimiter, String serverIP) {
		List<String> columnDefinitions = new ArrayList<>();
		BufferedReader reader = null;

		try {
			reader = Files.newBufferedReader(Paths.get(csvFilePath));
			String headerLine = reader.readLine();

			if (headerLine != null) {
				String[] headers = headerLine.split(Pattern.quote(delimiter), -1);

				for (int i = 0; i < headers.length; i++) {
					String columnName = sanitizeColumnName(headers[i].trim());
					if (columnName.isEmpty()) {
						columnName = "column_" + (i + 1);
					}
					columnDefinitions.add(columnName + " TEXT");
				}
			}

			log("INFO",
					String.format("CSV structure analyzed - File:%s Columns:%d", csvFilePath, columnDefinitions.size()),
					serverIP, "CSV_ANALYZED");

		} catch (IOException e) {
			log("ERROR",
					String.format("Failed to analyze CSV structure - File:%s Error:%s", csvFilePath, e.getMessage()),
					serverIP, "CSV_ANALYSIS_ERROR");
		} finally {
			closeQuietly(reader);
		}

		return columnDefinitions;
	}

	// Column name sanitization
	private String sanitizeColumnName(String columnName) {
		if (columnName == null || columnName.trim().isEmpty()) {
			return "";
		}

		// Remove quotes and special characters, replace spaces with underscores
		return columnName.replaceAll("[\"'`]", "").replaceAll("[^a-zA-Z0-9_]", "_").replaceAll("_{2,}", "_")
				.replaceAll("^_|_$", "").toLowerCase();
	}

	// importCSVUsingPsql(file.toString(), config.delimiter, config.hasHeader,
	// table.tableName, config.targetDb,
	// pdbHost, bulkSize);
	private void importCSVUsingPsql(String csvFilePath, String tableName, DatabaseConfig dbConfig, String serverIP,
			int bulkSize) throws Exception {

		Connection conn = null;

		int totalRows = 0;
		int chunkCount = 0;

		char delim = '|'; // PIPE delimiter

		try {
			conn = DriverManager.getConnection(dbConfig.url, dbConfig.user, dbConfig.password);

			try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {

				// -------- HEADER ----------
				String headerLine = br.readLine();
				if (headerLine == null) {
					throw new Exception("CSV file is empty: " + csvFilePath);
				}

				List<String> headerCols = parseCsvLineOption2(headerLine, delim);
				if (headerCols.isEmpty()) {
					throw new Exception("Header could not be parsed: " + csvFilePath);
				}

				List<String> columns = headerCols.stream()
						.map(s -> s == null ? "" : s.replace("\"", "").trim().toLowerCase())
						.collect(Collectors.toList());

				String copySQL = "COPY " + tableName + " (" + String.join(",", columns)
						+ ") FROM STDIN WITH (FORMAT csv, HEADER false, DELIMITER '|', "
						+ "QUOTE '\"', ESCAPE '\"', NULL 'NULL')";

				List<String> chunkLines = new ArrayList<>(bulkSize);

				// -------- DATA ----------
				String line;
				int rowNo = 1; // header already read

				while ((line = br.readLine()) != null) {
					rowNo++;

					if (line == null || line.trim().isEmpty()) {
						continue;
					}

					List<String> parsed = parseCsvLineOption2(line, delim);

					// STRICT: fail if mismatch
					if (parsed.size() != columns.size()) {
						throw new Exception("Column mismatch! Expected=" + columns.size() + " Found=" + parsed.size()
								+ " File=" + csvFilePath + " RowNo=" + rowNo + " RawRow=" + line);
					}

					String fixedLine = buildSafeCopyRow(parsed.toArray(new String[0]), delim);
					chunkLines.add(fixedLine);
					totalRows++;

					if (chunkLines.size() >= bulkSize) {
						executeCopyChunk(conn, copySQL, chunkLines);
						chunkCount++;
						log("INFO", "Processed chunk " + chunkCount + " with " + chunkLines.size() + " rows", serverIP,
								"PSQL_CHUNK");
						chunkLines.clear();
					}
				}

				if (!chunkLines.isEmpty()) {
					chunkCount++;
					executeCopyChunk(conn, copySQL, chunkLines);
					log("INFO", "Processed final chunk " + chunkCount + " with " + chunkLines.size() + " rows",
							serverIP, "PSQL_CHUNK");
				}

				log("INFO", "PostgreSQL COPY completed - tableName:" + tableName + " Total_Rows:" + totalRows + " File:"
						+ csvFilePath, serverIP, "PSQL_IMPORT_SUCCESS");
			}

		} catch (Exception e) {
			log("ERROR", "COPY failed: " + e.getMessage(), serverIP, "PSQL_IMPORT_ERROR");
			throw e;
		} finally {
			closeQuietly(conn);
		}
	}

	private List<String> parseCsvLineOption2(String line, char delimiter) {
		List<String> fields = new ArrayList<>();
		if (line == null)
			return fields;

		StringBuilder field = new StringBuilder();
		boolean inQuotes = false;

		for (int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);

			if (c == '"') {

				if (!inQuotes) {
					// opening quote
					inQuotes = true;
					continue;
				} else {
					// inQuotes == true
					// treat this quote as CLOSING only if next is delimiter or
					// end-of-line
					boolean isEnd = (i == line.length() - 1);
					boolean nextIsDelimiter = (!isEnd && line.charAt(i + 1) == delimiter);

					if (isEnd || nextIsDelimiter) {
						inQuotes = false;
						continue;
					} else {
						// quote is part of data
						field.append('"');
						continue;
					}
				}
			}

			if (c == delimiter && !inQuotes) {
				fields.add(field.toString());
				field.setLength(0);
				continue;
			}

			field.append(c);
		}

		fields.add(field.toString());
		return fields;
	}

	private String buildSafeCopyRow(String[] row, char delimiter) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < row.length; i++) {
			String v = row[i];
			if (v == null)
				v = "";

			// remove CR/LF + trim
			v = v.replace("\r", " ").replace("\n", " ").trim();

			// FIX: Convert MySQL "zero date" into NULL for PostgreSQL
			if (v.equals("0000-00-00 00:00:00") || v.equals("0000-00-00")) {
				v = "NULL";
			}

			// NULL handling
			if (v.isEmpty() || v.equalsIgnoreCase("null") || v.equalsIgnoreCase("\\N") || v.equalsIgnoreCase("NULL")) {
				sb.append("NULL");
			} else {
				// escape quotes for CSV COPY
				v = v.replace("\"", "\"\"");
				sb.append("\"").append(v).append("\"");
			}

			if (i < row.length - 1)
				sb.append(delimiter);
		}

		return sb.toString();
	}

	private void executeCopyChunk(Connection conn, String copySQL, List<String> chunkLines) throws Exception {
		CopyManager copyManager = new CopyManager((BaseConnection) conn);
		StringBuilder sb = new StringBuilder();

		for (String row : chunkLines) {
			sb.append(row).append("\n");
		}

		copyManager.copyIn(copySQL, new StringReader(sb.toString()));
	}

	// MySQL CSV import with proper resource management and batching
	// MySQL CSV import with proper delimiter support (| / , / etc.)
	private void importCSVToMySQL(String csvFilePath, String delimiter, boolean hasHeader, String tableName,
			Connection conn, String serverIP, int bulkSize) throws Exception {

		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(csvFilePath));
			String headerLine = reader.readLine();

			if (headerLine == null) {
				throw new Exception("CSV file is empty: " + csvFilePath);
			}

			// IMPORTANT: Pattern.quote() so delimiter like "|" works correctly
			String[] csvHeaders = headerLine.split(Pattern.quote(delimiter), -1);

			// Fetch DB column names
			List<String> dbColumns = new ArrayList<>();
			try (Statement stmt = conn.createStatement();
					ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 1")) {

				ResultSetMetaData meta = rs.getMetaData();
				for (int i = 1; i <= meta.getColumnCount(); i++) {
					dbColumns.add(meta.getColumnName(i));
				}
			}

			// Call fixed loader (uses delimiter properly)
			// String cleanedPath = sanitizeCsvForMySQL(csvFilePath, delimiter,
			// serverIP);

			// insertPartialCSV(cleanedPath, tableName, conn, serverIP,
			// csvHeaders, dbColumns, hasHeader, delimiter);
			insertPartialCSV(csvFilePath, tableName, conn, serverIP, csvHeaders, dbColumns, hasHeader, delimiter);

		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception ignore) {
				}
			}
		}
	}

	// public void insertPartialCSV(String csvFilePath, String tableName,
	// Connection conn, String serverIP,
	// String[] csvHeaders, List<String> dbColumns, boolean hasHeader, String
	// delimiter) throws Exception {
	//
	// // Match CSV headers with DB columns
	// List<String> matchedCols = new ArrayList<>();
	// for (String csvCol : csvHeaders) {
	// String cleanCol = (csvCol == null) ? "" : csvCol.replace("\"",
	// "").trim();
	// for (String dbCol : dbColumns) {
	// if (dbCol != null && dbCol.equalsIgnoreCase(cleanCol)) {
	// matchedCols.add(dbCol);
	// break;
	// }
	// }
	// }
	//
	// if (matchedCols.isEmpty()) {
	// throw new Exception("No matching columns found between CSV and table: " +
	// tableName);
	// }
	//
	// File csvFile = new File(csvFilePath);
	// if (!csvFile.exists() || !csvFile.canRead()) {
	// throw new FileNotFoundException("CSV file not found or not readable: " +
	// csvFile.getAbsolutePath());
	// }
	//
	// String safeDelimiter = (delimiter == null || delimiter.trim().isEmpty())
	// ? "|" : delimiter.trim();
	// String filePathForSQL = csvFile.getAbsolutePath().replace("\\", "\\\\");
	//
	// // Use user variables so we can NULLIF() everything safely
	// List<String> userVars = matchedCols.stream()
	// .map(c -> "@v_" + c)
	// .collect(Collectors.toList());
	//
	// // SET col = NULLIF(@v_col,'NULL')
	// List<String> setClauses = matchedCols.stream()
	// .map(c -> c + " = NULLIF(@v_" + c + ", 'NULL')")
	// .collect(Collectors.toList());
	//
	//// String sql =
	//// "LOAD DATA LOCAL INFILE '" + filePathForSQL + "' " +
	//// "INTO TABLE " + tableName + " " +
	//// "FIELDS TERMINATED BY '" + safeDelimiter + "' " +
	//// "OPTIONALLY ENCLOSED BY '\"' " +
	//// "LINES TERMINATED BY '\\n' " +
	//// (hasHeader ? "IGNORE 1 LINES " : "") +
	//// "(" + String.join(",", userVars) + ") " +
	//// "SET " + String.join(", ", setClauses);
	//
	// String sql =
	// "LOAD DATA LOCAL INFILE '" + filePathForSQL + "' " +
	// "INTO TABLE " + tableName + " " +
	// "CHARACTER SET latin1 " +
	// "FIELDS TERMINATED BY '" + safeDelimiter + "' " +
	// "OPTIONALLY ENCLOSED BY '\"' " +
	// "ESCAPED BY '\"' " +
	// "LINES TERMINATED BY '\\n' " +
	// (hasHeader ? "IGNORE 1 LINES " : "") +
	// "(" + String.join(",", userVars) + ") " +
	// "SET " + String.join(", ", setClauses);
	//
	//
	// log("INFO", "SQL CMD: " + sql, serverIP, "MYSQL_LOAD_SQL");
	//
	// try (Statement stmt = conn.createStatement()) {
	// stmt.execute(sql);
	//
	// log("INFO",
	// "CSV loaded successfully - tableName:" + tableName + " File:" +
	// csvFile.getName(),
	// serverIP,
	// "CSV_LOAD");
	//
	// } catch (SQLException e) {
	// throw new Exception("Failed to load CSV into table: " + tableName, e);
	// }
	// }
	//
	public void insertPartialCSV(String csvFilePath, String tableName, Connection conn, String serverIP,
	        String[] csvHeaders, List<String> dbColumns, boolean hasHeader, String delimiter) throws Exception {

	    File csvFile = new File(csvFilePath);

	    // staging table name
	    String stagingTable = buildStagingTableName(tableName);

	    // sanitize CSV headers (keep original names but clean quotes)
	    List<String> csvCols = Arrays.stream(csvHeaders)
	            .map(h -> h == null ? "" : h.replace("\"", "").trim())
	            .filter(h -> !h.isEmpty())
	            .collect(Collectors.toList());

	    if (csvCols.isEmpty()) {
	        throw new Exception("CSV header is empty or invalid - File:" + csvFile.getName());
	    }

	    try {

	        // 1) Read FINAL table schema (INFORMATION_SCHEMA)
	        List<ColumnMeta> finalCols = getTableColumnsMeta(conn, tableName);

	        if (finalCols == null || finalCols.isEmpty()) {
	            throw new Exception("Unable to read final table schema from INFORMATION_SCHEMA for table=" + tableName);
	        }

	        // 2) Create STAGING table using CSV headers only (LONGTEXT)
	        log("INFO",
	                "Staging validation started - tableName:" + tableName
	                        + " File:" + csvFile.getName()
	                        + " staging:" + stagingTable,
	                serverIP, "STAGING_START");

	        createStagingTableFromCSVHeaders(conn, stagingTable, csvHeaders);

	        log("INFO",
	                "Staging table created - " + stagingTable + " Columns:" + csvCols.size(),
	                serverIP, "STAGING_CREATED");

	        // 3) Load CSV into staging
	        long stagingRows = loadCSVIntoStaging(conn, csvFilePath, stagingTable, hasHeader, delimiter, serverIP, csvHeaders);

	        // 4) Get staging table actual columns (avoid Unknown column crash)
	        Set<String> stagingColsLower = getStagingColumns(conn, stagingTable);

	        // 5) Keep ONLY columns that exist in BOTH final table + staging table
	        List<ColumnMeta> matchedCols = finalCols.stream()
	                .filter(c -> c != null && c.name != null)
	                .filter(c -> stagingColsLower.contains(c.name.toLowerCase()))
	                .collect(Collectors.toList());

	        if (matchedCols.isEmpty()) {
	            log("ERROR",
	                    "CSV validation FAILED - tableName:" + tableName
	                            + " File:" + csvFile.getName()
	                            + " Reason:No matching columns between STAGING and FINAL table schema",
	                    serverIP, "CSV_VALIDATION_FAILED");

	            throw new Exception("No matching columns found between CSV/STAGING and final table: " + tableName);
	        }

	        // 6) Validate bad rows (datatype + not null) ONLY on matched columns
	        Map<String, Long> badReasons = findBadRowReasons(conn, stagingTable, matchedCols);

	        // IMPORTANT: real bad row count (not sum of reasons)
	        long badRowTotal = countBadRows(conn, stagingTable, matchedCols);

	        if (badRowTotal > 0) {

	            log("ERROR",
	                    "CSV validation FAILED - tableName:" + tableName
	                            + " File:" + csvFile.getName()
	                            + " StagingRows:" + stagingRows
	                            + " BadRows:" + badRowTotal
	                            + " MatchedCols:" + matchedCols.size(),
	                    serverIP, "CSV_VALIDATION_FAILED");

	            // log only top reasons (avoid wall of trash)
	            int maxReasonsToPrint = 8;
	            int printed = 0;

	            for (Map.Entry<String, Long> e : badReasons.entrySet()) {
	                log("ERROR",
	                        "BadReason => " + e.getKey() + " Count:" + e.getValue(),
	                        serverIP, "CSV_BAD_REASON");
	                printed++;
	                if (printed >= maxReasonsToPrint) break;
	            }

	            // sample bad rows (max 5)
	            int maxSamples = 5;
	            List<String> samples = fetchSampleBadRows(conn, stagingTable, matchedCols, maxSamples);

	            for (String s : samples) {
	                log("ERROR", s, serverIP, "CSV_BAD_ROW_SAMPLE");
	            }

	            // STOP HERE (no insert into final)
	            throw new Exception("CSV contains invalid rows (datatype / not null). Import stopped. table=" + tableName);
	        }

	        // 7) Insert into final ONLY if clean
	        long inserted = insertFromStagingToFinal(conn, stagingTable, tableName, matchedCols);

	        log("INFO",
	                "Final insert SUCCESS - tableName:" + tableName
	                        + " Inserted_Rows:" + inserted
	                        + " StagingRows:" + stagingRows
	                        + " File:" + csvFile.getName(),
	                serverIP, "CSV_LOAD");

	    } finally {
	        // 8) Always cleanup staging
	        dropTableQuietly(conn, stagingTable, serverIP);
	    }
	}

	private Set<String> getStagingColumns(Connection conn, String stagingTable) throws SQLException {
	    Set<String> cols = new HashSet<>();

	    try (Statement st = conn.createStatement();
	         ResultSet rs = st.executeQuery("SELECT * FROM " + stagingTable + " LIMIT 1")) {

	        ResultSetMetaData md = rs.getMetaData();
	        for (int i = 1; i <= md.getColumnCount(); i++) {
	            cols.add(md.getColumnName(i).toLowerCase());
	        }
	    }

	    return cols;
	}


	private long countBadRows(Connection conn, String stagingTable, List<ColumnMeta> cols) throws SQLException {

		List<String> conditions = new ArrayList<>();

		for (ColumnMeta c : cols) {

			if (!c.nullable) {
			    conditions.add(
			        "(" + c.name + " IS NULL OR UPPER(TRIM(" + c.name + "))='NULL')");
			}


			if (isNumericType(c.type)) {
				conditions.add("(" + c.name + " IS NOT NULL AND TRIM(" + c.name + ")<>'' AND UPPER(TRIM(" + c.name
						+ "))<>'NULL' " + "AND TRIM(" + c.name + ") NOT REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$')");
			}

			if (isDateType(c.type)) {
				conditions.add("(" + c.name + " IS NOT NULL AND TRIM(" + c.name + ")<>'' AND UPPER(TRIM(" + c.name
						+ "))<>'NULL' " + "AND STR_TO_DATE(" + c.name + ", '%Y-%m-%d %H:%i:%s') IS NULL "
						+ "AND STR_TO_DATE(" + c.name + ", '%Y-%m-%d') IS NULL)");
			}
		}

		if (conditions.isEmpty())
			return 0;

		String where = String.join(" OR ", conditions);

		String sql = "SELECT COUNT(*) FROM " + stagingTable + " WHERE " + where;

		try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
			rs.next();
			return rs.getLong(1);
		}
	}

	private String buildStagingTableName(String tableName) {
		// ckyc_uat.user_master -> user_master__stg_1700000
		String base = tableName.contains(".") ? tableName.split("\\.")[1] : tableName;
		return base + "__stg_" + System.currentTimeMillis();
	}

	static class ColumnMeta {
		String name;
		String type; // int, varchar, datetime, etc
		boolean nullable;
	}

	private List<ColumnMeta> getTableColumnsMeta(Connection conn, String tableName) throws SQLException {
		String schema = null;
		String tbl = tableName;

		if (tableName.contains(".")) {
			String[] parts = tableName.split("\\.");
			schema = parts[0];
			tbl = parts[1];
		}

		String sql = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE " + "FROM INFORMATION_SCHEMA.COLUMNS "
				+ "WHERE TABLE_NAME = ? " + (schema != null ? "AND TABLE_SCHEMA = ? " : "")
				+ "ORDER BY ORDINAL_POSITION";

		List<ColumnMeta> cols = new ArrayList<>();
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, tbl);
			if (schema != null)
				ps.setString(2, schema);

			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					ColumnMeta c = new ColumnMeta();
					c.name = rs.getString("COLUMN_NAME");
					c.type = rs.getString("DATA_TYPE").toLowerCase();
					c.nullable = "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"));
					cols.add(c);
				}
			}
		}
		return cols;
	}

	private void createStagingTableFromCSVHeaders(Connection conn, String stagingTable, String[] csvHeaders)
	        throws SQLException {

	    StringBuilder sb = new StringBuilder();
	    sb.append("CREATE TABLE ").append(stagingTable).append(" (");

	    for (int i = 0; i < csvHeaders.length; i++) {
	        String col = (csvHeaders[i] == null ? "" : csvHeaders[i].replace("\"", "").trim());

	        if (col.isEmpty()) {
	            col = "col_" + (i + 1);
	        }

	        col = col.replace("`", "");

	        sb.append("`").append(col).append("` LONGTEXT");
	        if (i < csvHeaders.length - 1) sb.append(", ");
	    }

	    sb.append(")");

	    try (Statement st = conn.createStatement()) {
	        st.execute(sb.toString());
	    }
	}


	private void dropTableQuietly(Connection conn, String tableName, String serverIP) {
		try (Statement st = conn.createStatement()) {
			st.execute("DROP TABLE IF EXISTS " + tableName);
			log("INFO", "Staging table dropped: " + tableName, serverIP, "STAGING_DROP");
		} catch (Exception e) {
			log("ERROR", "Failed to drop staging table: " + tableName + " Error:" + e.getMessage(), serverIP,
					"STAGING_DROP_ERROR");
		}
	}

	private long loadCSVIntoStaging(Connection conn, String csvFilePath, String stagingTable, boolean hasHeader,
	        String delimiter, String serverIP, String[] csvHeaders) throws Exception {

	    File csvFile = new File(csvFilePath);

	    if (!csvFile.exists() || !csvFile.canRead()) {
	        throw new FileNotFoundException("CSV file not found or not readable: " + csvFile.getAbsolutePath());
	    }

	    String safeDelimiter = (delimiter == null || delimiter.trim().isEmpty()) ? "|" : delimiter.trim();
	    String filePathForSQL = csvFile.getAbsolutePath().replace("\\", "\\\\");

	    // build column list (skip blanks + sanitize)
	    List<String> cols = new ArrayList<>();
	    for (String h : csvHeaders) {
	        if (h == null) continue;

	        String col = h.replace("\"", "").trim();
	        if (col.isEmpty()) continue;

	        // wrap in backticks safely
	        cols.add("`" + col.replace("`", "") + "`");
	    }

	    if (cols.isEmpty()) {
	        throw new Exception("CSV headers are empty, cannot build LOAD DATA column list. File=" + csvFile.getName());
	    }

	    String sql =
	            "LOAD DATA LOCAL INFILE '" + filePathForSQL + "' " +
	            "INTO TABLE " + stagingTable + " " +
	            "CHARACTER SET latin1 " +
	            "FIELDS TERMINATED BY '" + safeDelimiter + "' " +
	            "OPTIONALLY ENCLOSED BY '\"' " +
	            "ESCAPED BY '\\\\' " +
	            "LINES TERMINATED BY '\\n' " +
	            (hasHeader ? "IGNORE 1 LINES " : "") +
	            "(" + String.join(",", cols) + ")";

	    log("INFO", "MYSQL STAGING LOAD CMD: " + sql, serverIP, "MYSQL_STAGING_LOAD_SQL");

	    try (Statement stmt = conn.createStatement()) {
	        stmt.execute(sql);

	        long inserted = stmt.getUpdateCount();

	        log("INFO",
	                "CSV loaded into STAGING - Rows:" + inserted
	                        + " Cols:" + cols.size()
	                        + " File:" + csvFile.getName(),
	                serverIP, "MYSQL_STAGING_LOAD_DONE");

	        return inserted;
	    } catch (SQLException e) {
	        log("ERROR",
	                "MYSQL STAGING LOAD FAILED - table:" + stagingTable
	                        + " File:" + csvFile.getName()
	                        + " Error:" + e.getMessage(),
	                serverIP, "MYSQL_STAGING_LOAD_ERROR");
	        throw e;
	    }
	}

	private boolean isNumericType(String type) {
		return type.contains("int") || type.contains("decimal") || type.contains("numeric") || type.contains("float")
				|| type.contains("double") || type.contains("bigint") || type.contains("smallint")
				|| type.contains("tinyint");
	}

	private boolean isDateType(String type) {
		return type.contains("date") || type.contains("datetime") || type.contains("timestamp");
	}

	private Map<String, Long> findBadRowReasons(Connection conn, String stagingTable, List<ColumnMeta> cols)
			throws SQLException {
		Map<String, Long> reasonCount = new LinkedHashMap<>();

		// Build WHERE for each reason
		List<String> reasonQueries = new ArrayList<>();

		for (ColumnMeta c : cols) {

			// NOT NULL fail
			if (!c.nullable) {
			    reasonQueries.add("SELECT 'NOT_NULL_FAIL:" + c.name + "' AS reason, COUNT(*) cnt " + "FROM "
			            + stagingTable + " " + "WHERE " + c.name + " IS NULL OR UPPER(TRIM(" + c.name + "))='NULL'");
			}


			// NUMERIC fail
			if (isNumericType(c.type)) {
				reasonQueries.add("SELECT 'DATATYPE_FAIL_NUMERIC:" + c.name + "' AS reason, COUNT(*) cnt " + "FROM "
						+ stagingTable + " " + "WHERE " + c.name + " IS NOT NULL " + "AND TRIM(" + c.name + ") <> '' "
						+ "AND UPPER(TRIM(" + c.name + ")) <> 'NULL' " + "AND TRIM(" + c.name
						+ ") NOT REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$'");
			}

			// DATE fail (simple check)
			if (isDateType(c.type)) {
				reasonQueries.add("SELECT 'DATATYPE_FAIL_DATE:" + c.name + "' AS reason, COUNT(*) cnt " + "FROM "
						+ stagingTable + " " + "WHERE " + c.name + " IS NOT NULL " + "AND TRIM(" + c.name + ") <> '' "
						+ "AND UPPER(TRIM(" + c.name + ")) <> 'NULL' " + "AND STR_TO_DATE(" + c.name
						+ ", '%Y-%m-%d %H:%i:%s') IS NULL " + "AND STR_TO_DATE(" + c.name + ", '%Y-%m-%d') IS NULL");
			}
		}

		// Execute each reason query (fast enough, also clean)
		for (String rq : reasonQueries) {
			try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(rq)) {
				if (rs.next()) {
					String reason = rs.getString("reason");
					long cnt = rs.getLong("cnt");
					if (cnt > 0)
						reasonCount.put(reason, cnt);
				}
			}
		}

		return reasonCount;
	}

	private List<String> fetchSampleBadRows(Connection conn, String stagingTable, List<ColumnMeta> cols, int limit)
			throws SQLException {

		List<String> conditions = new ArrayList<>();

		for (ColumnMeta c : cols) {
			if (!c.nullable) {
			    conditions.add(
			        "(" + c.name + " IS NULL OR UPPER(TRIM(" + c.name + "))='NULL')");
			}

			if (isNumericType(c.type)) {
				conditions.add("(" + c.name + " IS NOT NULL AND TRIM(" + c.name + ")<>'' AND UPPER(TRIM(" + c.name
						+ "))<>'NULL' " + "AND TRIM(" + c.name + ") NOT REGEXP '^-?[0-9]+(\\\\.[0-9]+)?$')");
			}
			if (isDateType(c.type)) {
				conditions.add("(" + c.name + " IS NOT NULL AND TRIM(" + c.name + ")<>'' AND UPPER(TRIM(" + c.name
						+ "))<>'NULL' " + "AND STR_TO_DATE(" + c.name + ", '%Y-%m-%d %H:%i:%s') IS NULL "
						+ "AND STR_TO_DATE(" + c.name + ", '%Y-%m-%d') IS NULL)");
			}
		}

		String where = String.join(" OR ", conditions);

		String sql = "SELECT * FROM " + stagingTable + " WHERE " + where + " LIMIT " + limit;

		List<String> rows = new ArrayList<>();
		try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {

			ResultSetMetaData md = rs.getMetaData();
			int colCount = md.getColumnCount();

			while (rs.next()) {
				StringBuilder sb = new StringBuilder();
				sb.append("BAD_ROW_SAMPLE => ");

				for (int i = 1; i <= colCount; i++) {
					String col = md.getColumnName(i);
					String val = rs.getString(i);
					if (val == null)
						val = "NULL";

					// limit big values (like certificates)
					if (val.length() > 120)
						val = val.substring(0, 120) + "...(trimmed)";

					sb.append(col).append("=").append(val);
					if (i < colCount)
						sb.append(" | ");
				}

				rows.add(sb.toString());
			}
		}
		return rows;
	}

	private long insertFromStagingToFinal(Connection conn, String stagingTable, String finalTable,
			List<ColumnMeta> cols) throws SQLException {

		List<String> colNames = cols.stream().map(c -> c.name).collect(Collectors.toList());

		// For numeric/date columns we CAST safely
		List<String> selectExpr = new ArrayList<>();
		for (ColumnMeta c : cols) {
			if (isNumericType(c.type)) {
				selectExpr.add("NULLIF(TRIM(" + c.name + "), 'NULL')");
			} else if (isDateType(c.type)) {
				selectExpr.add("NULLIF(TRIM(" + c.name + "), 'NULL')");
			} else {
				selectExpr.add("NULLIF(" + c.name + ", 'NULL')");
			}
		}

		String sql = "INSERT INTO " + finalTable + " (" + String.join(",", colNames) + ") " + "SELECT "
				+ String.join(",", selectExpr) + " FROM " + stagingTable;

		try (Statement st = conn.createStatement()) {
			return st.executeUpdate(sql);
		}
	}

	private void closeQuietly(BufferedReader reader) {
		if (reader != null) {
			try {
				reader.close();
			} catch (IOException e) {
				System.err.println("Warning: Failed to close BufferedReader: " + e.getMessage());
			}
		}
	}

	// Main method for testing - creates instance
	public void main(String[] args) {

	}

}
