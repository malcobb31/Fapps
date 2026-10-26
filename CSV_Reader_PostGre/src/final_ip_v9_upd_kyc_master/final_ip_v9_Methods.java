package final_ip_v9_upd_kyc_master;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class final_ip_v9_Methods {

	private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	// private static final int FIXED_THREAD_COUNT = 5;
	private final String processName = "IPV8";
	private final String processVersion = "8.0.0";
	private final boolean debugEnabled = false;
	// private final Map<String, TableMeta> pgMetaCache = new
	// ConcurrentHashMap<>();
	private HikariDataSource postgresPool;
	private HikariDataSource mysqlPool;

	private final Comparator<Path> naturalFileComparator = (path1, path2) -> {
		String name1 = path1.getFileName().toString();
		String name2 = path2.getFileName().toString();
		return compareNaturally(name1, name2);
	};

	private HikariDataSource createPool(DatabaseConfig db) {

		HikariConfig config = new HikariConfig();

		config.setJdbcUrl(db.url);
		config.setUsername(db.user);
		config.setPassword(db.password);

		config.addDataSourceProperty("tcpKeepAlive", "true");

		// config.setMaximumPoolSize(FIXED_THREAD_COUNT + 2);
		config.setMinimumIdle(2);
		config.setLeakDetectionThreshold(60000);
		config.addDataSourceProperty("stringtype", "unspecified");

		config.setAutoCommit(false);

		config.addDataSourceProperty("rewriteBatchedStatements", "true");
		config.addDataSourceProperty("useServerPrepStmts", "true");
		config.addDataSourceProperty("reWriteBatchedInserts", "true");
		config.setConnectionTimeout(300000); // how much time will it wait to
												// get the connection from
												// conneciton pool
		config.addDataSourceProperty("binaryTransfer", "true");
		config.addDataSourceProperty("prepareThreshold", "0");
		config.setIdleTimeout(600000); // for how much time a connection can be
										// idle
		config.setMaxLifetime(0); // for how much time can the connect stay
									// alive

		return new HikariDataSource(config);
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

	private String capitalize(String level) {
		if (level == null || level.isEmpty())
			return "";
		return level.substring(0, 1).toUpperCase() + level.substring(1).toLowerCase();
	}

	// ---------- NEW: helper to format elapsed time as "X min Y secs"
	// ----------
	private String formatDuration(LocalDateTime start, LocalDateTime end) {
		long totalSeconds = java.time.Duration.between(start, end).getSeconds();
		if (totalSeconds < 0) {
			totalSeconds = 0;
		}
		long minutes = totalSeconds / 60;
		long seconds = totalSeconds % 60;
		return minutes + " min " + seconds + " secs";
	}

	public class MemoryAndCPUUsages {

		public String[] getMemoryAndCPUUsages() {

			String[] result = null;

			try {

				String[] memCmd = { "/bin/sh", "-c", "free | awk 'FNR == 2 {print ($3/$2)*100}'" };
				Process memProcess = Runtime.getRuntime().exec(memCmd);

				BufferedReader memBr = new BufferedReader(new InputStreamReader(memProcess.getInputStream()));

				String str;
				double currentMemory = 0;

				while ((str = memBr.readLine()) != null) {
					currentMemory = Double.parseDouble(str.trim());
				}

				memProcess.waitFor();
				memBr.close();
				memProcess.destroy();

				BufferedReader loadReader = new BufferedReader(new FileReader("/proc/loadavg"));

				String loadLine = loadReader.readLine();
				loadReader.close();

				double firstLoadAvg = 0;

				if (loadLine != null && !loadLine.isEmpty()) {

					String[] parts = loadLine.split("\\s+");
					firstLoadAvg = Double.parseDouble(parts[0]);
				}

				result = new String[] { String.valueOf(firstLoadAvg), String.valueOf(currentMemory) };

			} catch (Exception e) {
				e.printStackTrace();
			}

			return result;
		}
	}

	private boolean isEmptyOrHeaderlessCsv(Path file) throws IOException {
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(Files.newInputStream(file), java.nio.charset.StandardCharsets.ISO_8859_1))) {

			String firstLine = br.readLine();
			return firstLine == null || firstLine.trim().isEmpty();
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
				String bulkNumberStr, String dbTypeStr, final_ip_v9_Methods instance) {

			this.dirPath = dirPath;
			this.processedDirPath = processedDirPath;
			this.hasHeader = "yes".equalsIgnoreCase(headerFlag) || "true".equalsIgnoreCase(headerFlag);
			this.delimiter = (delimiter == null || delimiter.isEmpty()) ? "," : delimiter;
			this.numTablesToPick = Integer.parseInt(numTablesToPickStr);
			this.bulkSize = parseIntOrDefault(bulkNumberStr, 5000);
			this.threadCount = numTablesToPick;
			this.maxCpuThreshold = parseDoubleOrDefault(maxCpuUtil, 30.0);
			this.maxMemoryThreshold = parseDoubleOrDefault(maxMemUtil, 30.0);
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
				String pgUrl = String.format("jdbc:postgresql://%s:%s/%s?socketTimeout=0&tcpKeepAlive=true", pdbHost,
						pdbPort, pdbName);
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

		log("INFO", "CSV processing started", serverIP, "PROCESS_START");
		int[] usagesThreshold = new int[2];
		usagesThreshold[0] = Integer.parseInt(maxMemUtil); // memory threshold
		int ThreadAndTableToPick = Integer.parseInt(numTablesToPickStr); // 80%
		usagesThreshold[1] = Integer.parseInt(maxCpuUtil); // CPU threshold 70%

		logResourceUsage(usagesThreshold, serverIP);

		ProcessConfig config = new ProcessConfig(dirPath, processedDirPath, headerFlag, delimiter, sqldburl, sdbuser,
				sdbpass, serverIP, pdbHost, pdbPort, pdbName, pdbUser, pdbPass, destUrl, destUser, destPass,
				numTablesToPickStr, configTable, maxCpuUtil, maxMemUtil, bulkNumberStr, dbTypeStr, this);

		if ("psql".equalsIgnoreCase(config.dbType)) {
			postgresPool = createPool(config.targetDb);
		} else {
			mysqlPool = createPool(config.targetDb);
		}

		try {

			executeProcessing(config, serverIP, pdbHost, config.bulkSize, ThreadAndTableToPick);

			logResourceUsage(usagesThreshold, serverIP);
			log("INFO", "CSV processing completed successfully", serverIP, "PROCESS_END");
		} catch (Exception e) {
			log("ERROR", "Processing failed: " + e.getMessage(), serverIP, "PROCESS_ERROR");
			e.printStackTrace();
		} finally {

			if (postgresPool != null) {
				postgresPool.close();
			}

			if (mysqlPool != null) {
				mysqlPool.close();
			}
		}

	}

	private void executeProcessing(ProcessConfig config, String serverIP, String pdbHost, int bulkSize,
			int numTablesToPickStr) {

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
		processTablesInParallel(config, readyTables, tableFilesMap, serverIP, bulkSize, numTablesToPickStr);

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
		String selectQuery = "SELECT sno, table_name FROM " + config.configTable
				+ " WHERE sync_status = 'ready_to_import_new' LIMIT ?"; // CHANGE
																		// IT
		// AFTER
		// TEST

		String updateQuery = "UPDATE " + config.configTable
				+ " SET schedule_time = ?, last_inprocess_timestamp = ?, sync_status = 'in_process' WHERE sno = ?";

		String now = LocalDateTime.now().format(logFormat);

		try (Connection conn = DriverManager.getConnection(config.sourceDb.url, config.sourceDb.user,
				config.sourceDb.password)) {

			conn.setAutoCommit(false);

			try (PreparedStatement ps = conn.prepareStatement(selectQuery)) {
				ps.setInt(1, config.numTablesToPick);

				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						tables.add(new TableEntry(rs.getString("sno"), rs.getString("table_name")));
					}
				}
			}

			try (PreparedStatement ups = conn.prepareStatement(updateQuery)) {
				for (TableEntry table : tables) {
					ups.setString(1, now);
					ups.setString(2, now);
					ups.setString(3, table.sno);
					ups.addBatch();
				}
				ups.executeBatch();
			}

			conn.commit();

			log("INFO", "Tables picked and schedule_time updated - Count:" + tables.size(), serverIP, "TABLE_PICKED");

		} catch (Exception e) {
			log("ERROR", "Failed to fetch/update ready tables: " + e.getMessage(), serverIP, "TABLE_PICK_ERROR");
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
			Map<String, List<Path>> tableFilesMap, String serverIP, int bulkSize, int numTablesToPickStr) {

		log("INFO", String.format("Parallel processing started - Thread_Count:%d Tables_To_Process:%d",
				config.threadCount, readyTables.size()), serverIP, "PARALLEL_START");

		ExecutorService executor = Executors.newFixedThreadPool(numTablesToPickStr);
		List<Future<TableProcessResult>> futures = new ArrayList<>();

		for (TableEntry table : readyTables) {
			List<Path> files = tableFilesMap.getOrDefault(table.sno, Collections.emptyList());
			futures.add(executor.submit(() -> {

				try (Connection conn = "psql".equalsIgnoreCase(config.dbType) ? postgresPool.getConnection()
						: mysqlPool.getConnection()) {

					conn.setAutoCommit(false);
					conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
					if ("psql".equalsIgnoreCase(config.dbType)) {
						try (Statement st = conn.createStatement()) {
							st.execute("SET synchronous_commit = OFF");
							st.execute("SET wal_compression = ON");
							st.execute("SET work_mem = '256MB'");
							st.execute("SET maintenance_work_mem = '512MB'");

						}
					}

					return processTable(config, table, files, serverIP, bulkSize, conn);
				}
			}));
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
			int bulkSize, Connection conn) {

		LocalDateTime tableStart = LocalDateTime.now();

		log("INFO", "Table processing started - tableName:" + table.tableName + " SNO:" + table.sno + " Files_Count:"
				+ files.size(), serverIP, "TABLE_START");

		int processedCount = 0;

		try {

			if (files.isEmpty()) {

				LocalDateTime tableEnd = LocalDateTime.now();
				long tableDuration = java.time.Duration.between(tableStart, tableEnd).getSeconds();

				updateFinalTableStatus(table.sno, "success", config, serverIP);

				log("INFO",
						"TABLE_SUMMARY_SUCCESS | Table=" + table.tableName + ", SNO=" + table.sno
								+ ", File_Count=0, File_Inserted=0" + ", Duration=" + tableDuration + "s",
						serverIP, "TABLE_SUMMARY_SUCCESS");

				return new TableProcessResult(table, true, 0, null);
			}

			for (Path file : files) {

				// ---------- NEW: track per-CSV start time ----------
				LocalDateTime fileStart = LocalDateTime.now();
				String csvFileName = file.getFileName().toString();

				try {

					if (isEmptyOrHeaderlessCsv(file)) {
						log("WARNING",
								"CSV file is empty or header is missing - skipping file:" + file.getFileName()
										+ " Table:" + table.tableName + " CSV_File:" + csvFileName + " Target_Table:"
										+ table.tableName,
								serverIP, "CSV_EMPTY_SKIP");

						// moveFileToProcessed(file,
						// Paths.get(config.processedDirPath), serverIP);
						continue;
					}

					// -------- FILE SIZE ----------
					long fileSizeBytes = Files.size(file);
					double fileSizeMB = fileSizeBytes / (1024.0 * 1024.0);

					// -------- ROW COUNT ----------
					long rowCount = getRowCountUsingWC(file);
					if (config.hasHeader && rowCount > 0) {
						rowCount--;
					}

					// -------- PROCESS FILE ----------
					Path destPath = processFile(config, table, file, serverIP, bulkSize, conn);
					processedCount++;

					// -------- TIME CALC ----------
					// ---------- NEW: end time = when copy/import + move has
					// completed ----------
					LocalDateTime fileEnd = LocalDateTime.now();
					long durationMs = java.time.Duration.between(fileStart, fileEnd).toMillis();
					String csvDuration = formatDuration(fileStart, fileEnd); // NEW

					// -------- FINAL LOG ----------
					log("INFO", "FILE_SUMMARY_SUCCESS | " + "Table=" + table.tableName + ", File=" + file.getFileName()
							+ ", File_Size_MB=" + String.format("%.2f", fileSizeMB) + ", Row_Count=" + rowCount
							+ ", Time_Taken_MS=" + durationMs + ", Source=" + file.toAbsolutePath() + ", Destination="
							+ (destPath != null ? destPath.toAbsolutePath() : "NA") + ", CSV_File=" + csvFileName
							+ ", Target_Table=" + table.tableName + ", CSV_Total_Time=" + csvDuration, serverIP,
							"FILE_SUMMARY_SUCCESS");

				} catch (Exception fileEx) {

					// ---------- NEW: end time = exact moment of error
					// ----------
					LocalDateTime fileErrorTime = LocalDateTime.now();
					long duration = java.time.Duration.between(fileStart, fileErrorTime).getSeconds();
					String csvDuration = formatDuration(fileStart, fileErrorTime); // NEW

					log("ERROR", "FILE_SUMMARY_ERROR | Table=" + table.tableName + ", File=" + file.getFileName()
							+ ", Duration=" + duration + "s" + ", Error=" + fileEx.getMessage() + ", CSV_File="
							+ csvFileName + ", Target_Table=" + table.tableName + ", CSV_Total_Time=" + csvDuration,
							serverIP, "FILE_SUMMARY_ERROR");

					throw fileEx;
				}
			}

			LocalDateTime tableEnd = LocalDateTime.now();
			long tableDuration = java.time.Duration.between(tableStart, tableEnd).getSeconds();

			updateFinalTableStatus(table.sno, "success", config, serverIP);

			log("INFO",
					"TABLE_SUMMARY_SUCCESS | Table=" + table.tableName + ", SNO=" + table.sno + ", File_Count="
							+ files.size() + ", File_Inserted=" + processedCount + ", Duration=" + tableDuration + "s",
					serverIP, "TABLE_SUMMARY_SUCCESS");

			return new TableProcessResult(table, true, processedCount, null);

		} catch (Exception e) {

			LocalDateTime tableErrorTime = LocalDateTime.now();
			long tableDuration = java.time.Duration.between(tableStart, tableErrorTime).getSeconds();

			updateFinalTableStatus(table.sno, "error", config, serverIP);

			log("ERROR", "TABLE_SUMMARY_ERROR | Table=" + table.tableName + ", SNO=" + table.sno + ", Duration="
					+ tableDuration + "s" + ", Error=" + e.getMessage(), serverIP, "TABLE_SUMMARY_ERROR");

			return new TableProcessResult(table, false, processedCount, e.getMessage());
		}
	}

	private long getRowCountUsingWC(Path file) throws Exception {

		ProcessBuilder pb = new ProcessBuilder("wc", "-l", file.toAbsolutePath().toString());
		pb.redirectErrorStream(true);

		Process process = pb.start();

		BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

		String output = reader.readLine();

		int exitCode = process.waitFor();
		reader.close();
		process.destroy();

		if (exitCode != 0 || output == null) {
			throw new RuntimeException("wc -l failed for file: " + file);
		}

		// Output format: "12345 filename.csv"
		String[] parts = output.trim().split("\\s+");
		return Long.parseLong(parts[0]);
	}

	private void updateFinalTableStatus(String sno, String status, ProcessConfig config, String serverIP) {

		String now = LocalDateTime.now().format(logFormat);

		String query;

		if ("success".equalsIgnoreCase(status)) {
			query = "UPDATE " + config.configTable
					+ " SET last_import_timestamp = ?, remarks = 'success', sync_status = 'init' WHERE sno = ?";
		} else {
			query = "UPDATE " + config.configTable + " SET remarks = 'error', sync_status = 'error' WHERE sno = ?";
		}

		try (Connection conn = DriverManager.getConnection(config.sourceDb.url, config.sourceDb.user,
				config.sourceDb.password); PreparedStatement ps = conn.prepareStatement(query)) {

			if ("success".equalsIgnoreCase(status)) {
				ps.setString(1, now);
				ps.setString(2, sno);
			} else {
				ps.setString(1, sno);
			}

			ps.executeUpdate();

			log("INFO", "Final table status updated - SNO=" + sno + " Status=" + status, serverIP,
					"FINAL_STATUS_UPDATE");

		} catch (Exception e) {
			log("ERROR", "Failed to update final table status - SNO=" + sno + " Error=" + e.getMessage(), serverIP,
					"FINAL_STATUS_ERROR");
		}
	}

	private Path processFile(ProcessConfig config, TableEntry table, Path file, String serverIP, int bulkSize,
			Connection conn) throws Exception {

		// ---------- NEW: per-CSV tracking variables for this scope ----------
		String csvFileName = file.getFileName().toString();
		LocalDateTime fileProcessStart = LocalDateTime.now();

		boolean tableExists = tableExists(conn, table.tableName);

		if (!tableExists) {
			log("INFO", String.format("Table not found, will be created - tableName:%s CSV_File:%s Target_Table:%s",
					table.tableName, csvFileName, table.tableName), serverIP, "TABLE_NOT_FOUND");
		} else {
			log("INFO", String.format("Table found - tableName:%s CSV_File:%s Target_Table:%s", table.tableName,
					csvFileName, table.tableName), serverIP, "TABLE_FOUND");
		}

		try {
			if ("psql".equalsIgnoreCase(config.dbType)) {
				processFilePostgreSQL(config, table, file, serverIP, bulkSize, conn, fileProcessStart);
			} else {
				processFileMySQL(config, table, file, serverIP, conn, fileProcessStart);
			}
		} catch (Exception e) {
			// ---------- NEW: error end time captured at point of throw inside
			// called methods; rethrow unchanged ----------
			throw e;
		}

		return moveFileToProcessed(file, Paths.get(config.processedDirPath), serverIP);
	}

	private void processFilePostgreSQL(ProcessConfig config, TableEntry table, Path file, String serverIP, int bulkSize,
			Connection conn, LocalDateTime fileProcessStart) throws Exception {

		String csvFileName = file.getFileName().toString(); // NEW

		boolean exists = tableExists(conn, table.tableName);

		if (!exists) {

			// createTableUsingPsql(table.tableName, file.toString(),
			// config.delimiter, serverIP, conn);

			log("INFO", "Table - tableName:" + table.tableName + " Database:PostgreSQL" + " CSV_File:" + csvFileName
					+ " Target_Table:" + table.tableName, serverIP, "TABLE_NOT_CREATED");
		}

		importCSVUsingPsqlCLI(config, table.tableName, file, serverIP, conn, fileProcessStart);
	}

	private void processFileMySQL(ProcessConfig config, TableEntry table, Path file, String serverIP, Connection conn,
			LocalDateTime fileProcessStart) throws Exception {

		Process process = null;
		BufferedReader reader = null;

		String csvFileName = file.getFileName().toString(); // NEW

		try {

			log("INFO",
					String.format("Starting MySQL CLI import - tableName:%s File:%s CSV_File:%s Target_Table:%s",
							table.tableName, file.getFileName(), csvFileName, table.tableName),
					serverIP, "MYSQL_CLI_IMPORT_START");

			String filePath = file.toAbsolutePath().toString();

			// Resolve database name
			String database = config.targetDb.database != null ? config.targetDb.database
					: extractDatabaseFromUrl(config.targetDb.url);

			if (database == null || database.isEmpty()) {
				throw new RuntimeException("Database name could not be resolved for MySQL import");
			}

			// Build command WITHOUT password in CLI
			String command = String.format("mysql -u %s %s < \"%s\"", config.targetDb.user, database, filePath);

			ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);

			// Secure password via environment variable
			pb.environment().put("MYSQL_PWD", config.targetDb.password);

			pb.redirectErrorStream(true);

			process = pb.start();

			reader = new BufferedReader(new InputStreamReader(process.getInputStream()));

			String line;
			StringBuilder output = new StringBuilder();

			while ((line = reader.readLine()) != null) {
				output.append(line).append("\n");
			}

			// Wait with timeout (important for large imports)
			boolean finished = process.waitFor(60, TimeUnit.MINUTES);

			if (!finished) {
				process.destroyForcibly();
				throw new RuntimeException("MySQL CLI import timed out");
			}

			int exitCode = process.exitValue();

			if (exitCode != 0) {
				throw new RuntimeException("MySQL CLI failed. ExitCode=" + exitCode + " Output=" + output.toString());
			}

			// ---------- NEW: success end time + duration ----------
			LocalDateTime fileProcessEnd = LocalDateTime.now();
			String csvDuration = formatDuration(fileProcessStart, fileProcessEnd);

			log("INFO",
					String.format(
							"MySQL CLI import completed successfully - tableName:%s File:%s CSV_File:%s Target_Table:%s CSV_Total_Time:%s",
							table.tableName, file.getFileName(), csvFileName, table.tableName, csvDuration),
					serverIP, "MYSQL_CLI_IMPORT_SUCCESS");

		} catch (Exception e) {

			// ---------- NEW: error end time + duration ----------
			LocalDateTime fileProcessErrorTime = LocalDateTime.now();
			String csvDuration = formatDuration(fileProcessStart, fileProcessErrorTime);

			log("ERROR",
					"MySQL CLI import failed - tableName:" + table.tableName + " Error:" + e.getMessage() + " CSV_File:"
							+ csvFileName + " Target_Table:" + table.tableName + " CSV_Total_Time:" + csvDuration,
					serverIP, "MYSQL_CLI_IMPORT_ERROR");

			throw e;

		} finally {

			closeQuietly(reader);

			if (process != null) {
				process.destroy();
			}
		}
	}

	private String extractDatabaseFromUrl(String jdbcUrl) {
		if (jdbcUrl == null)
			return "";
		int slashIndex = jdbcUrl.lastIndexOf("/");
		if (slashIndex != -1 && slashIndex + 1 < jdbcUrl.length()) {
			return jdbcUrl.substring(slashIndex + 1);
		}
		return "";
	}

	// File moving with proper resource management
	private Path moveFileToProcessed(Path sourceFile, Path processedDir, String serverIP) {
		try {
			Files.createDirectories(processedDir);
			Path targetFile = processedDir.resolve(sourceFile.getFileName());

			if (Files.exists(targetFile)) {
				String fileName = sourceFile.getFileName().toString();
				String baseName = fileName.substring(0, fileName.lastIndexOf('.'));
				String extension = fileName.substring(fileName.lastIndexOf('.'));
				String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
				targetFile = processedDir.resolve(baseName + "_" + timestamp + extension);
			}

			Files.move(sourceFile, targetFile, StandardCopyOption.REPLACE_EXISTING);

			log("INFO", String.format("File moved - Source:%s Target:%s", sourceFile.toString(), targetFile.toString()),
					serverIP, "FILE_MOVED");

			return targetFile;

		} catch (IOException e) {
			log("ERROR",
					String.format("Failed to move file - Source:%s Error:%s", sourceFile.toString(), e.getMessage()),
					serverIP, "FILE_MOVE_ERROR");

			return null;
		}
	}

	private List<String[]> getTableColumns(Connection conn, String tableName) throws Exception {

		String schema = "public";
		String table = tableName;

		if (tableName.contains(".")) {
			String[] parts = tableName.split("\\.");
			schema = parts[0];
			table = parts[1];
		}

		List<String[]> columns = new ArrayList<>();

		String query = "SELECT column_name, data_type " + "FROM information_schema.columns "
				+ "WHERE table_schema = ? AND table_name = ? " + "ORDER BY ordinal_position";

		try (PreparedStatement ps = conn.prepareStatement(query)) {

			ps.setString(1, schema);
			ps.setString(2, table);

			ResultSet rs = ps.executeQuery();

			while (rs.next()) {

				columns.add(new String[] { rs.getString("column_name"), rs.getString("data_type") });
			}
		}

		return columns;
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
				String query = "SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' "
						+ "AND table_name = ? LIMIT 1";

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

	// MySQL table creation with proper resource management

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

	private String runPsqlStep(ProcessConfig config, String stepSql, String serverIP) throws Exception {

		Process process = null;
		BufferedReader reader = null;

		try {
			ProcessBuilder pb = new ProcessBuilder("psql", "-v", "ON_ERROR_STOP=1", "-h", config.targetDb.host, "-p",
					config.targetDb.port, "-U", config.targetDb.user, "-d", config.targetDb.database);

			pb.environment().put("PGPASSWORD", config.targetDb.password);
			pb.redirectErrorStream(true);

			process = pb.start();

			try (OutputStream os = process.getOutputStream();
					BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
				writer.write(stepSql);
				writer.flush();
			}

			reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line;
			StringBuilder output = new StringBuilder();

			while ((line = reader.readLine()) != null) {
				output.append(line).append("\n");
			}

			boolean finished = process.waitFor(60, TimeUnit.MINUTES);
			if (!finished) {
				process.destroyForcibly();
				throw new RuntimeException("PostgreSQL CLI step timed out. SQL=" + stepSql);
			}

			int exitCode = process.exitValue();
			String psqlOutput = output.toString();

			if (exitCode != 0 || containsPsqlError(psqlOutput)) {
				throw new RuntimeException("PostgreSQL CLI step failed. ExitCode=" + exitCode + " Output=" + psqlOutput
						+ " SQL=" + stepSql);
			}

			return psqlOutput;

		} finally {
			closeQuietly(reader);
			if (process != null) {
				process.destroy();
			}
		}
	}


	private void importCSVUsingPsqlCLI(ProcessConfig config, String tableName, Path file, String serverIP,
	        Connection conn, LocalDateTime fileProcessStart) throws Exception {

	    String stagingTable = tableName + "_staging";
	    String csvFileName = file.getFileName().toString();
	    String currentStep = "INIT";

	    try {

	        log("INFO",
	                String.format(
	                        "Starting PostgreSQL CLI import - tableName:%s File:%s CSV_File:%s Target_Table:%s",
	                        tableName,
	                        file.getFileName(),
	                        csvFileName,
	                        tableName),
	                serverIP,
	                "PSQL_CLI_IMPORT_START");

	        /*
	         * Get target table columns and PostgreSQL data types.
	         */
	        List<String[]> columns = getTableColumns(conn, tableName);

	        if (columns == null || columns.isEmpty()) {
	            throw new RuntimeException("No columns found for table: " + tableName);
	        }

	        Map<String, String> dbColMap = new LinkedHashMap<String, String>();

	        StringBuilder createCols = new StringBuilder();
	        StringBuilder insertCols = new StringBuilder();
	        StringBuilder selectCols = new StringBuilder();

	        /*
	         * Build:
	         *
	         * 1. Staging table definition
	         * 2. Target INSERT column list
	         * 3. SELECT conversion expressions
	         */
	        for (String[] col : columns) {

	            String colName = col[0];

	            /*
	             * Skip auto-generated ID.
	             */
	            if ("id".equalsIgnoreCase(colName)) {
	                continue;
	            }

	            String dataType = col[1];
	            String quotedCol = quotePgIdent(colName);

	            dbColMap.put(
	                    colName.toLowerCase(Locale.ROOT),
	                    colName
	            );

	            /*
	             * ---------------------------------------------------------
	             * STAGING TABLE
	             * ---------------------------------------------------------
	             *
	             * Load these types as TEXT first:
	             *
	             * bytea
	             * json
	             * jsonb
	             * date
	             * timestamp without time zone
	             * timestamp with time zone
	             * bigint
	             *
	             * This prevents COPY from failing on legacy/raw values such
	             * as:
	             *
	             *     1782570554
	             *     -1 (4294967295)
	             */
	            if ("bytea".equalsIgnoreCase(dataType)
	                    || "json".equalsIgnoreCase(dataType)
	                    || "jsonb".equalsIgnoreCase(dataType)
	                    || "date".equalsIgnoreCase(dataType)
	                    || "timestamp without time zone".equalsIgnoreCase(dataType)
	                    || "timestamp with time zone".equalsIgnoreCase(dataType)
	                    || "bigint".equalsIgnoreCase(dataType)) {

	                createCols.append(quotedCol)
	                        .append(" TEXT,");

	            } else {

	                createCols.append(quotedCol)
	                        .append(" ")
	                        .append(dataType)
	                        .append(",");
	            }

	            /*
	             * Target INSERT columns.
	             */
	            insertCols.append(quotedCol)
	                    .append(",");

	            /*
	             * ---------------------------------------------------------
	             * BYTEA
	             * ---------------------------------------------------------
	             */
	            if ("bytea".equalsIgnoreCase(dataType)) {

	                selectCols.append("decode(")
	                        .append(quotedCol)
	                        .append(", 'hex'),");

	            }

	            /*
	             * ---------------------------------------------------------
	             * JSON
	             * ---------------------------------------------------------
	             */
	            else if ("json".equalsIgnoreCase(dataType)) {

	                selectCols.append(quotedCol)
	                        .append("::json,");

	            }

	            /*
	             * ---------------------------------------------------------
	             * JSONB
	             * ---------------------------------------------------------
	             */
	            else if ("jsonb".equalsIgnoreCase(dataType)) {

	                selectCols.append(quotedCol)
	                        .append("::jsonb,");

	            }

	            /*
	             * ---------------------------------------------------------
	             * DATE
	             * ---------------------------------------------------------
	             *
	             * Supports:
	             *
	             *     2026-06-27
	             *     1782570554
	             *
	             * Epoch values are converted to IST first.
	             */
	            else if ("date".equalsIgnoreCase(dataType)) {

	                selectCols.append(
	                        "CASE "
	                                + "WHEN " + quotedCol + " IS NULL "
	                                + "OR TRIM(" + quotedCol + ") = '' "
	                                + "THEN NULL "

	                                + "WHEN TRIM(" + quotedCol + ") ~ '^[0-9]+$' "
	                                + "THEN ("
	                                + "to_timestamp(TRIM(" + quotedCol + ")::double precision) "
	                                + "AT TIME ZONE 'Asia/Kolkata'"
	                                + ")::date "

	                                + "ELSE " + quotedCol + "::date "
	                                + "END,"
	                );
	            }

	            /*
	             * ---------------------------------------------------------
	             * TIMESTAMP WITHOUT TIME ZONE
	             * ---------------------------------------------------------
	             *
	             * Supports:
	             *
	             *     2026-06-27 21:39:14
	             *     1782570554
	             *
	             * Epoch is explicitly converted to IST.
	             */
	            else if ("timestamp without time zone".equalsIgnoreCase(dataType)) {

	                selectCols.append(
	                        "CASE "
	                                + "WHEN " + quotedCol + " IS NULL "
	                                + "OR TRIM(" + quotedCol + ") = '' "
	                                + "THEN NULL "

	                                + "WHEN TRIM(" + quotedCol + ") ~ '^[0-9]+$' "
	                                + "THEN ("
	                                + "to_timestamp(TRIM(" + quotedCol + ")::double precision) "
	                                + "AT TIME ZONE 'Asia/Kolkata'"
	                                + ") "

	                                + "ELSE " + quotedCol + "::timestamp "
	                                + "END,"
	                );
	            }

	            /*
	             * ---------------------------------------------------------
	             * TIMESTAMP WITH TIME ZONE
	             * ---------------------------------------------------------
	             */
	            else if ("timestamp with time zone".equalsIgnoreCase(dataType)) {

	                selectCols.append(
	                        "CASE "
	                                + "WHEN " + quotedCol + " IS NULL "
	                                + "OR TRIM(" + quotedCol + ") = '' "
	                                + "THEN NULL "

	                                + "WHEN TRIM(" + quotedCol + ") ~ '^[0-9]+$' "
	                                + "THEN to_timestamp("
	                                + "TRIM(" + quotedCol + ")::double precision"
	                                + ") "

	                                + "ELSE " + quotedCol + "::timestamptz "
	                                + "END,"
	                );
	            }

	            /*
	             * ---------------------------------------------------------
	             * BIGINT
	             * ---------------------------------------------------------
	             *
	             * Supports normal values:
	             *
	             *     123
	             *     456789
	             *     -1
	             *
	             * And the legacy unsigned representation:
	             *
	             *     -1 (4294967295)
	             *
	             * which is converted to:
	             *
	             *     -1
	             */
	            else if ("bigint".equalsIgnoreCase(dataType)) {

	                selectCols.append(
	                        "CASE "
	                                + "WHEN " + quotedCol + " IS NULL "
	                                + "OR TRIM(" + quotedCol + ") = '' "
	                                + "THEN NULL "

	                                + "WHEN TRIM(" + quotedCol
	                                + ") ~ '^-1[[:space:]]*\\([[:space:]]*4294967295[[:space:]]*\\)$' "
	                                + "THEN -1::bigint "

	                                + "ELSE TRIM(" + quotedCol + ")::bigint "
	                                + "END,"
	                );
	            }

	            /*
	             * ---------------------------------------------------------
	             * ALL OTHER DATA TYPES
	             * ---------------------------------------------------------
	             */
	            else {

	                selectCols.append(quotedCol)
	                        .append(",");
	            }
	        }

	        /*
	         * Remove trailing commas.
	         */
	        createCols.setLength(createCols.length() - 1);
	        insertCols.setLength(insertCols.length() - 1);
	        selectCols.setLength(selectCols.length() - 1);

	        /*
	         * ---------------------------------------------------------
	         * READ CSV HEADERS
	         * ---------------------------------------------------------
	         */
	        String[] csvHeaders = readCsvHeaders(file, config.delimiter);

	        if (csvHeaders == null || csvHeaders.length == 0) {

	            log("WARNING",
	                    "CSV file is empty or header is missing - skipping import for tableName:"
	                            + tableName
	                            + " File:" + file.getFileName()
	                            + " CSV_File:" + csvFileName
	                            + " Target_Table:" + tableName,
	                    serverIP,
	                    "CSV_EMPTY_SKIP");

	            return;
	        }

	        log("INFO",
	                "CSV Headers: " + Arrays.toString(csvHeaders)
	                        + " CSV_File:" + csvFileName
	                        + " Target_Table:" + tableName,
	                serverIP,
	                "CSV_HEADERS");

	        /*
	         * ---------------------------------------------------------
	         * MAP CSV HEADERS TO DATABASE COLUMNS
	         * ---------------------------------------------------------
	         */
	        List<String> copyCols = new ArrayList<String>();

	        for (String header : csvHeaders) {

	            String normalizedHeader =
	                    normalizeCsvHeader(header)
	                            .toLowerCase(Locale.ROOT);

	            String actualDbCol =
	                    dbColMap.get(normalizedHeader);

	            if (actualDbCol == null) {

	                throw new RuntimeException(
	                        "CSV header column not found in target table: "
	                                + header
	                                + " for table: "
	                                + tableName
	                );
	            }

	            copyCols.add(
	                    quotePgIdent(actualDbCol)
	            );
	        }

	        String copyColumnList =
	                "(" + String.join(",", copyCols) + ")";

	        /*
	         * ---------------------------------------------------------
	         * EXECUTE SINGLE TRANSACTION
	         * ---------------------------------------------------------
	         */
	        currentStep = "RUN_SINGLE_TRANSACTION";

	        String sql =
	                "BEGIN;\n" +

	                "DROP TABLE IF EXISTS " + stagingTable + ";\n" +

	                /*
	                 * Create staging table.
	                 */
	                "CREATE UNLOGGED TABLE " + stagingTable
	                        + " (" + createCols + ");\n" +

	                /*
	                 * COPY raw CSV values into staging.
	                 */
	                "\\COPY " + stagingTable
	                        + " " + copyColumnList
	                        + " FROM '"
	                        + file.toAbsolutePath()
	                                .toString()
	                                .replace("'", "''")
	                        + "' "
	                        + "WITH (FORMAT csv, "
	                        + "HEADER true, "
	                        + "DELIMITER '|', "
	                        + "QUOTE '\"', "
	                        + "ESCAPE '\\', "
	                        + "NULL '\\N');\n" +

	                /*
	                 * Convert staging TEXT values into actual target types.
	                 */
	                "INSERT INTO " + tableName
	                        + " (" + insertCols + ") "
	                        + "SELECT " + selectCols
	                        + " FROM " + stagingTable
	                        + " ON CONFLICT DO NOTHING;\n" +

	                "DROP TABLE IF EXISTS " + stagingTable + ";\n" +

	                "COMMIT;\n";

	        log("INFO",
	                "Step RUN_SINGLE_TRANSACTION starting - CSV_File:"
	                        + csvFileName
	                        + " Target_Table:" + tableName,
	                serverIP,
	                "PSQL_STEP_START");

	        String output =
	                runPsqlStep(config, sql, serverIP);

	        long copyCount =
	                extractPsqlCount(output, "COPY");

	        long insertCount =
	                extractInsertCount(output);

	        log("INFO",
	                "Step RUN_SINGLE_TRANSACTION completed - CopiedRows:"
	                        + copyCount
	                        + " InsertedRows:" + insertCount
	                        + " CSV_File:" + csvFileName
	                        + " Target_Table:" + tableName,
	                serverIP,
	                "PSQL_STEP_SUCCESS");

	        /*
	         * COPY succeeded but nothing inserted.
	         */
	        if (insertCount == 0 && copyCount > 0) {

	            log("WARNING",
	                    "No rows inserted (possibly duplicates / ON CONFLICT DO NOTHING) - tableName:"
	                            + tableName
	                            + " File:" + file.getFileName()
	                            + " CSV_File:" + csvFileName
	                            + " Target_Table:" + tableName,
	                    serverIP,
	                    "PSQL_NO_INSERTS");
	        }

	        /*
	         * Success duration.
	         */
	        LocalDateTime fileProcessEnd =
	                LocalDateTime.now();

	        String csvDuration =
	                formatDuration(
	                        fileProcessStart,
	                        fileProcessEnd
	                );

	        log("INFO",
	                String.format(
	                        "PostgreSQL CLI import completed successfully - "
	                                + "tableName:%s File:%s CopiedRows:%d "
	                                + "InsertedRows:%d CSV_File:%s "
	                                + "Target_Table:%s CSV_Total_Time:%s",
	                        tableName,
	                        file.getFileName(),
	                        copyCount,
	                        insertCount,
	                        csvFileName,
	                        tableName,
	                        csvDuration),
	                serverIP,
	                "PSQL_CLI_IMPORT_SUCCESS");

	    } catch (Exception e) {

	        StringWriter sw =
	                new StringWriter();

	        e.printStackTrace(
	                new PrintWriter(sw)
	        );

	        LocalDateTime fileProcessErrorTime =
	                LocalDateTime.now();

	        String csvDuration =
	                formatDuration(
	                        fileProcessStart,
	                        fileProcessErrorTime
	                );

	        log("ERROR",
	                "PostgreSQL CLI import failed - tableName:"
	                        + tableName
	                        + " Failed_At_Step:" + currentStep
	                        + " Error:" + e.getMessage()
	                        + " StackTrace:" + sw.toString()
	                        + " CSV_File:" + csvFileName
	                        + " Target_Table:" + tableName
	                        + " CSV_Total_Time:" + csvDuration,
	                serverIP,
	                "PSQL_CLI_IMPORT_ERROR");

	        throw e;
	    }
	}


	private String[] readCsvHeaders(Path file, String delimiter) throws IOException {
		try (BufferedReader br = new BufferedReader(
				new InputStreamReader(Files.newInputStream(file), java.nio.charset.StandardCharsets.ISO_8859_1))) {

			String headerLine = br.readLine();
			if (headerLine == null || headerLine.trim().isEmpty()) {
				return new String[0];
			}

			return Arrays.stream(headerLine.split(Pattern.quote(delimiter), -1)).map(this::normalizeCsvHeader)
					.filter(s -> !s.isEmpty()).toArray(String[]::new);
		}
	}

	private String normalizeCsvHeader(String header) {
		if (header == null)
			return "";
		String h = header.trim();
		if (h.startsWith("\"") && h.endsWith("\"") && h.length() >= 2) {
			h = h.substring(1, h.length() - 1);
		}
		return h.trim();
	}

	private String quotePgIdent(String ident) {
		return "\"" + ident.replace("\"", "\"\"") + "\"";
	}

	private boolean containsPsqlError(String output) {
		if (output == null)
			return false;
		String upper = output.toUpperCase(Locale.ROOT);
		return upper.contains("ERROR:") || upper.contains("FATAL:") || upper.contains("PANIC:");
	}

	private long extractPsqlCount(String output, String token) {
		if (output == null)
			return 0L;

		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(?m)^" + token + "\\s+(\\d+)\\s*$");
		java.util.regex.Matcher matcher = pattern.matcher(output);

		long value = 0L;
		while (matcher.find()) {
			value = Long.parseLong(matcher.group(1));
		}
		return value;
	}

	private long extractInsertCount(String output) {
		if (output == null)
			return 0L;

		// Typical psql output: INSERT 0 123
		java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(?m)^INSERT\\s+\\d+\\s+(\\d+)\\s*$");
		java.util.regex.Matcher matcher = pattern.matcher(output);

		long value = 0L;
		while (matcher.find()) {
			value = Long.parseLong(matcher.group(1));
		}
		return value;
	}

	public void insertPartialCSV(String csvFilePath, String tableName, Connection conn, String serverIP,
			String[] csvHeaders, List<String> dbColumns, boolean hasHeader, String delimiter) throws Exception {

		File csvFile = new File(csvFilePath);
		if (!csvFile.exists() || !csvFile.canRead()) {
			throw new FileNotFoundException("CSV file not found or not readable: " + csvFile.getAbsolutePath());
		}

		// Clean CSV headers
		List<String> csvCols = Arrays.stream(csvHeaders).map(h -> h == null ? "" : h.replace("\"", "").trim())
				.filter(h -> !h.isEmpty()).collect(Collectors.toList());

		if (csvCols.isEmpty()) {
			throw new Exception("CSV header is empty/invalid: " + csvFile.getName());
		}

		// Match CSV columns with DB columns
		List<String> matchedCols = new ArrayList<>();
		for (String csvCol : csvCols) {
			for (String dbCol : dbColumns) {
				if (dbCol != null && dbCol.equalsIgnoreCase(csvCol)) {
					matchedCols.add(dbCol);
					break;
				}
			}
		}

		if (matchedCols.isEmpty()) {
			throw new Exception("No matching columns found between CSV and table: " + tableName);
		}

		// Build column list
		String columnSql = matchedCols.stream().map(c -> "`" + c.replace("`", "") + "`")
				.collect(Collectors.joining(","));

		String filePath = csvFile.getAbsolutePath().replace("\\", "/");
		String ignoreHeader = hasHeader ? "IGNORE 1 ROWS " : "";

		String loadSql = "LOAD DATA INFILE '" + filePath + "' " + "IGNORE INTO TABLE " + tableName + " "
				+ "FIELDS TERMINATED BY '" + delimiter + "' " + "ENCLOSED BY '\"' " + "LINES TERMINATED BY '\\n' "
				+ ignoreHeader + "(" + columnSql + ")";

		log("INFO", "LOAD SQL: " + loadSql, serverIP, "MYSQL_LOAD_SQL");

		try (Statement stmt = conn.createStatement()) {

			stmt.execute(loadSql);
			conn.commit();

			log("INFO", "LOAD DATA completed successfully - table=" + tableName + " File=" + csvFile.getName(),
					serverIP, "MYSQL_LOAD_SUCCESS");

		} catch (Exception e) {

			conn.rollback();

			log("ERROR", "LOAD DATA failed - table=" + tableName + " Error=" + e.getMessage(), serverIP,
					"MYSQL_LOAD_ERROR");

			throw e;
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