package binlog_reader_post_v2;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class binlog_reader_post_methods_old_working {

	private static final int BATCH_SIZE = 500;

	private static final String COL_ID = "id";
	private static final String COL_FILE_NAME = "file_name";
	private static final String COL_STATUS = "status";
	private static final String COL_SOURCE_PATH = "source_path";
	private static final String COL_DEST_PATH = "dest_path";
	private static final String COL_LAST_PROCESSED = "last_time_processed";

	private static final String STATUS_INIT = "INIT";
	private static final String STATUS_IN_PROCESS = "IN_PROCESS";
	private static final String STATUS_COMPLETED = "COMPLETED";
	private static final String STATUS_FAILED = "FAILED";

	private static final String SQL_TRIGGER_INSERT = "INSERT INTO data_insertion_trigger (record_time, table_name, primary_columns_name, primary_columns_value, type_of_operation,sync_status) VALUES (?, ?, ?, ?, ?,'init')";

	private static final String SQL_COLUMN_META = "SELECT column_name, ordinal_position FROM information_schema.columns WHERE table_name = ? AND table_schema = ? ORDER BY ordinal_position";

	private static final String SQL_PK_META = "SELECT column_name FROM information_schema.key_column_usage WHERE table_name = ? AND table_schema = ? AND constraint_name = 'PRIMARY'";

	private static final Pattern COLUMN_PATTERN = Pattern.compile("@(\\d+)=(.*)");

	private static final String EVT_START = "START";
	private static final String EVT_FETCH_FILE = "FETCH_FILE";
	private static final String EVT_STATUS_UPDATE = "STATUS_UPDATE";
	private static final String EVT_BINLOG_READ = "BINLOG_READ";
	private static final String EVT_METADATA_LOAD = "METADATA_LOAD";
	private static final String EVT_DB_WRITE = "DB_WRITE";
	private static final String EVT_FILE_MOVE = "FILE_MOVE";
	private static final String EVT_COMPLETED = "COMPLETED";
	private static final String EVT_FAILED = "FAILED";
	private static final String EVT_SUMMARY = "SUMMARY";
	private static final String EVT_ERROR = "ERROR";
	private static final String EVT_END = "END";

	private final String processName;
	private final String processVersion;
	private final DateTimeFormatter logFormat;
	private final boolean debugEnabled;
	private final String configTableName;

	private String mysqlUrl;
	private String mysqlUser;
	private String mysqlPass;

	private final Map<String, TableMetadata> metadataCache = new ConcurrentHashMap<>();

	// Sequential DB-write state (no worker pool / no queue — strictly in-order
	// writes)
	private PreparedStatement triggerPs;
	private Connection triggerConn;
	private List<TriggerRow> eventBatch;

	private final AtomicLong totalRowsInserted = new AtomicLong(0L);

	private String currentOperation;
	private String currentTable;
	private boolean readingWhere;
	private boolean readingSet;
	private Map<String, String> whereValues;
	private Map<String, String> setValues;
	private long eventsQueued;

	public binlog_reader_post_methods_old_working(String processName, String processVersion, DateTimeFormatter logFormat,
			boolean debugEnabled, String configTableName) {

		this.processName = processName;
		this.processVersion = processVersion;
		this.logFormat = logFormat;
		this.debugEnabled = debugEnabled;
		this.configTableName = validateIdentifier(configTableName);
	}

	/**
	 * Processes INIT-status files one after another (strictly sequential —
	 * never in parallel), because these binlog files are a continuation of each
	 * other and must be applied in order.
	 *
	 * @param maxFilesToProcess
	 *            optional cap on number of files to process in this run. Pass
	 *            null to process ALL files currently in INIT status.
	 */
	public void runPostProcess(String mysqlbinlogPath, String archiveFolder, String mysqlUrl, String mysqlUser,
			String mysqlPass, Integer maxFilesToProcess) throws Exception {

		this.mysqlUrl = mysqlUrl;
		this.mysqlUser = mysqlUser;
		this.mysqlPass = mysqlPass;

		log("INFO", "Post-process pipeline started. MaxFilesToProcess: "
				+ (maxFilesToProcess != null ? maxFilesToProcess : "ALL (no limit)"), null, EVT_START);

		File archiveDir = prepareArchiveFolder(archiveFolder);

		int completedCount = 0;
		int failedCount = 0;

		try (Connection controlConn = openConnection(mysqlUrl, mysqlUser, mysqlPass)) {
			controlConn.setAutoCommit(true);

			while (maxFilesToProcess == null || (completedCount + failedCount) < maxFilesToProcess) {

				ConfigRow configRow = fetchNextInitFile(controlConn);
				if (configRow == null) {
					log("INFO", "No more INIT files found in [" + configTableName + "].", null, EVT_FETCH_FILE);
					break;
				}

				log("INFO", "File picked for processing. ID: [" + configRow.id + "] FileName: [" + configRow.fileName
						+ "] SourcePath: [" + configRow.sourcePath + "]", null, EVT_FETCH_FILE);

				updateStatus(controlConn, configRow.id, STATUS_IN_PROCESS);

				log("INFO", "Status changed to IN_PROCESS. ID: [" + configRow.id + "] FileName: [" + configRow.fileName
						+ "]", null, EVT_STATUS_UPDATE);

				boolean parseSuccess = parseBinlogFile(mysqlbinlogPath, configRow);

				if (parseSuccess) {
					String movedPath = moveFile(configRow.sourcePath, archiveDir);
					updateCompleted(controlConn, configRow.id, movedPath);

					log("INFO",
							"Processing COMPLETED. ID: [" + configRow.id + "] FileName: [" + configRow.fileName
									+ "] ArchivedTo: [" + movedPath + "] RowsInserted: " + totalRowsInserted.get()
									+ " EventsQueued: " + eventsQueued,
							null, EVT_COMPLETED);

					completedCount++;
				} else {
					updateStatus(controlConn, configRow.id, STATUS_FAILED);

					log("ERROR", "Processing FAILED. Status updated. ID: [" + configRow.id + "] FileName: ["
							+ configRow.fileName + "]", null, EVT_FAILED);

					failedCount++;
				}

				log("INFO",
						"Pipeline summary. FileID: [" + configRow.id + "] FileName: [" + configRow.fileName
								+ "] Result: [" + (parseSuccess ? STATUS_COMPLETED : STATUS_FAILED) + "] RowsInserted: "
								+ totalRowsInserted.get() + " EventsQueued: " + eventsQueued,
						null, EVT_SUMMARY);
			}

			log("INFO", "Post-process pipeline finished. FilesCompleted: " + completedCount + " FilesFailed: "
					+ failedCount, null, EVT_END);
		}
	}

	private ConfigRow fetchNextInitFile(Connection conn) throws SQLException {
		String sql = "SELECT " + COL_ID + ", " + COL_FILE_NAME + ", " + COL_SOURCE_PATH + " FROM " + configTableName
				+ " WHERE " + COL_STATUS + " = ?" + " ORDER BY id ASC" + " LIMIT 1";

		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, STATUS_INIT);
			try (ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					ConfigRow row = new ConfigRow();
					row.id = rs.getLong(COL_ID);
					row.fileName = rs.getString(COL_FILE_NAME);
					row.sourcePath = rs.getString(COL_SOURCE_PATH);
					return row;
				}
			}
		}

		return null;
	}

	private void updateStatus(Connection conn, long id, String status) throws SQLException {
		String sql = "UPDATE " + configTableName + " SET " + COL_STATUS + " = ? WHERE " + COL_ID + " = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, status);
			ps.setLong(2, id);
			ps.executeUpdate();
		}
	}

	private void updateCompleted(Connection conn, long id, String destPath) throws SQLException {
		String sql = "UPDATE " + configTableName + " SET " + COL_STATUS + " = ?, " + COL_DEST_PATH + " = ?, "
				+ COL_LAST_PROCESSED + " = ?" + " WHERE " + COL_ID + " = ?";

		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, STATUS_COMPLETED);
			ps.setString(2, destPath);
			ps.setTimestamp(3, Timestamp.valueOf(LocalDateTime.now()));
			ps.setLong(4, id);
			ps.executeUpdate();
		}
	}

	private boolean parseBinlogFile(String mysqlbinlogPath, ConfigRow configRow) {
		resetParseState();

		log("INFO", "Binlog parsing started. FileName: [" + configRow.fileName + "] SourcePath: ["
				+ configRow.sourcePath + "] Mode: SEQUENTIAL", null, EVT_BINLOG_READ);

		boolean success = false;

		// Single dedicated connection for this file's trigger-row writes,
		// opened and closed
		// strictly around this one file so writes for different files never
		// overlap.
		try (Connection dbConn = openConnection(mysqlUrl, mysqlUser, mysqlPass);
				PreparedStatement ps = dbConn.prepareStatement(SQL_TRIGGER_INSERT)) {

			dbConn.setAutoCommit(false);

			this.triggerConn = dbConn;
			this.triggerPs = ps;
			this.eventBatch = new ArrayList<>(BATCH_SIZE);

			executeMysqlbinlog(mysqlbinlogPath, configRow.sourcePath);
			flushCurrentEvent();

			if (!eventBatch.isEmpty()) {
				executeBatch(eventBatch, ps, dbConn);
				eventBatch.clear();
			}

			success = true;

			log("INFO", "Binlog decoding and parsing complete. FileName: [" + configRow.fileName + "] EventsQueued: "
					+ eventsQueued, null, EVT_BINLOG_READ);

		} catch (Exception e) {
			log("ERROR", "Error during binlog parsing. FileName: [" + configRow.fileName + "] Cause: " + e.getMessage(),
					null, EVT_ERROR);
			e.printStackTrace(System.err);
		} finally {
			this.triggerConn = null;
			this.triggerPs = null;
			this.eventBatch = null;
		}

		return success;
	}

	private void resetParseState() {
		currentOperation = null;
		currentTable = null;
		readingWhere = false;
		readingSet = false;
		whereValues = new LinkedHashMap<>();
		setValues = new LinkedHashMap<>();
		eventsQueued = 0L;
		totalRowsInserted.set(0L);
	}

	private void executeMysqlbinlog(String mysqlbinlogPath, String binlogFilePath) throws Exception {
		ProcessBuilder pb = new ProcessBuilder(mysqlbinlogPath, "--base64-output=DECODE-ROWS", "-v", binlogFilePath);

		pb.redirectErrorStream(false);
		Process process = pb.start();

		Thread stderrDrainer = new Thread(() -> {
			try (BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
				String line;
				while ((line = err.readLine()) != null) {
					log("DEBUG", "mysqlbinlog stderr: " + line, null, EVT_BINLOG_READ);
				}
			} catch (Exception ignored) {
			}
		});
		stderrDrainer.setDaemon(true);
		stderrDrainer.start();

		try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
			String line;
			while ((line = reader.readLine()) != null) {
				processLine(line);
			}
		}

		int exitCode = process.waitFor();
		if (exitCode != 0) {
			throw new Exception(
					"mysqlbinlog process exited with code [" + exitCode + "] for file: [" + binlogFilePath + "]");
		}
	}

	private void processLine(String line) {
		try {
			line = line.trim();

			if (line.startsWith("### INSERT INTO")) {
				flushCurrentEvent();
				currentOperation = "INSERT";
				currentTable = extractTableName(line);
				clearDmlState();
			} else if (line.startsWith("### UPDATE")) {
				flushCurrentEvent();
				currentOperation = "UPDATE";
				currentTable = extractTableName(line);
				clearDmlState();
			} else if (line.startsWith("### DELETE FROM")) {
				flushCurrentEvent();
				currentOperation = "DELETE";
				currentTable = extractTableName(line);
				clearDmlState();
			} else if (line.startsWith("### WHERE")) {
				readingWhere = true;
				readingSet = false;
			} else if (line.startsWith("### SET")) {
				readingWhere = false;
				readingSet = true;
			} else if (line.startsWith("###   @") || line.startsWith("###  @")) {
				parseColumnLine(line);
			}
		} catch (Exception e) {
			log("ERROR", "Error processing line. Cause: " + e.getMessage(), null, EVT_ERROR);
		}
	}

	private void clearDmlState() {
		readingWhere = false;
		readingSet = false;
		whereValues.clear();
		setValues.clear();
	}

	private void parseColumnLine(String line) throws Exception {
		if (currentTable == null)
			return;

		String stripped = line.replaceFirst("^###\\s*", "").trim();
		Matcher matcher = COLUMN_PATTERN.matcher(stripped);
		if (!matcher.find())
			return;

		int ordinal = Integer.parseInt(matcher.group(1));
		String value = cleanValue(matcher.group(2));

		TableMetadata metadata = getMetadata(currentTable);
		String columnName = metadata.columnIndexMap.get(ordinal);

		if (columnName == null)
			return;

		if (readingWhere)
			whereValues.put(columnName, value);
		if (readingSet)
			setValues.put(columnName, value);
	}

	/**
	 * Flushes the currently buffered DML event. Rows are appended to eventBatch
	 * and written to the database synchronously and in order — no queue, no
	 * worker threads.
	 */
	private void flushCurrentEvent() throws Exception {
		if (currentOperation == null || currentTable == null) {
			return;
		}

		Map<String, String> sourceMap = "INSERT".equals(currentOperation) ? setValues : whereValues;

		// Process only CKYC schema tables
		if (currentTable == null || !currentTable.startsWith("ckyc.")) {
			currentOperation = null;
			currentTable = null;
			clearDmlState();
			return;
		}

		TableMetadata metadata = getMetadata(currentTable);
		List<String> pkNames = new ArrayList<>();
		List<String> pkValues = new ArrayList<>();

		if (metadata != null && !metadata.primaryKeys.isEmpty()) {
			for (String pkCol : metadata.primaryKeys) {
				pkNames.add(pkCol);
				pkValues.add(sourceMap.getOrDefault(pkCol, "NULL"));
			}
		}

		if (pkNames.isEmpty()) {
			pkNames.add("UNKNOWN_PK");
			pkValues.add("UNKNOWN");
		}

		TriggerRow triggerRow = new TriggerRow(currentTable, joinList(pkNames, ","), joinList(pkValues, ","),
				currentOperation);

		eventBatch.add(triggerRow);
		eventsQueued++;

		if (eventBatch.size() >= BATCH_SIZE) {
			executeBatch(eventBatch, triggerPs, triggerConn);
			eventBatch.clear();
		}

		currentOperation = null;
		currentTable = null;
	}

	private TableMetadata getMetadata(String tableFullName) throws Exception {
		TableMetadata cached = metadataCache.get(tableFullName);
		if (cached != null)
			return cached;

		log("INFO", "Loading metadata for table: [" + tableFullName + "]", null, EVT_METADATA_LOAD);

		TableMetadata metadata = loadTableMetadata(tableFullName);
		metadataCache.put(tableFullName, metadata);

		log("INFO", "Metadata loaded. Table: [" + tableFullName + "] Columns: " + metadata.columnIndexMap.size()
				+ " PKs: [" + joinList(new ArrayList<>(metadata.primaryKeys), ",") + "]", null, EVT_METADATA_LOAD);

		return metadata;
	}

	private TableMetadata loadTableMetadata(String tableFullName) throws Exception {
		TableMetadata metadata = new TableMetadata();

		try (Connection conn = openConnection(mysqlUrl, mysqlUser, mysqlPass)) {
			String[] parts = tableFullName.split("\\.", 2);
			String schema = parts.length > 1 ? parts[0] : conn.getCatalog();
			String table = parts.length > 1 ? parts[1] : parts[0];

			try (PreparedStatement ps = conn.prepareStatement(SQL_COLUMN_META)) {
				ps.setString(1, table);
				ps.setString(2, schema);
				try (ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						metadata.columnIndexMap.put(rs.getInt("ordinal_position"), rs.getString("column_name"));
					}
				}
			}

			try (PreparedStatement ps = conn.prepareStatement(SQL_PK_META)) {
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

	private File prepareArchiveFolder(String archiveFolder) throws Exception {
		if (archiveFolder == null || archiveFolder.trim().isEmpty()) {
			throw new IllegalArgumentException("archive_folder argument is null or empty.");
		}

		File dir = new File(archiveFolder);

		if (!dir.exists()) {
			log("INFO", "Archive folder does not exist — creating: [" + archiveFolder + "]", null, EVT_FILE_MOVE);
			boolean created = dir.mkdirs();
			if (!created) {
				throw new Exception("Failed to create archive folder: [" + archiveFolder + "]");
			}
		}

		if (!dir.isDirectory()) {
			throw new IllegalArgumentException("Archive path is not a directory: [" + archiveFolder + "]");
		}

		if (!dir.canWrite()) {
			throw new IllegalArgumentException("Archive folder is not writable: [" + archiveFolder + "]");
		}

		return dir;
	}

	private String moveFile(String sourcePath, File archiveDir) throws Exception {
		Path source = Paths.get(sourcePath);

		if (!Files.exists(source)) {
			throw new Exception("Source file not found for archive move: [" + sourcePath + "]");
		}

		String fileName = source.getFileName().toString();
		Path target = Paths.get(archiveDir.getAbsolutePath(), fileName);

		Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);

		String movedPath = target.toAbsolutePath().toString();

		log("INFO", "File moved to archive. Source: [" + sourcePath + "] Target: [" + movedPath + "]", null,
				EVT_FILE_MOVE);

		return movedPath;
	}

	private Connection openConnection(String url, String user, String password) throws SQLException {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
		} catch (ClassNotFoundException modern) {
			try {
				Class.forName("com.mysql.jdbc.Driver");
			} catch (ClassNotFoundException legacy) {
				throw new SQLException("MySQL JDBC driver not found on classpath.", legacy);
			}
		}

		return DriverManager.getConnection(url, user, password);
	}

	private String extractTableName(String line) {
		int firstTick = line.indexOf('`');
		int lastTick = line.lastIndexOf('`');

		if (firstTick != -1 && lastTick != -1 && lastTick > firstTick) {
			return line.substring(firstTick + 1, lastTick).replace("`.`", ".");
		}

		return null;
	}

	private String cleanValue(String value) {
		if (value == null)
			return null;
		value = value.trim();

		if ("NULL".equals(value))
			return null;

		if (value.startsWith("'") && value.endsWith("'") && value.length() >= 2) {
			return value.substring(1, value.length() - 1);
		}

		return value;
	}

	private String joinList(List<String> items, String delimiter) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < items.size(); i++) {
			if (i > 0)
				sb.append(delimiter);
			sb.append(items.get(i));
		}
		return sb.toString();
	}

	public void log(String level, String message, String serverIP, String evtType) {
		if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled)
			return;

		String timestamp = LocalDateTime.now().format(logFormat);
		StringBuilder sb = new StringBuilder(512);

		sb.append("Time:").append(timestamp).append(" [").append(capitalize(level)).append("] ")
				.append("Process_Name: ").append(processName).append(" ");

		if (serverIP != null && !serverIP.isEmpty()) {
			sb.append("Server_IP: ").append(serverIP).append(" ");
		}

		sb.append("Process_Version: ").append(processVersion).append(" ").append("Log_Level: EVT_LOG ")
				.append(" EVT_Type: ").append(evtType != null ? evtType : "NA").append(" Description: ")
				.append(message != null ? message : "");

		System.out.println(sb.toString().trim());
	}

	private String capitalize(String input) {
		if (input == null || input.isEmpty())
			return "";
		return Character.toUpperCase(input.charAt(0)) + input.substring(1).toLowerCase();
	}

	/**
	 * Writes a batch of trigger rows synchronously, in order, on the given
	 * connection. Called only from the single parsing thread — never from a
	 * worker pool.
	 */
	private void executeBatch(List<TriggerRow> batch, PreparedStatement ps, Connection conn) {
		if (batch.isEmpty())
			return;

		try {
			for (TriggerRow row : batch) {
				ps.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
				ps.setString(2, row.table);
				ps.setString(3, row.pkName);
				ps.setString(4, row.pkValue);
				ps.setString(5, row.operation);
				ps.addBatch();
			}

			ps.executeBatch();
			conn.commit();

			totalRowsInserted.addAndGet(batch.size());

			log("INFO", "Batch committed. BatchSize: " + batch.size() + " TotalInserted: " + totalRowsInserted.get(),
					null, EVT_DB_WRITE);

		} catch (Exception e) {
			log("ERROR", "Batch commit failed. BatchSize: " + batch.size() + " Cause: " + e.getMessage(), null,
					EVT_ERROR);
			try {
				conn.rollback();
			} catch (SQLException re) {
				log("ERROR", "Rollback failed after batch error. Cause: " + re.getMessage(), null, EVT_ERROR);
			}
			// Re-throw so the file is marked FAILED instead of silently
			// continuing out of order.
			throw new RuntimeException("Batch commit failed, aborting file to preserve ordering.", e);
		}
	}

	private static final class ConfigRow {
		long id;
		String fileName;
		String sourcePath;
	}

	private static final class TableMetadata {
		final Map<Integer, String> columnIndexMap = new HashMap<>();
		final Set<String> primaryKeys = new LinkedHashSet<>();
	}

	private static final class TriggerRow {
		final String table;
		final String pkName;
		final String pkValue;
		final String operation;

		TriggerRow(String table, String pkName, String pkValue, String operation) {
			this.table = table;
			this.pkName = pkName;
			this.pkValue = pkValue;
			this.operation = operation;
		}
	}

	private String validateIdentifier(String name) {
		if (name == null || name.trim().isEmpty()) {
			throw new IllegalArgumentException("Config table name cannot be empty.");
		}

		String trimmed = name.trim();
		if (!trimmed.matches("[A-Za-z0-9_]+")) {
			throw new IllegalArgumentException("Invalid config table name: " + name);
		}
		return trimmed;
	}
}