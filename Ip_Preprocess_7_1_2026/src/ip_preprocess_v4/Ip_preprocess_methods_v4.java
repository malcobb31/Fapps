package ip_preprocess_v4;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;

public class Ip_preprocess_methods_v4 {

	private static final Logger logger = Logger.getLogger(Ip_preprocess_methods_v4.class.getName());

	private static final String PROCESS_NAME = "Ftp_Postgre";
	private static final String PROCESS_VERSION = "2.0.0";

	private static String serverIp = "UNKNOWN";

	private static void setupLogger(String ip) {
		serverIp = ip != null ? ip : "UNKNOWN";
		logger.setUseParentHandlers(false);

		Handler[] handlers = logger.getHandlers();
		for (Handler handler : handlers) {
			logger.removeHandler(handler);
		}

		ConsoleHandler handler = new ConsoleHandler();
		handler.setFormatter(new CustomLogFormatter());
		handler.setLevel(Level.ALL);
		logger.addHandler(handler);
		logger.setLevel(Level.ALL);
	}

	public static void process(String path, String dbUrl, String dbUser, String dbPass,
			String destinDir, String scheduleTable, String serverIp) {

		setupLogger(serverIp);

		logEvent(Level.INFO, "EVT_Log", "PROCESS_START",
				"Starting file processing. Source: " + path + ", Destination: " + destinDir);

		try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {

			logEvent(Level.INFO, "EVT_Log", "DB_CONNECTION",
					"Connected to database. AutoCommit=" + conn.getAutoCommit());

			Path sourceFolder = Paths.get(path);
			if (!Files.exists(sourceFolder) || !Files.isDirectory(sourceFolder)) {
				logEvent(Level.SEVERE, "EVT_Log", "INVALID_PATH",
						"Source folder not found or not a directory: " + path);
				return;
			}

			Path destFolder = Paths.get(destinDir);
			if (!Files.exists(destFolder)) {
				try {
					Files.createDirectories(destFolder);
					logEvent(Level.INFO, "EVT_Log", "DIR_CREATED",
							"Created destination directory: " + destinDir);
				} catch (IOException e) {
					logEvent(Level.SEVERE, "EVT_Log", "DIR_CREATE_FAILED",
							"Failed to create destination directory: " + e.getMessage());
					return;
				}
			}

			processDirectory(conn, sourceFolder, destFolder, scheduleTable);

		} catch (SQLException e) {
			logEvent(Level.SEVERE, "EVT_Log", "DB_CONNECTION_ERROR",
					"Database connection error: " + e.getMessage());
		} catch (Exception e) {
			logEvent(Level.SEVERE, "EVT_Log", "UNEXPECTED_ERROR",
					"Unexpected error during processing: " + e.getMessage());
		}

		logEvent(Level.INFO, "EVT_Log", "PROCESS_END", "File processing completed");
	}

	private static void processDirectory(Connection conn, Path sourceFolder, Path destFolder, String scheduleTable) {

		int totalFiles = 0;
		int processedFiles = 0;
		int skippedFiles = 0;
		int errorFiles = 0;

		try (DirectoryStream<Path> stream = Files.newDirectoryStream(sourceFolder, "*.csv")) {

			Map<String, List<Path>> filesBySerial = new HashMap<String, List<Path>>();

			for (Path file : stream) {
				totalFiles++;
				String fileName = file.getFileName().toString();

				if (!isValidFileName(fileName)) {
					skippedFiles++;
					continue;
				}

				if (!isFileOlderThanFiveMinutes(file)) {
					skippedFiles++;
					continue;
				}

				String serialNo = extractSerialNumber(fileName);
				if (serialNo == null) {
					errorFiles++;
					continue;
				}

				List<Path> list = filesBySerial.get(serialNo);
				if (list == null) {
					list = new ArrayList<Path>();
					filesBySerial.put(serialNo, list);
				}
				list.add(file);
			}

			logEvent(Level.INFO, "EVT_Log", "GROUPING_DONE",
					"Grouped files by SNO. Total groups: " + filesBySerial.size());

			for (Map.Entry<String, List<Path>> entry : filesBySerial.entrySet()) {
				String serialNo = entry.getKey();
				List<Path> filesForThisSno = entry.getValue();

				logEvent(Level.INFO, "EVT_Log", "GROUP_PROCESS_START",
						"Processing SNO=" + serialNo + ", File Count=" + filesForThisSno.size());

				FileProcessResult result = processMultipleFilesForSameSno(
						conn, filesForThisSno, destFolder, scheduleTable, serialNo);

				switch (result) {
				case PROCESSED:
					processedFiles += filesForThisSno.size();
					break;
				case SKIPPED:
					skippedFiles += filesForThisSno.size();
					break;
				case ERROR:
					errorFiles += filesForThisSno.size();
					break;
				}
			}

		} catch (IOException e) {
			logEvent(Level.SEVERE, "EVT_Log", "DIRECTORY_READ_ERROR",
					"Error reading directory: " + e.getMessage());
		}

		logEvent(Level.INFO, "EVT_Log", "PROCESS_SUMMARY",
				String.format("Summary - Total:%d, Processed:%d, Skipped:%d, Errors:%d",
						totalFiles, processedFiles, skippedFiles, errorFiles));
	}

	private static boolean isFileOlderThanFiveMinutes(Path file) {
		try {
			long lastModifiedMillis = Files.getLastModifiedTime(file).toMillis();
			long nowMillis = System.currentTimeMillis();
			long diffMillis = nowMillis - lastModifiedMillis;

			return diffMillis >= (5L * 60L * 1000L);
		} catch (Exception e) {
			logEvent(Level.WARNING, "EVT_Log", "FILE_TIME_CHECK_ERROR",
					"Could not check last modified time for file: " + file.getFileName());
			return false;
		}
	}

	private static FileProcessResult processMultipleFilesForSameSno(Connection conn,
			List<Path> files,
			Path destFolder,
			String scheduleTable,
			String serialNo) {

		String lockQuery = "UPDATE " + scheduleTable +
				" SET sync_status = 'in_move_process' " +
				" WHERE sno = ? AND sync_status = 'init'";

		String updateQuery = "UPDATE " + scheduleTable +
				" SET sync_status = 'ready_to_import_new' " +
				" WHERE sno = ?";

		boolean originalAutoCommit = true;

		try {
			originalAutoCommit = conn.getAutoCommit();
			if (originalAutoCommit) {
				conn.setAutoCommit(false);
			}

			try (PreparedStatement lockPs = conn.prepareStatement(lockQuery)) {

				lockPs.setInt(1, Integer.parseInt(serialNo));
				int locked = lockPs.executeUpdate();

				if (locked == 0) {
					if (!originalAutoCommit) {
						conn.rollback();
					}
					logEvent(Level.INFO, "EVT_Log", "GROUP_SKIPPED",
							"SNO=" + serialNo + " skipped because status was not init.");
					return FileProcessResult.SKIPPED;
				}

				logEvent(Level.INFO, "EVT_Log", "LOCK_ACQUIRED",
						"Lock acquired for SNO=" + serialNo);

				for (Path file : files) {
					Path destination = destFolder.resolve(file.getFileName());
					Files.move(file, destination, StandardCopyOption.REPLACE_EXISTING);
				}

				logEvent(Level.INFO, "EVT_Log", "FILES_MOVED",
						"Moved " + files.size() + " file(s) for SNO=" + serialNo + " to " + destFolder.toString());

				try (PreparedStatement ups = conn.prepareStatement(updateQuery)) {
					ups.setInt(1, Integer.parseInt(serialNo));
					int updated = ups.executeUpdate();

					if (updated == 0) {
						throw new SQLException("Status update affected 0 rows for SNO=" + serialNo);
					}

					logEvent(Level.INFO, "EVT_Log", "STATUS_UPDATED",
							"SNO=" + serialNo + " updated to ready_to_import_new");
				}

				if (!originalAutoCommit) {
					conn.commit();
				} else {
					conn.commit();
				}

				logEvent(Level.INFO, "EVT_Log", "GROUP_COMPLETED",
						"All files processed successfully for SNO=" + serialNo);

				return FileProcessResult.PROCESSED;
			}

		} catch (Exception e) {
			try {
				conn.rollback();
			} catch (SQLException ignore) {
				// ignore rollback error
			}

			logEvent(Level.SEVERE, "EVT_Log", "GROUP_ERROR",
					"Error processing SNO=" + serialNo + ": " + e.getMessage());

			return FileProcessResult.ERROR;

		} finally {
			try {
				if (originalAutoCommit) {
					conn.setAutoCommit(true);
				}
			} catch (SQLException ignore) {
				// ignore restore error
			}
		}
	}

	private static String extractSerialNumber(String fileName) {
		try {
			String[] parts = fileName.split("_");
			if (parts.length >= 5) {
				return parts[4];
			}
		} catch (Exception e) {
			logEvent(Level.WARNING, "EVT_Log", "SERIAL_PARSE_ERROR",
					"Error parsing serial number from filename: " + fileName);
		}
		return null;
	}

	private static boolean isValidFileName(String fileName) {
		String regex = "^(raw|process).*\\.csv$";
		boolean isValid = fileName.matches(regex);

		if (!isValid) {
			logEvent(Level.WARNING, "EVT_Log", "FILENAME_VALIDATION",
					"Filename validation failed for: " + fileName);
		}

		return isValid;
	}

	private static void logEvent(Level level, String evtLog, String evtType, String description) {

		String timestamp = LocalDateTime.now()
				.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

		String formattedLevel = capitalize(level.getName());

		StringBuilder logBuilder = new StringBuilder(512);

		logBuilder.append("Time:").append(timestamp)
				.append(" [").append(formattedLevel).append("] ")
				.append("Process_Name: ").append(PROCESS_NAME).append(" ")
				.append("Server_IP: ").append(serverIp != null ? serverIp : "UNKNOWN").append(" ")
				.append("Process_Version: ").append(PROCESS_VERSION).append(" ")
				.append("Log_Level: ").append(evtLog != null ? evtLog : "EVT_LOG").append("  ")
				.append("EVT_Type: ").append(evtType != null ? evtType : "NA")
				.append(" Description: ")
				.append(description != null ? description : "");

		System.out.println(logBuilder.toString());
	}

	private static String capitalize(String str) {
		if (str == null || str.isEmpty()) {
			return "";
		}
		return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
	}

	private enum FileProcessResult {
		PROCESSED, SKIPPED, ERROR
	}

	static class CustomLogFormatter extends Formatter {
		private final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

		@Override
		public String format(LogRecord record) {
			String time = LocalDateTime.now().format(dtf);

			String[] params = (String[]) record.getParameters();
			String evtLog = params != null && params.length > 0 ? params[0] : "EVT_Log";
			String evtType = params != null && params.length > 1 ? params[1] : "UNKNOWN";
			String description = params != null && params.length > 2 ? params[2] : record.getMessage();

			return String.format(
					"Time:%s [Debug] Process_Name:%s Server_IP:%s Process_Version:%s Log_Level:%s %s EVT_Type:%s Description: %s%n",
					time, PROCESS_NAME, serverIp, PROCESS_VERSION, record.getLevel().getName(), evtLog, evtType,
					description);
		}
	}
}