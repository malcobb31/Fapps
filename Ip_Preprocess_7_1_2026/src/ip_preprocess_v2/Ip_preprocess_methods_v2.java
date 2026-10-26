package ip_preprocess_v2;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.logging.*;
import java.util.logging.Formatter;

public class Ip_preprocess_methods_v2 {

	private static final Logger logger = Logger.getLogger(Ip_preprocess_methods_v2.class.getName());

	// Fixed metadata
	private static final String PROCESS_NAME = "Ftp_Postgre";
	private static final String PROCESS_VERSION = "2.0.0";

	private static String serverIp = "UNKNOWN";

	private static void setupLogger(String ip) {
		serverIp = ip != null ? ip : "UNKNOWN";
		logger.setUseParentHandlers(false);

		// Clear existing handlers to avoid duplicates
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

	public static void process(String path, String dbUrl, String dbUser, String dbPass, String destinDir,
			String scheduleTable, String serverIp) {

		setupLogger(serverIp);

		logEvent(Level.INFO, "EVT_Log", "PROCESS_START",
				"Starting file processing. Source: " + path + ", Destination: " + destinDir);

		try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {

			// Validate source directory
			Path sourceFolder = Paths.get(path);
			if (!Files.exists(sourceFolder) || !Files.isDirectory(sourceFolder)) {
				logEvent(Level.SEVERE, "EVT_Log", "INVALID_PATH",
						"Source folder not found or not a directory: " + path);
				return;
			}

			// Validate destination directory
			Path destFolder = Paths.get(destinDir);
			if (!Files.exists(destFolder)) {
				try {
					Files.createDirectories(destFolder);
					logEvent(Level.INFO, "EVT_Log", "DIR_CREATED", "Created destination directory: " + destinDir);
				} catch (IOException e) {
					logEvent(Level.SEVERE, "EVT_Log", "DIR_CREATE_FAILED",
							"Failed to create destination directory: " + e.getMessage());
					return;
				}
			}

			// Process all CSV files in the directory
			processDirectory(conn, sourceFolder, destFolder, scheduleTable);

		} catch (SQLException e) {
			logEvent(Level.SEVERE, "EVT_Log", "DB_CONNECTION_ERROR", "Database connection error: " + e.getMessage());
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

	        //  Collect files grouped by serial number
	        Map<String, List<Path>> filesBySerial = new HashMap<>();

	        for (Path file : stream) {
	            totalFiles++;
	            String fileName = file.getFileName().toString();

	            if (!isValidFileName(fileName)) {
	                skippedFiles++;
	                continue;
	            }

	            String serialNo = extractSerialNumber(fileName);
	            if (serialNo == null) {
	                errorFiles++;
	                continue;
	            }

	            filesBySerial.computeIfAbsent(serialNo, k -> new ArrayList<>()).add(file);
	        }

	        logEvent(Level.INFO, "EVT_Log", "GROUPING_DONE",
	                "Grouped files by SNO. Total groups: " + filesBySerial.size());

	        //  Process each SNO group
	        for (Map.Entry<String, List<Path>> entry : filesBySerial.entrySet()) {

	            String serialNo = entry.getKey();
	            List<Path> filesForThisSno = entry.getValue();

	            logEvent(Level.INFO, "EVT_Log", "GROUP_PROCESS_START",
	                    "Processing SNO=" + serialNo + " with file count=" + filesForThisSno.size());

	            FileProcessResult result =
	                    processMultipleFilesForSameSno(conn, filesForThisSno, destFolder, scheduleTable, serialNo);

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
	            String.format("Processing Summary - Total: %d, Processed: %d, Skipped: %d, Errors: %d",
	                    totalFiles, processedFiles, skippedFiles, errorFiles));
	}
	
	private static FileProcessResult processMultipleFilesForSameSno(Connection conn,
	        List<Path> files,
	        Path destFolder,
	        String scheduleTable,
	        String serialNo) {

	    String lockQuery = "UPDATE " + scheduleTable +
	            " SET sync_status = 'in_pre_process' " +
	            " WHERE sno = ? AND sync_status = 'init'";

	    try (PreparedStatement lockPs = conn.prepareStatement(lockQuery)) {

	        lockPs.setInt(1, Integer.parseInt(serialNo));
	        int locked = lockPs.executeUpdate();

	        if (locked == 0) {
	            logEvent(Level.INFO, "EVT_Log", "GROUP_SKIPPED",
	                    "SNO=" + serialNo + " skipped - already locked or not init");
	            return FileProcessResult.SKIPPED;
	        }

	        logEvent(Level.INFO, "EVT_Log", "LOCK_ACQUIRED",
	                "Lock acquired for SNO=" + serialNo);

	        // Move ALL files
	        for (Path file : files) {
	            Path destination = destFolder.resolve(file.getFileName());
	            Files.move(file, destination, StandardCopyOption.REPLACE_EXISTING);

	            logEvent(Level.INFO, "EVT_Log", "FILE_MOVED",
	                    "Moved file: " + file.getFileName());
	        }

	        // Update status once
	        String updateQuery = "UPDATE " + scheduleTable +
	                " SET sync_status = 'ready_to_import' " +
	                " WHERE sno = ? AND sync_status = 'in_pre_process'";

	        try (PreparedStatement ups = conn.prepareStatement(updateQuery)) {
	            ups.setInt(1, Integer.parseInt(serialNo));
	            ups.executeUpdate();
	        }

	        logEvent(Level.INFO, "EVT_Log", "GROUP_COMPLETED",
	                "All files processed successfully for SNO=" + serialNo);

	        return FileProcessResult.PROCESSED;

	    } catch (Exception e) {

	        logEvent(Level.SEVERE, "EVT_Log", "GROUP_ERROR",
	                "Error processing SNO=" + serialNo + ": " + e.getMessage());

	        return FileProcessResult.ERROR;
	    }
	}

	private static String extractSerialNumber(String fileName) {
		try {
			String[] parts = fileName.split("_");
			if (parts.length >= 5) {
				return parts[4]; // 5th position (0-indexed = 4)
			}
		} catch (Exception e) {
			logEvent(Level.WARNING, "EVT_Log", "SERIAL_PARSE_ERROR",
					"Error parsing serial number from filename: " + fileName);
		}
		return null;
	}
	
//	private static FileProcessResult processFileBasedOnConfig(Connection conn, Path file, Path destFolder,
//	        String scheduleTable, String serialNo) {
//
//	    String fileName = file.getFileName().toString();
//
//	    //  ATOMIC LOCK
//	    String lockQuery = "UPDATE " + scheduleTable +
//	    	    " SET sync_status = 'in_pre_process' " +
//	    	    " WHERE sno = ? AND sync_status = 'init'";
//
//	    try (PreparedStatement lockPs = conn.prepareStatement(lockQuery)) {
//
//	        lockPs.setInt(1, Integer.parseInt(serialNo));
//	        int locked = lockPs.executeUpdate();
//
//	        if (locked == 0) {
//
//	            logEvent(Level.INFO, "EVT_Log", "FILE_SKIPPED_STATUS",
//	                    String.format("File %s skipped - sync_status is not 'init' or already locked", fileName));
//	            
//
//	            return FileProcessResult.SKIPPED;
//	        }
//
//	        logEvent(Level.INFO, "EVT_Log", "LOCK_ACQUIRED",
//	                String.format("Lock acquired for SNO=%s. Status changed to in_pre_process", serialNo));
//
//	        //  Move file + mark ready
//	        return moveFileAndUpdateStatus(conn, file, destFolder, scheduleTable, serialNo, null);
//
//	    } catch (NumberFormatException e) {
//
//	        logEvent(Level.ALL, "EVT_Log", "INVALID_SERIAL_NUMBER",
//	                "Invalid serial number format: " + serialNo + " for file: " + fileName);
//
//	        return FileProcessResult.ERROR;
//
//	    } catch (SQLException e) {
//
//	        logEvent(Level.ALL, "EVT_Log", "DB_LOCK_ERROR",
//	                "Database lock error for SNO=" + serialNo + ": " + e.getMessage());
//
//	        return FileProcessResult.ERROR;
//	    }
//	}




//	private static FileProcessResult moveFileAndUpdateStatus(Connection conn, Path file, Path destFolder,
//			String scheduleTable, String serialNo, String tableName) {
//		String fileName = file.getFileName().toString();
//
//		try {
//			// Move file to destination
//			Path destination = destFolder.resolve(fileName);
//			Files.move(file, destination, StandardCopyOption.REPLACE_EXISTING);
//			
//
//			logEvent(Level.INFO, "EVT_Log", "FILE_MOVED",
//					String.format("File moved successfully: %s -> %s", fileName, destination));
//
//			// Update sync_status in database
//			String updateQuery = "UPDATE " + scheduleTable +
//				    " SET sync_status = 'ready_to_import' " +
//				    " WHERE sno = ? AND sync_status = 'in_pre_process'";
//			try (PreparedStatement ups = conn.prepareStatement(updateQuery)) {
//				ups.setInt(1, Integer.parseInt(serialNo));
//				int rowsUpdated = ups.executeUpdate();
//
//				if (rowsUpdated > 0) {
//					logEvent(Level.INFO, "EVT_Log", "STATUS_UPDATED", String.format(
//							"Updated sync_status to 'ready_to_import' for SNO=%s, Table=%s", serialNo, tableName));
//					return FileProcessResult.PROCESSED;
//				} else {
//					logEvent(Level.WARNING, "EVT_Log", "STATUS_UPDATE_FAILED", "No rows updated for SNO=" + serialNo);
//					return FileProcessResult.ERROR;
//				}
//			}
//
//		} catch (IOException e) {
//			logEvent(Level.ALL, "EVT_Log", "FILE_MOVE_ERROR",
//					"Failed to move file " + fileName + ": " + e.getMessage());
//			return FileProcessResult.ERROR;
//		} catch (SQLException e) {
//			logEvent(Level.ALL, "EVT_Log", "STATUS_UPDATE_ERROR",
//					"Failed to update status for SNO=" + serialNo + ": " + e.getMessage());
//			return FileProcessResult.ERROR;
//		}
//	}

	private static boolean isValidFileName(String fileName) {
		// Expected format: raw_NA_NA_NA_3_2025_07_18_13_24_57.csv
		// String regex =
		// "^raw_[^_]+_[^_]+_[^_]+_\\d+_\\d{4}_\\d{2}_\\d{2}_\\d{2}_\\d{2}_\\d{2}\\.csv$";
		// boolean isValid = fileName.matches(regex);
		String regex = "^(raw|process).*\\.csv$";
		boolean isValid = fileName.matches(regex);

		if (!isValid) {
			logEvent(Level.ALL, "EVT_Log", "FILENAME_VALIDATION", "Filename validation failed for: " + fileName
					+ " (expected format: raw_*_*_*_SNO_YYYY_MM_DD_HH_MM_SS.csv)");
		}

		return isValid;
	}

	private static void logEvent(Level level, String evtLog, String evtType, String description) {

	    String timestamp = LocalDateTime.now()
	            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

	    // Convert Level to "Info", "Warning", "Severe"
	    String formattedLevel = capitalize(level.getName());

	    StringBuilder logBuilder = new StringBuilder(1000);

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
	    if (str == null || str.isEmpty())
	        return "";
	    return str.substring(0, 1).toUpperCase() + str.substring(1).toLowerCase();
	}
	// Enum for file processing results
	private enum FileProcessResult {
		PROCESSED, SKIPPED, ERROR
	}

	// Custom Formatter for logs
	static class CustomLogFormatter extends Formatter {
		private final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

		@Override
		public String format(LogRecord record) {
			String time = LocalDateTime.now().format(dtf);

			// Handle cases where parameters might be null
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
