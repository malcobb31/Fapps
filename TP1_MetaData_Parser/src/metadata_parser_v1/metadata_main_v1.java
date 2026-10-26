package metadata_parser_v1;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;



public class metadata_main_v1 {

    private static final String            processName    = "metadata-parser";
    private static final String            processVersion = "1.0";
    private static final boolean           debugEnabled   = true;
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    private static void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp)
                .append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(processName).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(processVersion)
                .append(" Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                .append(" Description: ").append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    public static void main(String[] args) {

        log("INFO", "========================================", null, "APP_START");
        log("INFO", "CSV to PostgreSQL Parser - STARTED", null, "APP_START");
        log("INFO", "========================================", null, "APP_START");

        if (args.length < 7) {
            log("ERROR", "Invalid arguments provided. Expected at least 7: " +
                    "MySQL URL, MySQL Username, MySQL Password, Config Table Name, " +
                    "PostgreSQL URL, PostgreSQL Username, PostgreSQL Password, " +
                    "(Optional) Destination Table Filter", null, "CONFIG_LOAD");
            return;
        }

        String mysqlURL    = args[0];
        String mysqlUser   = args[1];
        String mysqlPass   = args[2];
        String configTable = args[3];

        String pgURL  = args[4];
        String pgUser = args[5];
        String pgPass = args[6];

        String filterTable = (args.length >= 8) ? args[7] : null;

        // ===== PRINT ARGUMENTS =====
        log("INFO", "------------- ARGUMENTS -------------", null, "CONFIG_LOAD");
        log("INFO", "MySQL URL: " + mysqlURL, null, "CONFIG_LOAD");
        log("INFO", "MySQL User: " + mysqlUser, null, "CONFIG_LOAD");
        log("INFO", "Config Table: " + configTable, null, "CONFIG_LOAD");
        log("INFO", "PostgreSQL URL: " + pgURL, null, "CONFIG_LOAD");
        log("INFO", "PostgreSQL User: " + pgUser, null, "CONFIG_LOAD");
        if (filterTable != null) {
            log("INFO", "Destination Table Filter: " + filterTable, null, "CONFIG_LOAD");
        }
        log("INFO", "-------------------------------------", null, "CONFIG_LOAD");

        try {
            Connection conn = new MySQLConnection().getConnection(mysqlURL, mysqlUser, mysqlPass);

            String query = "SELECT source_path, destination_table_name, destination_path, parameters FROM " + configTable;

            PreparedStatement ps;

            if (filterTable != null) {
                query += " WHERE destination_table_name = ?";
                ps = conn.prepareStatement(query);
                ps.setString(1, filterTable);
            } else {
                ps = conn.prepareStatement(query);
            }

            log("DEBUG", "Executing config query: " + query, null, "DB_QUERY");

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {

                String sourcePath = rs.getString("source_path");
                String destTable  = rs.getString("destination_table_name");
                String destPath   = rs.getString("destination_path");

                String parameters = rs.getString("parameters");
                if (parameters == null || parameters.trim().isEmpty()) {
                    log("WARN", "Missing parameters for table: " + destTable + " - skipping row", null, "CONFIG_LOAD");
                    continue;
                }

                String[] parts = parameters.split("\\|", -1);

                String directColumns  = parts.length > 0 ? parts[0] : "";
                int metadataCount     = parts.length > 1 ? Integer.parseInt(parts[1]) : 0;
                String destMetaCols   = parts.length > 2 ? parts[2] : "";
                String sourceMetaCols = parts.length > 3 ? parts[3] : "";
                String jsonKeys       = parts.length > 4 ? parts[4] : "";

                if (sourcePath == null || destTable == null || destPath == null) {
                    log("WARN", "Skipping invalid config row - one or more required fields are null", null, "CONFIG_LOAD");
                    continue;
                }

                // ===== CONFIG LOG =====
                log("INFO", "=========== CONFIG ROW ===========", null, "CONFIG_LOAD");
                log("INFO", "Source Path: " + sourcePath, null, "CONFIG_LOAD");
                log("INFO", "Destination Table: " + destTable, null, "CONFIG_LOAD");
                log("INFO", "Destination Directory: " + destPath, null, "CONFIG_LOAD");
                log("DEBUG", "Direct Columns: " + directColumns, null, "CONFIG_LOAD");
                log("DEBUG", "Metadata Count: " + metadataCount, null, "CONFIG_LOAD");
                log("INFO", "==================================", null, "CONFIG_LOAD");

                File folder = new File(sourcePath);

                if (!folder.exists() || !folder.isDirectory()) {
                    log("WARN", "Invalid or missing source directory: " + sourcePath, null, "FILE_PROCESS");
                    continue; // ADD EVENT HERE
                }

                File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

                if (files == null || files.length == 0) {
                    log("WARN", "No CSV files found in source directory: " + sourcePath, null, "FILE_PROCESS");
                    continue; //ADD EVENT HERE
                }

                Arrays.sort(files); // CHANGE TO NATURAL SORT HERE

                log("INFO", "Scanning source folder: " + sourcePath, null, "FILE_PROCESS");
                log("INFO", "Total CSV files found: " + files.length + " | Destination table: " + destTable, null, "FILE_PROCESS");

                for (File file : files) {

                    log("INFO", "--- FILE START ---", null, "FILE_PROCESS");
                    log("INFO", "Processing file: " + file.getName() +
                            " | Size: " + file.length() + " bytes" +
                            " | Path: " + file.getAbsolutePath(), null, "FILE_PROCESS");

                    boolean status = new metadata_parser_process().parseAndTransferData(
                            pgURL,
                            pgUser,
                            pgPass,
                            file.getAbsolutePath(),
                            destTable,
                            directColumns,
                            metadataCount,
                            destMetaCols,
                            sourceMetaCols,
                            jsonKeys
                    );

                    try {

                        log("INFO", "Processing result for file: " + file.getName() + " | Status: " + status, null, "FILE_PROCESS");

                        Path targetDir = Paths.get(destPath);

                        if (!Files.exists(targetDir)) {
                            Files.createDirectories(targetDir);
                        }

                        if (status == true) {

                            log("INFO", "File processed successfully - moving to destination folder: " + destPath, null, "FILE_PROCESS");

                            Files.move(file.toPath(),
                                    targetDir.resolve(file.getName()),
                                    StandardCopyOption.REPLACE_EXISTING);

                        } else {

                            log("WARN", "File processing failed - file will not be moved: " + file.getName(), null, "FILE_PROCESS");
                        }

                    } catch (Exception ex) {
                        log("ERROR", "Failed to move file: " + file.getName() + " | Error: " + ex.getMessage(), null, "FILE_PROCESS");
                    }
                }
            }
            rs.close();
            ps.close();
            conn.close();

            log("INFO", "========================================", null, "APP_END");
            log("INFO", "PROCESS COMPLETED", null, "APP_END");
            log("INFO", "========================================", null, "APP_END");

        } catch (Exception e) {
            log("ERROR", "Fatal error in main process: " + e.getMessage(), null, "APP_ERROR");
        }
    }
}
