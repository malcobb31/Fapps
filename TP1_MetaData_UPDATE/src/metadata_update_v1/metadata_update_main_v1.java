package metadata_update_v1;

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

public class metadata_update_main_v1 {

    private static final String            processName    = "metadata-update";
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
        log("INFO", "CSV to PostgreSQL Update Process - STARTED", null, "APP_START");
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
            Connection conn = new MySQLConnection_update().getConnection(mysqlURL, mysqlUser, mysqlPass);

            // FIX: Added primary_key to SELECT; use AND (not WHERE) for optional filter
            String query = "SELECT source_path, destination_table_name, destination_path, parameters, primary_key " +
                    "FROM " + configTable + " WHERE is_active = 1";

            PreparedStatement ps;

            if (filterTable != null) {
                query += " AND destination_table_name = ?";
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

                // FIX: PK mapping now lives in its own column - not inside parameters
                String pkMapping = rs.getString("primary_key");

                // parameters format: directColumns|metadataCount|destMetaCols|sourceMetaCols|jsonKeys
                // (pkMapping is NO LONGER part of this string - it is read from primary_key column above)
                String[] parts = parameters.split("\\|", -1);

                // FIX: .trim() applied to every segment to eliminate \n / whitespace issues
                String directColumns  = parts.length > 0 ? parts[0].trim() : "";
                int    metadataCount  = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
                String destMetaCols   = parts.length > 2 ? parts[2].trim() : "";
                String sourceMetaCols = parts.length > 3 ? parts[3].trim() : "";
                String jsonKeys       = parts.length > 4 ? parts[4].trim() : "";

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
                log("DEBUG", "PK Mapping: " + pkMapping, null, "CONFIG_LOAD");
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

                    boolean status = new metadata_parser_process_update().parseAndTransferData(
                            pgURL,
                            pgUser,
                            pgPass,
                            file.getAbsolutePath(),
                            destTable,
                            directColumns,
                            metadataCount,
                            destMetaCols,
                            sourceMetaCols,
                            jsonKeys,
                            pkMapping
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
