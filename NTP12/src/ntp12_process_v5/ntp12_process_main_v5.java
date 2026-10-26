package ntp12_process_v5;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ntp12_process_main_v5 {

    private static final String processName = "NTP12";
    private static final String processVersion = "5.0";
    private static final boolean debugEnabled = true;
    private static final DateTimeFormatter logFormat =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    public static void main(String[] args) {

        log("INFO", "========================================", null, "APP_START");
        log("INFO", "User Migration Parser - STARTED", null, "APP_START");
        log("INFO", "========================================", null, "APP_START");

        if (args.length < 5) {
            log("ERROR", "Expected 5 arguments: pgUrl pgUser pgPass csvFolderPath processedFolderPath", null, "CONFIG_LOAD");
            System.exit(1);
        }

        String pgUrl = args[0];
        String pgUser = args[1];
        String pgPass = args[2];
        String csvFolderPath = args[3];
        String processedFolderPath = args[4];

        log("INFO", "------------- ARGUMENTS -------------", null, "CONFIG_LOAD");
        log("INFO", "PostgreSQL URL:      " + pgUrl, null, "CONFIG_LOAD");
        log("INFO", "PostgreSQL User:     " + pgUser, null, "CONFIG_LOAD");
        log("INFO", "CSV Folder:          " + csvFolderPath, null, "CONFIG_LOAD");
        log("INFO", "Processed Folder:    " + processedFolderPath, null, "CONFIG_LOAD");
        log("INFO", "-------------------------------------", null, "CONFIG_LOAD");

        File csvFolder = new File(csvFolderPath);
        if (!csvFolder.exists() || !csvFolder.isDirectory()) {
            log("ERROR", "CSV folder does not exist or is not a directory: " + csvFolderPath, null, "CONFIG_LOAD");
            System.exit(1);
        }

        ntp12_process_methods_table_v1 processor =
                new ntp12_process_methods_table_v1(
                        pgUrl,
                        pgUser,
                        pgPass
                );

        boolean success = processor.run(csvFolderPath);

        if (success) {
            moveProcessedFiles(csvFolderPath, processedFolderPath);
            log("INFO", "========================================", null, "APP_END");
            log("INFO", "USER MIGRATION COMPLETED SUCCESSFULLY", null, "APP_END");
            log("INFO", "========================================", null, "APP_END");
        } else {
            log("ERROR", "========================================", null, "APP_END");
            log("ERROR", "USER MIGRATION FAILED", null, "APP_END");
            log("ERROR", "========================================", null, "APP_END");
            System.exit(1);
        }
    }

    private static void moveProcessedFiles(String csvFolderPath, String processedFolderPath) {
        try {
            Path processedDir = Paths.get(processedFolderPath);
            if (!Files.exists(processedDir)) {
                Files.createDirectories(processedDir);
                log("INFO", "Created processed folder: " + processedFolderPath, null, "FILE_MOVE");
            }

            File[] files = new File(csvFolderPath)
                    .listFiles((dir, name) -> name.toLowerCase().endsWith(".csv"));

            if (files == null || files.length == 0) {
                log("WARN", "No CSV files found to move in: " + csvFolderPath, null, "FILE_MOVE");
                return;
            }

            for (File file : files) {
                try {
                    Files.move(file.toPath(),
                            processedDir.resolve(file.getName()),
                            StandardCopyOption.REPLACE_EXISTING);
                    log("INFO", "Moved: " + file.getName() + " -> " + processedFolderPath, null, "FILE_MOVE");
                } catch (Exception e) {
                    log("ERROR", "Failed to move file: " + file.getName()
                            + " | Error: " + e.getMessage(), null, "FILE_MOVE");
                }
            }

        } catch (Exception e) {
            log("ERROR", "Failed to setup processed folder | Error: " + e.getMessage(), null, "FILE_MOVE");
        }
    }

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
}