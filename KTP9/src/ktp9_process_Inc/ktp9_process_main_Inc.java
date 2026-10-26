package ktp9_process_Inc;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ktp9_process_main_Inc {

    private static final boolean debugEnabled = false;
    private static final String processName = "KTP9";
    private static final String processVersion = "1.0";
    private static final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    public static void main(String[] args) {
        log("INFO", "process=kyc_related_parties status=START", null, "APP_START");

        if (args == null || (args.length != 6 && args.length != 7)) {
            log("ERROR", "reason=Invalid arguments. Usage: sourcePath processedDir jdbcUrl jdbcUser jdbcPassword serverIP [batchSize]", null, "APP_END");
            return;
        }

        String sourcePathStr = safeTrim(args[0]);
        String processedDirStr = safeTrim(args[1]);
        String jdbcUrl = safeTrim(args[2]);
        String jdbcUser = safeTrim(args[3]);
        String jdbcPassword = args[4];
        String serverIP = safeTrim(args[5]);
        String batchSizeStr = (args.length == 7) ? safeTrim(args[6]) : null;

        if (sourcePathStr == null || processedDirStr == null || jdbcUrl == null || jdbcUser == null || jdbcPassword == null || serverIP == null) {
            log("ERROR", "reason=Required arguments must not be null", serverIP, "APP_END");
            return;
        }

        File sourcePath = new File(sourcePathStr);
        if (!sourcePath.exists()) {
            log("ERROR", "reason=Source path does not exist path=" + sourcePathStr, serverIP, "APP_END");
            return;
        }

        File processedDir = new File(processedDirStr);
        if (!processedDir.exists() && !processedDir.mkdirs()) {
            log("ERROR", "reason=Unable to create processed directory path=" + processedDirStr, serverIP, "APP_END");
            return;
        }

        int batchSize = 100000;
        if (batchSizeStr != null) {
            try {
                batchSize = Integer.parseInt(batchSizeStr);
                if (batchSize <= 0) {
                    log("ERROR", "reason=Batch size must be greater than zero", serverIP, "APP_END");
                    return;
                }
            } catch (Exception e) {
                log("ERROR", "reason=Invalid batch size value", serverIP, "APP_END");
                return;
            }
        }

        ktp9_process_methods methods = new ktp9_process_methods();
        try {
            methods.process(sourcePath, processedDir, jdbcUrl, jdbcUser, jdbcPassword, batchSize, serverIP);
        } catch (Exception e) {
            log("ERROR", "reason=" + rootCauseMessage(e), serverIP, "APP_END");
        }
    }

    private static String safeTrim(String value) {
        if (value == null) {
            return null;
        }
        String t = value.trim();
        if (t.length() == 0) {
            return null;
        }
        return t;
    }

    private static String rootCauseMessage(Throwable t) {
        if (t == null) {
            return "unknown";
        }
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        String msg = current.getMessage();
        if (msg == null || msg.trim().length() == 0) {
            return current.getClass().getSimpleName();
        }
        return current.getClass().getSimpleName() + ": " + msg;
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    private static void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
            return;
        }
        String timestamp = LocalDateTime.now().format(logFormat);
        StringBuilder logBuilder = new StringBuilder(512);
        logBuilder.append("Time:").append(timestamp).append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(processName).append(" ");
        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }
        logBuilder.append("Process_Version: ").append(processVersion).append(" ")
                .append("Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                .append(" Description: ").append(message != null ? message : "");
        System.out.println(logBuilder.toString().trim());
    }
}