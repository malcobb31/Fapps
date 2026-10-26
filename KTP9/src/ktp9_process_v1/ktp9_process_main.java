package ktp9_process_v1;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ktp9_process_main {

    private static final String PROCESS_NAME = "KTP9";
    private static final String PROCESS_VERSION = "1.0";
    private static final DateTimeFormatter LOG_FMT =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");
    private static boolean debugEnabled = false;

    public static void main(String[] args) {

        if (args == null || (args.length != 6 && args.length != 7)) {
            log("ERROR", "reason=Invalid arguments. Usage: sourcePath processedDir jdbcUrl jdbcUser jdbcPassword serverIP [batchSize]",
                    null, "ARG_ERROR");
            return;
        }

        String sourcePath = safeTrim(args[0]);
        String processedDirPath = safeTrim(args[1]);
        String jdbcUrl = safeTrim(args[2]);
        String jdbcUser = safeTrim(args[3]);
        String jdbcPassword = args[4];
        String serverIP = safeTrim(args[5]);
        String batchSizeArg = (args.length == 7) ? safeTrim(args[6]) : null;

        log("INFO", "process=kyc_related_parties status=START", serverIP, "APP_START");

        if (sourcePath == null || processedDirPath == null || jdbcUrl == null || jdbcUser == null || jdbcPassword == null) {
            log("ERROR", "reason=Required arguments must not be null", serverIP, "ARG_ERROR");
            return;
        }

        File source = new File(sourcePath);
        if (!source.exists()) {
            log("ERROR", "reason=Source path does not exist path=" + sourcePath, serverIP, "CONFIG_LOAD");
            return;
        }

        File processedDir = new File(processedDirPath);
        if (!processedDir.exists()) {
            if (!processedDir.mkdirs()) {
                log("ERROR", "reason=Unable to create processed directory path=" + processedDirPath, serverIP, "CONFIG_LOAD");
                return;
            }
        }

        int batchSize = 20;
        if (batchSizeArg != null && batchSizeArg.length() > 0) {
            try {
                batchSize = Integer.parseInt(batchSizeArg);
                if (batchSize <= 0) {
                    log("ERROR", "reason=Batch size must be greater than zero", serverIP, "ARG_ERROR");
                    return;
                }
            } catch (Exception e) {
                log("ERROR", "reason=Invalid batch size value", serverIP, "ARG_ERROR");
                return;
            }
        }

        ktp9_process_methods methods = new ktp9_process_methods();
        try {
            methods.process(source, processedDir, jdbcUrl, jdbcUser, jdbcPassword, batchSize, serverIP);
        } catch (Exception e) {
            log("FATAL", "reason=" + rootCauseMessage(e), serverIP, "ETL_ERROR");
        }

        log("INFO", "process=kyc_related_parties status=END", serverIP, "APP_END");
    }

    private static void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled)
            return;
        String timestamp = LocalDateTime.now().format(LOG_FMT);
        StringBuilder logBuilder = new StringBuilder(512);
        logBuilder.append("Time:").append(timestamp).append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(PROCESS_NAME).append(" ");
        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }
        logBuilder.append("Process_Version: ").append(PROCESS_VERSION).append(" ").append("Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA").append(" Description: ")
                .append(message != null ? message : "");
        System.out.println(logBuilder.toString().trim());
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
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
}