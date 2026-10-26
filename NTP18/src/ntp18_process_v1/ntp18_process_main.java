package ntp18_process_v1;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ntp18_process_main {

    private static final Logger LOGGER = Logger.getLogger(ntp18_process_main.class.getName());
    private final String processName = "ntp18_process_main";
    private final String processVersion = "1.0.0";
    private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss", Locale.ENGLISH);
    private final boolean debugEnabled = false;

    public static void main(String[] args) {
        new ntp18_process_main().run(args);
    }

    private void run(String[] args) {
        if (args == null || args.length < 6) {
            log("ERROR", "fatal_error=insufficient_arguments", null, "FATAL");
            log("ERROR", "usage=inputPath processedDir jdbcUrl dbUser dbPass ipWhitelistedForValue", null, "FATAL");
            return;
        }

        String inputPath = safeTrim(args[0]);
        String processedDir = safeTrim(args[1]);
        String jdbcUrl = safeTrim(args[2]);
        String dbUser = safeTrim(args[3]);
        String dbPass = args[4] == null ? "" : args[4];
        String ipWhitelistedForValue = safeTrim(args[5]);

        if (inputPath.length() == 0) {
            log("ERROR", "fatal_error=missing_input_path", null, "FATAL");
            return;
        }
        if (processedDir.length() == 0) {
            log("ERROR", "fatal_error=missing_processed_dir", null, "FATAL");
            return;
        }
        if (jdbcUrl.length() == 0) {
            log("ERROR", "fatal_error=missing_jdbc_url", null, "FATAL");
            return;
        }
        if (dbUser.length() == 0) {
            log("ERROR", "fatal_error=missing_db_user", null, "FATAL");
            return;
        }
        if (ipWhitelistedForValue.length() == 0) {
            log("ERROR", "fatal_error=missing_ip_whitelisted_for_value", null, "FATAL");
            return;
        }

        File input = new File(inputPath);
        if (!input.exists()) {
            log("ERROR", "fatal_error=input_path_not_found path=" + inputPath, null, "FATAL");
            return;
        }

        File processed = new File(processedDir);
        if (!processed.exists()) {
            boolean created = processed.mkdirs();
            if (!created && !processed.exists()) {
                log("ERROR", "fatal_error=cannot_create_processed_dir path=" + processedDir, null, "FATAL");
                return;
            }
        }

        ntp18_methods methods = new ntp18_methods();
        methods.process(inputPath, processedDir, jdbcUrl, dbUser, dbPass, ipWhitelistedForValue);
    }

    private String safeTrim(String value) {
        if (value == null) {
            return "";
        }
        return value.trim();
    }

    private String capitalize(String value) {
        if (value == null || value.length() == 0) {
            return "INFO";
        }
        String lower = value.toLowerCase(Locale.ENGLISH);
        return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
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
}