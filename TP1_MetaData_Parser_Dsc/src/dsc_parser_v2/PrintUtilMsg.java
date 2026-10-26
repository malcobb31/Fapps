package dsc_parser_v2;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PrintUtilMsg {

    private static final String            processName    = "metadata-parser";
    private static final String            processVersion = "1.0";
    private static final boolean           debugEnabled   = true;
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    private void log(String level, String message, String serverIP, String evtType) {
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

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    public static String imsiToDis = "";
    public static String msgType   = "";

    public void sop(String tagName, String msg) {
        log("INFO", "[" + tagName + "] " + msg, null, "APP_LOG");
    }

    public void printMap(String TAG, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            log("INFO", "[" + TAG + "] Possible event: " + entry.getKey() +
                    " | Possible action: " + entry.getValue(), null, "APP_LOG");
        }
    }
}
