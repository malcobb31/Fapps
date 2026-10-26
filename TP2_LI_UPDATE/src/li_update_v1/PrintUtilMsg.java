package li_update_v1;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

public class PrintUtilMsg {

    public static String imsiToDis = "";
    public static String msgType   = "";

    private static final boolean             debugEnabled = true;
    private static final String              processName  = "TP2_PROCESS";
    private static final String              processVersion = "1.0";
    private static final DateTimeFormatter   logFormat    =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public void sop(String evtType, String message) {
        log("DEBUG", message, null, evtType);
    }

    public void eventLog(String level, String evtType, String desc) {
        log(level, desc, "172.20.131.100", evtType);
    }

    public void printMap(String TAG, Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            log("DEBUG",
                "possible event [" + entry.getKey() + "], possible action [" + entry.getValue() + "]",
                null, TAG);
        }
    }

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
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }
}
