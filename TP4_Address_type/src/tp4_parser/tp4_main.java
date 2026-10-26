package tp4_parser;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class tp4_main {

    private static final String            PROCESS_NAME    = "TP4_ADDRESS_PARSER";
    private static final String            PROCESS_VERSION = "1.0";
    private static final boolean           DEBUG_ENABLED   = true;
    private static final DateTimeFormatter LOG_FORMAT      =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    private static void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !DEBUG_ENABLED) return;
        String timestamp = LocalDateTime.now().format(LOG_FORMAT);
        StringBuilder sb = new StringBuilder(512);
        sb.append("Time:").append(timestamp)
          .append(" [").append(capitalize(level)).append("] ")
          .append("Process_Name: ").append(PROCESS_NAME).append(" ");
        if (serverIP != null && !serverIP.isEmpty())
            sb.append("Server_IP: ").append(serverIP).append(" ");
        sb.append("Process_Version: ").append(PROCESS_VERSION)
          .append(" Log_Level: EVT_LOG ")
          .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
          .append(" Description: ").append(message != null ? message : "");
        System.out.println(sb.toString().trim());
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }

    public static void main(String[] args) {

        log("INFO", "========================================", null, "APP_START");
        log("INFO", "TP4 Address Parser - STARTED",            null, "APP_START");
        log("INFO", "Arguments received: " + args.length,      null, "APP_START");
        log("INFO", "========================================", null, "APP_START");

        if (args.length != 9) {
            log("ERROR", "Invalid argument count | Expected: 9 | Received: " + args.length,
                    null, "ARG_ERROR");
            log("ERROR", "  [1] MySQL JDBC URL          e.g. jdbc:mysql://host:3306/configdb",  null, "ARG_ERROR");
            log("ERROR", "  [2] MySQL Username",                                                  null, "ARG_ERROR");
            log("ERROR", "  [3] MySQL Password",                                                  null, "ARG_ERROR");
            log("ERROR", "  [4] Config Table Name       e.g. metadata_parser_config",            null, "ARG_ERROR");
            log("ERROR", "  [5] PostgreSQL JDBC URL     e.g. jdbc:postgresql://host:5432/targetdb", null, "ARG_ERROR");
            log("ERROR", "  [6] PostgreSQL Username",                                             null, "ARG_ERROR");
            log("ERROR", "  [7] PostgreSQL Password",                                             null, "ARG_ERROR");
            log("ERROR", "  [8] TM_ID                   filter value for config table",          null, "ARG_ERROR");
            log("ERROR", "  [9] Flag Column Name        pass \"\" to use default 'flag'",        null, "ARG_ERROR");
            return;
        }

        String mysqlUrl    = args[0];
        String mysqlUser   = args[1];
        String mysqlPass   = args[2];
        String configTable = args[3];
        String pgUrl       = args[4];
        String pgUser      = args[5];
        String pgPass      = args[6];
        String tmId        = args[7];
        String flagColArg  = args[8];

        String flagColumn = (flagColArg == null || flagColArg.trim().isEmpty())
                ? "flag"
                : flagColArg.trim();

        log("INFO", "------------- ARGUMENTS -------------", null, "CONFIG_LOAD");
        log("INFO", "MySQL URL:       " + mysqlUrl,    null, "CONFIG_LOAD");
        log("INFO", "MySQL User:      " + mysqlUser,   null, "CONFIG_LOAD");
        log("INFO", "Config Table:    " + configTable, null, "CONFIG_LOAD");
        log("INFO", "PostgreSQL URL:  " + pgUrl,       null, "CONFIG_LOAD");
        log("INFO", "PostgreSQL User: " + pgUser,      null, "CONFIG_LOAD");
        log("INFO", "TM_ID:           " + tmId,        null, "CONFIG_LOAD");
        log("INFO", "Flag Column:     " + flagColumn,  null, "CONFIG_LOAD");
        log("INFO", "-------------------------------------", null, "CONFIG_LOAD");

        long startTime = System.currentTimeMillis();

        boolean result = new ProcessData().run(
                mysqlUrl, mysqlUser, mysqlPass,
                configTable,
                pgUrl, pgUser, pgPass,
                tmId, flagColumn
        );

        long elapsed = System.currentTimeMillis() - startTime;

        log("INFO", "========================================", null, "APP_END");
        if (result) {
            log("INFO",  "TP4 Parser COMPLETED SUCCESSFULLY | elapsed=" + elapsed + "ms", null, "APP_END");
        } else {
            log("ERROR", "TP4 Parser COMPLETED WITH FAILURES  | elapsed=" + elapsed + "ms", null, "APP_END");
        }
        log("INFO", "========================================", null, "APP_END");
    }
}
