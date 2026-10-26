package ntp9_process_v3;

public class ntp9_process_main_v3 {

    private static final String processName = "ReConsolidationProcessor";
    private static final String processVersion = "1.0.0";

    
    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Usage: java ntp9_process_main <parent_csv> <child_csv> <db_url> <db_user> <db_pass> <table_name>");
            return;
        }

        String parentCsv = args[0];
        String childCsv  = args[1];
        String dbUrl     = args[2];
        String dbUser    = args[3];
        String dbPass    = args[4];
        String tableName = args[5];
        System.out.println("Length of arguments: " + args.length);

        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        try {
            log("INFO", "Starting ReConsolidation process", "PROCESS_START");

            ntp9_process_methods methods = new ntp9_process_methods();
            methods.process(parentCsv, childCsv, dbUrl, dbUser, dbPass, tableName);

            log("INFO", "ReConsolidation process completed successfully", "PROCESS_DONE");
        } catch (Exception e) {
            log("ERROR", "Fatal error: " + e.getMessage(), "APP_ERROR");
            e.printStackTrace();
        }
    }

    private static void log(String level, String message, String evtType) {
        String timestamp = java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));

        System.out.println(
                "Time:" + timestamp +
                " [" + level + "] " +
                "Process_Name: " + processName + " " +
                "Process_Version: " + processVersion + " " +
                "Log_Level: EVT_LOG " +
                " EVT_Type: " + evtType +
                " Description: " + message
        );
    }
}