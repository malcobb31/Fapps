package binlog_reader_post_final_v2;

import java.time.format.DateTimeFormatter;

public class binlog_reader_post_main_final_v2 {

    private static final String PROCESS_NAME = "binlog_postprocessor_maxSno";
    private static final String PROCESS_VERSION = "1.0.0";
    private static final DateTimeFormatter LOG_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final boolean DEBUG_ENABLED = false;

    public static void main(String[] args) {
        if (args == null || args.length < 6) {
            printUsage();
            System.exit(1);
        }

        String mysqlbinlogPath = args[0].trim();
        String archiveFolder = args[1].trim();
        String configTableName = args[2].trim();
        String mysqlUrl = args[3].trim();
        String mysqlUser = args[4].trim();
        String mysqlPass = args[5];

        // Optional 7th arg: how many INIT files to process in this run.
        // If not provided, ALL files currently in INIT status are processed.
        Integer maxFilesToProcess = null;
        if (args.length >= 7 && args[6] != null && !args[6].trim().isEmpty()) {
            try {
                maxFilesToProcess = Integer.parseInt(args[6].trim());
                if (maxFilesToProcess <= 0) {
                    System.out.println("max_files_to_process must be a positive integer. Got: " + args[6]);
                    System.exit(1);
                }
            } catch (NumberFormatException nfe) {
                System.out.println("max_files_to_process must be a valid integer. Got: " + args[6]);
                System.exit(1);
            }
        }

        binlog_reader_post_methods processor = new binlog_reader_post_methods(
                PROCESS_NAME,
                PROCESS_VERSION,
                LOG_FORMAT,
                DEBUG_ENABLED,
                configTableName
        );

        processor.log("INFO",
                "Post-processor starting. mysqlbinlog: [" + mysqlbinlogPath + "] ArchiveFolder: [" + archiveFolder
                        + "] ConfigTable: [" + configTableName + "] MySQL_URL: [" + mysqlUrl + "] MySQL_User: ["
                        + mysqlUser + "] MaxFilesToProcess: [" + (maxFilesToProcess != null ? maxFilesToProcess : "ALL") + "]",
                null,
                "START");

        try {
            processor.runPostProcess(mysqlbinlogPath, archiveFolder, mysqlUrl, mysqlUser, mysqlPass, maxFilesToProcess);
            processor.log("INFO", "Post-processor finished successfully.", null, "END");
        } catch (Exception e) {
            processor.log("ERROR", "Fatal error  post-processor aborted. Cause: " + e.getMessage(), null, "ERROR");
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }

    private static void printUsage() {
        System.out.println("============================================================");
        System.out.println("  Binlog Post-Processor v1.0.0");
        System.out.println("============================================================");
        System.out.println("Usage:");
        System.out.println("  java binlog_reader_post.binlog_reader_post_main");
        System.out.println("       <mysqlbinlog_path> <archive_folder> <config_table_name>");
        System.out.println("       <mysql_url> <mysql_user> <mysql_pass> [max_files_to_process]");
        System.out.println();
        System.out.println("  max_files_to_process is OPTIONAL. If omitted, ALL files with");
        System.out.println("  status INIT in the config table are processed, one after another.");
        System.out.println();
        System.out.println("Example (process all INIT files):");
        System.out.println("  java binlog_reader_post.binlog_reader_post_main");
        System.out.println("       /usr/bin/mysqlbinlog /data/archive binlog_file_config");
        System.out.println("       jdbc:mysql://localhost:3306/pview root admin@321");
        System.out.println();
        System.out.println("Example (process only next 10 INIT files):");
        System.out.println("  java binlog_reader_post.binlog_reader_post_main");
        System.out.println("       /usr/bin/mysqlbinlog /data/archive binlog_file_config");
        System.out.println("       jdbc:mysql://localhost:3306/pview root admin@321 10");
        System.out.println("============================================================");
    }
}