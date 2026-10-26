package binlog_reader_post_v3;

import java.time.format.DateTimeFormatter;

public class binlog_reader_post_main_v3 {

    private static final String PROCESS_NAME = "binlog_postprocessor_v1";
    private static final String PROCESS_VERSION = "1.0.0";
    private static final DateTimeFormatter LOG_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final boolean DEBUG_ENABLED = false;

    public static void main(String[] args) {
        if (args == null || args.length < 7) {
            printUsage();
            System.exit(1);
        }

        String mysqlbinlogPath = args[0].trim();
        String archiveFolder = args[1].trim();
        String configTableName = args[2].trim();
        String mysqlUrl = args[3].trim();
        String mysqlUser = args[4].trim();
        String mysqlPass = args[5];
        String targetTableName = args[6].trim();

        Integer maxFilesToProcess = null;
        if (args.length >= 8 && args[7] != null && !args[7].trim().isEmpty()) {
            try {
                maxFilesToProcess = Integer.parseInt(args[7].trim());
                if (maxFilesToProcess <= 0) {
                    System.out.println("max_files_to_process must be a positive integer. Got: " + args[7]);
                    System.exit(1);
                }
            } catch (NumberFormatException nfe) {
                System.out.println("max_files_to_process must be a valid integer. Got: " + args[7]);
                System.exit(1);
            }
        }

        binlog_reader_post_methods processor = new binlog_reader_post_methods(
                PROCESS_NAME,
                PROCESS_VERSION,
                LOG_FORMAT,
                DEBUG_ENABLED,
                configTableName,
                targetTableName
        );

        processor.log("INFO",
                "Post-processor starting. mysqlbinlog: [" + mysqlbinlogPath + "] ArchiveFolder: [" + archiveFolder
                        + "] ConfigTable: [" + configTableName + "] TargetTable: [" + targetTableName + "] MySQL_URL: ["
                        + mysqlUrl + "] MySQL_User: [" + mysqlUser + "] MaxFilesToProcess: ["
                        + (maxFilesToProcess != null ? maxFilesToProcess : "ALL") + "]",
                null,
                "START");

        try {
            processor.runPostProcess(mysqlbinlogPath, archiveFolder, mysqlUrl, mysqlUser, mysqlPass, maxFilesToProcess);
            processor.log("INFO", "Post-processor finished successfully.", null, "END");
        } catch (Exception e) {
            processor.log("ERROR", "Fatal error - post-processor aborted. Cause: " + e.getMessage(), null, "ERROR");
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
        System.out.println("       <mysql_url> <mysql_user> <mysql_pass> <target_table_name> [max_files_to_process]");
        System.out.println();
        System.out.println("  target_table_name is REQUIRED. Only INSERT/UPDATE/DELETE events for");
        System.out.println("  this table will be processed from the binlog files.");
        System.out.println();
        System.out.println("  max_files_to_process is OPTIONAL. If omitted, ALL files with");
        System.out.println("  status INIT in the config table are processed, one after another.");
        System.out.println();
        System.out.println("Example (process all INIT files for kyc_master):");
        System.out.println("  java binlog_reader_post.binlog_reader_post_main");
        System.out.println("       /usr/bin/mysqlbinlog /data/archive binlog_file_config");
        System.out.println("       jdbc:mysql://localhost:3306/pview root admin@321 kyc_master");
        System.out.println();
        System.out.println("Example (process only next 10 INIT files for kyc_master):");
        System.out.println("  java binlog_reader_post.binlog_reader_post_main");
        System.out.println("       /usr/bin/mysqlbinlog /data/archive binlog_file_config");
        System.out.println("       jdbc:mysql://localhost:3306/pview root admin@321 kyc_master 10");
        System.out.println("============================================================");
    }
}