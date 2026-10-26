package binlog_reader_post;

import java.time.format.DateTimeFormatter;

public class binlog_reader_post_main {

    private static final String PROCESS_NAME = "binlog_postprocessor_v1";
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
                        + mysqlUser + "]",
                null,
                "START");

        try {
            processor.runPostProcess(mysqlbinlogPath, archiveFolder, mysqlUrl, mysqlUser, mysqlPass);
            processor.log("INFO", "Post-processor finished successfully.", null, "END");
        } catch (Exception e) {
            processor.log("ERROR", "Fatal error — post-processor aborted. Cause: " + e.getMessage(), null, "ERROR");
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
        System.out.println("       <mysql_url> <mysql_user> <mysql_pass>");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java binlog_reader_post.binlog_reader_post_main");
        System.out.println("       /usr/bin/mysqlbinlog /data/archive binlog_file_config");
        System.out.println("       jdbc:mysql://localhost:3306/pview root admin@321");
        System.out.println("============================================================");
    }
}