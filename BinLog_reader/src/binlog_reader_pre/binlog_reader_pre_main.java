package binlog_reader_pre;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class binlog_reader_pre_main {

    private static final String PROCESS_NAME = "binlog_preprocessor_v1";
    private static final String PROCESS_VERSION = "1.0.0";
    private static final DateTimeFormatter LOG_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private static final boolean DEBUG_ENABLED = false;

    public static void main(String[] args) {

        if (args == null || args.length < 6) {
            printUsage();
            System.exit(1);
        }

        String binlogFolder = args[0].trim();
        String preBinlogFolder = args[1].trim();
        String configTableName = args[2].trim();
        String mysqlUrl = args[3].trim();
        String mysqlUser = args[4].trim();
        String mysqlPass = args[5];

        binlog_reader_pre_methods processor = new binlog_reader_pre_methods(
                PROCESS_NAME,
                PROCESS_VERSION,
                LOG_FORMAT,
                DEBUG_ENABLED,
                configTableName
        );
        processor.log("INFO",
                "Pre-processor starting. BinlogFolder: [" + binlogFolder + "] PreBinlogFolder: [" + preBinlogFolder
                        + "] ConfigTable: [" + configTableName + "] MySQL_URL: [" + mysqlUrl + "] MySQL_User: [" + mysqlUser + "]",
                null,
                "START");

        try {
            processor.runPreProcess(binlogFolder, preBinlogFolder, mysqlUrl, mysqlUser, mysqlPass);
            processor.log("INFO", "Pre-processor finished successfully.", null, "END");
        } catch (Exception e) {
            processor.log("ERROR", "Fatal error — pre-processor aborted. Cause: " + e.getMessage(), null, "ERROR");
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }

    private static void printUsage() {
        System.out.println("============================================================");
        System.out.println("  Binlog Pre-Processor v1.0.0");
        System.out.println("============================================================");
        System.out.println("Usage:");
        System.out.println("  java binlog_reader_pre.binlog_reader_pre_main");
        System.out.println("       <binlog_folder> <pre_binlog_folder> <config_table_name>");
        System.out.println("       <mysql_url> <mysql_user> <mysql_pass>");
        System.out.println();
        System.out.println("Example:");
        System.out.println("  java binlog_reader_pre.binlog_reader_pre_main");
        System.out.println("       /data/binlogs /data/pre_binlogs binlog_file_config");
        System.out.println("       jdbc:mysql://localhost:3306/pview root admin@321");
        System.out.println("============================================================");
    }
}