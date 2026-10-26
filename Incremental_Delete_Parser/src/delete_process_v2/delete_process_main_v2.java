package delete_process_v2;

import java.net.InetAddress;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Entry point for the V2 where_clause-driven DELETE synchronizer between
 * MySQL trigger tables and PostgreSQL target tables.
 *
 * This is the V2 counterpart of delete_process_main_v1. The command-line
 * contract, argument validation, logging format, and per-configuration
 * connection lifecycle are unchanged. The only difference is that this
 * class delegates to delete_process_methods_v2 (where_clause + 220_Col /
 * Psql_Col mapping) instead of delete_process_methods (V1's
 * primary_columns_name/value mapping).
 *
 * Arguments:
 *
 * 1. config_table
 * 2. mysql_db_url
 * 3. mysql_user
 * 4. mysql_password
 * 5. psql_db_url
 * 6. psql_user
 * 7. psql_password
 * 8. batch_size
 *
 * Example:
 *
 * java -cp "Delete_Parser_Incremental.jar:lib/*" \
 * delete_process_v1.delete_process_main_v2 \
 * "config_delete" \
 * "jdbc:mysql://localhost:3306/pview?allowLoadLocalInfile=true&useSSL=false&trustServerCertificate=true" \
 * "root" \
 * "admin@321" \
 * "jdbc:postgresql://172.20.131.56:5432/ckyprodmgdb" \
 * "ckyc_prod_rw" \
 * "ckyc_prod_rw@321" \
 * 1
 */
public class delete_process_main_v2 {

    private static final String PROCESS_NAME = "CKYC_DELETE_PARSER";
    private static final String PROCESS_VERSION = "2.0.0";

    private static final DateTimeFormatter LOG_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final String SERVER_IP = resolveServerIp();

    private static boolean debugEnabled = false;

    public static void main(String[] args) {

        log("INFO", "PARSER_START", "Process starting");

        /*
         * ---------------------------------------------------------------
         * 1. Validate command-line arguments
         * ---------------------------------------------------------------
         */
        if (args == null || args.length != 8) {

            log(
                    "FATAL",
                    "PARSER_FATAL",
                    "Invalid arguments. Expected exactly 8 arguments: "
                            + "<config_table> "
                            + "<mysql_db_url> "
                            + "<mysql_user> "
                            + "<mysql_password> "
                            + "<psql_db_url> "
                            + "<psql_user> "
                            + "<psql_password> "
                            + "<batch_size>"
            );

            System.exit(1);
            return;
        }

        String configTable = args[0];
        String mysqlUrl = args[1];
        String mysqlUser = args[2];
        String mysqlPassword = args[3];

        String psqlUrl = args[4];
        String psqlUser = args[5];
        String psqlPassword = args[6];

        int batchSize;

        try {
            batchSize = Integer.parseInt(args[7]);

            if (batchSize <= 0) {
                throw new IllegalArgumentException(
                        "batch_size must be greater than zero"
                );
            }

        } catch (NumberFormatException ex) {

            log(
                    "FATAL",
                    "PARSER_FATAL",
                    "Invalid batch_size: " + args[7]
            );

            System.exit(1);
            return;

        } catch (IllegalArgumentException ex) {

            log(
                    "FATAL",
                    "PARSER_FATAL",
                    ex.getMessage()
            );

            System.exit(1);
            return;
        }

        /*
         * ---------------------------------------------------------------
         * 2. Basic argument validation
         * ---------------------------------------------------------------
         */
        if (isBlank(configTable)) {
            fatalArgument("config_table");
        }

        if (isBlank(mysqlUrl)) {
            fatalArgument("mysql_db_url");
        }

        if (isBlank(mysqlUser)) {
            fatalArgument("mysql_user");
        }

        if (isBlank(mysqlPassword)) {
            fatalArgument("mysql_password");
        }

        if (isBlank(psqlUrl)) {
            fatalArgument("psql_db_url");
        }

        if (isBlank(psqlUser)) {
            fatalArgument("psql_user");
        }

        if (isBlank(psqlPassword)) {
            fatalArgument("psql_password");
        }

        /*
         * ---------------------------------------------------------------
         * 3. Initialize business/service layer
         * ---------------------------------------------------------------
         */
        delete_process_methods.Logger serviceLogger =
                delete_process_main_v2::log;

        delete_process_methods service =
                new delete_process_methods(serviceLogger);

        /*
         * ---------------------------------------------------------------
         * 4. Load eligible configuration rows
         *
         * This bootstrap connection is used ONLY to read the configuration.
         * The actual processing connection pair is opened later for each
         * configuration row.
         * ---------------------------------------------------------------
         */
        List<delete_process_methods.ParserConfigV2> configs;

        try (Connection bootstrapMysqlConn =
                     openMysqlConnection(
                             mysqlUrl,
                             mysqlUser,
                             mysqlPassword
                     )) {

            log(
                    "INFO",
                    "CONFIG_LOAD_START",
                    "Loading eligible configurations from table="
                            + configTable
            );

            configs = service.loadEligibleConfigurations(
                    bootstrapMysqlConn,
                    configTable
            );

        } catch (SQLException ex) {

            log(
                    "FATAL",
                    "PARSER_FATAL",
                    "Could not load configuration rows: "
                            + ex.getMessage()
            );

            System.exit(1);
            return;
        } catch (RuntimeException ex) {

            log(
                    "FATAL",
                    "PARSER_FATAL",
                    "Unexpected error while loading configuration rows: "
                            + ex.getMessage()
            );

            System.exit(1);
            return;
        }

        /*
         * ---------------------------------------------------------------
         * 5. No eligible configuration
         * ---------------------------------------------------------------
         */
        if (configs == null || configs.isEmpty()) {

            log(
                    "INFO",
                    "PARSER_COMPLETED",
                    "No eligible configuration rows found. Nothing to process."
            );

            return;
        }

        log(
                "INFO",
                "CONFIG_LOAD_COMPLETED",
                "Eligible configurations found="
                        + configs.size()
        );

        /*
         * ---------------------------------------------------------------
         * 6. Process configurations one by one
         * ---------------------------------------------------------------
         */
        int processedCount = 0;

        for (delete_process_methods.ParserConfigV2 config : configs) {

            if (config == null) {

                log(
                        "ERROR",
                        "CONFIG_FAILED",
                        "Encountered null configuration row. Skipping."
                );

                continue;
            }

            processOneConfiguration(
                    service,
                    config,
                    configTable,
                    batchSize,
                    mysqlUrl,
                    mysqlUser,
                    mysqlPassword,
                    psqlUrl,
                    psqlUser,
                    psqlPassword
            );

            processedCount++;
        }

        /*
         * ---------------------------------------------------------------
         * 7. Process completed
         * ---------------------------------------------------------------
         */
        log(
                "INFO",
                "PARSER_COMPLETED",
                "Process finished. Configurations processed="
                        + processedCount
        );
    }

    /**
     * Processes one configuration row.
     *
     * Exactly:
     *      1 MySQL connection
     *      1 PostgreSQL connection
     *
     * are opened for the complete lifecycle of this configuration.
     */
    private static void processOneConfiguration(
            delete_process_methods service,
            delete_process_methods.ParserConfigV2 config,
            String configTable,
            int batchSize,
            String mysqlUrl,
            String mysqlUser,
            String mysqlPassword,
            String psqlUrl,
            String psqlUser,
            String psqlPassword) {

        log(
                "INFO",
                "CONFIG_IN_PROCESS",
                "Starting configuration"
                        + " sno=" + config.sno
                        + " source=" + config.sourceTableName
                        + " target=" + config.psqlTableName
                        + " source_col=" + config.sourceCol
                        + " target_col=" + config.psqlCol
                        + " batch_size=" + batchSize
        );

        try (
                Connection mysqlConn =
                        openMysqlConnection(
                                mysqlUrl,
                                mysqlUser,
                                mysqlPassword
                        );

                Connection psqlConn =
                        openPsqlConnection(
                                psqlUrl,
                                psqlUser,
                                psqlPassword
                        )
        ) {

            /*
             * Keep both connections open for the entire configuration.
             */
            mysqlConn.setAutoCommit(true);
            psqlConn.setAutoCommit(true);

            log(
                    "DEBUG",
                    "DB_CONNECTIONS_OPENED",
                    "MySQL and PostgreSQL connections opened for config_sno="
                            + config.sno
            );

            service.processConfiguration(
                    mysqlConn,
                    psqlConn,
                    configTable,
                    config,
                    batchSize
            );

            log(
                    "INFO",
                    "CONFIG_PROCESS_COMPLETED",
                    "Configuration processing completed"
                            + " sno=" + config.sno
                            + " source=" + config.sourceTableName
                            + " target=" + config.psqlTableName
            );

        } catch (SQLException ex) {

            log(
                    "ERROR",
                    "CONFIG_FAILED",
                    "config_sno=" + config.sno
                            + " database error="
                            + ex.getMessage()
            );

        } catch (RuntimeException ex) {

            log(
                    "ERROR",
                    "CONFIG_FAILED",
                    "config_sno=" + config.sno
                            + " unexpected error="
                            + ex.getMessage()
            );

        } finally {

            log(
                    "DEBUG",
                    "DB_CONNECTIONS_CLOSED",
                    "Database connections closed for config_sno="
                            + config.sno
            );
        }
    }

    /**
     * Opens MySQL connection.
     */
    private static Connection openMysqlConnection(
            String url,
            String user,
            String password) throws SQLException {

        return DriverManager.getConnection(
                url,
                user,
                password
        );
    }

    /**
     * Opens PostgreSQL connection.
     */
    private static Connection openPsqlConnection(
            String url,
            String user,
            String password) throws SQLException {

        return DriverManager.getConnection(
                url,
                user,
                password
        );
    }

    /**
     * Checks blank/null values.
     */
    private static boolean isBlank(String value) {

        return value == null || value.trim().isEmpty();
    }

    /**
     * Fatal argument handler.
     */
    private static void fatalArgument(String argumentName) {

        log(
                "FATAL",
                "PARSER_FATAL",
                "Missing or empty required argument: "
                        + argumentName
        );

        System.exit(1);
    }

    /*
     * ------------------------------------------------------------------
     * Logging
     * ------------------------------------------------------------------
     */

    private static void log(
            String level,
            String evtType,
            String message) {

        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
            return;
        }

        String timestamp =
                LocalDateTime.now().format(LOG_FORMAT);

        System.out.println(
                "Time:" + timestamp
                        + " [" + capitalize(level) + "] "
                        + "Process_Name: " + PROCESS_NAME + " "
                        + "Server_IP: " + SERVER_IP + " "
                        + "Process_Version: " + PROCESS_VERSION
                        + " Log_Level: EVT_LOG"
                        + " EVT_Type: "
                        + (evtType != null ? evtType : "NA")
                        + " Description: "
                        + (message != null ? message : "")
        );
    }

    private static String capitalize(String value) {

        if (value == null || value.isEmpty()) {
            return value;
        }

        return value.substring(0, 1).toUpperCase()
                + value.substring(1).toLowerCase();
    }

    private static String resolveServerIp() {

        try {
            return InetAddress
                    .getLocalHost()
                    .getHostAddress();

        } catch (Exception ex) {

            return "UNKNOWN";
        }
    }
}