package ktp10_update_v1;

import java.io.File;

/**
 * Entry point for the KYC linked-institutions CSV-to-CSV ETL process.
 *
 * NOTE: The original ktp10_process_main_v2 source was not provided, so this
 * class has been reconstructed to match the argument order, validation style,
 * and overall flow described for this pipeline. If your existing main class
 * differs in some other respect (e.g. additional flags), carry those over
 * into this version.
 *
 * This version no longer inserts into the target DB, so the target DB
 * credentials that used to be required have been removed. A new outputDir
 * argument was added instead: the directory where the transformed, enriched
 * CSV (header = target table columns) is written. archiveDir keeps its old
 * meaning - the directory the original source CSV is moved to on success.
 *
 * Argument order:
 * inputPath archiveDir outputDir appJdbcUrl appDbUser appDbPass serverIP [flushSize]
 */
public class ktp10_update_main_v1 {

    private static final int MIN_ARGS = 7;
    private static final int MAX_ARGS = 8;

    public static void main(String[] args) {
        if (args == null || args.length < MIN_ARGS || args.length > MAX_ARGS) {
            printUsageAndExit();
            return;
        }

        String inputPathArg = args[0];
        String archiveDirArg = args[1];
        String outputDirArg = args[2];
        String appJdbcUrl = args[3];
        String appDbUser = args[4];
        String appDbPass = args[5];
        String serverIP = args[6];

        int flushSize = 0;
        if (args.length == MAX_ARGS) {
            flushSize = parseFlushSizeArg(args[7], serverIP);
        }

        if (isBlank(inputPathArg)) {
            fail("input_path is required", serverIP);
            return;
        }
        if (isBlank(archiveDirArg)) {
            fail("archive_dir is required", serverIP);
            return;
        }
        if (isBlank(outputDirArg)) {
            fail("output_dir is required", serverIP);
            return;
        }
        if (isBlank(appJdbcUrl)) {
            fail("app_jdbc_url is required", serverIP);
            return;
        }
        if (isBlank(appDbUser)) {
            fail("app_db_user is required", serverIP);
            return;
        }
        if (isBlank(appDbPass)) {
            fail("app_db_pass is required", serverIP);
            return;
        }
        if (isBlank(serverIP)) {
            fail("server_ip is required", serverIP);
            return;
        }

        File inputPath = new File(inputPathArg);
        if (!inputPath.exists()) {
            fail("input_path does not exist: " + inputPathArg, serverIP);
            return;
        }

        File archiveDir = new File(archiveDirArg);
        if (!archiveDir.exists()) {
            fail("archive_dir does not exist: " + archiveDirArg, serverIP);
            return;
        }
        if (!archiveDir.isDirectory()) {
            fail("archive_dir is not a directory: " + archiveDirArg, serverIP);
            return;
        }

        File outputDir = new File(outputDirArg);
        if (outputDir.exists() && !outputDir.isDirectory()) {
            fail("output_dir exists but is not a directory: " + outputDirArg, serverIP);
            return;
        }
        // output_dir is allowed not to exist yet - ktp10_process_methods creates
        // it on demand before writing the first output CSV.

        try {
        	ktp10_update_methods.process(
                    inputPath,
                    archiveDir,
                    outputDir,
                    appJdbcUrl,
                    appDbUser,
                    appDbPass,
                    serverIP,
                    flushSize);
        } catch (Exception ex) {
        	ktp10_update_methods.log(
                    "ERROR",
                    "fatal_error=main_uncaught_exception reason=" + rootCause(ex),
                    serverIP,
                    "FATAL_ERROR");
            System.exit(1);
        }
    }

    private static int parseFlushSizeArg(String value, String serverIP) {
        if (isBlank(value)) {
            return 0;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException ex) {
        	ktp10_update_methods.log(
                    "ERROR",
                    "invalid_flush_size=" + value + " reason=falling_back_to_default",
                    serverIP,
                    "ARG_WARNING");
            return 0;
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }

    private static void fail(String reason, String serverIP) {
    	ktp10_update_methods.log(
                "ERROR",
                "fatal_error=invalid_arguments reason=" + reason,
                serverIP,
                "FATAL_ERROR");
        printUsage();
        System.exit(1);
    }

    private static void printUsageAndExit() {
        printUsage();
        System.exit(1);
    }

    private static void printUsage() {
        System.err.println(
                "Usage: ktp10_process_main_v2 inputPath archiveDir outputDir appJdbcUrl appDbUser appDbPass "
                        + "serverIP [flushSize]");
    }

    private static String rootCause(Throwable t) {
        if (t == null) {
            return "unknown";
        }
        Throwable current = t;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        String msg = current.getMessage();
        if (msg == null || msg.trim().length() == 0) {
            msg = current.getClass().getSimpleName();
        }
        return msg;
    }
}