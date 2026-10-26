package ktp10_process_inc_inst;

import java.io.File;

public class ktp10_process_inc_inst_main {

    // args[0]=inputPath args[1]=archiveDir args[2]=sourceJdbcUrl args[3]=dbUser
    // args[4]=dbPass args[5]=serverIP args[6]=targetJdbcUrl args[7]=batchSize
    private static final int EXPECTED_ARG_COUNT = 8;

    public static void main(String[] args) {

        String serverIP = (args != null && args.length > 5) ? args[5] : null;

        if (args == null || args.length < EXPECTED_ARG_COUNT) {
            ktp10_process_methods.log(
                    "ERROR",
                    "invalid_arguments expected=" + EXPECTED_ARG_COUNT + " received=" + (args == null ? 0 : args.length),
                    serverIP,
                    "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        String inputPathArg = args[0];
        String archiveDirArg = args[1];
        String sourceJdbcUrl = args[2];
        String dbUser = args[3];
        String dbPass = args[4];
        serverIP = args[5];
        String targetJdbcUrl = args[6];
        String batchSizeArg = args[7];

        if (isBlank(inputPathArg)) {
            ktp10_process_methods.log("ERROR", "invalid_argument=input_path", serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        if (isBlank(archiveDirArg)) {
            ktp10_process_methods.log("ERROR", "invalid_argument=archive_dir", serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        if (isBlank(sourceJdbcUrl) || !sourceJdbcUrl.startsWith("jdbc:postgresql:")) {
            ktp10_process_methods.log("ERROR", "invalid_argument=source_jdbc_url value=" + sourceJdbcUrl, serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        if (isBlank(targetJdbcUrl) || !targetJdbcUrl.startsWith("jdbc:postgresql:")) {
            ktp10_process_methods.log("ERROR", "invalid_argument=target_jdbc_url value=" + targetJdbcUrl, serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        if (isBlank(dbUser)) {
            ktp10_process_methods.log("ERROR", "invalid_argument=db_user", serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        if (isBlank(dbPass)) {
            ktp10_process_methods.log("ERROR", "invalid_argument=db_pass", serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        if (isBlank(serverIP)) {
            ktp10_process_methods.log("ERROR", "invalid_argument=server_ip", serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        File inputPath = new File(inputPathArg);
        if (!inputPath.exists()) {
            ktp10_process_methods.log("ERROR", "input_path_not_found path=" + inputPathArg, serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        File archiveDir = new File(archiveDirArg);
        if (!archiveDir.exists() || !archiveDir.isDirectory()) {
            ktp10_process_methods.log("ERROR", "archive_dir_not_found path=" + archiveDirArg, serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        int configuredBatchSize;
        try {
            configuredBatchSize = Integer.parseInt(batchSizeArg.trim());
        } catch (Exception ex) {
            ktp10_process_methods.log("ERROR", "invalid_argument=batch_size value=" + batchSizeArg, serverIP, "ARG_VALIDATION_ERROR");
            System.exit(1);
            return;
        }

        ktp10_process_methods.process(inputPath, archiveDir, sourceJdbcUrl, dbUser, dbPass, serverIP, targetJdbcUrl, configuredBatchSize);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }
}