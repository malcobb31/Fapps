package ktp10_upd_process_v1;

import java.io.File;

public class ktp10_upd_process_main {

    public static void main(String[] args) {
        // Required:
        // 0 inputPath
        // 1 archiveDir
        // 2 outputDir
        // 3 appJdbcUrl
        // 4 appDbUser
        // 5 appDbPass
        // 6 kycJdbcUrl
        // 7 kycDbUser
        // 8 kycDbPass
        // 9 serverIP
        // 10 flushSize (optional)

        if (args == null || args.length < 10) {
            ktp10_upd_process_methods.log(
                    "ERROR",
                    "fatal_error=invalid_arguments\nusage=inputPath archiveDir outputDir appJdbcUrl appDbUser appDbPass "
                            + "kycJdbcUrl kycDbUser kycDbPass serverIP [flushSize]",
                    null,
                    "FATAL_ERROR");
            return;
        }

        String inputPath = args[0];
        String archiveDir = args[1];
        String outputDir = args[2];
        String appJdbcUrl = args[3];
        String appDbUser = args[4];
        String appDbPass = args[5];
        String kycJdbcUrl = args[6];
        String kycDbUser = args[7];
        String kycDbPass = args[8];
        String serverIP = args[9];

        int flushSize = 50000;
        if (args.length >= 11) {
            try {
                flushSize = Integer.parseInt(args[10]);
                if (flushSize <= 0) {
                    ktp10_upd_process_methods.log(
                            "ERROR",
                            "fatal_error=invalid_flush_size",
                            serverIP,
                            "FATAL_ERROR");
                    return;
                }
            } catch (Exception ex) {
                ktp10_upd_process_methods.log(
                        "ERROR",
                        "fatal_error=invalid_flush_size",
                        serverIP,
                        "FATAL_ERROR");
                return;
            }
        }

        if (inputPath == null || inputPath.trim().length() == 0) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=missing_input_path", serverIP, "FATAL_ERROR");
            return;
        }

        if (archiveDir == null || archiveDir.trim().length() == 0) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=missing_archive_dir", serverIP, "FATAL_ERROR");
            return;
        }

        if (outputDir == null || outputDir.trim().length() == 0) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=missing_output_dir", serverIP, "FATAL_ERROR");
            return;
        }

        if (appJdbcUrl == null || appJdbcUrl.trim().length() == 0) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=missing_app_jdbc_url", serverIP, "FATAL_ERROR");
            return;
        }

        if (appDbUser == null || appDbUser.trim().length() == 0) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=missing_app_db_user", serverIP, "FATAL_ERROR");
            return;
        }

        if (appDbPass == null || appDbPass.trim().length() == 0) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=missing_app_db_pass", serverIP, "FATAL_ERROR");
            return;
        }

        boolean kycConfigured = kycJdbcUrl != null && kycJdbcUrl.trim().length() > 0;
        if (kycConfigured) {
            if (kycDbUser == null || kycDbUser.trim().length() == 0) {
                ktp10_upd_process_methods.log("ERROR", "fatal_error=missing_kyc_db_user", serverIP, "FATAL_ERROR");
                return;
            }

            if (kycDbPass == null || kycDbPass.trim().length() == 0) {
                ktp10_upd_process_methods.log("ERROR", "fatal_error=missing_kyc_db_pass", serverIP, "FATAL_ERROR");
                return;
            }
        } else {
            kycJdbcUrl = null;
            kycDbUser = null;
            kycDbPass = null;
        }

        File input = new File(inputPath);
        File archive = new File(archiveDir);
        File output = new File(outputDir);

        if (!input.exists()) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=input_path_not_found path=" + inputPath, serverIP, "FATAL_ERROR");
            return;
        }

        if (!input.canRead()) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=input_path_not_readable path=" + inputPath, serverIP, "FATAL_ERROR");
            return;
        }

        if (!archive.exists()) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=archive_dir_not_found path=" + archiveDir, serverIP, "FATAL_ERROR");
            return;
        }

        if (!archive.isDirectory()) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=archive_path_not_directory path=" + archiveDir, serverIP, "FATAL_ERROR");
            return;
        }

        if (!archive.canWrite()) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=archive_dir_not_writable path=" + archiveDir, serverIP, "FATAL_ERROR");
            return;
        }

        if (!output.exists()) {
            output.mkdirs();
        }

        if (!output.isDirectory()) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=output_path_not_directory path=" + outputDir, serverIP, "FATAL_ERROR");
            return;
        }

        if (!output.canWrite()) {
            ktp10_upd_process_methods.log("ERROR", "fatal_error=output_dir_not_writable path=" + outputDir, serverIP, "FATAL_ERROR");
            return;
        }

        ktp10_upd_process_methods.process(
                input,
                archive,
                output,
                appJdbcUrl,
                appDbUser,
                appDbPass,
                kycConfigured ? kycJdbcUrl : null,
                kycDbUser,
                kycDbPass,
                serverIP,
                flushSize);
    }
}