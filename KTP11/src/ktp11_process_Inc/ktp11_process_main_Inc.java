package ktp11_process_Inc;

import java.io.File;

public class ktp11_process_main_Inc {

    public static void main(String[] args) {
        if (args == null || args.length < 8 || args.length > 10) {
            System.err.println("Usage: java ktp11_process_main <input_csv_or_dir> <processed_dir> <target_jdbc_url> <target_db_user> <target_db_password> <app_jdbc_url> <app_db_user> <app_db_password> [batch_size] [server_ip]");
            return;
        }

        String inputPath = args[0];
        String processedDir = args[1];
        String targetJdbcUrl = args[2];
        String targetDbUser = args[3];
        String targetDbPassword = args[4];
        String appJdbcUrl = args[5];
        String appDbUser = args[6];
        String appDbPassword = args[7];

        int batchSize = 20;
        String serverIP = "";

        if (args.length == 9) {
            Integer parsed = tryParseInt(args[8]);
            if (parsed != null) {
                batchSize = parsed.intValue();
            } else {
                serverIP = args[8];
            }
        } else if (args.length == 10) {
            Integer parsed = tryParseInt(args[8]);
            if (parsed == null) {
                System.err.println("Invalid batch_size: " + args[8]);
                return;
            }
            batchSize = parsed.intValue();
            serverIP = args[9];
        }

        File input = new File(inputPath);
        if (!input.exists()) {
            System.err.println("Input path does not exist: " + inputPath);
            return;
        }

        File processed = new File(processedDir);
        if (!processed.exists() || !processed.isDirectory()) {
            System.err.println("Processed directory does not exist or is not a directory: " + processedDir);
            return;
        }

        if (isBlank(targetJdbcUrl) || isBlank(targetDbUser) || isBlank(targetDbPassword)
                || isBlank(appJdbcUrl) || isBlank(appDbUser) || isBlank(appDbPassword)) {
            System.err.println("Target DB and App DB arguments are required.");
            return;
        }

        ktp11_process_methods methods = new ktp11_process_methods(
                inputPath,
                processedDir,
                targetJdbcUrl,
                targetDbUser,
                targetDbPassword,
                appJdbcUrl,
                appDbUser,
                appDbPassword,
                batchSize,
                serverIP
        );

        methods.process();
    }

    private static Integer tryParseInt(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Integer.valueOf(value.trim());
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().length() == 0;
    }
}