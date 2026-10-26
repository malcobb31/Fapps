package ktp11_process_v1;

import java.io.File;

public class ktp11_process_main {

    public static void main(String[] args) {
        if (args == null || args.length < 5 || args.length > 7) {
            System.err.println("Usage: java ktp11_process_main <input_csv_or_dir> <processed_dir> <jdbc_url> <db_user> <db_password> [batch_size] [server_ip]");
            return;
        }

        String inputPath = args[0];
        String processedDir = args[1];
        String jdbcUrl = args[2];
        String dbUser = args[3];
        String dbPassword = args[4];

        int batchSize = 20;
        String serverIP = "";

        if (args.length == 6) {
            Integer parsed = tryParseInt(args[5]);
            if (parsed != null) {
                batchSize = parsed.intValue();
            } else {
                serverIP = args[5];
            }
        } else if (args.length == 7) {
            Integer parsed = tryParseInt(args[5]);
            if (parsed == null) {
                System.err.println("Invalid batch_size: " + args[5]);
                return;
            }
            batchSize = parsed.intValue();
            serverIP = args[6];
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

        ktp11_process_methods methods = new ktp11_process_methods(
                inputPath,
                processedDir,
                jdbcUrl,
                dbUser,
                dbPassword,
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
}