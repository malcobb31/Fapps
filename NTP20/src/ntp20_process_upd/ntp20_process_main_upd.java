package ntp20_process_upd;

import java.io.File;

public class ntp20_process_main_upd {
    public static void main(String[] args) {
        if (args == null || args.length < 4) {
            System.err.println("Usage: java ntp20_update_main <inputFileOrDirectory> <jdbcUrl> <dbUser> <dbPassword> [delimiter] [serverIP]");
            return;
        }

        String inputPath = args[0] != null ? args[0].trim() : "";
        String jdbcUrl = args[1] != null ? args[1].trim() : "";
        String dbUser = args[2] != null ? args[2].trim() : "";
        String dbPassword = args[3] != null ? args[3] : "";
        String delimiter = (args.length >= 5 && args[4] != null && args[4].trim().length() > 0) ? args[4].trim() : "|";
        String serverIP = (args.length >= 6 && args[5] != null) ? args[5].trim() : "";

        if (inputPath.length() == 0) {
            System.err.println("Input path is required.");
            return;
        }
        if (jdbcUrl.length() == 0) {
            System.err.println("JDBC URL is required.");
            return;
        }
        if (dbUser.length() == 0) {
            System.err.println("DB user is required.");
            return;
        }
        if (dbPassword == null) {
            System.err.println("DB password is required.");
            return;
        }

        File input = new File(inputPath);
        if (!input.exists()) {
            System.err.println("Input path does not exist: " + inputPath);
            return;
        }
        if (!input.isFile() && !input.isDirectory()) {
            System.err.println("Input path is neither a file nor a directory: " + inputPath);
            return;
        }

        ntp20_update_methods methods = new ntp20_update_methods("NTP20", "1.0.0", jdbcUrl, dbUser, dbPassword, delimiter, serverIP);
        methods.process(input);
    }
}