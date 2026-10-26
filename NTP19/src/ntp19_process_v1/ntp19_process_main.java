package ntp19_process_v1;

import java.io.File;

public class ntp19_process_main {

    public static void main(String[] args) {
        if (args == null || args.length < 6) {
            System.err.println("Usage: java -cp <jar>:lib/* ntp19_process_v1.ntp19_process_main <inputPath> <jdbcUrl> <dbUser> <dbPassword> [delimiter] [processedDir] [serverIp]");
            return;
        }

        String inputPath = args[0];
        String jdbcUrl = args[1];
        String dbUser = args[2];
        String dbPassword = args[3];
        String delimiter = args.length > 4 ? args[4] : "|";
        String processedDir = args.length > 5 ? args[5] : null;
        String serverIp = args.length > 6 ? args[6] : null;

        if (inputPath == null || inputPath.trim().isEmpty()) {
            System.err.println("Input path is required.");
            return;
        }

        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            System.err.println("JDBC URL is required.");
            return;
        }

        if (dbUser == null || dbUser.trim().isEmpty()) {
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

        try {
            ntp19_process_methods methods = new ntp19_process_methods(
                    jdbcUrl,
                    dbUser,
                    dbPassword,
                    delimiter,
                    processedDir,
                    serverIp
            );
            methods.process(input);
        } catch (Exception e) {
            System.err.println("Fatal error: " + (e.getMessage() == null ? "Unknown error" : e.getMessage()));
        }
    }
}
