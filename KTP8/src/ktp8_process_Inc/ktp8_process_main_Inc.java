package ktp8_process_Inc;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ktp8_process_main_Inc {

    public static void main(String[] args) {
        if (args == null || args.length != 6) {
            System.err.println(
                    "Usage: java ktp8_process_main <jdbcUrl> <dbUser> <dbPass> <inputDirectory> <outputDirectory> <serverIP>");
            return;
        }

        String jdbcUrl = args[0];
        String dbUser = args[1];
        String dbPass = args[2];
        String inputDirectory = args[3];
        String outputDirectory = args[4];
        String serverIP = args[5];

        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()
                || dbUser == null || dbUser.trim().isEmpty()
                || dbPass == null || dbPass.trim().isEmpty()
                || inputDirectory == null || inputDirectory.trim().isEmpty()
                || outputDirectory == null || outputDirectory.trim().isEmpty()) {
            System.err.println("Invalid arguments: jdbcUrl, dbUser, dbPass, inputDirectory and outputDirectory cannot be empty.");
            return;
        }

        File sourceDir = new File(inputDirectory);
        if (!sourceDir.exists() || !sourceDir.isDirectory()) {
            System.err.println("Input directory not found: " + inputDirectory);
            return;
        }

        File outDir = new File(outputDirectory);
        if (!outDir.exists()) {
            outDir.mkdirs();
        }

        new ktp8_process_methods().processDirectory(
                inputDirectory,
                outputDirectory,
                jdbcUrl,
                dbUser,
                dbPass,
                serverIP);
    }
}