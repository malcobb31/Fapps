package error_parser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.regex.*;

public class error_parser_main {

    private static final String OUTPUT_HEADER =
            "Table_Name,Sno,Status,Error,Raw_File";

    // Regex patterns
    private static final Pattern TABLE_PATTERN =
            Pattern.compile("tableName:([^\\s]+)");

    private static final Pattern FILE_PATTERN =
            Pattern.compile("File:([^\\s]+\\.csv)");

    private static final Pattern ERROR_PATTERN =
            Pattern.compile("Error:Duplicate entry (.+)");

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Usage: java FileErrorParser <input_log.csv> <output.csv>");
            return;
        }

        String inputFile = args[0];
        String outputFile = args[1];

        parseFileErrors(inputFile, outputFile);
    }

    public static void parseFileErrors(String inputFile, String outputFile) {

        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get(inputFile), StandardCharsets.UTF_8);
             BufferedWriter writer = Files.newBufferedWriter(
                     Paths.get(outputFile), StandardCharsets.UTF_8)) {

            writer.write(OUTPUT_HEADER);
            writer.newLine();

            String line;

            while ((line = reader.readLine()) != null) {

                // Filter only FILE_ERROR logs
                if (!line.contains("EVT_Type: FILE_ERROR"))
                    continue;

                String tableName = extract(TABLE_PATTERN, line);
                String fileName = extract(FILE_PATTERN, line);
                String errorMsg = extract(ERROR_PATTERN, line);

                if (tableName == null || fileName == null || errorMsg == null)
                    continue;

                // Extract Sno from filename
                // raw_NA_NA_NA_247_2026_02_26_11_36_02.csv
                String sno = extractSnoFromFile(fileName);

                writer.write(String.format("\"%s\",\"%s\",\"\",\"%s\",\"%s\"",
                        tableName,
                        sno,
                        errorMsg,
                        fileName));
                writer.newLine();
            }

            System.out.println("Parsing completed successfully.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String extract(Pattern pattern, String line) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.find()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    private static String extractSnoFromFile(String fileName) {

        try {
            // raw_NA_NA_NA_247_2026_02_26_11_36_02.csv
            String[] parts = fileName.split("_");

            if (parts.length >= 5) {
                return parts[4]; // 247
            }
        } catch (Exception ignored) {}

        return "";
    }
}