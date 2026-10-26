package ipv2_parser;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ipv2_parser_main {

    public static void main(String[] args) {

        if (args.length != 2) {
            System.out.println("Usage:");
            System.out.println("<input_csv_file> <output_csv_file>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        System.out.println("Length of arguments: " + args.length);
        System.out.println("-------------- " + inputPath + " ------------");
        System.out.println("-------------- " + outputPath + " ------------");

        try {

            Path inputFile = Paths.get(inputPath);

            if (!Files.exists(inputFile) || !Files.isRegularFile(inputFile)) {
                System.err.println("ERROR: Input file does not exist or is not a file.");
                System.exit(2);
            }

            Path outputFile = Paths.get(outputPath);
            Path parentDir = outputFile.getParent();

            if (parentDir != null && !Files.exists(parentDir)) {
                Files.createDirectories(parentDir);
            }

            ipv2_parser_V1.process(inputPath, outputPath);

            System.out.println("Parser executed successfully.");
            System.exit(0);

        } catch (Exception e) {

            System.err.println("Parser execution failed:");
            e.printStackTrace();
            System.exit(3);
        }
    }
}