package eyeq_parser;

import java.io.*;
import java.nio.file.*;
import java.util.regex.*;

public class eyeq_parser_v1 {

    private static final Pattern LINE_PATTERN = Pattern.compile(
        "^(\\d{4}-\\d{2}-\\d{2})\\s+\\d{2}:\\d{2}:\\d{2}\\s+-\\s+Name:\\s*(.*?)\\s+-\\s+Phone:\\s*(.*?)\\s+-\\s+Subject:\\s*(.*)$"
    );

    public void downloadAndConvert(String url, String outputCsvPath) throws Exception {

        String tempTxt = outputCsvPath + ".tmp_download.txt";

        System.out.println("===============================================");
        System.out.println("Downloading TXT from: " + url);
        System.out.println("Temp file saved at  : " + tempTxt);
        System.out.println("Output CSV          : " + outputCsvPath);
        System.out.println("===============================================");

        boolean downloaded = downloadUsingWget(url, tempTxt);

        if (!downloaded) {
            throw new Exception("Download failed using wget. Please try manual wget once.");
        }

        System.out.println(" Download complete. Now parsing...");
        convertLocalTxtToCsv(tempTxt, outputCsvPath);

        // Optional cleanup
        try {
            new File(tempTxt).delete();
        } catch (Exception ignore) {}
    }

    private boolean downloadUsingWget(String url, String outputFile) {

        // No -q so you can see error if any
        String[] cmd = new String[]{
                "wget",
                "-O", outputFile,
                url
        };

        return runCommand(cmd);
    }

    private boolean runCommand(String[] cmd) {
        try {
            System.out.println("[CMD] " + join(cmd));

            ProcessBuilder pb = new ProcessBuilder(cmd);
            pb.redirectErrorStream(true);
            Process p = pb.start();

            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }

            int exitCode = p.waitFor();
            System.out.println("[CMD EXIT CODE] " + exitCode);

            return exitCode == 0;

        } catch (Exception e) {
            System.out.println("[CMD ERROR] " + e.getMessage());
            return false;
        }
    }

    private String join(String[] arr) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arr.length; i++) {
            sb.append(arr[i]);
            if (i != arr.length - 1) sb.append(" ");
        }
        return sb.toString();
    }

    public void convertLocalTxtToCsv(String inputTxtPath, String outputCsvPath) throws Exception {

        long total = 0, success = 0, failed = 0;

        BufferedReader br = null;
        BufferedWriter bw = null;

        try {
            br = new BufferedReader(new FileReader(inputTxtPath));
            bw = new BufferedWriter(new FileWriter(outputCsvPath));

            bw.write("Date,Name,Phone,Subject");
            bw.newLine();

            String line;
            while ((line = br.readLine()) != null) {
                total++;

                line = line.trim();
                if (line.isEmpty()) continue;

                Matcher matcher = LINE_PATTERN.matcher(line);

                if (matcher.matches()) {
                    String date = matcher.group(1).trim();
                    String name = escapeCsv(matcher.group(2).trim());
                    String phone = escapeCsv(matcher.group(3).trim());
                    String subject = escapeCsv(matcher.group(4).trim());

                    bw.write(date + "," + name + "," + phone + "," + subject);
                    bw.newLine();
                    success++;
                } else {
                    failed++;
                }
            }

            bw.flush();

        } finally {
            try { if (br != null) br.close(); } catch (Exception ignore) {}
            try { if (bw != null) bw.close(); } catch (Exception ignore) {}
        }

        System.out.println("===============================================");
        System.out.println("Total Lines   : " + total);
        System.out.println("Parsed Success: " + success);
        System.out.println("Failed Lines  : " + failed);
        System.out.println("CSV Output    : " + outputCsvPath);
        System.out.println("===============================================");
    }

    private String escapeCsv(String value) {
        if (value == null) return "";
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            value = value.replace("\"", "\"\"");
            return "\"" + value + "\"";
        }
        return value;
    }
}
