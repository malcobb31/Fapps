package li_parser_v3;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * TP2_CSVSplitter - Splits a CSV file into two files based on a flag column.
 *
 * Produces: <originalName>_L.csv - rows where flag == 'L'
 *           <originalName>_I.csv - rows where flag == 'I'
 *
 * Rules: - Headers are written to BOTH output files regardless of row count. -
 * Rows with flag values other than 'L' or 'I' are skipped with a warning log. -
 * If the flag column is absent a SplitException is thrown (fail the file). -
 * Empty result files are still created so TP1 can be called even on 0-row
 * splits.
 */
public class TP2_CSVSplitter {

    // =========================================================================
    // LOGGING INFRASTRUCTURE
    // =========================================================================

    private static final String PROCESS_NAME    = "TP2";
    private static final String PROCESS_VERSION = "1.0";
    private static final boolean debugEnabled   = false;
    private static final DateTimeFormatter LOG_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String SERVER_IP;
    static {
        String ip;
        try {
            ip = java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            ip = "UNKNOWN";
        }
        SERVER_IP = ip;
    }

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(LOG_FORMAT);

        StringBuilder logBuilder = new StringBuilder(512);
        logBuilder.append("Time:").append(timestamp)
                  .append(" [").append(capitalize(level)).append("] ")
                  .append("Process_Name: ").append(PROCESS_NAME).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(PROCESS_VERSION)
                  .append(" Log_Level: EVT_LOG ")
                  .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                  .append(" Description: ").append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    // =========================================================================
    // PUBLIC DATA STRUCTURES
    // =========================================================================

    /** Result returned to the caller after a successful split. */
    public static class SplitResult {
        public final File lFile;
        public final File iFile;
        public final int lRowCount;
        public final int iRowCount;
        public final int skippedRowCount;

        SplitResult(File lFile, File iFile, int lRowCount, int iRowCount, int skippedRowCount) {
            this.lFile = lFile;
            this.iFile = iFile;
            this.lRowCount = lRowCount;
            this.iRowCount = iRowCount;
            this.skippedRowCount = skippedRowCount;
        }
    }

    /** Thrown when the split cannot proceed (e.g. missing flag column). */
    public static class SplitException extends Exception {
        public SplitException(String message) {
            super(message);
        }
    }

    // =========================================================================
    // PUBLIC API
    // =========================================================================

    /**
     * Split a CSV file by its flag column.
     *
     * @param sourceFile
     *            The CSV file to split.
     * @param flagColumnName
     *            Name of the column that contains 'L' or 'I'. Pass null or
     *            empty to attempt auto-detection (first column named "flag",
     *            case-insensitive).
     * @param outputDir
     *            Directory where split files will be written. Uses the same
     *            directory as sourceFile if null.
     * @return SplitResult with paths and row counts.
     * @throws SplitException
     *             if the file cannot be processed.
     */
    public SplitResult split(File sourceFile, String flagColumnName, File outputDir) throws SplitException {

        long fileSizeKB = sourceFile.exists() ? sourceFile.length() / 1024 : -1;
        long startTime  = System.currentTimeMillis();

        log("INFO",
            "CSV split started. File: " + sourceFile.getName()
            + ", size: " + fileSizeKB + " KB"
            + ", flagColumn: " + (flagColumnName != null ? flagColumnName : "auto-detect"),
            SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);

        if (!sourceFile.exists() || !sourceFile.isFile()) {
            log("ERROR",
                "Source file does not exist or is not a regular file: " + sourceFile.getAbsolutePath(),
                SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);
            throw new SplitException("Source file does not exist: " + sourceFile.getAbsolutePath());
        }

        // Resolve output directory
        File outDir = (outputDir != null) ? outputDir : sourceFile.getParentFile();
        if (!outDir.exists()) {
            outDir.mkdirs();
        }

        // Derive base name (strip .csv)
        String baseName = sourceFile.getName();
        if (baseName.toLowerCase().endsWith(".csv")) {
            baseName = baseName.substring(0, baseName.length() - 4);
        }

        File lFile = new File(outDir, baseName + "_L.csv");
        File iFile = new File(outDir, baseName + "_I.csv");

        int lCount       = 0;
        int iCount       = 0;
        int skippedCount = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(sourceFile));
             BufferedWriter lWriter = new BufferedWriter(new FileWriter(lFile));
             BufferedWriter iWriter = new BufferedWriter(new FileWriter(iFile))) {

            // ---- Parse header ----
            String headerLine = reader.readLine();
            if (headerLine == null || headerLine.trim().isEmpty()) {
                log("ERROR",
                    "CSV file is empty or has no header row. File: " + sourceFile.getName(),
                    SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);
                throw new SplitException("CSV file is empty or has no header: " + sourceFile.getName());
            }

            String[] headers  = parseCsvLine(headerLine);
            int flagIndex     = resolveFlagColumnIndex(headers, flagColumnName, sourceFile.getName());

            // Write headers to both output files
            lWriter.write(headerLine);
            lWriter.newLine();
            iWriter.write(headerLine);
            iWriter.newLine();

            log("INFO",
                "Header parsed. Flag column: " + headers[flagIndex]
                + " at index " + flagIndex
                + " in file: " + sourceFile.getName()
                + ". Total header columns: " + headers.length,
                SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);

            // ---- Process data rows ----
            String line;
            int lineNum = 1; // 1 = header already consumed

            while ((line = reader.readLine()) != null) {
                lineNum++;

                if (line.trim().isEmpty()) {
                    continue; // skip blank lines silently
                }

                String[] cols = parseCsvLine(line);

                // Guard against short rows
                if (flagIndex >= cols.length) {
                    log("WARN",
                        "Row " + lineNum + " in " + sourceFile.getName()
                        + " has fewer columns than expected (got " + cols.length
                        + ", need flag at index " + flagIndex + "). Skipping row.",
                        SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);
                    skippedCount++;
                    continue;
                }

                String flagValue = cols[flagIndex] == null ? "" : cols[flagIndex].trim();

                if ("L".equalsIgnoreCase(flagValue)) {
                    lWriter.write(line);
                    lWriter.newLine();
                    lCount++;
                } else if ("I".equalsIgnoreCase(flagValue)) {
                    iWriter.write(line);
                    iWriter.newLine();
                    iCount++;
                } else {
                    log("WARN",
                        "Row " + lineNum + " in " + sourceFile.getName()
                        + " has invalid flag value: [" + flagValue + "]. Expected L or I. Skipping row.",
                        SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);
                    skippedCount++;
                }

                if ((lCount + iCount) % 5000 == 0 && (lCount + iCount) > 0) {
                    log("DEBUG",
                        "Split progress for " + sourceFile.getName()
                        + ": L=" + lCount + ", I=" + iCount
                        + ", skipped=" + skippedCount
                        + ", current line=" + lineNum,
                        SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);
                }
            }

        } catch (SplitException se) {
            throw se;
        } catch (Exception e) {
            log("ERROR",
                "Unexpected error while splitting " + sourceFile.getName()
                + ". Cause: " + e.getMessage(),
                SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);
            throw new SplitException(
                    "Unexpected error while splitting " + sourceFile.getName() + ": " + e.getMessage());
        }

        long durationMs = System.currentTimeMillis() - startTime;

        log("INFO",
            "CSV split complete. File: " + sourceFile.getName()
            + " | L rows=" + lCount
            + " | I rows=" + iCount
            + " | skipped=" + skippedCount
            + " | output L: " + lFile.getName()
            + " | output I: " + iFile.getName()
            + " | duration: " + durationMs + " ms",
            SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);

        return new SplitResult(lFile, iFile, lCount, iCount, skippedCount);
    }

    // =========================================================================
    // PRIVATE HELPERS
    // =========================================================================

    /**
     * Resolve the index of the flag column. Priority: 1. Exact match on
     * flagColumnName (case-insensitive). 2. Auto-detect: first column whose
     * name equals "flag" (case-insensitive).
     *
     * Throws SplitException if not found.
     */
    private int resolveFlagColumnIndex(String[] headers, String flagColumnName, String fileName) throws SplitException {

        String target = (flagColumnName != null && !flagColumnName.trim().isEmpty()) ? flagColumnName.trim() : "flag"; // default
                                                                                                                       // auto-detect
                                                                                                                       // name

        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null && headers[i].trim().equalsIgnoreCase(target)) {
                return i;
            }
        }

        // Auto-detect failed -- report clearly
        StringBuilder available = new StringBuilder();
        for (String h : headers) {
            available.append("'").append(h).append("' ");
        }

        log("ERROR",
            "Flag column not found. Target column: [" + target + "] in file: " + fileName
            + ". Available columns: " + available.toString().trim(),
            SERVER_IP, TP2_Logger.EVT_CSV_SPLIT);

        throw new SplitException("Flag column '" + target + "' not found in file: " + fileName + ". Available columns: "
                + available.toString().trim());
    }

    /**
     * Parse a single CSV line respecting double-quoted fields. Trims
     * leading/trailing whitespace from each field value.
     */
    private String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder sb    = new StringBuilder();
        boolean inQuotes    = false;

        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == '|' && !inQuotes) {
                fields.add(cleanValue(sb.toString()));
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        fields.add(cleanValue(sb.toString())); // last field

        return fields.toArray(new String[0]);
    }

    private String cleanValue(String val) {
        if (val == null)
            return null;
        val = val.trim();

        if (val.startsWith("\"") && val.endsWith("\"")) {
            val = val.substring(1, val.length() - 1);
        }
        return val;
    }
}
