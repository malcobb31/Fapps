package tp3_update;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.LinkedHashMap;

/**
 * CSVSourceData - Data Retrieval from CSV File (TP3)
 *
 * Mirrors SourceData.java in structure and return format.
 * Returns HashMap<UniqueKey, String[]> where each record contains:
 *   [sourceCol0, sourceCol1, ..., sourceColN, flagColumnValue]
 *
 * Methods:
 * 1. fetchCSVData() - Read CSV, locate required columns by header name,
 *                     return data in same format as SourceData.fetchSourceData()
 */
public class CSVSourceData {

    PrintUtilMsg pu = new PrintUtilMsg();

    /**
     * Fetch data from CSV file.
     *
     * @param csvFilePath      - Absolute path to the CSV file
     * @param sourceColumns    - Array of source column names to read (must match CSV header)
     * @param sourceCheckColumn - Column header name containing 'I'/'A' flag values
     * @return HashMap<UniqueKey, String[]> with same layout as SourceData, or null on failure
     */


    // =========================================================================
    // Private helpers
    // =========================================================================

    /**
     * Find the index of a column name in the CSV header array.
     * Package-private: used by ProcessData.updateDataStream() for streaming processing.
     * Comparison is case-insensitive and trimmed.
     *
     * @return 0-based index, or -1 if not found
     */
    int findColumnIndex(String[] headers, String columnName) {
        String target = columnName.trim().toLowerCase();
        for (int i = 0; i < headers.length; i++) {
            if (headers[i] != null && headers[i].trim().toLowerCase().equals(target)) {
                return i;
            }
        }
        pu.log("DEBUG",
               "Column lookup failed - column_name: " + columnName + " not found in " + headers.length + " headers",
               "CSV_PARSE");
        return -1;
    }

    /**
     * Parse a single CSV line following RFC 4180 rules.
     * Package-private: used by ProcessData.updateDataStream() for streaming processing.
     * - Fields may be enclosed in double-quotes.
     * - A double-quote inside a quoted field is represented by two consecutive double-quotes.
     * - Fields may contain commas and newlines when quoted.
     * - Leading/trailing spaces outside quotes are preserved (raw field value returned).
     * Does NOT use String.split(",") - handles all edge cases properly.
     *
     * @param line - Raw CSV line
     * @return Array of parsed field values
     */
    String[] parseCsvLine(String line) {

        java.util.List<String> fields = new java.util.ArrayList<String>();

        if (line == null) {
            pu.log("WARN", "parseCsvLine received null line - returning empty field array", "CSV_PARSE");
            return new String[0];
        }

        int length = line.length();
        int i = 0;

        while (i <= length) {

            if (i == length) {
                // Trailing comma produced an empty field
                fields.add("");
                break;
            }

            if (line.charAt(i) == '"') {
                // ===== Quoted field =====
                StringBuilder sb = new StringBuilder();
                i++; // skip opening quote

                while (i < length) {
                    char c = line.charAt(i);

                    if (c == '"') {
                        // Peek ahead for escaped double-quote ""
                        if (i + 1 < length && line.charAt(i + 1) == '"') {
                            sb.append('"');
                            i += 2;
                        } else {
                            // Closing quote
                            i++;
                            break;
                        }
                    } else {
                        sb.append(c);
                        i++;
                    }
                }

                fields.add(sb.toString());

                // After closing quote, next char must be comma or end-of-line
                if (i < length && line.charAt(i) == '|') {
                    i++; // skip separator, continue to next field
                }

            } else {
                // ===== Unquoted field =====
                int start = i;

                while (i < length && line.charAt(i) != '|') {
                    i++;
                }

                fields.add(line.substring(start, i));

                if (i < length && line.charAt(i) == '|') {
                    i++; // skip comma
                    // If comma was the last character, add empty trailing field on next iteration
                    if (i == length) {
                        fields.add("");
                        break;
                    }
                } else {
                    // End of line
                    break;
                }
            }
        }

        return fields.toArray(new String[0]);
    }
}
