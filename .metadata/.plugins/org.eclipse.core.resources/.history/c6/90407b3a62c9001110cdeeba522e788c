package ntp12_process_upd;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CSVUtil {

    public static List<Map<String, String>> loadCsv(String filePath) throws IOException {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return rows;
            }

            String[] headers = parseLine(headerLine);
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] values = parseLine(line);
                Map<String, String> row = new LinkedHashMap<String, String>();

                for (int i = 0; i < headers.length; i++) {
                    String key = headers[i] == null ? "" : headers[i].trim().toLowerCase();
                    String val = (i < values.length && values[i] != null) ? values[i].trim() : "";
                    row.put(key, val);
                }

                rows.add(row);
            }
        }

        return rows;
    }

    public static Map<String, Map<String, String>> buildLookup(
            List<Map<String, String>> rows, String keyColumn) {

        Map<String, Map<String, String>> lookup = new HashMap<String, Map<String, String>>();
        if (rows == null) {
            return lookup;
        }

        String lookupKey = keyColumn == null ? "" : keyColumn.trim().toLowerCase();

        for (Map<String, String> row : rows) {
            if (row == null) continue;
            String key = row.get(lookupKey);
            if (key != null && !key.trim().isEmpty()) {
                lookup.put(key.trim(), row);
            }
        }

        return lookup;
    }

    private static String[] parseLine(String line) {
        List<String> tokens = new ArrayList<String>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                tokens.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        tokens.add(current.toString());
        return tokens.toArray(new String[0]);
    }
}