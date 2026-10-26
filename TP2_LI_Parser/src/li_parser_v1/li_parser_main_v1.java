package li_parser_v1;

import java.io.File;
import java.nio.file.*;
import java.sql.*;
import java.util.*;

public class li_parser_main_v1 {

    static PrintUtilMsg pu = new PrintUtilMsg();

    public static void main(String[] args) {

        if (args.length < 8) {
            pu.sop("MAIN", "Invalid arguments");
            return;
        }

        String mysqlURL = args[0];
        String mysqlUser = args[1];
        String mysqlPass = args[2];
        String configTable = args[3];

        String pgURL = args[4];
        String pgUser = args[5];
        String pgPass = args[6];

        String filterTable = args[7];
        String flagColumn = (args.length >= 9) ? args[8] : "flag";

        try {
            Connection conn = new MySQLConnection().getConnection(mysqlURL, mysqlUser, mysqlPass);

            String query = "SELECT source_path, destination_table_name, destination_path, parameters FROM " + configTable;
            PreparedStatement ps = conn.prepareStatement(query);
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {

                String sourcePath = rs.getString("source_path");
                String destTable = rs.getString("destination_table_name");
                String destPath = rs.getString("destination_path");
                String parameters = rs.getString("parameters");

                String[] parts = parameters.split("\\|", -1);

                String directColumns = parts[0];
                int metadataCount = Integer.parseInt(parts[1]);
                String destMetaCols = parts[2];
                String sourceMetaCols = parts[3];
                String jsonKeys = parts[4];

                File folder = new File(sourcePath);

                File[] files = folder.listFiles((d, name) -> name.endsWith(".csv"));

                if (files == null || files.length == 0) continue;

                Arrays.sort(files, (f1, f2) -> compareNaturally(f1.getName(), f2.getName()));

                for (File file : files) {

                    pu.sop("MAIN", "Processing File: " + file.getName());

                    // ===== TP2 =====
                    li_parser_tp2 splitter = new li_parser_tp2();

                    Map<String, String> splitFiles = splitter.splitFile(file.getAbsolutePath(), flagColumn);

                    if (splitFiles == null) {
                        pu.sop("MAIN", "TP2 FAILED");
                        continue;
                    }

                    // ===== MULTI TABLE =====
                    String[] tables = destTable.split(",");

                    boolean finalStatus = true;

                    for (int i = 0; i < tables.length; i++) {

                        String table = tables[i].trim();

                        String fileToProcess = (i == 0)
                                ? splitFiles.get("L")
                                : splitFiles.get("I");

                        pu.sop("MAIN", "Table: " + table + " File: " + fileToProcess);

                        boolean status = new li_parser_process().parseAndTransferData(
                                pgURL,
                                pgUser,
                                pgPass,
                                fileToProcess,
                                table,
                                directColumns,
                                metadataCount,
                                destMetaCols,
                                sourceMetaCols,
                                jsonKeys
                        );

                        if (!status) {
                            finalStatus = false;
                        }
                    }

                    // ===== MOVE FILE ONLY IF SUCCESS =====
                    if (finalStatus) {

                        Path targetDir = Paths.get(destPath);

                        if (!Files.exists(targetDir)) {
                            Files.createDirectories(targetDir);
                        }

                        Files.move(file.toPath(),
                                targetDir.resolve(file.getName()),
                                StandardCopyOption.REPLACE_EXISTING);

                        pu.sop("MAIN", "SUCCESS - FILE MOVED");

                    } else {
                        pu.sop("MAIN", "FAILED - FILE NOT MOVED");
                    }
                }
            }

            conn.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ===== NATURAL SORT =====
    private static int compareNaturally(String str1, String str2) {

        int i = 0, j = 0;

        while (i < str1.length() && j < str2.length()) {

            char c1 = str1.charAt(i);
            char c2 = str2.charAt(j);

            if (Character.isDigit(c1) && Character.isDigit(c2)) {

                StringBuilder num1 = new StringBuilder();
                StringBuilder num2 = new StringBuilder();

                while (i < str1.length() && Character.isDigit(str1.charAt(i)))
                    num1.append(str1.charAt(i++));

                while (j < str2.length() && Character.isDigit(str2.charAt(j)))
                    num2.append(str2.charAt(j++));

                int cmp = Integer.compare(
                        Integer.parseInt(num1.toString()),
                        Integer.parseInt(num2.toString())
                );

                if (cmp != 0) return cmp;

            } else {
                if (c1 != c2) return Character.compare(c1, c2);
                i++; j++;
            }
        }

        return Integer.compare(str1.length(), str2.length());
    }
}