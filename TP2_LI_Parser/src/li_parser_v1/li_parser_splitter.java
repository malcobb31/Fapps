package li_parser_v1;

import java.io.*;
import java.util.*;

public class li_parser_splitter {

    PrintUtilMsg pu = new PrintUtilMsg();

    public Map<String, String> splitFile(String inputFile, String flagColumn) {

        Map<String, String> map = new HashMap<>();

        String base = inputFile.replace(".csv", "");
        String lFile = base + "_L.csv";
        String iFile = base + "_I.csv";

        try (
                BufferedReader br = new BufferedReader(new FileReader(inputFile));
                BufferedWriter bwL = new BufferedWriter(new FileWriter(lFile));
                BufferedWriter bwI = new BufferedWriter(new FileWriter(iFile))
        ) {

            String header = br.readLine();
            if (header == null) return null;

            String[] headers = header.split(",", -1);

            int flagIndex = -1;

            for (int i = 0; i < headers.length; i++) {
                if (headers[i].equalsIgnoreCase(flagColumn)) {
                    flagIndex = i;
                    break;
                }
            }

            if (flagIndex == -1) {
                pu.sop("TP2", "Flag column NOT FOUND");
                return null;
            }

            bwL.write(header); bwL.newLine();
            bwI.write(header); bwI.newLine();

            String line;
            int l = 0, i = 0;

            while ((line = br.readLine()) != null) {

                String[] parts = line.split(",", -1);

                if (flagIndex >= parts.length) continue;

                if ("L".equalsIgnoreCase(parts[flagIndex])) {
                    bwL.write(line); bwL.newLine();
                    l++;
                } else if ("I".equalsIgnoreCase(parts[flagIndex])) {
                    bwI.write(line); bwI.newLine();
                    i++;
                }
            }

            pu.sop("TP2", "Split Done L=" + l + " I=" + i);

            map.put("L", lFile);
            map.put("I", iFile);

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        return map;
    }
}