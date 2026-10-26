package binlog_reader_v1;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class binlog_methods {

    private Connection connection;

    private String mysqlbinlogPath;
    private String binlogFile;
    private String startDatetime;
    private String stopDatetime;

    private String currentOperation;
    private String currentTable;

    private boolean readingWhere = false;
    private boolean readingSet = false;

    private Map<String, TableMetadata> metadataCache = new HashMap<>();

    private Map<String, String> whereValues = new LinkedHashMap<>();
    private Map<String, String> setValues = new LinkedHashMap<>();

    private static final Pattern COLUMN_PATTERN = Pattern.compile("@(\\d+)=(.*)");

    public void startBinlogReader(
            String mysqlbinlogPath,
            String binlogFile,
            String startDatetime,
            String stopDatetime,
            String dbUrl,
            String dbUser,
            String dbPass
    ) throws Exception {

        this.mysqlbinlogPath = mysqlbinlogPath;
        this.binlogFile = binlogFile;
        this.startDatetime = startDatetime;
        this.stopDatetime = stopDatetime;

        connection = DriverManager.getConnection(dbUrl, dbUser, dbPass);

        System.out.println("MySQL connection established");

        readBinlog();
    }

    private void readBinlog() throws Exception {

        ProcessBuilder pb = new ProcessBuilder(
                mysqlbinlogPath,
                "--base64-output=DECODE-ROWS",
                "-v",
                "--start-datetime=" + startDatetime,
                "--stop-datetime=" + stopDatetime,
                binlogFile
        );

        Process process = pb.start();

        BufferedReader reader =
                new BufferedReader(new InputStreamReader(process.getInputStream()));

        String line;

        while ((line = reader.readLine()) != null) {

            processLine(line);

        }

        process.waitFor();

        flushPreviousEvent();

        System.out.println("Binlog reading completed");
    }

    private void processLine(String line) {

        try {

            line = line.trim();

            if (line.startsWith("### INSERT INTO")) {

                flushPreviousEvent();

                currentOperation = "INSERT";
                currentTable = extractTable(line);

                resetFlags();
            }

            else if (line.startsWith("### UPDATE")) {

                flushPreviousEvent();

                currentOperation = "UPDATE";
                currentTable = extractTable(line);

                resetFlags();
            }

            else if (line.startsWith("### DELETE FROM")) {

                flushPreviousEvent();

                currentOperation = "DELETE";
                currentTable = extractTable(line);

                resetFlags();
            }

            else if (line.startsWith("### WHERE")) {

                readingWhere = true;
                readingSet = false;
            }

            else if (line.startsWith("### SET")) {

                readingWhere = false;
                readingSet = true;
            }

            else if (line.startsWith("###   @")) {

                parseColumnLine(line);
            }

        }

        catch (Exception e) {

            e.printStackTrace();
        }
    }

    private void resetFlags() {

        readingWhere = false;
        readingSet = false;

        whereValues.clear();
        setValues.clear();
    }

    private void parseColumnLine(String line) throws Exception {

        if (currentTable == null) return;

        TableMetadata metadata = getMetadata(currentTable);

        Matcher matcher = COLUMN_PATTERN.matcher(line.replace("###", "").trim());

        if (!matcher.find()) return;

        int columnIndex = Integer.parseInt(matcher.group(1));
        String value = cleanValue(matcher.group(2));

        String columnName = metadata.columnIndexMap.get(columnIndex);

        if (columnName == null) return;

        if (readingWhere) {

            whereValues.put(columnName, value);
        }

        if (readingSet) {

            setValues.put(columnName, value);
        }
    }

    private void flushPreviousEvent() throws Exception {

        if (currentOperation == null || currentTable == null) return;

        TableMetadata metadata = getMetadata(currentTable);

        Map<String, String> source;

        if ("INSERT".equals(currentOperation)) {

            source = setValues;

        } else {

            source = whereValues;
        }

        List<String> pkNames = new ArrayList<>();
        List<String> pkValues = new ArrayList<>();

        for (String pk : metadata.primaryKeys) {

            if (source.containsKey(pk)) {

                pkNames.add(pk);
                pkValues.add(source.get(pk));
            }
        }

        if (pkNames.isEmpty()) {

            pkNames.add("UNKNOWN_PK");
            pkValues.add("UNKNOWN");
        }

        insertTrigger(
                currentTable,
                String.join(",", pkNames),
                String.join(",", pkValues),
                currentOperation
        );

        currentOperation = null;
        currentTable = null;
    }

    private void insertTrigger(
            String table,
            String pkName,
            String pkValue,
            String operation
    ) throws Exception {

        String sql =
                "INSERT INTO data_insertion_trigger " +
                "(record_time, table_name, primary_columns_name, primary_columns_value, type_of_operation) " +
                "VALUES (?, ?, ?, ?, ?)";

        try (PreparedStatement ps = connection.prepareStatement(sql)) {

            ps.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
            ps.setString(2, table);
            ps.setString(3, pkName);
            ps.setString(4, pkValue);
            ps.setString(5, operation);

            ps.executeUpdate();
        }
    }

    private TableMetadata getMetadata(String table) throws Exception {

        if (metadataCache.containsKey(table)) {

            return metadataCache.get(table);
        }

        TableMetadata metadata = loadTableMetadata(table);

        metadataCache.put(table, metadata);

        return metadata;
    }

    private TableMetadata loadTableMetadata(String tableFullName) throws Exception {

        TableMetadata metadata = new TableMetadata();

        String[] parts = tableFullName.split("\\.");

        String schema = parts.length > 1 ? parts[0] : connection.getCatalog();
        String table = parts.length > 1 ? parts[1] : parts[0];

        String columnQuery =
                "SELECT column_name, ordinal_position " +
                "FROM information_schema.columns " +
                "WHERE table_name=? AND table_schema=? " +
                "ORDER BY ordinal_position";

        try (PreparedStatement ps = connection.prepareStatement(columnQuery)) {

            ps.setString(1, table);
            ps.setString(2, schema);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {

                int pos = rs.getInt("ordinal_position");
                String col = rs.getString("column_name");

                metadata.columnIndexMap.put(pos, col);
            }
        }

        String pkQuery =
                "SELECT column_name " +
                "FROM information_schema.key_column_usage " +
                "WHERE table_name=? AND table_schema=? AND constraint_name='PRIMARY'";

        try (PreparedStatement ps = connection.prepareStatement(pkQuery)) {

            ps.setString(1, table);
            ps.setString(2, schema);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {

                metadata.primaryKeys.add(rs.getString("column_name"));
            }
        }

        return metadata;
    }

    private String extractTable(String line) {

        int firstTick = line.indexOf("`");
        int lastTick = line.lastIndexOf("`");

        if (firstTick != -1 && lastTick != -1 && lastTick > firstTick) {

            return line.substring(firstTick + 1, lastTick).replace("`.`", ".");
        }

        return null;
    }

    private String cleanValue(String value) {

        value = value.trim();

        if (value.equals("NULL")) return null;

        if (value.startsWith("'") && value.endsWith("'")) {

            value = value.substring(1, value.length() - 1);
        }

        return value;
    }

    static class TableMetadata {

        Map<Integer, String> columnIndexMap = new HashMap<>();
        Set<String> primaryKeys = new LinkedHashSet<>();
    }
}