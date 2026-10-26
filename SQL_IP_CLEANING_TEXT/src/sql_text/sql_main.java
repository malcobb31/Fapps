package sql_text;

import java.io.File;
import java.sql.*;
import java.util.*;

public class sql_main {

    static String MYSQL_URL = "jdbc:mysql://localhost:3306/pview?allowLoadLocalInfile=true";
    static String MYSQL_USER = "root";
    static String MYSQL_PASS = "admin@321";

    static String CSV_DIR = "/datadir/IP_dest_UAT/";

    public static void main(String[] args) {
        try (Connection con = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASS)) {

            // Your issue table name
            String issueTable = "pview.issue_table_fix_list";

            List<IssueRow> rows = fetchIssueRows(con, issueTable);

            // process unique table_name (same table may come twice NOT NULL + DATATYPE)
            Map<String, IssueRow> uniqueTableMap = new LinkedHashMap<>();
            for (IssueRow r : rows) {
                uniqueTableMap.putIfAbsent(r.tableName, r);
            }

            for (IssueRow row : uniqueTableMap.values()) {
                System.out.println("===========================================");
                System.out.println("Processing table: " + row.tableName);
                System.out.println("===========================================");

                String fullCsvPath = resolveCsvPath(row.rawFile);
                System.out.println("CSV resolved: " + fullCsvPath);

                String dbName = row.tableName.split("\\.")[0];
                String tblName = row.tableName.split("\\.")[1];

                String dupTable = dbName + "." + tblName + "_duplicate";
                String orgTable = dbName + "." + tblName + "_org";

                // STEP 1: drop duplicate if exists
                exec(con, "DROP TABLE IF EXISTS " + dupTable);

                // STEP 2: create duplicate structure
                exec(con, "CREATE TABLE " + dupTable + " LIKE " + row.tableName);

                // STEP 3: make all columns TEXT NULL (skip PK + auto_increment)
                alterAllColumnsToTextNullable(con, dupTable);

                // STEP 4: load csv into duplicate
                loadCsvIntoTable(con, fullCsvPath, dupTable);

                // STEP 5: clean NOT NULL + datatype issues (basic safe cleaning)
                cleanDuplicateTable(con, dbName, tblName + "_duplicate");
                
                trimDataToOriginalLengths(con, row.tableName, dupTable);


                // STEP 6: convert duplicate back to original datatype + NOT NULL
                syncDuplicateSchemaToOriginal(con, row.tableName, dupTable);
                
                fixNotNullColumns(con, row.tableName, dupTable);


                // STEP 7: rename original -> _org
                exec(con, "DROP TABLE IF EXISTS " + orgTable);
                exec(con, "RENAME TABLE " + row.tableName + " TO " + orgTable);

                // STEP 8: rename duplicate -> original
                exec(con, "RENAME TABLE " + dupTable + " TO " + row.tableName);

                System.out.println("DONE: " + row.tableName);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ============================================================
    // FETCH ISSUE TABLE ROWS
    // ============================================================
    static List<IssueRow> fetchIssueRows(Connection con, String issueTable) throws Exception {
        List<IssueRow> list = new ArrayList<>();

        String sql = "SELECT table_name, raw_file FROM " + issueTable + " ORDER BY id";
        try (PreparedStatement ps = con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                IssueRow r = new IssueRow();
                r.tableName = rs.getString("table_name");
                r.rawFile = rs.getString("raw_file");
                list.add(r);
            }
        }
        return list;
    }

    // ============================================================
    // RESOLVE CSV NAME
    // ============================================================
    static String resolveCsvPath(String rawFile) throws Exception {
        if (rawFile.contains("....csv")) {
            // prefix search
            String prefix = rawFile.replace("....csv", "");
            File dir = new File(CSV_DIR);

            File[] matches = dir.listFiles((d, name) -> name.startsWith(prefix) && name.endsWith(".csv"));
            if (matches == null || matches.length == 0) {
                throw new Exception("CSV not found for prefix: " + prefix);
            }
            return matches[0].getAbsolutePath();
        } else {
            return CSV_DIR + rawFile;
        }
    }

    // ============================================================
    // STEP 3: ALTER ALL COLS TO TEXT NULL (SKIP PK + AUTO_INCREMENT)
    // ============================================================
    public static void alterAllColumnsToTextNullable(Connection con, String tableName) throws Exception {

        String q = "SHOW COLUMNS FROM " + tableName;

        try (Statement st = con.createStatement();
             ResultSet rs = st.executeQuery(q)) {

            while (rs.next()) {

                String col = rs.getString("Field");
                String key = rs.getString("Key");     // PRI / UNI / MUL / ""
                String extra = rs.getString("Extra"); // auto_increment

                boolean isAutoInc = extra != null && extra.toLowerCase().contains("auto_increment");

                //  Skip: PK + UNIQUE + INDEXED columns + auto_increment
                // key can be PRI / UNI / MUL
                boolean isIndexed = key != null && !key.trim().isEmpty();

                if (isAutoInc || isIndexed) {
                    continue;
                }

                String alter = "ALTER TABLE " + tableName + " MODIFY `" + col + "` TEXT NULL";
                exec(con, alter);
            }
        }
    }

    

    // ============================================================
    // STEP 4: LOAD CSV INTO DUP TABLE
    // ============================================================
    static void loadCsvIntoTable(Connection con, String csvPath, String tableName) throws Exception {

        String sql = "LOAD DATA LOCAL INFILE '" + csvPath.replace("\\", "\\\\") + "' " +
                "INTO TABLE " + tableName + " " +
                "CHARACTER SET latin1 " +
                "FIELDS TERMINATED BY '|' " +
                "OPTIONALLY ENCLOSED BY '\"' " +
                "ESCAPED BY '\\\\' " +
                "LINES TERMINATED BY '\\n' " +
                "IGNORE 1 LINES";

        exec(con, sql);
    }

    // ============================================================
    // STEP 5: CLEAN (basic cleaning)
    // ============================================================
    static void cleanDuplicateTable(Connection con, String schema, String table) throws Exception {

        String sqlCols = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE " +
                "FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";

        List<String> safeCols = new ArrayList<>();

        try (PreparedStatement ps = con.prepareStatement(sqlCols)) {
            ps.setString(1, schema);
            ps.setString(2, table);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {

                    String col = rs.getString("COLUMN_NAME");
                    String type = rs.getString("DATA_TYPE");       // varchar, text, decimal, int, datetime...
                    String isNullable = rs.getString("IS_NULLABLE"); // YES / NO

                    //  only varchar/text columns AND only nullable
                    if ("YES".equalsIgnoreCase(isNullable) &&
                            (type.equalsIgnoreCase("varchar") ||
                             type.equalsIgnoreCase("text") ||
                             type.equalsIgnoreCase("longtext") ||
                             type.equalsIgnoreCase("mediumtext") ||
                             type.equalsIgnoreCase("tinytext"))) {

                        safeCols.add(col);
                    }
                }
            }
        }

        for (String col : safeCols) {
            String upd = "UPDATE " + schema + "." + table +
                    " SET `" + col + "` = NULL WHERE `" + col + "`=''";
            exec(con, upd);
        }
    }

    static void trimDataToOriginalLengths(Connection con, String originalTable, String duplicateTable) throws Exception {

        String schema = originalTable.split("\\.")[0];
        String table = originalTable.split("\\.")[1];

        String q = "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH " +
                "FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";

        try (PreparedStatement ps = con.prepareStatement(q)) {
            ps.setString(1, schema);
            ps.setString(2, table);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {

                    String col = rs.getString("COLUMN_NAME");
                    String type = rs.getString("DATA_TYPE");

                    Long maxLen = rs.getLong("CHARACTER_MAXIMUM_LENGTH");
                    if (rs.wasNull()) {
                        maxLen = null;
                    }

                    // only for varchar/char where length exists
                    if (maxLen != null && (type.equalsIgnoreCase("varchar") || type.equalsIgnoreCase("char"))) {

                        String upd = "UPDATE " + duplicateTable +
                                " SET `" + col + "` = LEFT(`" + col + "`, " + maxLen + ") " +
                                "WHERE `" + col + "` IS NOT NULL AND LENGTH(`" + col + "`) > " + maxLen;

                        exec(con, upd);
                    }
                }
            }
        }
    }


    // ============================================================
    // STEP 6: MAKE DUPLICATE SAME AS ORIGINAL (datatype + NOT NULL)
    // ============================================================
    static void syncDuplicateSchemaToOriginal(Connection con, String originalTable, String duplicateTable) throws Exception {

        String tempCopy = duplicateTable + "_copy";

        exec(con, "DROP TABLE IF EXISTS " + tempCopy);
        exec(con, "CREATE TABLE " + tempCopy + " LIKE " + originalTable);

        exec(con, "INSERT INTO " + tempCopy + " SELECT * FROM " + duplicateTable);

        exec(con, "DROP TABLE " + duplicateTable);
        exec(con, "RENAME TABLE " + tempCopy + " TO " + duplicateTable);
    }
    
    static void fixNotNullColumns(Connection con, String originalTable, String duplicateTable) throws Exception {

        String schema = originalTable.split("\\.")[0];
        String table = originalTable.split("\\.")[1];

        String q = "SELECT COLUMN_NAME, DATA_TYPE " +
                "FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND IS_NULLABLE='NO'";

        try (PreparedStatement ps = con.prepareStatement(q)) {
            ps.setString(1, schema);
            ps.setString(2, table);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {

                    String col = rs.getString("COLUMN_NAME");
                    String type = rs.getString("DATA_TYPE").toLowerCase();

                    // skip auto_increment PK type safety (do not update)
                    if ("id".equalsIgnoreCase(col) || col.toLowerCase().contains("ref_no")) {
                        // optional skip if you want
                    }

                    String defaultValue;

                    if (type.contains("int") || type.contains("decimal") || type.contains("double") || type.contains("float")) {
                        defaultValue = "0";
                    } else if (type.contains("date") || type.contains("time") || type.contains("timestamp") || type.contains("datetime")) {
                        defaultValue = "'1970-01-01 00:00:00'";
                    } else {
                        defaultValue = "'NA'";
                    }

                    String upd = "UPDATE " + duplicateTable +
                            " SET `" + col + "` = " + defaultValue +
                            " WHERE `" + col + "` IS NULL";

                    exec(con, upd);
                }
            }
        }
    }


    // ============================================================
    // SQL EXEC HELPER
    // ============================================================
    static void exec(Connection con, String sql) throws Exception {
        try (Statement st = con.createStatement()) {
            st.execute(sql);
        }
    }

    // ============================================================
    static class IssueRow {
        String tableName;
        String rawFile;
    }
}
