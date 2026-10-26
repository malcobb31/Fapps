package audit_process;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class TableAuditMethods {

    private static final String PROCESS_NAME = "TableAudit";
    private static final String PROCESS_VERSION = "1.0.0";
    private static final boolean DEBUG_ENABLED = false;
    private static final DateTimeFormatter LOG_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static final String SQL_QUERY1 =
            "SELECT table_name, SUM(size), COUNT(Export_Status) " +
            "FROM ep_events " +
            "WHERE event_name = 'event_end' " +
            "AND Export_Status = 'true' " +
            "GROUP BY table_name " +
            "ORDER BY table_name";

    private static final String SQL_QUERY2 =
            "SELECT Table_Name, No_Record " +
            "FROM All_Table_Data_Audit_Report_Prod_count " +
            "WHERE Data_Date = ? " +
            "ORDER BY Table_Name";

    private static class Query1Row {
        String tableName;
        Long sumSize;
        Long exportCount;

        Query1Row(String tableName, Long sumSize, Long exportCount) {
            this.tableName = tableName;
            this.sumSize = sumSize;
            this.exportCount = exportCount;
        }
    }

    private static class Query2Row {
        String tableName;
        Long noRecord;

        Query2Row(String tableName, Long noRecord) {
            this.tableName = tableName;
            this.noRecord = noRecord;
        }
    }

    private static String capitalize(String s) {
        if (s == null || s.isEmpty()) {
            return s;
        }
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase(Locale.ENGLISH);
    }

    private static void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !DEBUG_ENABLED) {
            return;
        }

        String timestamp = LocalDateTime.now().format(LOG_FORMAT);
        StringBuilder sb = new StringBuilder(512);
        sb.append("Time:").append(timestamp)
          .append(" [").append(capitalize(level)).append("] ")
          .append("Process_Name: ").append(PROCESS_NAME).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            sb.append("Server_IP: ").append(serverIP).append(" ");
        }

        sb.append("Process_Version: ").append(PROCESS_VERSION)
          .append(" Log_Level: EVT_LOG ")
          .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
          .append(" Description: ").append(message != null ? message : "");

        System.out.println(sb.toString().trim());
    }

    private static String normalize(String tableName) {
        if (tableName == null) {
            return null;
        }
        String value = tableName.trim();
        int dot = value.lastIndexOf('.');
        if (dot >= 0) {
            value = value.substring(dot + 1);
        }
        return value.trim().toUpperCase(Locale.ENGLISH);
    }

    private static String sanitizeIdentifier(String name) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Identifier must not be null or blank.");
        }
        if (!name.matches("[A-Za-z0-9_.]+")) {
            throw new IllegalArgumentException(
                    "Identifier contains disallowed characters. Only letters, digits, underscores, and dots are permitted.");
        }
        return name;
    }

    private static Long getNullableLong(ResultSet rs, int columnIndex) throws SQLException {
        long value = rs.getLong(columnIndex);
        return rs.wasNull() ? null : value;
    }

    private Map<String, Query1Row> fetchQuery1(Connection conn, String serverIP) throws SQLException {
        Map<String, Query1Row> result = new LinkedHashMap<String, Query1Row>();

        try (PreparedStatement ps = conn.prepareStatement(SQL_QUERY1);
             ResultSet rs = ps.executeQuery()) {

            while (rs.next()) {
                String rawName = rs.getString(1);
                Long sumSize = getNullableLong(rs, 2);
                Long exportCount = getNullableLong(rs, 3);
                String key = normalize(rawName);

                Query1Row existing = result.get(key);
                if (existing == null) {
                    result.put(key, new Query1Row(rawName, sumSize, exportCount));
                } else {
                    long mergedSum = (existing.sumSize == null ? 0L : existing.sumSize.longValue())
                                   + (sumSize == null ? 0L : sumSize.longValue());
                    long mergedCount = (existing.exportCount == null ? 0L : existing.exportCount.longValue())
                                     + (exportCount == null ? 0L : exportCount.longValue());

                    existing.sumSize = mergedSum;
                    existing.exportCount = mergedCount;

                    log("WARN",
                            "Duplicate table in Query 1 after normalization: " + rawName + ". Values were merged.",
                            serverIP,
                            "DUPLICATE_Q1");
                }
            }
        }

        return result;
    }

    private Map<String, Query2Row> fetchQuery2(Connection conn, String auditDate, String serverIP) throws SQLException {
        Map<String, Query2Row> result = new LinkedHashMap<String, Query2Row>();

        try (PreparedStatement ps = conn.prepareStatement(SQL_QUERY2)) {
            ps.setDate(1, java.sql.Date.valueOf(auditDate));

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String rawName = rs.getString(1);
                    Long noRecord = getNullableLong(rs, 2);
                    String key = normalize(rawName);

                    Query2Row existing = result.get(key);
                    if (existing == null) {
                        result.put(key, new Query2Row(rawName, noRecord));
                    } else {
                        long mergedValue;
                        if (existing.noRecord == null && noRecord == null) {
                            mergedValue = 0L;
                        } else if (existing.noRecord == null) {
                            mergedValue = noRecord.longValue();
                        } else if (noRecord == null) {
                            mergedValue = existing.noRecord.longValue();
                        } else {
                            mergedValue = Math.max(existing.noRecord.longValue(), noRecord.longValue());
                        }

                        existing.noRecord = mergedValue;

                        log("WARN",
                                "Duplicate table in Query 2 after normalization: " + rawName + ". Maximum value was kept.",
                                serverIP,
                                "DUPLICATE_Q2");
                    }
                }
            }
        }

        return result;
    }

    private int mergeAndInsert(Connection conn,
                               Map<String, Query1Row> q1,
                               Map<String, Query2Row> q2,
                               Set<String> allKeys,
                               String auditDate,
                               String masterTable,
                               String serverIP) throws SQLException {

        String insertSql =
                "INSERT INTO " + masterTable +
                " (Sno, TimeStamp, Data_Date, Table_Name, Sum_Size, Count_Export_Status, No_Record, Greater_Or_Nah) " +
                "VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)";

        Timestamp runTs = Timestamp.valueOf(LocalDateTime.now());
        java.sql.Date sqlDate = java.sql.Date.valueOf(auditDate);
        int insertedRows = 0;

        try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
            for (String key : allKeys) {
                Query1Row row1 = q1.get(key);
                Query2Row row2 = q2.get(key);

                String tableName = row1 != null ? row1.tableName : row2.tableName;
                Long sumSize = row1 != null ? row1.sumSize : null;
                Long exportCount = row1 != null ? row1.exportCount : null;
                Long noRecord = row2 != null ? row2.noRecord : null;

                String greaterOrNah;
                if (sumSize != null && noRecord != null) {
                    greaterOrNah = (sumSize.longValue() > noRecord.longValue()) ? "TRUE" : "FALSE";
                } else {
                    greaterOrNah = "N/A";
                }

                ps.setTimestamp(1, runTs);
                ps.setDate(2, sqlDate);
                ps.setString(3, tableName);

                if (sumSize != null) {
                    ps.setLong(4, sumSize.longValue());
                } else {
                    ps.setNull(4, Types.BIGINT);
                }

                if (exportCount != null) {
                    ps.setLong(5, exportCount.longValue());
                } else {
                    ps.setNull(5, Types.BIGINT);
                }

                if (noRecord != null) {
                    ps.setLong(6, noRecord.longValue());
                } else {
                    ps.setNull(6, Types.BIGINT);
                }

                ps.setString(7, greaterOrNah);
                ps.addBatch();
                insertedRows++;
            }

            ps.executeBatch();
        }

        return insertedRows;
    }

    public void runAudit(String dbUrl,
                         String dbUser,
                         String dbPass,
                         String dbName,
                         String auditDate,
                         String masterTable) {

        final String serverIP = dbUrl;

        log("INFO", "Process started.", serverIP, "START");
        log("INFO",
                "Configuration | DB_URL: " + dbUrl +
                " | DB_NAME: " + dbName +
                " | Audit Date: " + auditDate +
                " | Master Table: " + masterTable,
                serverIP,
                "CONFIG");

        sanitizeIdentifier(masterTable);

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
            conn.setAutoCommit(false);
            log("INFO", "Database connection established.", serverIP, "DB_CONNECT");

            try {
                log("INFO", "Executing Query 1.", serverIP, "QUERY1_START");
                Map<String, Query1Row> q1Data = fetchQuery1(conn, serverIP);
                log("INFO", "Query 1 completed. Rows: " + q1Data.size(), serverIP, "QUERY1_END");

                log("INFO", "Executing Query 2 for date " + auditDate + ".", serverIP, "QUERY2_START");
                Map<String, Query2Row> q2Data = fetchQuery2(conn, auditDate, serverIP);
                log("INFO", "Query 2 completed. Rows: " + q2Data.size(), serverIP, "QUERY2_END");

                Set<String> allKeys = new TreeSet<String>();
                allKeys.addAll(q1Data.keySet());
                allKeys.addAll(q2Data.keySet());

                log("INFO", "Merge process started. Total unique tables: " + allKeys.size(), serverIP, "MERGE_START");
                int insertedRows = mergeAndInsert(conn, q1Data, q2Data, new LinkedHashSet<String>(allKeys), auditDate, masterTable, serverIP);

                conn.commit();
                log("INFO", "Insert completed successfully. Rows inserted: " + insertedRows, serverIP, "INSERT_SUCCESS");
                log("INFO", "Process completed.", serverIP, "END");

            } catch (Exception e) {
                conn.rollback();
                log("ERROR", "Process failed. Transaction rolled back. Message: " + e.getMessage(), serverIP, "ERROR");
                throw new RuntimeException(e);
            }

        } catch (SQLException e) {
            log("ERROR",
                    "Database error | SQLState: " + e.getSQLState() +
                    " | ErrorCode: " + e.getErrorCode() +
                    " | Message: " + e.getMessage(),
                    serverIP,
                    "DB_ERROR");
            throw new RuntimeException(e);
        }
    }
}