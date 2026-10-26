package tp4_parser;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class SourceData {

    private static final String            PROCESS_NAME    = "TP4_ADDRESS_PARSER";
    private static final String            PROCESS_VERSION = "1.0";
    private static final boolean           DEBUG_ENABLED   = true;
    private static final DateTimeFormatter LOG_FORMAT      =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    // =========================================================================
    // Inner DTO: one row from the config table
    // =========================================================================

    public static class ConfigRow {
        public int    sno;
        public String frequency;
        public String sourcePath;
        public String destinationTableName;
        public String destinationPath;
        public String tmId;
        public String parameters;
        public String primaryKey;

        @Override
        public String toString() {
            return "ConfigRow{sno=" + sno
                    + ", tm_id=" + tmId
                    + ", destTable=" + destinationTableName
                    + ", sourcePath=" + sourcePath
                    + ", destPath=" + destinationPath
                    + ", primaryKey=" + primaryKey + "}";
        }
    }

    // =========================================================================
    // Fetch config rows from MySQL where tm_id = tmId
    // =========================================================================

    public List<ConfigRow> fetchConfigRows(String mysqlUrl,
                                            String mysqlUser,
                                            String mysqlPass,
                                            String configTable,
                                            String tmId) {

        List<ConfigRow> rows = new ArrayList<>();
        Connection conn      = null;
        PreparedStatement ps = null;
        ResultSet rs         = null;

        log("INFO", "Connecting to MySQL for config fetch | URL: " + mysqlUrl
                + " | User: " + mysqlUser, null, "DB_CONNECT");

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPass);
            log("INFO", "MySQL connection established", null, "DB_CONNECT");

            String sql = "SELECT sno, frequency, source_path, destination_table_name, "
                    + "destination_path, tm_id, parameters, primary_key "
                    + "FROM " + configTable + " WHERE tm_id = ?";

            log("INFO", "Config query: " + sql + " | tm_id=" + tmId, null, "DB_QUERY");

            ps = conn.prepareStatement(sql);
            ps.setString(1, tmId);
            rs = ps.executeQuery();

            int loaded = 0;
            while (rs.next()) {
                ConfigRow r = new ConfigRow();
                r.sno                  = rs.getInt("sno");
                r.frequency            = rs.getString("frequency");
                r.sourcePath           = rs.getString("source_path");
                r.destinationTableName = rs.getString("destination_table_name");
                r.destinationPath      = rs.getString("destination_path");
                r.tmId                 = rs.getString("tm_id");
                r.parameters           = rs.getString("parameters");
                r.primaryKey           = rs.getString("primary_key");
                rows.add(r);
                loaded++;
                log("DEBUG", "Config row loaded: " + r.toString(), null, "DB_RESULT");
            }

            log("INFO", "Config rows fetched: " + loaded + " | tm_id=" + tmId, null, "DB_RESULT");

        } catch (ClassNotFoundException ex) {
            log("ERROR", "MySQL JDBC driver not found | Error: " + ex.getMessage(), null, "DB_ERROR");
        } catch (Exception ex) {
            log("ERROR", "Failed to fetch config rows | Table: " + configTable
                    + " | tm_id=" + tmId + " | Error: " + ex.getMessage(), null, "DB_ERROR");
        } finally {
            closeQuietly(rs, ps, conn);
        }

        return rows;
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void closeQuietly(ResultSet rs, java.sql.Statement stmt, Connection conn) {
        try { if (rs   != null) rs.close();   } catch (Exception ignored) {}
        try { if (stmt != null) stmt.close();  } catch (Exception ignored) {}
        try { if (conn != null) conn.close();  } catch (Exception ignored) {}
    }

    // =========================================================================
    // Logging
    // =========================================================================

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !DEBUG_ENABLED) return;
        String timestamp = LocalDateTime.now().format(LOG_FORMAT);
        StringBuilder sb = new StringBuilder(512);
        sb.append("Time:").append(timestamp)
          .append(" [").append(capitalize(level)).append("] ")
          .append("Process_Name: ").append(PROCESS_NAME).append(" ");
        if (serverIP != null && !serverIP.isEmpty())
            sb.append("Server_IP: ").append(serverIP).append(" ");
        sb.append("Process_Version: ").append(PROCESS_VERSION)
          .append(" Log_Level: EVT_LOG ")
          .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
          .append(" Description: ").append(message != null ? message : "");
        System.out.println(sb.toString().trim());
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }
}
