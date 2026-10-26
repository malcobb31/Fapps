package li_parser_v3;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;


/**
 * SourceData - Data Retrieval from PostgreSQL Source Table
 * 
 * Methods:
 * 1. fetchSourceData() - Get all data from source table with specified columns
 */
public class metadata_parser_source {

    // =========================================================================
    // LOGGING INFRASTRUCTURE
    // =========================================================================

    private static final String PROCESS_NAME    = "metadata_parser_source";
    private static final String PROCESS_VERSION = "3.0";
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
    // BUSINESS LOGIC
    // =========================================================================

    /**
     * Fetch data from source table
     * 
     * @param sourceTableName - Source table name
     * @param directColumns - Array of direct column names
     * @param sourceMetaColumnsList - List of source metadata column arrays
     * @param dbURL - Database URL
     * @param dbUser - Database username
     * @param dbPass - Database password
     * @return HashMap<UniqueKey, String[]> - All source data
     */
    public HashMap<String, String[]> fetchSourceData(String sourceTableName,
                                                      String[] directColumns,
                                                      List<String[]> sourceMetaColumnsList,
                                                      String dbURL,
                                                      String dbUser,
                                                      String dbPass) {

        log("INFO",
            "fetchSourceData started. Table: " + sourceTableName
            + ", direct columns: " + directColumns.length
            + ", metadata column groups: " + sourceMetaColumnsList.size(),
            SERVER_IP, "DB_FETCH_START");

        HashMap<String, String[]> sourceData = null;
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String query = null;
        try {
            // Build SELECT query
            StringBuilder selectQuery = new StringBuilder("SELECT ");
            
            // Add direct columns
            for (int i = 0; i < directColumns.length; i++) {
                selectQuery.append(directColumns[i]);
                if (i < directColumns.length - 1 || sourceMetaColumnsList.size() > 0) {
                    selectQuery.append(", ");
                }
            }
            
            // Add all metadata columns
            for (int g = 0; g < sourceMetaColumnsList.size(); g++) {
                String[] sourceMetaCols = sourceMetaColumnsList.get(g);

                for (int i = 0; i < sourceMetaCols.length; i++) {
                    selectQuery.append(sourceMetaCols[i]);

                    boolean isLastGroup = (g == sourceMetaColumnsList.size() - 1);
                    boolean isLastColInGroup = (i == sourceMetaCols.length - 1);

                    if (!isLastGroup || !isLastColInGroup) {
                        selectQuery.append(", ");
                    }
                }
            }
            
            selectQuery.append(" FROM ").append(sourceTableName);
            
            query = selectQuery.toString();
            
            log("DEBUG",
                "Constructed SELECT query: " + query,
                SERVER_IP, "DB_QUERY");

            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            log("DEBUG",
                "Database connection established. Executing query on table: " + sourceTableName,
                SERVER_IP, "DB_CONNECT");

            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery(query);
            
            sourceData = new LinkedHashMap<>();

            int totalColumns = directColumns.length;
            for (String[] group : sourceMetaColumnsList) {
                totalColumns += group.length;
            }

            log("DEBUG",
                "Query executed successfully. Total columns per record: " + totalColumns
                + ". Starting row iteration.",
                SERVER_IP, "DB_QUERY");

            int count = 0;

            while (rs.next()) {
                String[] record = new String[totalColumns];

                int colIndex = 0;

                for (int i = 0; i < directColumns.length; i++) {
                    try {
                        record[colIndex] = rs.getString(directColumns[i]);
                    } catch (Exception e) {
                        record[colIndex] = null;
                    }
                    colIndex++;
                }

                for (String[] sourceMetaCols : sourceMetaColumnsList) {
                    for (int i = 0; i < sourceMetaCols.length; i++) {
                        try {
                            record[colIndex] = rs.getString(sourceMetaCols[i]);
                        } catch (Exception e) {
                            record[colIndex] = null;
                        }
                        colIndex++;
                    }
                }

                sourceData.put(String.valueOf(count), record);
                count++;

                if (count % 1000 == 0) {
                    log("DEBUG",
                        "fetchSourceData progress: " + count + " rows processed so far from table: " + sourceTableName,
                        SERVER_IP, "DB_FETCH_PROGRESS");
                }
            }

            log("INFO",
                "fetchSourceData complete. Table: " + sourceTableName
                + ", total records fetched: " + sourceData.size(),
                SERVER_IP, "DB_FETCH_COMPLETE");
            
        } catch (Exception ex) {
            log("ERROR",
                "fetchSourceData failed on table: " + sourceTableName
                + ". Cause: " + ex.getMessage(),
                SERVER_IP, "DB_ERROR");
            sourceData = null;
        } finally {
            try {
                if (conn != null) {
                    if (rs != null) rs.close();
                    if (stmt != null) stmt.close();
                    conn.close();
                    log("DEBUG",
                        "DB resources closed successfully for table: " + sourceTableName,
                        SERVER_IP, "DB_CLEANUP");
                }
            } catch (Exception e) {
                log("WARN",
                    "Failed to close one or more DB resources for table: " + sourceTableName
                    + ". Cause: " + e.getMessage(),
                    SERVER_IP, "DB_CLEANUP");
            }
        }
        
        return sourceData;
    }
}
