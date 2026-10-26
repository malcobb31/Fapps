package li_update_v1;

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

    private static final boolean           debugEnabled   = true;
    private static final String            processName    = "TP2_PROCESS";
    private static final String            processVersion = "1.0";
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
            
            log("INFO",
                "Fetching source data - table=" + sourceTableName
                + " | query=" + query,
                null, "PROCESS_START");
            
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery(query);
            
            sourceData = new LinkedHashMap<>();

            int totalColumns = directColumns.length;
            for (String[] group : sourceMetaColumnsList) {
                totalColumns += group.length;
            }

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
            }

            log("INFO",
                "Source data fetch complete - table=" + sourceTableName
                + " | total records=" + sourceData.size(),
                null, "PROCESS_START");
            
        } catch (Exception ex) {
            log("ERROR",
                "Exception in fetchSourceData - table=" + sourceTableName
                + " | query=" + query + " - " + ex.getMessage(),
                null, "ERROR");
            sourceData = null;
        } finally {
            try {
                if (conn != null) {
                    if (rs != null) rs.close();
                    if (stmt != null) stmt.close();
                    conn.close();
                }
            } catch (Exception e) {
                log("WARN",
                    "Failed to close DB resources after fetchSourceData - table=" + sourceTableName
                    + " - " + e.getMessage(),
                    null, "ERROR");
            }
        }
        
        return sourceData;
    }

    // =========================================================================
    // LOGGING
    // =========================================================================

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder logBuilder = new StringBuilder(512);

        logBuilder.append("Time:").append(timestamp)
                .append(" [").append(capitalize(level)).append("] ")
                .append("Process_Name: ").append(processName).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            logBuilder.append("Server_IP: ").append(serverIP).append(" ");
        }

        logBuilder.append("Process_Version: ").append(processVersion)
                .append(" Log_Level: EVT_LOG ")
                .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
                .append(" Description: ").append(message != null ? message : "");

        System.out.println(logBuilder.toString().trim());
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return Character.toUpperCase(s.charAt(0)) + s.substring(1).toLowerCase();
    }
}
