package ntp9_process_v1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ntp9_process_methods {

    private static final String processName = "ReConsolidationProcessor";
    private static final String processVersion = "1.0.0";
    private static final boolean debugEnabled = false;
    private static final DateTimeFormatter logFormat =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final int BATCH_SIZE = 500;
    private static final String DEFAULT_UPDATED_BY = "SYSTEM";
    private static final long DEFAULT_VERSION = 0L;

    private static final List<String> TARGET_COLUMNS = Arrays.asList(
            "id",
            "workflow_id",
            "reference_number",
            "consolidation_type",
            "consolidation_date",
            "status",
           
            "surviving_entity_name",
            "surviving_entity_fi_code",
            "target_entity_id",
            "target_entity_name",
            "target_entity_fi_code",
            "remarks",
            "reason",
            "initiated_by",
            "approved_by",
            "approved_at",
            "created_at",
            "updated_at",
            "created_by",
            "updated_by",
            "active",
            "version",
            "deactivation_executed",
            "deactivation_executed_at",
            "billing_transfer_status",
            "billing_transfer_message",
            "billing_transfer_at"
    );

    private static final DateTimeFormatter[] INPUT_TS_FORMATS = new DateTimeFormatter[] {
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")
    };

  
    private final Map<String, EntityInfo> targetCache = new LinkedHashMap<String, EntityInfo>();

    public void process(String parentCsvPath,
                        String childCsvPath,
                        String dbUrl,
                        String dbUser,
                        String dbPass,
                        String tableName) {

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
            conn.setAutoCommit(false);

            log("INFO", "Reading parent CSV: " + parentCsvPath, "CSV_READ");
            List<Map<String, String>> parentRows = readCsv(parentCsvPath);
            log("INFO", "Parent CSV loaded: " + parentRows.size() + " rows", "CSV_READ");

            log("INFO", "Reading child CSV: " + childCsvPath, "CSV_READ");
            List<Map<String, String>> childRows = readCsv(childCsvPath);
            log("INFO", "Child CSV loaded: " + childRows.size() + " rows", "CSV_READ");

            Map<String, List<Map<String, String>>> childIndex = groupChildByRefNo(childRows);

            List<Map<String, Object>> targetRows = buildTargetRows(conn, parentRows, childIndex);
            log("INFO", "Mapped rows ready for insert: " + targetRows.size(), "MAPPING_DONE");

            insertRows(conn, targetRows, tableName);

            conn.commit();
            log("INFO", "Insert commit done", "PROCESS_DONE");

        } catch (Exception e) {
            log("ERROR", "DB insert failed: " + e.getMessage(), "INSERT_ERROR");
            if (conn != null) {
                try {
                    conn.rollback();
                    log("INFO", "Transaction rolled back successfully", "ROLLBACK_DONE");
                } catch (SQLException ex) {
                    log("ERROR", "Rollback failed: " + ex.getMessage(), "ROLLBACK_ERROR");
                }
            }
            throw new RuntimeException(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    private List<Map<String, String>> readCsv(String filePath) {
        List<Map<String, String>> rows = new ArrayList<Map<String, String>>();

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String headerLine = reader.readLine();
            if (headerLine == null) {
                return rows;
            }

            String[] headers = parsePipeCsvLine(headerLine);

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] values = parsePipeCsvLine(line);
                Map<String, String> row = new LinkedHashMap<String, String>();

                for (int i = 0; i < headers.length; i++) {
                    String header = cleanCell(headers[i]);
                    String value = i < values.length ? cleanCell(values[i]) : null;
                    row.put(header, value);
                }
                rows.add(row);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read CSV: " + filePath, e);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException ignored) {
                }
            }
        }

        return rows;
    }

    private String[] parsePipeCsvLine(String line) {
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
            } else if (c == '|' && !inQuotes) {
                tokens.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        tokens.add(current.toString());
        return tokens.toArray(new String[0]);
    }

    private String cleanCell(String value) {
        if (value == null) return null;
        String v = value.trim();

        if (v.equals("\\N")) return null;
        if (v.equals("\"\"")) return null;
        if (v.equalsIgnoreCase("null")) return null;

        if (v.startsWith("\"") && v.endsWith("\"") && v.length() >= 2) {
            v = v.substring(1, v.length() - 1);
        }

        if (v.isEmpty()) return null;
        return v;
    }

    private Map<String, List<Map<String, String>>> groupChildByRefNo(List<Map<String, String>> childRows) {
        Map<String, List<Map<String, String>>> grouped = new LinkedHashMap<String, List<Map<String, String>>>();

        for (Map<String, String> child : childRows) {
            String refNo = firstNonBlank(child, "mdc_main_ref_no");
            if (refNo == null) continue;

            List<Map<String, String>> list = grouped.get(refNo);
            if (list == null) {
                list = new ArrayList<Map<String, String>>();
                grouped.put(refNo, list);
            }
            list.add(child);
        }

        return grouped;
    }

    private List<Map<String, Object>> buildTargetRows(Connection conn,
                                                      List<Map<String, String>> parentRows,
                                                      Map<String, List<Map<String, String>>> childIndex) throws SQLException {
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

        for (Map<String, String> parent : parentRows) {
            String refNo = firstNonBlank(parent, "mdm_ref_no");
            if (refNo == null) {
                log("WARN", "Skipping parent row with empty mdm_ref_no", "MAPPING_SKIP");
                continue;
            }

            List<Map<String, String>> children = childIndex.get(refNo);
            if (children == null || children.isEmpty()) {
                log("WARN", "No child records found for mdm_ref_no: " + refNo, "JOIN_MISS");
                continue;
            }

            for (Map<String, String> child : children) {
                result.add(mapRow(conn, parent, child));
            }
        }

        return result;
    }

    private Map<String, Object> mapRow(Connection conn,
                                       Map<String, String> parent,
                                       Map<String, String> child) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<String, Object>();

        String referenceNumber = firstNonBlank(parent, "mdm_ref_no");
        String survivingFiCode = firstNonBlank(parent, "mdm_req_fi");
        String targetFiCode = firstNonBlank(child, "mdc_deact_FI");

        
        EntityInfo target = lookupEntityByFiCode(conn, targetFiCode, targetCache);

        row.put("id", UUID.randomUUID()); // FIX: send UUID, not String
        row.put("workflow_id", generateWorkflowId(referenceNumber));
        row.put("reference_number", referenceNumber);
        row.put("consolidation_type", translateConsolidationType(parent.get("mdm_request_type")));
        row.put("consolidation_date", null);
        row.put("status", translateStatus(parent.get("mdm_req_status")));

        row.put("surviving_entity_name", null);
        row.put("surviving_entity_fi_code", survivingFiCode);

        row.put("target_entity_id", target != null ? target.id : null);
        row.put("target_entity_name", target != null ? target.name : null);
        row.put("target_entity_fi_code", targetFiCode);

        row.put("remarks", emptyToNull(parent.get("mdm_remarks")));
        row.put("reason", null);
        row.put("initiated_by", null);

        row.put("approved_by", emptyToNull(parent.get("mdm_checker_id")));
        row.put("approved_at", parseTimestamp(parent.get("mdm_checker_submit_time")));

        row.put("created_at", parseTimestamp(parent.get("mdm_maker_submit_time")));
        row.put("created_by", emptyToNull(parent.get("mdm_maker_id")));

        row.put("updated_at", currentTimestamp());
        row.put("updated_by", DEFAULT_UPDATED_BY);

        row.put("active", Boolean.TRUE);
        row.put("version", Long.valueOf(DEFAULT_VERSION));

        row.put("deactivation_executed", translateBoolean(child.get("mdc_deact_status"), false));
        row.put("deactivation_executed_at", parseTimestamp(child.get("mdc_deact_end_time")));

        row.put("billing_transfer_status", null);
        row.put("billing_transfer_message", null);
        row.put("billing_transfer_at", null);

        return row;
    }

    private void insertRows(Connection conn, List<Map<String, Object>> rows, String tableName) throws SQLException {
        if (rows == null || rows.isEmpty()) {
            log("WARN", "No rows to insert", "INSERT_SKIP");
            return;
        }

        validateTableName(tableName);

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(tableName).append(" (");

        for (int i = 0; i < TARGET_COLUMNS.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append(TARGET_COLUMNS.get(i));
        }

        sql.append(") VALUES (");
        for (int i = 0; i < TARGET_COLUMNS.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append("?");
        }
        sql.append(")");

        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql.toString());

            int batchCount = 0;
            int total = 0;

            for (Map<String, Object> row : rows) {
                for (int i = 0; i < TARGET_COLUMNS.size(); i++) {
                    String col = TARGET_COLUMNS.get(i);
                    Object val = row.get(col);
                    bind(ps, i + 1, val);
                }

                ps.addBatch();
                batchCount++;
                total++;

                if (batchCount >= BATCH_SIZE) {
                    ps.executeBatch();
                    conn.commit();
                    batchCount = 0;
                    log("INFO", "Batch committed. Total attempted inserts: " + total, "INSERT_BATCH");
                }
            }

            if (batchCount > 0) {
                ps.executeBatch();
                conn.commit();
            }

            log("INFO", "Insert complete. Total rows inserted: " + total, "INSERT_DONE");

        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ignored) {
                }
            }
        }
    }

    private void bind(PreparedStatement ps, int idx, Object value) throws SQLException {
        if (value == null) {
            ps.setObject(idx, null);
            return;
        }

        if (value instanceof UUID) {
            ps.setObject(idx, value); // FIX for uuid column
        } else if (value instanceof Timestamp) {
            ps.setTimestamp(idx, (Timestamp) value);
        } else if (value instanceof Boolean) {
            ps.setBoolean(idx, ((Boolean) value).booleanValue());
        } else if (value instanceof Long) {
            ps.setLong(idx, ((Long) value).longValue());
        } else if (value instanceof Integer) {
            ps.setInt(idx, ((Integer) value).intValue());
        } else {
            ps.setString(idx, String.valueOf(value));
        }
    }

    private String translateStatus(String code) {
        if (code == null) return null;

        String v = code.trim().toUpperCase();
        if (v.isEmpty()) return null;

        if ("PR".equals(v)) return "Registry approval pending";
        if ("A".equals(v)) return "Registry Approved";
        if ("CR".equals(v)) return "Registry Rejected";
        if ("C".equals(v)) return "APPROVED";
        if ("PA".equals(v)) return "FI approval pending";

        return code.trim();
    }

    private String translateConsolidationType(String code) {
        if (code == null) return null;

        String v = code.trim();
        if (v.isEmpty()) return null;

        if ("01".equals(v)) return "MERGER";
        if ("02".equals(v)) return "AMALGAMATION";
        if ("03".equals(v)) return "BUSINESS_CLOSURE";

        return v;
    }

    private Boolean translateBoolean(String raw, boolean defaultValue) {
        if (raw == null) return Boolean.valueOf(defaultValue);

        String v = raw.trim().toUpperCase();
        if (v.isEmpty()) return Boolean.valueOf(defaultValue);

        if ("Y".equals(v) || "YES".equals(v) || "T".equals(v) || "TRUE".equals(v) || "1".equals(v)) return Boolean.TRUE;
        if ("N".equals(v) || "NO".equals(v) || "F".equals(v) || "FALSE".equals(v) || "0".equals(v)) return Boolean.FALSE;

        return Boolean.valueOf(defaultValue);
    }

    private Timestamp parseTimestamp(String raw) {
        String v = emptyToNull(raw);
        if (v == null) return null;

        for (int i = 0; i < INPUT_TS_FORMATS.length; i++) {
            try {
                LocalDateTime dt = LocalDateTime.parse(v, INPUT_TS_FORMATS[i]);
                return Timestamp.valueOf(dt);
            } catch (Exception ignored) {
            }
        }

        try {
            return Timestamp.valueOf(v);
        } catch (Exception ignored) {
        }

        return null;
    }

    private Timestamp currentTimestamp() {
        return Timestamp.valueOf(LocalDateTime.now());
    }

    private String generateWorkflowId(String referenceNumber) {
        if (referenceNumber == null || referenceNumber.trim().isEmpty()) {
            return UUID.randomUUID().toString();
        }
        return UUID.nameUUIDFromBytes(("WF:" + referenceNumber.trim()).getBytes(StandardCharsets.UTF_8)).toString();
    }

    private String generateFallbackId(String prefix, String fiCode) {
        String seed = prefix + ":" + (fiCode == null ? "" : fiCode.trim());
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private EntityInfo lookupEntityByFiCode(Connection conn,
                                            String fiCode,
                                            Map<String, EntityInfo> cache) throws SQLException {
        if (fiCode == null || fiCode.trim().isEmpty()) {
            return null;
        }

        String key = fiCode.trim();
        EntityInfo cached = cache.get(key);
        if (cached != null) {
            return cached;
        }

        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = conn.prepareStatement(
                    "SELECT * FROM re_mgmt.reporting_entity WHERE fi_code = ? LIMIT 1"
            );
            ps.setString(1, key);
            rs = ps.executeQuery();

            if (rs.next()) {
                EntityInfo info = new EntityInfo();
                info.id = firstAvailable(rs, "id", "reporting_entity_id");
                info.name = firstAvailable(rs, "name", "entity_name", "reporting_entity_name", "fi_name");
                cache.put(key, info);
                return info;
            }
        } catch (SQLException e) {
            log("WARN", "reporting_entity lookup failed for fi_code=" + key + " | " + e.getMessage(), "LOOKUP_WARN");
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException ignored) {
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ignored) {
                }
            }
        }

        return null;
    }

    private String firstAvailable(ResultSet rs, String... columnNames) {
        if (rs == null || columnNames == null) return null;

        for (int i = 0; i < columnNames.length; i++) {
            try {
                String value = rs.getString(columnNames[i]);
                if (!isBlank(value)) {
                    return value.trim();
                }
            } catch (SQLException ignored) {
            }
        }

        return null;
    }

    private String firstNonBlank(Map<String, String> row, String... keys) {
        if (row == null || keys == null) return null;

        for (int i = 0; i < keys.length; i++) {
            String key = keys[i];
            if (key == null) continue;

            String value = row.get(key);
            if (!isBlank(value)) return value.trim();

            for (Map.Entry<String, String> entry : row.entrySet()) {
                if (entry.getKey() != null && entry.getKey().equalsIgnoreCase(key) && !isBlank(entry.getValue())) {
                    return entry.getValue().trim();
                }
            }
        }
        return null;
    }

    private String emptyToNull(String value) {
        if (value == null) return null;
        String v = value.trim();
        if (v.isEmpty()) return null;
        if ("\\N".equals(v)) return null;
        if ("\"\"".equals(v)) return null;
        if ("null".equalsIgnoreCase(v)) return null;
        return v;
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private void validateTableName(String tableName) {
        if (tableName == null || !tableName.matches("[a-zA-Z_][a-zA-Z0-9_.]*")) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }
    }

    private void log(String level, String message, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(logFormat);
        System.out.println(
                "Time:" + timestamp +
                " [" + capitalize(level) + "] " +
                "Process_Name: " + processName + " " +
                "Process_Version: " + processVersion + " " +
                "Log_Level: EVT_LOG " +
                " EVT_Type: " + evtType +
                " Description: " + message
        );
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    private static class EntityInfo {
        String id;
        String name;
    }
}