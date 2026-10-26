package dsc_parser_v1;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * CSV/DB metadata parser with:
 * - destination validation
 * - batch insert
 * - metadata JSON with guaranteed user_id injection
 * - X.509 parsing (SHA-256 thumbprint, LocalDate expiry)
 * - user_id extraction from CSV column
 */
public class metadata_parser_process {

    private static final String            processName    = "metadata-parser";
    private static final String            processVersion = "1.0";
    private static final boolean           debugEnabled   = true;
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    private static final boolean SKIP_MISSING_COLUMNS = false;

    // -------------------------------------------------------------------------
    // Logging
    // -------------------------------------------------------------------------

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
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    // -------------------------------------------------------------------------
    // String helpers
    // -------------------------------------------------------------------------

    private String[] parseCommaSeparated(String input) {
        if (input == null || input.trim().isEmpty()) return new String[0];
        String[] parts = input.split(",");
        for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
        return parts;
    }

    private String[] parseHashSeparated(String input) {
        if (input == null || input.trim().isEmpty()) return new String[0];
        String[] parts = input.split("#");
        for (int i = 0; i < parts.length; i++) parts[i] = parts[i].trim();
        return parts;
    }

    private String normalize(String s) {
        return s == null ? null : s.trim().toLowerCase(Locale.ROOT);
    }

    // -------------------------------------------------------------------------
    // CSV delimiter detection and line splitting
    // -------------------------------------------------------------------------

    private char detectDelimiter(String line) {
        if (line == null || line.isEmpty()) return ',';
        int pipeCount  = 0;
        int commaCount = 0;
        int tabCount   = 0;
        boolean inQuotes = false;

        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if      (c == '|') pipeCount++;
                else if (c == ',') commaCount++;
                else if (c == '\t') tabCount++;
            }
        }

        if (pipeCount >= commaCount && pipeCount >= tabCount && pipeCount > 0) return '|';
        if (commaCount >= tabCount && commaCount > 0) return ',';
        if (tabCount > 0) return '\t';
        return ',';
    }

    private String[] splitDelimitedLine(String line, char delimiter) {
        List<String> valuesList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (char c : line.toCharArray()) {
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (c == delimiter && !inQuotes) {
                valuesList.add(sb.toString());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }
        valuesList.add(sb.toString());

        String[] values = valuesList.toArray(new String[0]);
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                values[i] = values[i].replace("\"", "").trim();
            }
        }
        return values;
    }

    // -------------------------------------------------------------------------
    // Inner classes
    // -------------------------------------------------------------------------

    private static class CsvData {
        String[] headers;
        LinkedHashMap<String, String[]> rows = new LinkedHashMap<>();
    }

    /**
     * Holds all fields parsed from a single X.509 certificate.
     * thumbprintSha1 replaces the former SHA-1 field.
     * expiryDate (LocalDate) replaces the former Timestamp field.
     */
    private static class CertContext {
        String  rawWithSuffix;
        String  certificateBase64;
        String  extractedUserId;

        String    issuerDn;
        String    serialNumber;
        String    thumbprintSha1;  // SHA-256 uppercase hex
        LocalDate expiryDate;         // date-only (yyyy-MM-dd)
    }

    // -------------------------------------------------------------------------
    // Entry point
    // -------------------------------------------------------------------------

    /**
     * Main processing method.
     */
    public boolean parseAndTransferData(String dbURL, String dbUsername, String dbPassword,
            String sourceInput, String destTableName, String directColumns, int metadataCount,
            String destMetadataColumns, String sourceMetadataColumns, String jsonKeyNames) {

        boolean status = false;

        try {
            log("INFO", "===== START CSV/DB PROCESSING | Source: " + sourceInput +
                    " | Destination Table: " + destTableName + " =====", null, "FILE_PROCESS");

            boolean isCSV = sourceInput != null && sourceInput.toLowerCase(Locale.ROOT).endsWith(".csv");

            String[] directColArray   = parseCommaSeparated(directColumns);
            String[] destMetaColArray = parseCommaSeparated(destMetadataColumns);
            String[] sourceMetaGroups = parseHashSeparated(sourceMetadataColumns);
            String[] jsonKeyGroups    = parseHashSeparated(jsonKeyNames);

            List<String[]> sourceMetaColumnsList = new ArrayList<>();
            List<String[]> jsonKeyNamesList      = new ArrayList<>();

            for (int i = 0; i < metadataCount; i++) {
                String[] sourceCols = parseCommaSeparated(sourceMetaGroups[i]);
                String[] jsonKeys   = parseCommaSeparated(jsonKeyGroups[i]);

                if (sourceCols.length != jsonKeys.length) {
                    log("ERROR", "Metadata column count mismatch at index " + i +
                            " | Source cols: " + sourceCols.length +
                            " | JSON keys: " + jsonKeys.length, null, "VALIDATION");
                    return false;
                }

                sourceMetaColumnsList.add(sourceCols);
                jsonKeyNamesList.add(jsonKeys);
            }

            if (!validateTableExists(destTableName, dbURL, dbUsername, dbPassword)) {
                log("ERROR", "Destination table not found: " + destTableName, null, "VALIDATION");
                return false;
            }

            HashSet<String> destTableColumns = getTableColumns(destTableName, dbURL, dbUsername, dbPassword);

            for (String col : directColArray) {
                if (!destTableColumns.contains(col.toLowerCase(Locale.ROOT))) {
                    log("ERROR", "Direct column missing in destination table: " + col +
                            " | Table: " + destTableName, null, "VALIDATION");
                    return false;
                }
            }

            for (String col : destMetaColArray) {
                if (!destTableColumns.contains(col.toLowerCase(Locale.ROOT))) {
                    log("ERROR", "Metadata column missing in destination table: " + col +
                            " | Table: " + destTableName, null, "VALIDATION");
                    return false;
                }
            }

            if (isCSV) {
                log("INFO", "Reading data from CSV file: " + sourceInput, null, "FILE_PROCESS");
                CsvData csvData = readCSV(sourceInput);

                if (csvData == null || csvData.rows.isEmpty()) {
                    log("WARN", "No data found in source: " + sourceInput +
                            " | Records returned: 0", null, "DATA_FETCH");
                    return false;
                }

                log("INFO", "Data fetch complete | Source: " + sourceInput +
                        " | Total records fetched: " + csvData.rows.size(), null, "DATA_FETCH");

                boolean inserted = insertCsvDataToDestination(
                        csvData,
                        destTableName,
                        directColArray,
                        destMetaColArray,
                        sourceMetaColumnsList,
                        jsonKeyNamesList,
                        dbURL,
                        dbUsername,
                        dbPassword
                );

                if (!inserted) {
                    log("ERROR", "Data insert failed | Source: " + sourceInput +
                            " | Destination table: " + destTableName, null, "DATA_INSERT");
                    return false;
                }

                log("INFO", "Processing completed successfully | Source: " + sourceInput +
                        " | Destination table: " + destTableName +
                        " | Records processed: " + csvData.rows.size(), null, "FILE_PROCESS");

                status = true;

            } else {
                log("ERROR", "DB source path is not included in this CSV-focused version.", null, "FILE_PROCESS");
                return false;
            }

        } catch (Exception ex) {
            log("ERROR", "Unhandled exception in parseAndTransferData | Source: " + sourceInput +
                    " | Error: " + ex.getMessage(), null, "APP_ERROR");
            status = false;
        }

        return status;
    }

    // -------------------------------------------------------------------------
    // CSV reading
    // -------------------------------------------------------------------------

    private CsvData readCSV(String filePath) {
        CsvData data = new CsvData();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String headerLine = br.readLine();
            if (headerLine == null) {
                log("WARN", "Empty CSV file - no header line found | File: " + filePath, null, "FILE_PROCESS");
                return null;
            }

            headerLine = headerLine.replace("\uFEFF", "").trim();
            char delimiter = detectDelimiter(headerLine);

            data.headers = splitDelimitedLine(headerLine, delimiter);
            log("DEBUG", "CSV header parsed | File: " + filePath +
                    " | Delimiter: " + delimiter +
                    " | Column count: " + data.headers.length, null, "FILE_PROCESS");

            String line;
            int count = 0;

            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                String[] rawValues = splitDelimitedLine(line, delimiter);
                String[] finalRow  = new String[data.headers.length];

                for (int i = 0; i < data.headers.length; i++) {
                    finalRow[i] = (i < rawValues.length) ? rawValues[i] : null;
                }

                if (rawValues.length != data.headers.length) {
                    log("WARN", "Column count mismatch at row " + count +
                            " | File: " + filePath +
                            " | Expected: " + data.headers.length +
                            " | Found: " + rawValues.length +
                            " | Row padded with nulls for missing columns", null, "FILE_PROCESS");
                }

                data.rows.put(String.valueOf(count), finalRow);
                count++;
            }

            log("INFO", "CSV read complete | File: " + filePath +
                    " | Total rows read: " + data.rows.size(), null, "FILE_PROCESS");

        } catch (Exception e) {
            log("ERROR", "Failed to read CSV file: " + filePath +
                    " | Error: " + e.getMessage(), null, "FILE_PROCESS");
            return null;
        }

        return data;
    }

    // -------------------------------------------------------------------------
    // Insert
    // -------------------------------------------------------------------------

    private boolean insertCsvDataToDestination(
            CsvData csvData,
            String destTableName,
            String[] directColumns,
            String[] destMetaColumns,
            List<String[]> sourceMetaColumnsList,
            List<String[]> jsonKeyNamesList,
            String dbURL,
            String dbUser,
            String dbPass) {

        Connection        conn       = null;
        PreparedStatement ps         = null;
        boolean           status     = false;
        int               batchSize  = 200;
        int               rowCount   = 0;
        int               inserted   = 0;
        int               skipped    = 0;
        int               unknown    = 0;
        int               totalRows  = csvData.rows.size();
        long              beforeCount = -1;
        long              afterCount  = -1;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            HashMap<String, String> columnTypes =
                    getDestinationColumnTypes(destTableName, directColumns, destMetaColumns, conn);

            if (columnTypes == null || columnTypes.isEmpty()) {
                log("ERROR", "Could not fetch destination column types | Table: " + destTableName,
                        null, "DATA_INSERT");
                return false;
            }

            // --- CERT COLUMN AUTO-INJECTION ---
            // When the destination table owns cert-derived columns they are appended here
            // automatically. resolveDirectValue() fills their values from CertContext.
            // No configuration change is ever needed.
            final String[] CERT_DERIVED_COLUMNS = {
                "certificate_expiry_date",
                "certificate_issuer",
                "certificate_serial",
                "certificate_thumbprint"
            };
            List<String> directColList = new ArrayList<>(java.util.Arrays.asList(directColumns));
            for (String certCol : CERT_DERIVED_COLUMNS) {
                if (columnTypes.containsKey(certCol) && !directColList.contains(certCol)) {
                    directColList.add(certCol);
                    log("INFO", "Cert-derived column auto-injected into INSERT | Column: " + certCol +
                            " | Table: " + destTableName, null, "DATA_INSERT");
                }
            }
            directColumns = directColList.toArray(new String[0]);
            // --- END CERT COLUMN AUTO-INJECTION ---

            try (PreparedStatement countPs = conn.prepareStatement("SELECT COUNT(*) FROM " + destTableName)) {
                try (ResultSet countRs = countPs.executeQuery()) {
                    if (countRs.next()) beforeCount = countRs.getLong(1);
                }
            }

            // Build INSERT
            StringBuilder insertQuery = new StringBuilder("INSERT INTO ").append(destTableName).append(" (");
            for (int i = 0; i < directColumns.length; i++) {
                insertQuery.append(directColumns[i]);
                if (i < directColumns.length - 1 || destMetaColumns.length > 0) insertQuery.append(", ");
            }
            for (int i = 0; i < destMetaColumns.length; i++) {
                insertQuery.append(destMetaColumns[i]);
                if (i < destMetaColumns.length - 1) insertQuery.append(", ");
            }
            insertQuery.append(") VALUES (");
            int totalColumns = directColumns.length + destMetaColumns.length;
            for (int i = 0; i < totalColumns; i++) {
                insertQuery.append("?");
                if (i < totalColumns - 1) insertQuery.append(", ");
            }
            insertQuery.append(") ON CONFLICT DO NOTHING");

            log("DEBUG", "Prepared insert query | Table: " + destTableName +
                    " | Query: " + insertQuery, null, "DATA_INSERT");

            conn.setAutoCommit(false);
            ps = conn.prepareStatement(insertQuery.toString());

            for (Map.Entry<String, String[]> entry : csvData.rows.entrySet()) {
                String[]            record    = entry.getValue();
                Map<String, String> sourceMap = buildSourceMap(csvData.headers, record);

                // Parse certificate once per row; reuse CertContext everywhere below
                CertContext cert = parseCertificateContext(
                        sourceMap.get("certificate"),
                        sourceMap.get("user_id"));

                int paramIndex = 1;

                // Direct columns
                for (String directColumn : directColumns) {
                    String columnName = directColumn.toLowerCase(Locale.ROOT);
                    String columnType = columnTypes.get(columnName);
                    Object value      = resolveDirectValue(columnName, sourceMap, cert);

                    setParameterByType(ps, paramIndex, value, columnType, columnName);
                    paramIndex++;
                }

                // Metadata (JSON) columns
                for (int i = 0; i < destMetaColumns.length; i++) {
                    String[] sourceCols = sourceMetaColumnsList.get(i);
                    String[] jsonKeys   = jsonKeyNamesList.get(i);

                    String json = buildMetadataJson(sourceCols, jsonKeys, sourceMap, cert);

                    String metaColumnName = destMetaColumns[i].toLowerCase(Locale.ROOT);
                    String metaColumnType = columnTypes.get(metaColumnName);

                    if (metaColumnType != null && metaColumnType.toLowerCase(Locale.ROOT).contains("jsonb")) {
                        ps.setObject(paramIndex, json, java.sql.Types.OTHER);
                    } else {
                        ps.setString(paramIndex, json);
                    }
                    paramIndex++;
                }

                ps.addBatch();
                rowCount++;

                if (rowCount % batchSize == 0) {
                    int[] results = ps.executeBatch();
                    for (int r : results) {
                        if      (r == 1)                          inserted++;
                        else if (r == 0)                          skipped++;
                        else if (r == Statement.SUCCESS_NO_INFO)  unknown++;
                    }
                }
            }

            // Flush remaining batch
            if (ps != null) {
                int[] results = ps.executeBatch();
                for (int r : results) {
                    if      (r == 1)                          inserted++;
                    else if (r == 0)                          skipped++;
                    else if (r == Statement.SUCCESS_NO_INFO)  unknown++;
                }
            }

            conn.commit();

            try (PreparedStatement countPs = conn.prepareStatement("SELECT COUNT(*) FROM " + destTableName)) {
                try (ResultSet countRs = countPs.executeQuery()) {
                    if (countRs.next()) afterCount = countRs.getLong(1);
                }
            }

            status = true;

            log("INFO",
                    "Insert Summary | Table: " + destTableName +
                    " | Total Rows (CSV): " + totalRows +
                    " | Inserted (new): " + inserted +
                    " | Skipped (already exists): " + skipped +
                    " | Unknown (driver): " + unknown +
                    " | Rows Before: " + beforeCount +
                    " | Rows After: " + afterCount +
                    " | Net Added: " + (afterCount - beforeCount),
                    null, "DATA_INSERT");

        } catch (Exception ex) {
            log("ERROR", "Insert failed | Table: " + destTableName +
                    " | Rows processed before failure: " + rowCount +
                    " | Error: " + ex.getMessage(), null, "DATA_INSERT");
            try {
                if (conn != null) {
                    conn.rollback();
                    log("INFO", "Transaction rolled back successfully | Table: " + destTableName,
                            null, "DATA_INSERT");
                }
            } catch (Exception e) {
                log("ERROR", "Rollback failed | Table: " + destTableName +
                        " | Error: " + e.getMessage(), null, "DATA_INSERT");
            }
            status = false;

        } finally {
            try {
                if (conn != null) {
                    if (ps != null) ps.close();
                    conn.close();
                }
            } catch (Exception e) {
                log("ERROR", "Failed to close DB resources after insert | Error: " + e.getMessage(),
                        null, "DATA_INSERT");
            }
        }

        return status;
    }

    // -------------------------------------------------------------------------
    // Source map
    // -------------------------------------------------------------------------

    private Map<String, String> buildSourceMap(String[] headers, String[] record) {
        Map<String, String> sourceMap = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            String key   = normalize(headers[i]);
            String value = (i < record.length) ? record[i] : null;
            sourceMap.put(key, value);
        }
        return sourceMap;
    }

    // -------------------------------------------------------------------------
    // resolveDirectValue
    // Returns Object so that LocalDate can be passed directly for date columns
    // -------------------------------------------------------------------------

    private Object resolveDirectValue(String columnName, Map<String, String> sourceMap, CertContext cert) {
        // First try to get the value directly from the CSV source map
        Object value = sourceMap.get(columnName);

        // Fall back to cert-derived fields when the CSV does not carry them
        if (value == null && cert != null) {
            switch (columnName) {
                case "user_id":
                    value = cert.extractedUserId;
                    break;
                case "certificate_issuer":
                    value = cert.issuerDn;
                    break;
                case "certificate_serial":
                    value = cert.serialNumber;
                    break;
                case "certificate_thumbprint":
                    // SHA-256 uppercase hex
                    value = cert.thumbprintSha1;
                    break;
                case "certificate_expiry_date":
                    // Return LocalDate directly; setParameterByType handles sql.Date conversion
                    value = cert.expiryDate;
                    break;
                default:
                    break;
            }
        }

        return value;
    }

    // -------------------------------------------------------------------------
    // buildMetadataJson
    // Always injects user_id from the CSV (or CertContext fallback).
    // Merges with any other configured keys; never overwrites existing keys.
    // -------------------------------------------------------------------------

    private String buildMetadataJson(String[] sourceCols, String[] jsonKeys,
            Map<String, String> sourceMap, CertContext cert) {

        // Build an ordered map so that configured keys come first, then user_id is
        // appended (or already present if the config listed it explicitly).
        LinkedHashMap<String, String> jsonMap = new LinkedHashMap<>();

        // 1. Fill configured keys
        for (int j = 0; j < sourceCols.length; j++) {
            String sourceCol = normalize(sourceCols[j]);
            String jsonKey   = jsonKeys[j];

            String value = sourceMap.get(sourceCol);

            // Supplement missing values from CertContext when the source column
            // name matches a known cert field
            if ((value == null || value.isEmpty()) && cert != null) {
                switch (sourceCol) {
                    case "user_id":
                        value = cert.extractedUserId;
                        break;
                    case "certificate_issuer":
                        value = cert.issuerDn;
                        break;
                    case "certificate_serial":
                        value = cert.serialNumber;
                        break;
                    case "certificate_thumbprint":
                        value = cert.thumbprintSha1;
                        break;
                    case "certificate_expiry_date":
                        value = (cert.expiryDate != null) ? cert.expiryDate.toString() : null;
                        break;
                    default:
                        break;
                }
            }

            // Only put if key is not already present (never overwrite)
            if (!jsonMap.containsKey(jsonKey)) {
                jsonMap.put(jsonKey, value);
            }
        }

        // 2. Always guarantee user_id is present in the JSON output.
        //    Resolve it from the CSV first, then fall back to CertContext.
        if (!jsonMap.containsKey("user_id")) {
            String userId = sourceMap.get("user_id");
            if ((userId == null || userId.isEmpty()) && cert != null) {
                userId = cert.extractedUserId;
            }
            jsonMap.put("user_id", userId);
        }

        // 3. Serialise to JSON
        StringBuilder jsonBuilder = new StringBuilder("{");
        int idx = 0;
        for (Map.Entry<String, String> entry : jsonMap.entrySet()) {
            String escapedValue = entry.getValue();
            if (escapedValue != null) {
                escapedValue = escapedValue.replace("\\", "\\\\").replace("\"", "\\\"");
            }

            jsonBuilder.append("\"").append(entry.getKey()).append("\":");
            if (escapedValue != null) {
                jsonBuilder.append("\"").append(escapedValue).append("\"");
            } else {
                jsonBuilder.append("null");
            }

            if (idx < jsonMap.size() - 1) jsonBuilder.append(",");
            idx++;
        }
        jsonBuilder.append("}");

        return jsonBuilder.toString();
    }

    // -------------------------------------------------------------------------
    // parseCertificateContext
    // Robust: never throws; returns empty CertContext on any failure.
    // Supports PEM-wrapped and raw-base64 certificates.
    // Uses SHA-256 fingerprint; stores expiry as LocalDate (date only).
    // -------------------------------------------------------------------------

    private CertContext parseCertificateContext(String rawCertificate, String fallbackUserId) {
        CertContext ctx = new CertContext();
        ctx.rawWithSuffix = rawCertificate;

        // Resolve user_id: prefer explicit CSV value, then look for pipe-appended suffix
        if (fallbackUserId != null && !fallbackUserId.trim().isEmpty()) {
            ctx.extractedUserId = fallbackUserId.trim();
        }

        try {
            if (rawCertificate == null || rawCertificate.trim().isEmpty()) {
                log("WARN", "Certificate value is null or empty - skipping X.509 parsing", null, "CERT_PARSE");
                return ctx;
            }

            String certPart = rawCertificate.trim();

            // Support legacy pipe-appended user_id (e.g. "BASE64...|CU019")
            // Only extract if explicit fallbackUserId was not already supplied
            int pipeIndex = certPart.lastIndexOf('|');
            if (pipeIndex >= 0) {
                String suffix = certPart.substring(pipeIndex + 1).trim();
                if (!suffix.isEmpty() && (ctx.extractedUserId == null || ctx.extractedUserId.isEmpty())) {
                    ctx.extractedUserId = suffix;
                }
                certPart = certPart.substring(0, pipeIndex).trim();
            }

            ctx.certificateBase64 = certPart;

            // Strip PEM armour if present, then remove all whitespace
            certPart = certPart
                    .replace("-----BEGIN CERTIFICATE-----", "")
                    .replace("-----END CERTIFICATE-----", "")
                    .replaceAll("\\s+", "");

            byte[] certBytes = Base64.getDecoder().decode(certPart);

            CertificateFactory cf   = CertificateFactory.getInstance("X.509");
            X509Certificate    cert = (X509Certificate) cf.generateCertificate(
                                          new ByteArrayInputStream(certBytes));

            // Issuer DN (RFC 2253 / Java default format)
            ctx.issuerDn = cert.getIssuerX500Principal().getName();

            // Serial number as decimal string
            ctx.serialNumber = cert.getSerialNumber().toString();

            // Expiry as LocalDate (date portion only - no time, no timezone shift)
            ctx.expiryDate = cert.getNotAfter()
                    .toInstant()
                    .atZone(java.time.ZoneOffset.UTC)
                    .toLocalDate();

            // SHA-256 fingerprint (uppercase hex)
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(cert.getEncoded());

            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02X", b));
            }

            ctx.thumbprintSha1 = hex.toString();

            log("DEBUG", "X.509 parsed | Serial: " + ctx.serialNumber +
                    " | Expiry: " + ctx.expiryDate +
                    " | Thumbprint(SHA-256): " + ctx.thumbprintSha1 +
                    " | UserId: " + ctx.extractedUserId, null, "CERT_PARSE");

        } catch (Exception ex) {
            // Never throw - log and return partial context so the row is still inserted
            log("ERROR", "Failed to parse X.509 certificate | Error: " + ex.getMessage() +
                    " | Certificate-derived columns will be NULL for this row", null, "CERT_PARSE");
        }

        return ctx;
    }

    // -------------------------------------------------------------------------
    // Column type helpers
    // -------------------------------------------------------------------------

    private HashMap<String, String> getDestinationColumnTypes(String tableName, String[] directColumns,
            String[] metaColumns, Connection conn) {

        HashMap<String, String> columnTypes = new HashMap<>();

        try {
            DatabaseMetaData metaData = conn.getMetaData();

            String schemaName     = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts  = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase(Locale.ROOT);
                actualTableName = parts[1].toLowerCase(Locale.ROOT);
            } else {
                actualTableName = tableName.toLowerCase(Locale.ROOT);
            }

            // Always probe cert-derived columns so their types are available after
            // auto-injection in insertCsvDataToDestination.
            final String[] CERT_DERIVED_COLUMNS = {
                "certificate_expiry_date",
                "certificate_issuer",
                "certificate_serial",
                "certificate_thumbprint"
            };
            java.util.LinkedHashSet<String> allColumnSet = new java.util.LinkedHashSet<>();
            for (String c : directColumns)        allColumnSet.add(c);
            for (String c : metaColumns)          allColumnSet.add(c);
            for (String c : CERT_DERIVED_COLUMNS) allColumnSet.add(c);
            String[] allColumns = allColumnSet.toArray(new String[0]);

            for (String colName : allColumns) {
                ResultSet rs = metaData.getColumns(null, schemaName, actualTableName,
                        colName.toLowerCase(Locale.ROOT));
                if (rs.next()) {
                    String dataType = rs.getString("TYPE_NAME");
                    columnTypes.put(colName.toLowerCase(Locale.ROOT), dataType);
                    log("DEBUG", "Column type resolved | Column: " + colName +
                            " | Type: " + dataType + " | Table: " + tableName, null, "VALIDATION");
                }
                rs.close();
            }

        } catch (Exception ex) {
            log("ERROR", "Failed to fetch destination column types | Table: " + tableName +
                    " | Error: " + ex.getMessage(), null, "VALIDATION");
            return null;
        }

        return columnTypes;
    }

    // -------------------------------------------------------------------------
    // PreparedStatement parameter binding
    // -------------------------------------------------------------------------

    private void setParameterByType(PreparedStatement ps, int paramIndex, String value,
            String columnType, String columnName) throws Exception {
        setParameterByType(ps, paramIndex, (Object) value, columnType, columnName);
    }

    /**
     * Binds a value to a PreparedStatement parameter using the actual DB column
     * type. LocalDate is mapped to java.sql.Date (not Timestamp) so that
     * certificate_expiry_date is stored correctly as a PostgreSQL DATE.
     */
    private void setParameterByType(PreparedStatement ps, int paramIndex, Object value,
            String columnType, String columnName) throws Exception {

        if (value == null) {
            setNullByType(ps, paramIndex, columnType);
            return;
        }

        // LocalDate -> sql.Date (used for certificate_expiry_date and any date column)
        if (value instanceof LocalDate) {
            ps.setDate(paramIndex, java.sql.Date.valueOf((LocalDate) value));
            return;
        }

        // Legacy safety: java.sql.Timestamp passthrough
        if (value instanceof java.sql.Timestamp) {
            ps.setTimestamp(paramIndex, (java.sql.Timestamp) value);
            return;
        }

        // Legacy safety: java.util.Date passthrough
        if (value instanceof java.util.Date) {
            ps.setTimestamp(paramIndex, new java.sql.Timestamp(((java.util.Date) value).getTime()));
            return;
        }

        String str = String.valueOf(value);

        if (str.trim().isEmpty()) {
            if (columnType != null) {
                String type = columnType.toLowerCase(Locale.ROOT);
                if (type.contains("char") || type.contains("text") || type.contains("json")) {
                    ps.setString(paramIndex, str);
                } else {
                    setNullByType(ps, paramIndex, columnType);
                }
            } else {
                ps.setString(paramIndex, str);
            }
            return;
        }

        if (columnType != null) {
            String type = columnType.toLowerCase(Locale.ROOT);

            try {
                if (type.contains("bigint") || type.contains("int8") || type.contains("bigserial")) {
                    ps.setLong(paramIndex, Long.parseLong(str.trim()));
                } else if (type.contains("int") || type.contains("serial")) {
                    ps.setInt(paramIndex, Integer.parseInt(str.trim()));
                } else if (type.contains("numeric") || type.contains("decimal")) {
                    ps.setBigDecimal(paramIndex, new BigDecimal(str.trim()));
                } else if (type.contains("float") || type.contains("double") || type.contains("real")) {
                    ps.setDouble(paramIndex, Double.parseDouble(str.trim()));
                } else if (type.contains("bool")) {
                    ps.setBoolean(paramIndex, Boolean.parseBoolean(str.trim()));
                } else if (type.contains("timestamp")) {
                    ps.setTimestamp(paramIndex, java.sql.Timestamp.valueOf(str.trim()));
                } else if (type.contains("date")) {
                    // Use sql.Date (not Timestamp) for DATE columns
                    ps.setDate(paramIndex, java.sql.Date.valueOf(str.trim()));
                } else if (type.contains("time")) {
                    ps.setTime(paramIndex, java.sql.Time.valueOf(str.trim()));
                } else {
                    ps.setString(paramIndex, str);
                }
            } catch (Exception e) {
                log("ERROR", "Type conversion failed | Column: " + columnName +
                        " | DB Type: " + columnType +
                        " | Value: " + str +
                        " | Error: " + e.getMessage(), null, "DATA_INSERT");
                throw new Exception("Data type mismatch for column '" + columnName + "'");
            }
        } else {
            ps.setString(paramIndex, str);
        }
    }

    private void setNullByType(PreparedStatement ps, int paramIndex, String columnType) throws Exception {
        if (columnType == null) {
            ps.setNull(paramIndex, java.sql.Types.VARCHAR);
            return;
        }

        String type = columnType.toLowerCase(Locale.ROOT);

        if      (type.contains("int")  || type.contains("serial"))                           ps.setNull(paramIndex, java.sql.Types.BIGINT);
        else if (type.contains("timestamp"))                                                  ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
        else if (type.contains("date"))                                                       ps.setNull(paramIndex, java.sql.Types.DATE);
        else if (type.contains("time"))                                                       ps.setNull(paramIndex, java.sql.Types.TIME);
        else if (type.contains("numeric") || type.contains("decimal") ||
                 type.contains("float")   || type.contains("double"))                        ps.setNull(paramIndex, java.sql.Types.NUMERIC);
        else if (type.contains("bool"))                                                       ps.setNull(paramIndex, java.sql.Types.BOOLEAN);
        else                                                                                  ps.setNull(paramIndex, java.sql.Types.VARCHAR);
    }

    // -------------------------------------------------------------------------
    // Table / column validation
    // -------------------------------------------------------------------------

    private boolean validateTableExists(String tableName, String dbURL, String dbUser, String dbPass) {
        Connection conn   = null;
        boolean    exists = false;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            String schemaName     = "public";
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts  = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase(Locale.ROOT);
                actualTableName = parts[1].toLowerCase(Locale.ROOT);
            } else {
                actualTableName = tableName.toLowerCase(Locale.ROOT);
            }

            log("DEBUG", "Checking table existence | Schema: " + schemaName +
                    " | Table: " + actualTableName, null, "VALIDATION");

            String sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                ps.setString(2, actualTableName);

                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        exists = true;
                        log("INFO", "Table found | Schema: " + schemaName +
                                " | Table: " + actualTableName, null, "VALIDATION");
                    } else {
                        log("WARN", "Table not found | Schema: " + schemaName +
                                " | Table: " + actualTableName, null, "VALIDATION");
                    }
                }
            }

        } catch (Exception ex) {
            log("ERROR", "Exception during table existence check | Table: " + tableName +
                    " | Error: " + ex.getMessage(), null, "VALIDATION");
            exists = false;
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (Exception e) {
                log("ERROR", "Failed to close DB connection after table validation | Error: " + e.getMessage(),
                        null, "VALIDATION");
            }
        }

        return exists;
    }

    private HashSet<String> getTableColumns(String tableName, String dbURL, String dbUser, String dbPass) {
        Connection       conn    = null;
        HashSet<String>  columns = new HashSet<>();

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
            DatabaseMetaData metaData = conn.getMetaData();

            String schemaName     = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts  = tableName.split("\\.");
                schemaName      = parts[0].toLowerCase(Locale.ROOT);
                actualTableName = parts[1].toLowerCase(Locale.ROOT);
            } else {
                actualTableName = tableName.toLowerCase(Locale.ROOT);
            }

            log("DEBUG", "Fetching column list | Schema: " + schemaName +
                    " | Table: " + actualTableName, null, "VALIDATION");

            ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, null);

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                columns.add(columnName.toLowerCase(Locale.ROOT));
                log("DEBUG", "Column found: " + columnName + " | Table: " + tableName, null, "VALIDATION");
            }

            rs.close();

            log("INFO", "Column list fetched | Table: " + tableName +
                    " | Total columns: " + columns.size(), null, "VALIDATION");

        } catch (Exception ex) {
            log("ERROR", "Failed to retrieve column list | Table: " + tableName +
                    " | Error: " + ex.getMessage(), null, "VALIDATION");
            columns = null;
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (Exception e) {
                log("ERROR", "Failed to close DB connection after fetching columns | Error: " + e.getMessage(),
                        null, "VALIDATION");
            }
        }

        return columns;
    }
}