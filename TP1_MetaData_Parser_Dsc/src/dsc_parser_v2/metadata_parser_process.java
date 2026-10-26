package dsc_parser_v2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Hardcoded ETL for admin.dsc_certificate.
 *
 * Source CSV headers expected (case-insensitive):
 * - user_certificate.user_id
 * - user_certificate.user_certificate_key
 *
 * Target table:
 * - admin.dsc_certificate
 *
 * Mapping:
 * - user_certificate.user_id          -> dsc_certificate.user_id       (FK: must exist in admin.users.user_id)
 * - user_certificate.user_certificate_key -> dsc_certificate.certificate_data (base64, normalized)
 * - dsc_certificate.version           -> 1 (constant)
 * - dsc_certificate.id                -> random UUID
 * - dsc_certificate.serial            -> X509 certificate serial number
 * - dsc_certificate.issuer            -> X509 certificate issuer DN
 * - dsc_certificate.expiry            -> X509 certificate "Not After" date
 * - dsc_certificate.thumbprint        -> SHA-1 thumbprint of certificate content
 * - dsc_certificate.status            -> 'ACTIVE' (constant)
 * - dsc_certificate.certificate_id    -> null
 * - dsc_certificate.signed_data       -> original base64 encoded certificate data
 * - dsc_certificate.creation_datetime -> insert-time timestamp
 * - dsc_certificate.modification_datetime / cert_registration_date / cert_deregistration_date -> null
 */
public class metadata_parser_process {

    private static final String processName    = "metadata-parser";
    private static final String processVersion = "2.0";
    private static final boolean debugEnabled  = true;
    private static final DateTimeFormatter logFormat =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");

    private static final String TARGET_TABLE = "admin.dsc_certificate";
    private static final String USER_LOOKUP_TABLE = "admin.\"users\"";

    private static final int BATCH_SIZE = 50000;

    private static class CsvData {
        String[] headers;
        List<String[]> rows = new ArrayList<String[]>();
    }

    private static class CertDetails {
        String normalizedBase64;
        String serial;
        String issuer;
        LocalDate expiry;
        String thumbprintSha1;
    }

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
            return;
        }

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
        if (s == null || s.isEmpty()) {
            return s;
        }
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    private String normalize(String s) {
        return s == null ? null : s.trim().toLowerCase(Locale.ROOT);
    }

    private String safeTrim(String s) {
        return s == null ? null : s.trim();
    }

    private String firstNonBlank(Map<String, String> map, String... keys) {
        if (map == null || keys == null) {
            return null;
        }
        for (String key : keys) {
            if (key == null) {
                continue;
            }
            String val = map.get(normalize(key));
            if (val != null && val.trim().length() > 0) {
                return val.trim();
            }
        }
        return null;
    }

    private char detectDelimiter(String line) {
        if (line == null || line.length() == 0) {
            return ',';
        }

        int comma = 0;
        int pipe = 0;
        int tab = 0;
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                inQuotes = !inQuotes;
            } else if (!inQuotes) {
                if (c == ',') {
                    comma++;
                } else if (c == '|') {
                    pipe++;
                } else if (c == '\t') {
                    tab++;
                }
            }
        }

        if (pipe >= comma && pipe >= tab && pipe > 0) {
            return '|';
        }
        if (comma >= tab && comma > 0) {
            return ',';
        }
        if (tab > 0) {
            return '\t';
        }
        return ',';
    }

    private String[] splitDelimitedLine(String line, char delimiter) {
        List<String> out = new ArrayList<String>();
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
            } else if (c == delimiter && !inQuotes) {
                out.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        out.add(current.toString().trim());

        String[] values = out.toArray(new String[0]);
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                values[i] = values[i].replace("\uFEFF", "").trim();
                if (values[i].length() == 0) {
                    values[i] = null;
                }
            }
        }
        return values;
    }

    private CsvData readCSV(String filePath) {
        CsvData data = new CsvData();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(filePath));
            String headerLine = br.readLine();

            if (headerLine == null) {
                log("WARN", "Empty CSV file - no header found | File: " + filePath, null, "FILE_PROCESS");
                return null;
            }

            headerLine = headerLine.replace("\uFEFF", "").trim();
            char delimiter = detectDelimiter(headerLine);
            data.headers = splitDelimitedLine(headerLine, delimiter);

            log("INFO", "CSV header parsed | File: " + filePath +
                    " | Delimiter: " + delimiter +
                    " | Columns: " + data.headers.length,
                    null, "FILE_PROCESS");

            String line;
            int rowNo = 0;
            while ((line = br.readLine()) != null) {
                if (line.trim().length() == 0) {
                    continue;
                }

                String[] values = splitDelimitedLine(line, delimiter);
                String[] row = new String[data.headers.length];

                for (int i = 0; i < data.headers.length; i++) {
                    row[i] = (i < values.length) ? values[i] : null;
                }

                if (values.length != data.headers.length) {
                    log("WARN", "Column count mismatch at row " + rowNo +
                            " | Expected: " + data.headers.length +
                            " | Found: " + values.length +
                            " | Missing values padded with nulls",
                            null, "FILE_PROCESS");
                }

                data.rows.add(row);
                rowNo++;
            }

            log("INFO", "CSV read complete | File: " + filePath +
                    " | Total rows: " + data.rows.size(),
                    null, "FILE_PROCESS");

        } catch (Exception ex) {
            log("ERROR", "Failed to read CSV file: " + filePath +
                    " | Error: " + ex.getMessage(), null, "FILE_PROCESS");
            return null;
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (Exception ignore) {
            }
        }

        return data;
    }

    private Map<String, String> buildSourceMap(String[] headers, String[] row) {
        Map<String, String> map = new LinkedHashMap<String, String>();
        for (int i = 0; i < headers.length; i++) {
            String key = normalize(headers[i]);
            String value = (i < row.length) ? row[i] : null;
            if (value != null) {
                value = value.trim();
                if (value.length() == 0) {
                    value = null;
                }
            }
            map.put(key, value);
        }
        return map;
    }

    private String cleanCertificateText(String rawCertificate) {
        if (rawCertificate == null) {
            return null;
        }

        String cert = rawCertificate.trim();
        if (cert.length() == 0) {
            return null;
        }

        int pipeIndex = cert.lastIndexOf('|');
        if (pipeIndex > -1) {
            cert = cert.substring(0, pipeIndex).trim();
        }

        int commaIndex = cert.indexOf("base64,");
        if (commaIndex > -1) {
            cert = cert.substring(commaIndex + 7).trim();
        }

        cert = cert.replace("-----BEGIN CERTIFICATE-----", "")
                   .replace("-----END CERTIFICATE-----", "")
                   .replaceAll("\\s+", "");

        if (cert.length() == 0) {
            return null;
        }

        return cert;
    }

    private CertDetails parseCertificate(String base64Certificate) {
        try {
            String normalizedBase64 = cleanCertificateText(base64Certificate);
            if (normalizedBase64 == null) {
                return null;
            }

            byte[] certBytes = Base64.getDecoder().decode(normalizedBase64);

            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate cert = (X509Certificate) cf.generateCertificate(
                    new java.io.ByteArrayInputStream(certBytes));

            CertDetails details = new CertDetails();
            details.normalizedBase64 = normalizedBase64;
            details.serial = cert.getSerialNumber().toString();
            details.issuer = cert.getIssuerX500Principal().getName();
            details.expiry = cert.getNotAfter().toInstant().atZone(ZoneOffset.UTC).toLocalDate();

            // ETL mapping: Generate SHA-1 thumbprint from certificate content
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(cert.getEncoded());

            StringBuilder hex = new StringBuilder();
            for (byte b : digest) {
                hex.append(String.format("%02X", b));
            }
            details.thumbprintSha1 = hex.toString();

            return details;

        } catch (Exception ex) {
            log("ERROR", "Failed to parse certificate | Error: " + ex.getMessage(),
                    null, "CERT_PARSE");
            return null;
        }
    }

    private Set<String> loadAllowedUserIds(String dbURL, String dbUsername, String dbPassword) {
        Set<String> allowed = new HashSet<String>();
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUsername, dbPassword);
            if (conn == null) {
                return null;
            }

            String sql = "SELECT user_id FROM " + USER_LOOKUP_TABLE;
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();

            while (rs.next()) {
                String userId = safeTrim(rs.getString(1));
                if (userId != null && userId.length() > 0) {
                	allowed.add(userId.trim().toUpperCase(Locale.ROOT));
                }
            }

            log("INFO", "Loaded valid user_id list from lookup table | Table: " + USER_LOOKUP_TABLE +
                    " | Count: " + allowed.size(), null, "VALIDATION");

        } catch (Exception ex) {
            log("ERROR", "Failed to load user lookup table | Table: " + USER_LOOKUP_TABLE +
                    " | Error: " + ex.getMessage(), null, "VALIDATION");
            return null;
        } finally {
            try {
                if (rs != null) rs.close();
                if (ps != null) ps.close();
                if (conn != null) conn.close();
            } catch (Exception ignore) {
            }
        }

        return allowed;
    }

    private boolean isNotBlank(String s) {
        return s != null && s.trim().length() > 0;
    }

    private void setNull(PreparedStatement ps, int index) throws Exception {
        ps.setNull(index, java.sql.Types.NULL);
    }

    private boolean insertOneRow(PreparedStatement ps, Map<String, String> sourceMap,
                                 Set<String> allowedUserIds) throws Exception {

        String userId = firstNonBlank(sourceMap,
                "user_certificate.user_id",
                "user_id");

        String rawCertificate = firstNonBlank(
        	    sourceMap,
        	    "certificate",
        	    "user_certificate.user_certificate_key",
        	    "user_certificate_key");

        if (!isNotBlank(userId)) {
            log("WARN", "Skipping row because user_id is missing", null, "DATA_VALIDATION");
            return false;
        }

        boolean userFound = false;

        if (allowedUserIds != null) {

            String csvUserId = userId == null ? "" : userId.trim();

            for (String dbUserId : allowedUserIds) {

                if (dbUserId != null &&
                    dbUserId.trim().equalsIgnoreCase(csvUserId)) {

                    userFound = true;
                    break;
                }
            }

            if (!userFound) {

                log("WARN",
                    "Skipping row because user_id was not found in lookup table | user_id:[" +
                    csvUserId + "]",
                    null,
                    "DATA_VALIDATION");

                return false;
            }
        }

        if (!isNotBlank(rawCertificate)) {
            log("WARN", "Skipping row because certificate data is missing | user_id: " + userId,
                    null, "DATA_VALIDATION");
            return false;
        }

        CertDetails cert = parseCertificate(rawCertificate);
        if (cert == null) {
            log("WARN", "Skipping row because certificate could not be parsed | user_id: " + userId,
                    null, "CERT_PARSE");
            return false;
        }

        if (!isNotBlank(cert.normalizedBase64) ||
                !isNotBlank(cert.serial) ||
                !isNotBlank(cert.issuer) ||
                cert.expiry == null ||
                !isNotBlank(cert.thumbprintSha1)) {
            log("WARN", "Skipping row because one or more certificate fields are missing | user_id: " + userId,
                    null, "CERT_PARSE");
            return false;
        }

        LocalDateTime now = LocalDateTime.now();

        int p = 1;
        ps.setObject(p++, UUID.randomUUID());                             // id (uuid column - must use setObject, not setString)
        ps.setLong(p++, 1L);                                             // version
        ps.setString(p++, cert.normalizedBase64);                        // certificate_data
        ps.setString(p++, userId);                                       // user_id
        ps.setString(p++, cert.serial);                                  // serial
        ps.setString(p++, cert.issuer);                                  // issuer
        ps.setDate(p++, java.sql.Date.valueOf(cert.expiry));             // expiry
        ps.setString(p++, cert.thumbprintSha1);                          // thumbprint
        ps.setString(p++, "ACTIVE");                                     // status
        setNull(ps, p++);                                                // certificate_id
        ps.setString(p++, cert.normalizedBase64);                        // signed_data
        ps.setTimestamp(p++, Timestamp.valueOf(now));                    // creation_datetime
        setNull(ps, p++);                                                // modification_datetime
        setNull(ps, p++);                                                // cert_registration_date
        setNull(ps, p++);                                                // cert_deregistration_date

        ps.executeUpdate();
        return true;
    }

    public boolean parseAndTransferData(String dbURL, String dbUsername, String dbPassword,
            String sourceInput, String destTableName, String directColumns, int metadataCount,
            String destMetadataColumns, String sourceMetadataColumns, String jsonKeyNames) {

        Connection conn = null;
        PreparedStatement ps = null;
        boolean status = false;

        int totalRows = 0;
        int inserted = 0;
        int skipped = 0;
        String currentUserId = null;

        try {
            log("INFO", "===== START CSV/DB PROCESSING | Source: " + sourceInput +
                    " | Hardcoded Target Table: " + TARGET_TABLE + " =====",
                    null, "FILE_PROCESS");

            if (sourceInput == null || !sourceInput.toLowerCase(Locale.ROOT).endsWith(".csv")) {
                log("ERROR", "Input is not a CSV file | Input: " + sourceInput, null, "FILE_PROCESS");
                return false;
            }

            CsvData csvData = readCSV(sourceInput);
            if (csvData == null || csvData.headers == null || csvData.headers.length == 0) {
                log("WARN", "No header/data found in CSV | File: " + sourceInput, null, "FILE_PROCESS");
                return false;
            }

            Set<String> allowedUserIds = loadAllowedUserIds(dbURL, dbUsername, dbPassword);
            if (allowedUserIds == null) {
                log("ERROR", "Could not load lookup user table; aborting file | File: " + sourceInput,
                        null, "VALIDATION");
                return false;
            }

            conn = new PostgreSQLConnection().getConnection(dbURL, dbUsername, dbPassword);
            if (conn == null) {
                log("ERROR", "Failed to open PostgreSQL connection", null, "DB_CONNECT");
                return false;
            }

            conn.setAutoCommit(false);

            String insertSql =
                    "INSERT INTO " + TARGET_TABLE + " (" +
                    "id, version, certificate_data, user_id, serial, issuer, expiry, " +
                    "thumbprint, status, certificate_id, signed_data, creation_datetime, modification_datetime, " +
                    "cert_registration_date, cert_deregistration_date" +
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            ps = conn.prepareStatement(insertSql);

            for (String[] row : csvData.rows) {
                totalRows++;

                Map<String, String> sourceMap = buildSourceMap(csvData.headers, row);

                currentUserId = firstNonBlank(sourceMap,
                        "user_certificate.user_id",
                        "user_id");

                Savepoint rowSavepoint = null;
                try {
                    // NEW: savepoint before each row so one bad row can be rolled back
                    // individually instead of poisoning the whole transaction.
                    rowSavepoint = conn.setSavepoint();

                    boolean rowInserted = insertOneRow(ps, sourceMap, allowedUserIds);
                    if (rowInserted) {
                        inserted++;
                    } else {
                        skipped++;
                    }
                } catch (Exception rowEx) {

                    String failedUserId = firstNonBlank(sourceMap,
                            "user_certificate.user_id",
                            "user_id");

                    rowEx.printStackTrace();

                    log("ERROR",
                        "Row failed and will be skipped | user_id=" + failedUserId +
                        " | Error=" + rowEx.getClass().getName() +
                        " | Message=" + rowEx.getMessage(),
                        null,
                        "DATA_INSERT");

                    // NEW: roll back only this row's work, leaving prior committed/staged
                    // rows in the transaction intact and the connection usable again.
                    try {
                        if (rowSavepoint != null) {
                            conn.rollback(rowSavepoint);
                        }
                    } catch (Exception spEx) {
                        log("ERROR", "Failed to rollback to savepoint | File: " + sourceInput +
                                " | Error: " + spEx.getMessage(), null, "DATA_INSERT");
                    }

                    log("ERROR", "Row failed and will be skipped | File: " + sourceInput +
                            " | Error: " + rowEx.getMessage(), null, "DATA_INSERT");
                }

                if ((inserted + skipped) % BATCH_SIZE == 0) {
                    conn.commit();
                    log("INFO", "Intermediate commit completed | Processed rows: " + (inserted + skipped),
                            null, "DATA_INSERT");
                }
            }

            conn.commit();
            status = true;

            log("INFO", "Insert summary | Target Table: " + TARGET_TABLE +
                    " | Total Rows: " + totalRows +
                    " | Inserted: " + inserted +
                    " | Skipped: " + skipped,
                    null, "DATA_INSERT");

            log("INFO", "Processing completed successfully | File: " + sourceInput +
                    " | Target Table: " + TARGET_TABLE,
                    null, "FILE_PROCESS");

        } catch (Exception ex) {
            log("ERROR", "Unhandled exception in parseAndTransferData | File: " + sourceInput +
                    " | Error: " + ex.getMessage(),
                    null, "APP_ERROR");

            try {
                if (conn != null) {
                    conn.rollback();
                    log("INFO", "Transaction rolled back successfully", null, "DATA_INSERT");
                }
            } catch (Exception rbEx) {
                log("ERROR", "Rollback failed | Error: " + rbEx.getMessage(), null, "DATA_INSERT");
            }
            status = false;
        } finally {
            try {
                if (ps != null) {
                    ps.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (Exception closeEx) {
                log("ERROR", "Failed to close DB resources | Error: " + closeEx.getMessage(),
                        null, "APP_ERROR");
            }
        }

        return status;
    }
}