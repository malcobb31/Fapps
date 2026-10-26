package ntp17_process_v2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class ntp17_process_methods {

    private final String processName;
    private final String processVersion;
    private final boolean debugEnabled;
    private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    public ntp17_process_methods(String processName, String processVersion, boolean debugEnabled) {
        this.processName = processName;
        this.processVersion = processVersion;
        this.debugEnabled = debugEnabled;
    }

    public void process(String csvFile, Connection conn, String serverIP) throws Exception {
        log("INFO", "Process started", serverIP, "APP_START");

        List<SourceRow> sourceRows = readSourceFile(csvFile, serverIP);
        log("INFO", "Source rows read: " + sourceRows.size(), serverIP, "FILE_READ");

        if (sourceRows.isEmpty()) {
            log("INFO", "No records found in source file", serverIP, "NO_DATA");
            return;
        }

        sourceRows.sort(new Comparator<SourceRow>() {
            @Override
            public int compare(SourceRow r1, SourceRow r2) {
                int c = compareNullableLongDesc(r1.updatedMillis, r2.updatedMillis);
                if (c != 0) {
                    return c;
                }

                c = compareNullableLongDesc(r1.createdMillis, r2.createdMillis);
                if (c != 0) {
                    return c;
                }

                return Long.compare(r1.rowIndex, r2.rowIndex);
            }
        });

        int nextSeq = getNextUserSequence(conn, serverIP);
        log("INFO", "Starting user_id sequence from: " + nextSeq, serverIP, "USER_ID_SEQ");

        List<TargetRow> finalRows = new ArrayList<TargetRow>();
        Set<String> seenEmails = new HashSet<String>();
        Set<String> seenMobiles = new HashSet<String>();

        int skippedInvalid = 0;
        int skippedDuplicate = 0;
        int skippedEmailDuplicate = 0;
        int skippedMobileDuplicate = 0;

        for (SourceRow sr : sourceRows) {
            String emailKey = normalizeEmail(sr.email);
            String mobileKey = normalizeMobile(sr.mobile);

            if (isBlank(emailKey) || isBlank(mobileKey)) {
                skippedInvalid++;
                log("WARN", "Skipping row " + sr.rowIndex + " due to blank email/mobile", serverIP, "ROW_SKIPPED");
                continue;
            }

            if (seenEmails.contains(emailKey)) {
                skippedDuplicate++;
                skippedEmailDuplicate++;
                log("INFO",
                        "Skipping duplicate row " + sr.rowIndex + " email=" + emailKey + " mobile=" + mobileKey,
                        serverIP,
                        "DUPLICATE_SKIPPED");
                continue;
            }

            if (seenMobiles.contains(mobileKey)) {
                skippedDuplicate++;
                skippedMobileDuplicate++;
                log("INFO",
                        "Skipping duplicate row " + sr.rowIndex + " email=" + emailKey + " mobile=" + mobileKey,
                        serverIP,
                        "DUPLICATE_SKIPPED");
                continue;
            }

            TargetRow tr = buildTargetRow(sr, nextSeq++);
            if (tr == null) {
                skippedInvalid++;
                log("WARN", "Skipping row " + sr.rowIndex + " due to invalid mandatory data", serverIP, "ROW_SKIPPED");
                continue;
            }

            seenEmails.add(emailKey);
            seenMobiles.add(mobileKey);
            finalRows.add(tr);
        }

        log("INFO",
                "Rows after dedupe/validation: " + finalRows.size()
                        + ", skippedDuplicate=" + skippedDuplicate
                        + ", skippedEmailDuplicate=" + skippedEmailDuplicate
                        + ", skippedMobileDuplicate=" + skippedMobileDuplicate
                        + ", skippedInvalid=" + skippedInvalid,
                serverIP,
                "DEDUP_DONE");

        if (finalRows.isEmpty()) {
            log("INFO", "Nothing to insert after dedupe", serverIP, "NO_DATA");
            return;
        }

        int inserted = insertBatch(finalRows, conn, serverIP);
        log("INFO", "Insert completed. Rows inserted: " + inserted, serverIP, "PROCESS_COMPLETE");
    }

    private List<SourceRow> readSourceFile(String csvFile, String serverIP) throws IOException {
        List<SourceRow> rows = new ArrayList<SourceRow>();

        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(csvFile));

            String headerLine = br.readLine();
            if (headerLine == null) {
                return rows;
            }

            headerLine = stripBom(headerLine);

            List<String> headers = parsePipeLine(headerLine);
            Map<String, Integer> headerIndex = buildHeaderIndex(headers);

            log("INFO", "Header columns found: " + headers.size(), serverIP, "HEADER_READ");

            String line;
            long rowIndex = 0;

            while ((line = br.readLine()) != null) {
                if (isBlank(line)) {
                    continue;
                }

                rowIndex++;
                List<String> values = parsePipeLine(line);

                SourceRow sr = new SourceRow();
                sr.rowIndex = rowIndex;
                sr.fullName = getValue(values, headerIndex, "fim_compl_Officer_name");
                sr.email = getValue(values, headerIndex, "fim_compl_Officer_EmailId");
                sr.mobile = getValue(values, headerIndex, "fim_compl_officer_mobile");
                sr.createdBy = getValue(values, headerIndex, "fim_created_by");
                sr.createdAtRaw = getValue(values, headerIndex, "fim_created_date");
                sr.updatedAtRaw = getValue(values, headerIndex, "fim_update_date");

                sr.createdMillis = parseSortableMillis(sr.createdAtRaw);
                sr.updatedMillis = parseSortableMillis(sr.updatedAtRaw);

                rows.add(sr);
            }
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception ignored) {
                }
            }
        }

        return rows;
    }

    private Map<String, Integer> buildHeaderIndex(List<String> headers) {
        Map<String, Integer> map = new HashMap<String, Integer>();
        for (int i = 0; i < headers.size(); i++) {
            String key = normalizeHeader(headers.get(i));
            if (!map.containsKey(key)) {
                map.put(key, Integer.valueOf(i));
            }
        }
        return map;
    }

    private TargetRow buildTargetRow(SourceRow sr, int sequence) {
        String fullName = safeTrim(sr.fullName);
        String email = normalizeEmail(sr.email);
        String mobile = normalizeMobile(sr.mobile);
        String createdBy = safeTrim(sr.createdBy);

        if (isBlank(fullName) || isBlank(email) || isBlank(mobile)) {
            return null;
        }

        String[] nameParts = splitName(fullName);
        String firstName = nameParts[0];
        String middleName = nameParts[1];
        String lastName = nameParts[2];

        if (isBlank(firstName)) {
            return null;
        }

        TargetRow tr = new TargetRow();
        tr.id = UUID.randomUUID();
        tr.userId = String.format("NO%06d", Integer.valueOf(sequence));
        tr.email = email;
        tr.mobile = mobile;
        tr.ckycNo = null;
        tr.keyCloakUserId = null;
        tr.title = "Mx";
        tr.firstName = firstName;
        tr.middleName = middleName;
        tr.lastName = lastName;
        tr.citizenship = "Indian";
        tr.countryCode = "+91";
        tr.gender = "Male";
        tr.dscId = null;
        tr.createdAt = parseOffsetTime(sr.createdAtRaw);
        tr.updatedAt = parseOffsetTime(sr.updatedAtRaw);
        tr.userType = "NO";
        tr.version = null;
        tr.createdBy = createdBy;
        tr.updatedBy = createdBy;
        tr.active = null;

        log("INFO",
                "mobile=" + mobile +
                " createdAtRaw=[" + sr.createdAtRaw + "]" +
                " updatedAtRaw=[" + sr.updatedAtRaw + "]" +
                " parsedCreated=[" + parseOffsetTime(sr.createdAtRaw) + "]" +
                " parsedUpdated=[" + parseOffsetTime(sr.updatedAtRaw) + "]",
                null,
                "DEBUG");
        return tr;
    }

    private int insertBatch(List<TargetRow> rows, Connection conn, String serverIP) throws SQLException {
        String sql =
                "INSERT INTO re_mgmt.signup_user_v1 " +
                "(id, user_id, email, mobile, ckyc_no, key_cloak_user_id, title, first_name, middle_name, " +
                "last_name, citizenship, country_code, gender, dsc_id, created_at, updated_at, user_type, " +
                "version, created_by, updated_by, active) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        int inserted = 0;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int batchSize = 0;

            for (TargetRow r : rows) {
                if (existsInTarget(conn, r.email, r.mobile, r.userId)) {
                    log("INFO",
                            "Already exists in target, skipping email=" + r.email + " mobile=" + r.mobile + " userId=" + r.userId,
                            serverIP,
                            "TARGET_DUPLICATE");
                    continue;
                }

                int i = 1;
                ps.setObject(i++, r.id);
                ps.setString(i++, r.userId);
                ps.setString(i++, r.email);
                ps.setString(i++, r.mobile);
                ps.setString(i++, r.ckycNo);
                ps.setString(i++, r.keyCloakUserId);
                ps.setString(i++, r.title);
                ps.setString(i++, r.firstName);
                ps.setString(i++, r.middleName);
                ps.setString(i++, r.lastName);
                ps.setString(i++, r.citizenship);
                ps.setString(i++, r.countryCode);
                ps.setString(i++, r.gender);

                if (r.dscId != null) {
                    ps.setObject(i++, r.dscId);
                } else {
                    ps.setNull(i++, Types.OTHER);
                }

                if (r.createdAt != null) {
                    ps.setObject(i++, r.createdAt);
                } else {
                    ps.setNull(i++, Types.TIME_WITH_TIMEZONE);
                }

                if (r.updatedAt != null) {
                    ps.setObject(i++, r.updatedAt);
                } else {
                    ps.setNull(i++, Types.TIME_WITH_TIMEZONE);
                }

                ps.setString(i++, r.userType);

                if (r.version != null) {
                    ps.setLong(i++, r.version.longValue());
                } else {
                    ps.setNull(i++, Types.BIGINT);
                }

                ps.setString(i++, r.createdBy);
                ps.setString(i++, r.updatedBy);

                if (r.active != null) {
                    ps.setBoolean(i++, r.active.booleanValue());
                } else {
                    ps.setNull(i++, Types.BOOLEAN);
                }

                ps.addBatch();
                batchSize++;
                inserted++;

                if (batchSize % 500 == 0) {
                    ps.executeBatch();
                    conn.commit();
                    log("INFO", "Executed batch of 500 rows", serverIP, "BATCH_EXECUTED");
                    batchSize = 0;
                }
            }

            if (batchSize > 0) {
                ps.executeBatch();
                conn.commit();
                log("INFO", "Executed final batch", serverIP, "BATCH_EXECUTED");
            }
        } catch (SQLException e) {
            conn.rollback();
            log("ERROR", "Insert failed: " + e.getMessage(), serverIP, "APP_ERROR");
            throw e;
        }

        return inserted;
    }

    private boolean existsInTarget(Connection conn, String email, String mobile, String userId) throws SQLException {
        String sql =
                "SELECT 1 " +
                "FROM re_mgmt.signup_user_v1 " +
                "WHERE email = ? OR mobile = ? OR user_id = ? " +
                "LIMIT 1";

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, email);
            ps.setString(2, mobile);
            ps.setString(3, userId);

            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private int getNextUserSequence(Connection conn, String serverIP) throws SQLException {
        String sql =
                "SELECT user_id " +
                "FROM re_mgmt.signup_user_v1 " +
                "WHERE user_id LIKE 'NO%' " +
                "ORDER BY user_id DESC " +
                "LIMIT 1";

        int next = 1;

        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            if (rs.next()) {
                String lastUserId = safeTrim(rs.getString(1));
                if (!isBlank(lastUserId) && lastUserId.startsWith("NO") && lastUserId.length() > 2) {
                    String num = lastUserId.substring(2).replaceAll("[^0-9]", "");
                    if (!isBlank(num)) {
                        try {
                            next = Integer.parseInt(num) + 1;
                        } catch (NumberFormatException ignored) {
                            next = 1;
                        }
                    }
                }
            }
        }

        log("INFO", "Next user sequence resolved as " + next, serverIP, "USER_ID_SEQ");
        return next;
    }

    private String[] splitName(String fullName) {
        if (isBlank(fullName)) {
            return new String[] { null, null, null };
        }

        String[] parts = fullName.trim().split("\\s+");

        if (parts.length == 1) {
            return new String[] { parts[0], null, null };
        }

        if (parts.length == 2) {
            return new String[] { parts[0], null, parts[1] };
        }

        String first = parts[0];
        String last = parts[parts.length - 1];
        String middle = String.join(" ", Arrays.asList(Arrays.copyOfRange(parts, 1, parts.length - 1)));

        return new String[] { first, isBlank(middle) ? null : middle, last };
    }

    private OffsetTime parseOffsetTime(String raw) {
        if (isBlank(raw)) {
            return null;
        }

        String v = safeTrim(raw);

        try {
            return OffsetTime.parse(v);
        } catch (DateTimeParseException ignored) {
        }

        try {
            return LocalTime.parse(v).atOffset(ZoneOffset.UTC);
        } catch (DateTimeParseException ignored) {
        }

        try {
            return OffsetDateTime.parse(v).toOffsetTime();
        } catch (DateTimeParseException ignored) {
        }

        List<DateTimeFormatter> dateTimeFormats = Arrays.asList(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
        );

        for (DateTimeFormatter fmt : dateTimeFormats) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(v, fmt);
                return ldt.toLocalTime().atOffset(ZoneOffset.UTC);
            } catch (DateTimeParseException ignored) {
            }
        }

        List<DateTimeFormatter> dateFormats = Arrays.asList(
                DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                DateTimeFormatter.ofPattern("dd-MM-yyyy"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy"),
                DateTimeFormatter.ofPattern("yyyy/MM/dd")
        );

        for (DateTimeFormatter fmt : dateFormats) {
            try {
                LocalDate.parse(v, fmt);
                return OffsetTime.of(0, 0, 0, 0, ZoneOffset.UTC);
            } catch (DateTimeParseException ignored) {
            }
        }
        return null;
    }

    private Long parseSortableMillis(String raw) {
        if (isBlank(raw)) {
            return null;
        }

        String v = safeTrim(raw);

        try {
            return Long.valueOf(OffsetDateTime.parse(v).toInstant().toEpochMilli());
        } catch (DateTimeParseException ignored) {
        }

        try {
            return Long.valueOf(OffsetTime.parse(v).atDate(LocalDate.of(1970, 1, 1)).toInstant().toEpochMilli());
        } catch (DateTimeParseException ignored) {
        }

        try {
            return Long.valueOf(LocalDateTime.parse(v).toInstant(ZoneOffset.UTC).toEpochMilli());
        } catch (DateTimeParseException ignored) {
        }

        List<DateTimeFormatter> ldtFormats = Arrays.asList(
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")
        );

        for (DateTimeFormatter fmt : ldtFormats) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(v, fmt);
                return Long.valueOf(ldt.toInstant(ZoneOffset.UTC).toEpochMilli());
            } catch (DateTimeParseException ignored) {
            }
        }

        List<DateTimeFormatter> dateFormats = Arrays.asList(
                DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                DateTimeFormatter.ofPattern("dd-MM-yyyy"),
                DateTimeFormatter.ofPattern("dd/MM/yyyy"),
                DateTimeFormatter.ofPattern("yyyy/MM/dd")
        );

        for (DateTimeFormatter fmt : dateFormats) {
            try {
                LocalDate ld = LocalDate.parse(v, fmt);
                return Long.valueOf(ld.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli());
            } catch (DateTimeParseException ignored) {
            }
        }

        return null;
    }

    private int compareNullableLongDesc(Long left, Long right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return 1;
        }
        if (right == null) {
            return -1;
        }
        return right.compareTo(left);
    }

    private List<String> parsePipeLine(String line) {
        List<String> values = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    sb.append('"');
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == '|' && !inQuotes) {
                values.add(sb.toString().trim());
                sb.setLength(0);
            } else {
                sb.append(c);
            }
        }

        values.add(sb.toString().trim());

        for (int i = 0; i < values.size(); i++) {
            values.set(i, unquote(values.get(i)));
        }

        return values;
    }

    private String getValue(List<String> values, Map<String, Integer> headerIndex, String key) {
        Integer idx = headerIndex.get(normalizeHeader(key));
        if (idx == null || idx.intValue() < 0 || idx.intValue() >= values.size()) {
            return null;
        }
        return safeTrim(values.get(idx.intValue()));
    }

    private String normalizeHeader(String s) {
        return safeTrim(unquote(s)).replace("\uFEFF", "").toLowerCase(Locale.ROOT);
    }

    private String normalizeEmail(String s) {
        String v = safeTrim(unquote(s));
        return isBlank(v) ? null : v.toLowerCase(Locale.ROOT);
    }

    private String normalizeMobile(String s) {
        String v = safeTrim(unquote(s));
        return isBlank(v) ? null : v;
    }

    private String unquote(String s) {
        if (s == null) {
            return null;
        }
        String v = s.trim();
        if (v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")) {
            v = v.substring(1, v.length() - 1);
        }
        return v.trim();
    }

    private String stripBom(String s) {
        if (s == null) {
            return null;
        }
        return s.replace("\uFEFF", "");
    }

    private String safeTrim(String s) {
        return s == null ? null : s.trim();
    }

    private boolean isBlank(String s) {
        if (s == null) {
            return true;
        }

        String v = s.trim();

        return v.isEmpty()
                || "\\N".equalsIgnoreCase(v)
                || "NULL".equalsIgnoreCase(v);
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
            return "INFO";
        }
        String lower = s.toLowerCase(Locale.ROOT);
        return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
    }

    private static class SourceRow {
        long rowIndex;
        String fullName;
        String email;
        String mobile;
        String createdBy;
        String createdAtRaw;
        String updatedAtRaw;
        Long createdMillis;
        Long updatedMillis;
    }

    private static class TargetRow {
        UUID id;
        String userId;
        String email;
        String mobile;
        String ckycNo;
        String keyCloakUserId;
        String title;
        String firstName;
        String middleName;
        String lastName;
        String citizenship;
        String countryCode;
        String gender;
        UUID dscId;
        OffsetTime createdAt;
        OffsetTime updatedAt;
        String userType;
        Long version;
        String createdBy;
        String updatedBy;
        Boolean active;
    }
}