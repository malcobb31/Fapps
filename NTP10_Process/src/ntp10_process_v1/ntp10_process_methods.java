package ntp10_process_v1;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * ntp10_process_methods - CSV Transform and Load
 *
 * Supports:
 *   1) institution master CSVs  (headers start with fim_)
 *   2) user address CSVs        (all other headers)
 *
 * Architecture:
 *   Destination DB : 172.20.131.62:5432/ckyprodapdb  (re_mgmt.address)
 *   Lookup DB      : 172.20.131.64:5432/ckyprodwbdb  (public.country_master,
 *                                                       public.state_master)
 *   Same credentials are used for both databases.
 *
 * Performance:
 *   - ONE lookup DB connection is opened per processFile() call, before the
 *     transform loop, and closed unconditionally in the finally block.
 *   - lookupCountryName / lookupStateName NEVER open a connection themselves.
 *   - ConcurrentHashMap caches ensure each code is queried at most once per
 *     JVM lifetime of this instance; cache hits are pure in-memory operations.
 *
 * Business rules implemented:
 *   - country ISO-2 -> full name via public.country_master WHERE UPPER(iso_2)=?
 *   - state code    -> full name via public.state_master  WHERE UPPER("key")=?
 *   - same_as_correspondence_address: "on"->true, null/empty/other->false
 *   - updated_at: always current wall-clock timestamp; source CSV value ignored
 *   - metadata: fim_FI_code->etl_fi_code,
 *               fim_corres_state_others->fim_Reg_Off_state_others
 */
public class ntp10_process_methods {

    // -------------------------------------------------------------------------
    // Constants
    // -------------------------------------------------------------------------

    private static final String            processName    = "address-parser";
    private static final String            processVersion = "1.0";
    private static final boolean           debugEnabled   = true;
    private static final DateTimeFormatter logFormat      =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");
    private static final DateTimeFormatter DB_TS_FORMAT   =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Lookup DB coordinates — host .64, same credentials as destination. */
    private static final String LOOKUP_DB_URL =
            "jdbc:postgresql://172.20.131.64:5432/ckyprodwbdb";

    private static final int BATCH_SIZE = 200;

    /**
     * Destination columns in insert order.
     * Must stay aligned with buildUserAddressRecord() and
     * buildInstitutionMasterRecords().
     */
    private static final String[] DEST_COLUMNS = {
        "line1",
        "line2",
        "line3",
        "city",
        "country",
        "updated_at",
        "updated_by",
        "created_by",
        "pincode",
        "state",
        "address_type",
        "same_as_correspondence_address",
        "metadata"
    };

    // -------------------------------------------------------------------------
    // Per-instance caches — survive across processFile() calls on the same
    // instance so that repeated codes are never re-queried across files.
    // -------------------------------------------------------------------------

    private final Map<String, String> countryCache = new ConcurrentHashMap<>();
    private final Map<String, String> stateCache   = new ConcurrentHashMap<>();

    /**
     * Shared lookup DB connection for the duration of one processFile() call.
     * Opened once before the transform loop; nulled and closed in finally.
     * Must never be referenced outside processFile() or after its finally runs.
     */
    private Connection lookupConn = null;

    // -------------------------------------------------------------------------
    // Logging
    // -------------------------------------------------------------------------

    private void log(String level, String message, String serverIP, String evtType) {
        if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) return;

        String timestamp = LocalDateTime.now().format(logFormat);

        StringBuilder lb = new StringBuilder(512);
        lb.append("Time:").append(timestamp)
          .append(" [").append(capitalize(level)).append("] ")
          .append("Process_Name: ").append(processName).append(" ");

        if (serverIP != null && !serverIP.isEmpty()) {
            lb.append("Server_IP: ").append(serverIP).append(" ");
        }

        lb.append("Process_Version: ").append(processVersion)
          .append(" Log_Level: EVT_LOG")
          .append(" EVT_Type: ").append(evtType != null ? evtType : "NA")
          .append(" Description: ").append(message != null ? message : "");

        System.out.println(lb.toString().trim());
    }

    private String capitalize(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
    }

    // =========================================================================
    // PUBLIC ENTRY POINTS
    // =========================================================================

    /**
     * Backward-compatible 5-argument overload.
     *
     * Called by the existing scheduler / ntp10_process_main.  Derives the
     * lookup DB URL from the fixed constant (host .64) and reuses the same
     * credentials supplied for the destination DB (host .62), since both
     * databases share the same user and password.
     *
     * @param pgURL       destination DB JDBC URL  (jdbc:postgresql://172.20.131.62:5432/ckyprodapdb)
     * @param pgUser      destination DB user       (ckyc_prod_rw)
     * @param pgPass      destination DB password   (ckyc_prod_rw@321)
     * @param csvFilePath absolute path to the CSV file
     * @param destTable   fully-qualified destination table (e.g. re_mgmt.address)
     * @return true on success, false on any failure
     */
    public boolean processFile(String pgURL,       String pgUser, String pgPass,
                               String csvFilePath, String destTable) {

        return processFile(
                pgURL,       pgUser, pgPass,
                LOOKUP_DB_URL, pgUser, pgPass,   // same credentials, different host
                csvFilePath, destTable);
    }

    /**
     * Full 8-argument overload — explicit lookup DB credentials.
     *
     * Use this when the lookup DB credentials differ from the destination DB,
     * or for testing against a non-production lookup DB.
     *
     * @param pgURL        destination DB JDBC URL
     * @param pgUser       destination DB user
     * @param pgPass       destination DB password
     * @param lookupDbURL  lookup DB JDBC URL  (public.country_master / public.state_master)
     * @param lookupDbUser lookup DB user
     * @param lookupDbPass lookup DB password
     * @param csvFilePath  absolute path to the CSV file
     * @param destTable    fully-qualified destination table (e.g. re_mgmt.address)
     * @return true on success, false on any failure
     */
    public boolean processFile(String pgURL,       String pgUser,       String pgPass,
                               String lookupDbURL, String lookupDbUser, String lookupDbPass,
                               String csvFilePath, String destTable) {

        boolean status = false;

        try {
            log("INFO",
                "===== START | File: " + csvFilePath + " | Destination: " + destTable + " =====",
                null, "FILE_PROCESS");

            // ------------------------------------------------------------------
            // Open ONE shared lookup connection for this entire file.
            // Closed unconditionally in the finally block below.
            // ------------------------------------------------------------------
            lookupConn = new PostgreSQLConnection().getConnection(
                    lookupDbURL, lookupDbUser, lookupDbPass);

            if (lookupConn == null) {
                log("ERROR",
                    "Unable to establish lookup DB connection | URL: " + lookupDbURL,
                    null, "DB_CONNECT");
                return false;
            }
            log("INFO", "Lookup DB connection opened | URL: " + lookupDbURL, null, "DB_CONNECT");

            // ------------------------------------------------------------------
            // STEP 1 - Validate destination table exists
            // ------------------------------------------------------------------
            if (!validateTableExists(destTable, pgURL, pgUser, pgPass)) {
                log("ERROR", "Destination table not found: " + destTable, null, "VALIDATION");
                return false;
            }

            // ------------------------------------------------------------------
            // STEP 2 - Validate required destination columns exist
            // ------------------------------------------------------------------
            HashSet<String> destCols = getTableColumns(destTable, pgURL, pgUser, pgPass);

            for (String col : DEST_COLUMNS) {
                if (!destCols.contains(col.toLowerCase())) {
                    log("ERROR",
                        "Required column missing in destination table | Column: " + col +
                        " | Table: " + destTable,
                        null, "VALIDATION");
                    return false;
                }
            }

            log("INFO",
                "Destination table validation passed | Table: " + destTable +
                " | Required columns verified: " + DEST_COLUMNS.length,
                null, "VALIDATION");

            // ------------------------------------------------------------------
            // STEP 3 - Read CSV
            // ------------------------------------------------------------------
            log("INFO", "Reading CSV file: " + csvFilePath, null, "FILE_PROCESS");

            String[] headers = new String[1];
            HashMap<String, String[]> rawData = readCSV(csvFilePath, headers);

            if (rawData == null || rawData.isEmpty()) {
                log("WARN",
                    "No data rows found in CSV file: " + csvFilePath + " | Records returned: 0",
                    null, "FILE_PROCESS");
                return false;
            }

            log("INFO",
                "CSV read complete | File: " + csvFilePath + " | Total rows: " + rawData.size(),
                null, "FILE_PROCESS");

            Map<String, Integer> headerIndex = buildHeaderIndex(headers[0].split("\\|", -1));

            log("DEBUG",
                "Header index map built | Column count: " + headerIndex.size(),
                null, "FILE_PROCESS");

            // ------------------------------------------------------------------
            // STEP 4 - Transform rows
            // ------------------------------------------------------------------
            HashMap<String, String[]> transformedData = new LinkedHashMap<>();

            String firstHeader =
                headers[0].split("\\|", -1)[0].replace("\"", "").trim().toLowerCase();
            boolean isInstitutionMaster = firstHeader.startsWith("fim_");

            log("INFO",
                "Source type detected | First header: " + firstHeader +
                " | Mode: " + (isInstitutionMaster ? "INSTITUTION_MASTER" : "USER_ADDRESS"),
                null, "FILE_PROCESS");

            int transformSkipped = 0;

            for (Map.Entry<String, String[]> entry : rawData.entrySet()) {
                try {
                    if (isInstitutionMaster) {
                        List<String[]> records =
                            buildInstitutionMasterRecords(entry.getValue(), headerIndex);
                        int dupIndex = 0;
                        for (String[] rec : records) {
                            transformedData.put(entry.getKey() + "_" + (dupIndex++), rec);
                        }
                    } else {
                        String[] transformed =
                            buildUserAddressRecord(entry.getValue(), headerIndex);
                        transformedData.put(entry.getKey(), transformed);
                    }
                } catch (Exception ex) {
                    transformSkipped++;
                    log("WARN",
                        "Skipping row due to transform error | Row: " + entry.getKey() +
                        " | Error: " + ex.getMessage(),
                        null, "FILE_PROCESS");
                }
            }

            log("INFO",
                "Transform complete | Rows ready: " + transformedData.size() +
                " | Rows skipped: " + transformSkipped,
                null, "FILE_PROCESS");

            if (transformedData.isEmpty()) {
                log("ERROR",
                    "No rows available for insert after transform | File: " + csvFilePath,
                    null, "FILE_PROCESS");
                return false;
            }

            // ------------------------------------------------------------------
            // STEP 5 - Batch insert into destination table
            // ------------------------------------------------------------------
            status = insertDataToDestination(transformedData, destTable, pgURL, pgUser, pgPass);

            if (!status) {
                log("ERROR",
                    "Insert failed | File: " + csvFilePath + " | Table: " + destTable,
                    null, "DATA_INSERT");
            } else {
                log("INFO",
                    "===== COMPLETE | File: " + csvFilePath + " | Table: " + destTable + " =====",
                    null, "FILE_PROCESS");
            }

        } catch (Exception ex) {
            log("ERROR",
                "Unhandled exception in processFile | File: " + csvFilePath +
                " | Error: " + ex.getMessage(),
                null, "APP_ERROR");
            status = false;

        } finally {
            // Close shared lookup connection unconditionally — runs even on exception.
            closeLookupConnection();
        }

        return status;
    }

    // =========================================================================
    // CSV READING
    // =========================================================================

    private HashMap<String, String[]> readCSV(String filePath, String[] headerOut) {

        HashMap<String, String[]> data = new LinkedHashMap<>();

        try (
            Reader reader = Files.newBufferedReader(
                    Paths.get(filePath),
                    StandardCharsets.UTF_8);

            CSVParser parser = CSVFormat.DEFAULT
                    .builder()
                    .setDelimiter('|')
                    .setQuote('"')
                    .setEscape('\\')
                    .setHeader()
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(reader)
        ) {

            Map<String, Integer> headerMap = parser.getHeaderMap();

            String[] headers = headerMap.keySet()
                    .stream()
                    .map(h -> h.replace("\"", "").trim().toLowerCase())
                    .toArray(String[]::new);

            headerOut[0] = String.join("|", headers);

            int rowNum = 0;

            for (CSVRecord record : parser) {

                String[] row = new String[headers.length];

                for (int i = 0; i < headers.length; i++) {

                    String val = record.get(i);

                    if (val != null) {
                        val = val.trim();

                        if (val.isEmpty()
                                || "\\N".equalsIgnoreCase(val)
                                || "NULL".equalsIgnoreCase(val)) {
                            val = null;
                        }
                    }

                    row[i] = val;
                }

                data.put(String.valueOf(rowNum++), row);
            }

            log("INFO",
                "CSV read complete | File: " + filePath +
                " | Total rows: " + data.size(),
                null,
                "FILE_PROCESS");

        } catch (Exception ex) {

            log("ERROR",
                "Failed to read CSV file: " + filePath +
                " | Error: " + ex.getMessage(),
                null,
                "FILE_PROCESS");

            return null;
        }

        return data;
    }
    
    // =========================================================================
    // TRANSFORM
    // =========================================================================

    private Map<String, Integer> buildHeaderIndex(String[] headers) {
        Map<String, Integer> index = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            String h = headers[i].trim().toLowerCase();
            if (!h.isEmpty()) {
                index.put(h, i);
            }
        }
        return index;
    }

    /**
     * Builds one destination record for a user-address CSV row.
     *
     * Country and state lookups use this.lookupConn (shared, opened once per
     * file) via lookupCountryName() / lookupStateName().  No new connection is
     * opened inside this method.
     *
     * updated_at is always the current timestamp; any source CSV timestamp
     * field is intentionally ignored.
     */
    private String[] buildUserAddressRecord(String[] rawRow,
                                            Map<String, Integer> headerIdx) {

        String[] record = new String[DEST_COLUMNS.length];

        // line1 .. line3
        record[0] = trimToNull(getField(rawRow, headerIdx, "address1"));
        record[1] = trimToNull(getField(rawRow, headerIdx, "address2"));
        record[2] = buildLine3(
                        trimToNull(getField(rawRow, headerIdx, "address3")),
                        trimToNull(getField(rawRow, headerIdx, "address4")));

        // city
        record[3] = trimToNull(getField(rawRow, headerIdx, "city"));

        // country: ISO-2 -> full name; cache-backed, uses shared lookupConn
        record[4] = lookupCountryName(
                        trimToNull(getField(rawRow, headerIdx, "country")));

        // updated_at: always current timestamp (source value ignored)
        record[5] = currentDbTimestamp();

        // updated_by / created_by
        record[6] = trimToNull(getField(rawRow, headerIdx, "modified_by"));
        record[7] = null;

        // pincode
        record[8] = trimToNull(getField(rawRow, headerIdx, "pincode"));

        // state: code -> full name; cache-backed, uses shared lookupConn
        record[9] = lookupStateName(
                        trimToNull(getField(rawRow, headerIdx, "state_code")));

        // address_type: not applicable for user-address source
        record[10] = null;

        // same_as_correspondence_address: no flag on user-address source -> false
        record[11] = "false";

        // metadata
        LinkedHashMap<String, String> metaMap = new LinkedHashMap<>();
        metaMap.put("email_id",     "email_id");
        metaMap.put("fax",          "fax");
        metaMap.put("phone1",       "phone1");
        metaMap.put("phone2",       "phone2");
        metaMap.put("phone3",       "phone3");
        metaMap.put("user_id",      "user_id");
        metaMap.put("village_code", "village_code");

        record[12] = buildMetadataJson(rawRow, headerIdx, metaMap);

        return record;
    }

    private String buildLine3(String address3, String address4) {
        address3 = trimToNull(address3);
        address4 = trimToNull(address4);
        if (address3 == null && address4 == null) return null;
        if (address3 == null) return address4;
        if (address4 == null) return address3;
        return address3 + " " + address4;
    }

    /**
     * Builds two destination records (REGISTERED + CORRESPONDENCE) from one
     * institution-master CSV row.
     *
     * Business rules applied:
     *   same_as_correspondence_address:
     *       fim_corres_perm_flag == "on"  -> "true"
     *       null / empty / other          -> "false"
     *
     *   metadata mappings:
     *       fim_FI_code             -> etl_fi_code
     *       fim_corres_state_others -> fim_Reg_Off_state_others
     *
     *   updated_at: always current timestamp; source CSV value ignored.
     *
     * Country and state lookups use this.lookupConn (shared) — no new
     * connection is opened here.
     */
    private List<String[]> buildInstitutionMasterRecords(String[] rawRow,
                                                          Map<String, Integer> headerIdx) {

        List<String[]> out = new ArrayList<>(2);

        String createdBy  = trimToNull(getField(rawRow, headerIdx, "fim_created_by"));
        String permFlag   = trimToNull(getField(rawRow, headerIdx, "fim_corres_perm_flag"));
        String sameAsCorr = deriveSameAsCorrespondenceAddress(permFlag);
        String nowTs      = currentDbTimestamp();

        LinkedHashMap<String, String> metaMap = new LinkedHashMap<>();
        metaMap.put("fim_FI_code",            "etl_fi_code");
        metaMap.put("fim_corres_state_others", "fim_Reg_Off_state_others");

        // ---- REGISTERED ----
        String[] registered = new String[DEST_COLUMNS.length];

        registered[0]  = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_addr1"));
        registered[1]  = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_addr2"));
        registered[2]  = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_addr3"));
        registered[3]  = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_city"));
        registered[4]  = lookupCountryName(
                             trimToNull(getField(rawRow, headerIdx, "fim_reg_off_country")));
        registered[5]  = nowTs;
        registered[6]  = createdBy;
        registered[7]  = createdBy;
        registered[8]  = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_pin"));
        registered[9]  = lookupStateName(
                             trimToNull(getField(rawRow, headerIdx, "fim_reg_off_state")));
        registered[10] = "REGISTERED";
        registered[11] = sameAsCorr;
        registered[12] = buildMetadataJson(rawRow, headerIdx, metaMap);

        out.add(registered);

        // ---- CORRESPONDENCE ----
        String[] correspondence = new String[DEST_COLUMNS.length];

        correspondence[0]  = trimToNull(getField(rawRow, headerIdx, "fim_corres_addr1"));
        correspondence[1]  = trimToNull(getField(rawRow, headerIdx, "fim_corres_addr2"));
        correspondence[2]  = trimToNull(getField(rawRow, headerIdx, "fim_corres_addr3"));
        correspondence[3]  = trimToNull(getField(rawRow, headerIdx, "fim_corres_city"));
        correspondence[4]  = lookupCountryName(
                                 trimToNull(getField(rawRow, headerIdx, "fim_corres_country")));
        correspondence[5]  = nowTs;
        correspondence[6]  = createdBy;
        correspondence[7]  = createdBy;
        correspondence[8]  = trimToNull(getField(rawRow, headerIdx, "fim_corres_pin"));
        correspondence[9]  = lookupStateName(
                                 trimToNull(getField(rawRow, headerIdx, "fim_corres_state")));
        correspondence[10] = "CORRESPONDENCE";
        correspondence[11] = sameAsCorr;
        correspondence[12] = buildMetadataJson(rawRow, headerIdx, metaMap);

        out.add(correspondence);

        return out;
    }

    private String buildMetadataJson(String[] rawRow,
                                     Map<String, Integer> headerIdx,
                                     Map<String, String> sourceToJsonKeyMap) {

        StringBuilder json = new StringBuilder("{");
        boolean first = true;

        for (Map.Entry<String, String> entry : sourceToJsonKeyMap.entrySet()) {
            String val = trimToNull(getField(rawRow, headerIdx, entry.getKey()));
            if (val == null) continue;

            if (!first) json.append(",");
            json.append("\"").append(escapeJson(entry.getValue()))
                .append("\":\"").append(escapeJson(val)).append("\"");
            first = false;
        }

        json.append("}");
        return json.toString();
    }

    private String escapeJson(String val) {
        if (val == null) return "";
        return val.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // =========================================================================
    // LOOKUP METHODS
    //
    // Contract: both methods rely exclusively on this.lookupConn, which is
    // opened once in processFile() and never touched here.  They prepare and
    // close their own PreparedStatement and ResultSet in a finally block but
    // leave the connection open for the next lookup call.
    //
    // Cache contract: once a key is stored, it is never evicted within the
    // JVM lifetime of this ntp10_process_methods instance, so "IN -> India"
    // will be looked up only once no matter how many files are processed.
    // =========================================================================

    /**
     * Resolves an ISO-2 country code to its full country name.
     *
     * SQL: SELECT country FROM public.country_master WHERE UPPER(iso_2) = ?
     *
     * @param iso2 raw value from CSV (may be null or blank)
     * @return full country name; falls back to the uppercased code if no row
     *         is found; returns null if the input is null or blank
     */
    private String lookupCountryName(String iso2) {
        if (iso2 == null) return null;

        String key = iso2.trim().toUpperCase();
        if (key.isEmpty()) return null;

        String cached = countryCache.get(key);
        if (cached != null) {
            log("DEBUG", "Country cache hit | " + key + " -> " + cached, null, "LOOKUP");
            return cached;
        }

        if (lookupConn == null) {
            log("WARN",
                "lookupConn is null during country lookup | Code: " + key +
                " | Returning raw code",
                null, "LOOKUP");
            countryCache.put(key, key);
            return key;
        }

        String resolved = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = lookupConn.prepareStatement(
                    "SELECT country FROM public.country_master WHERE UPPER(iso_2) = ?");
            ps.setString(1, key);
            rs = ps.executeQuery();
            if (rs.next()) {
                resolved = trimToNull(rs.getString(1));
            }
            log("DEBUG", "Country lookup DB hit | " + key + " -> " + resolved, null, "LOOKUP");
        } catch (Exception ex) {
            log("WARN",
                "Country lookup failed | Code: " + key + " | Error: " + ex.getMessage(),
                null, "LOOKUP");
        } finally {
            closeQuietly(rs);
            closeQuietly(ps);
        }

        if (resolved == null) {
            resolved = key;
            log("DEBUG", "Country not found in master | Using raw code: " + key, null, "LOOKUP");
        }

        countryCache.put(key, resolved);
        return resolved;
    }

    /**
     * Resolves a state code to its full state name.
     *
     * SQL: SELECT value FROM public.state_master WHERE UPPER("key") = ?
     *
     * @param stateKey raw value from CSV (may be null or blank)
     * @return full state name; falls back to the uppercased code if no row
     *         is found; returns null if the input is null or blank
     */
    private String lookupStateName(String stateKey) {
        if (stateKey == null) return null;

        String key = stateKey.trim().toUpperCase();
        if (key.isEmpty()) return null;

        String cached = stateCache.get(key);
        if (cached != null) {
            log("DEBUG", "State cache hit | " + key + " -> " + cached, null, "LOOKUP");
            return cached;
        }

        if (lookupConn == null) {
            log("WARN",
                "lookupConn is null during state lookup | Code: " + key +
                " | Returning raw code",
                null, "LOOKUP");
            stateCache.put(key, key);
            return key;
        }

        String resolved = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            ps = lookupConn.prepareStatement(
                    "SELECT value FROM public.state_master WHERE UPPER(\"key\") = ?");
            ps.setString(1, key);
            rs = ps.executeQuery();
            if (rs.next()) {
                resolved = trimToNull(rs.getString(1));
            }
            log("DEBUG", "State lookup DB hit | " + key + " -> " + resolved, null, "LOOKUP");
        } catch (Exception ex) {
            log("WARN",
                "State lookup failed | Code: " + key + " | Error: " + ex.getMessage(),
                null, "LOOKUP");
        } finally {
            closeQuietly(rs);
            closeQuietly(ps);
        }

        if (resolved == null) {
            resolved = key;
            log("DEBUG", "State not found in master | Using raw code: " + key, null, "LOOKUP");
        }

        stateCache.put(key, resolved);
        return resolved;
    }

    /**
     * Derives same_as_correspondence_address from fim_corres_perm_flag.
     *   "on"  (case-insensitive) -> "true"
     *   null / empty / any other -> "false"
     */
    private String deriveSameAsCorrespondenceAddress(String permFlag) {
        return (permFlag != null && "on".equalsIgnoreCase(permFlag.trim()))
               ? "true"
               : "false";
    }

    /**
     * Returns the current wall-clock time formatted for PostgreSQL TIMESTAMP.
     * This is the value written to updated_at.  Source CSV timestamps are
     * never used.
     */
    private String currentDbTimestamp() {
        return LocalDateTime.now().format(DB_TS_FORMAT);
    }

    // =========================================================================
    // INSERT
    // =========================================================================

    private boolean insertDataToDestination(HashMap<String, String[]> transformedData,
                                            String destTable,
                                            String dbURL, String dbUser, String dbPass) {

        Connection        conn   = null;
        PreparedStatement ps     = null;
        boolean           status = false;

        int  rowCount    = 0;
        int  inserted    = 0;
        int  skipped     = 0;
        int  unknown     = 0;
        int  totalRows   = transformedData.size();
        long beforeCount = -1;
        long afterCount  = -1;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            if (conn == null) {
                log("ERROR",
                    "Unable to establish destination DB connection for insert | Table: " + destTable,
                    null, "DATA_INSERT");
                return false;
            }

            HashMap<String, String> columnTypes =
                getDestinationColumnTypes(destTable, DEST_COLUMNS, conn);

            if (columnTypes == null || columnTypes.isEmpty()) {
                log("ERROR",
                    "Could not fetch destination column types | Table: " + destTable,
                    null, "DATA_INSERT");
                return false;
            }

            try (PreparedStatement cntPs =
                        conn.prepareStatement("SELECT COUNT(*) FROM " + destTable);
                 ResultSet cntRs = cntPs.executeQuery()) {
                if (cntRs.next()) beforeCount = cntRs.getLong(1);
            }

            StringBuilder insertSQL =
                new StringBuilder("INSERT INTO ").append(destTable).append(" (id, ");

            for (int i = 0; i < DEST_COLUMNS.length; i++) {
                insertSQL.append(DEST_COLUMNS[i]);
                if (i < DEST_COLUMNS.length - 1) insertSQL.append(", ");
            }

            insertSQL.append(") VALUES (gen_random_uuid(), ");

            for (int i = 0; i < DEST_COLUMNS.length; i++) {
                insertSQL.append("?");
                if (i < DEST_COLUMNS.length - 1) insertSQL.append(", ");
            }

            insertSQL.append(") ON CONFLICT DO NOTHING");

            log("DEBUG",
                "Prepared insert query | Table: " + destTable + " | Query: " + insertSQL,
                null, "DATA_INSERT");

            conn.setAutoCommit(false);
            ps = conn.prepareStatement(insertSQL.toString());

            for (Map.Entry<String, String[]> entry : transformedData.entrySet()) {
                String[] record = entry.getValue();

                for (int i = 0; i < DEST_COLUMNS.length; i++) {
                    String colName = DEST_COLUMNS[i].toLowerCase();
                    String colType = columnTypes.get(colName);
                    String value   = (i < record.length) ? record[i] : null;
                    setParameterByType(ps, i + 1, value, colType, colName);
                }

                ps.addBatch();
                rowCount++;

                if (rowCount % BATCH_SIZE == 0) {
                    int[] results = ps.executeBatch();
                    for (int r : results) {
                        if      (r == 1)                                      inserted++;
                        else if (r == 0)                                      skipped++;
                        else if (r == java.sql.Statement.SUCCESS_NO_INFO)     unknown++;
                    }
                    log("DEBUG",
                        "Mid-batch executed | Table: " + destTable +
                        " | Rows so far: " + rowCount + " | Total: " + totalRows,
                        null, "DATA_INSERT");
                }
            }

            // Flush remaining rows in the last partial batch
            int[] results = ps.executeBatch();
            for (int r : results) {
                if      (r == 1)                                  inserted++;
                else if (r == 0)                                  skipped++;
                else if (r == java.sql.Statement.SUCCESS_NO_INFO) unknown++;
            }

            conn.commit();

            try (PreparedStatement cntPs =
                        conn.prepareStatement("SELECT COUNT(*) FROM " + destTable);
                 ResultSet cntRs = cntPs.executeQuery()) {
                if (cntRs.next()) afterCount = cntRs.getLong(1);
            }

            status = true;

            log("INFO",
                "Insert Summary | Table: " + destTable +
                " | Total Rows (CSV): "   + totalRows  +
                " | Inserted (new): "     + inserted   +
                " | Skipped (conflict): " + skipped    +
                " | Unknown (driver): "   + unknown    +
                " | Rows Before: "        + beforeCount +
                " | Rows After: "         + afterCount  +
                " | Net Added: "          + (afterCount - beforeCount),
                null, "DATA_INSERT");

        } catch (Exception ex) {
            log("ERROR",
                "Insert failed | Table: " + destTable +
                " | Rows processed before failure: " + rowCount +
                " | Error: " + ex.getMessage(),
                null, "DATA_INSERT");

            try {
                if (conn != null) {
                    conn.rollback();
                    log("INFO",
                        "Transaction rolled back successfully | Table: " + destTable,
                        null, "DATA_INSERT");
                }
            } catch (Exception rollbackEx) {
                log("ERROR",
                    "Rollback failed | Table: " + destTable +
                    " | Error: " + rollbackEx.getMessage(),
                    null, "DATA_INSERT");
            }

            status = false;

        } finally {
            closeQuietly(ps);
            closeQuietly(conn);
        }

        return status;
    }

    // =========================================================================
    // DATABASE VALIDATION HELPERS
    // Each opens its own short-lived connection to the destination DB.
    // Called once per file (not per row) so the cost is acceptable.
    // =========================================================================

    private boolean validateTableExists(String tableName, String dbURL,
                                        String dbUser, String dbPass) {

        Connection conn   = null;
        boolean    exists = false;

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            String schemaName     = "public";
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.", 2);
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            log("DEBUG",
                "Checking table existence | Schema: " + schemaName +
                " | Table: " + actualTableName,
                null, "VALIDATION");

            String sql =
                "SELECT 1 FROM information_schema.tables " +
                "WHERE table_schema = ? AND table_name = ?";

            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, schemaName);
                ps.setString(2, actualTableName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        exists = true;
                        log("INFO",
                            "Table found | Schema: " + schemaName +
                            " | Table: " + actualTableName,
                            null, "VALIDATION");
                    } else {
                        log("WARN",
                            "Table not found | Schema: " + schemaName +
                            " | Table: " + actualTableName,
                            null, "VALIDATION");
                    }
                }
            }

        } catch (Exception ex) {
            log("ERROR",
                "Exception during table existence check | Table: " + tableName +
                " | Error: " + ex.getMessage(),
                null, "VALIDATION");
        } finally {
            closeQuietly(conn);
        }

        return exists;
    }

    private HashSet<String> getTableColumns(String tableName, String dbURL,
                                            String dbUser, String dbPass) {

        Connection      conn    = null;
        HashSet<String> columns = new HashSet<>();

        try {
            conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

            String schemaName     = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.", 2);
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            log("DEBUG",
                "Fetching column list | Schema: " + schemaName +
                " | Table: " + actualTableName,
                null, "VALIDATION");

            DatabaseMetaData meta = conn.getMetaData();
            try (ResultSet rs = meta.getColumns(null, schemaName, actualTableName, null)) {
                while (rs.next()) {
                    columns.add(rs.getString("COLUMN_NAME").toLowerCase());
                }
            }

            log("INFO",
                "Column list fetched | Table: " + tableName +
                " | Total columns: " + columns.size(),
                null, "VALIDATION");

        } catch (Exception ex) {
            log("ERROR",
                "Exception fetching column list | Table: " + tableName +
                " | Error: " + ex.getMessage(),
                null, "VALIDATION");
        } finally {
            closeQuietly(conn);
        }

        return columns;
    }

    private HashMap<String, String> getDestinationColumnTypes(String tableName,
                                                               String[] columns,
                                                               Connection conn) {

        HashMap<String, String> columnTypes = new HashMap<>();

        try {
            DatabaseMetaData meta = conn.getMetaData();

            String schemaName     = null;
            String actualTableName = tableName;

            if (tableName.contains(".")) {
                String[] parts = tableName.split("\\.", 2);
                schemaName      = parts[0].toLowerCase();
                actualTableName = parts[1].toLowerCase();
            } else {
                actualTableName = tableName.toLowerCase();
            }

            for (String col : columns) {
                try (ResultSet rs =
                        meta.getColumns(null, schemaName, actualTableName, col.toLowerCase())) {
                    if (rs.next()) {
                        String type = rs.getString("TYPE_NAME");
                        columnTypes.put(col.toLowerCase(), type);
                        log("DEBUG",
                            "Column type resolved | Column: " + col +
                            " | Type: " + type + " | Table: " + tableName,
                            null, "VALIDATION");
                    }
                }
            }

        } catch (Exception ex) {
            log("ERROR",
                "Failed to fetch destination column types | Table: " + tableName +
                " | Error: " + ex.getMessage(),
                null, "VALIDATION");
            return null;
        }

        return columnTypes;
    }

    // =========================================================================
    // PREPARED STATEMENT HELPER
    // =========================================================================

    private void setParameterByType(PreparedStatement ps, int paramIndex,
                                    String value, String columnType, String columnName)
            throws Exception {

        if (value == null) {
            if (columnType != null) {
                String t = columnType.toLowerCase();
                if      (t.contains("int")       || t.contains("serial"))   ps.setNull(paramIndex, java.sql.Types.BIGINT);
                else if (t.contains("timestamp")  || t.contains("date") ||
                         t.contains("time"))                                 ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
                else if (t.contains("numeric")    || t.contains("decimal") ||
                         t.contains("float")      || t.contains("double"))  ps.setNull(paramIndex, java.sql.Types.NUMERIC);
                else if (t.contains("bool"))                                 ps.setNull(paramIndex, java.sql.Types.BOOLEAN);
                else                                                         ps.setNull(paramIndex, java.sql.Types.VARCHAR);
            } else {
                ps.setNull(paramIndex, java.sql.Types.VARCHAR);
            }
            return;
        }

        if (value.trim().isEmpty()) {
            if (columnType != null) {
                String t = columnType.toLowerCase();
                if (t.contains("char") || t.contains("text") || t.contains("json")) {
                    ps.setString(paramIndex, value);
                    return;
                }
                if      (t.contains("int")       || t.contains("serial"))   ps.setNull(paramIndex, java.sql.Types.BIGINT);
                else if (t.contains("timestamp")  || t.contains("date") ||
                         t.contains("time"))                                 ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
                else if (t.contains("numeric")    || t.contains("decimal") ||
                         t.contains("float")      || t.contains("double"))  ps.setNull(paramIndex, java.sql.Types.NUMERIC);
                else if (t.contains("bool"))                                 ps.setNull(paramIndex, java.sql.Types.BOOLEAN);
                else                                                         ps.setString(paramIndex, value);
            } else {
                ps.setString(paramIndex, value);
            }
            return;
        }

        if (columnType != null) {
            String t = columnType.toLowerCase();
            try {
                if (t.contains("bigint") || t.contains("int8") || t.contains("bigserial")) {
                    ps.setLong(paramIndex, Long.parseLong(value.trim()));
                } else if (t.contains("int") || t.contains("serial")) {
                    ps.setInt(paramIndex, Integer.parseInt(value.trim()));
                } else if (t.contains("numeric") || t.contains("decimal")) {
                    ps.setBigDecimal(paramIndex, new java.math.BigDecimal(value.trim()));
                } else if (t.contains("float") || t.contains("double") || t.contains("real")) {
                    ps.setDouble(paramIndex, Double.parseDouble(value.trim()));
                } else if (t.contains("bool")) {
                    ps.setBoolean(paramIndex, Boolean.parseBoolean(value.trim()));
                } else if (t.contains("jsonb")) {
                    ps.setObject(paramIndex, value, java.sql.Types.OTHER);
                } else if (t.contains("timestamp")) {
                    try {
                        ps.setTimestamp(paramIndex, java.sql.Timestamp.valueOf(value.trim()));
                    } catch (IllegalArgumentException e) {
                        log("ERROR",
                            "Timestamp parse failure | Column: " + columnName +
                            " | Expected type: " + columnType +
                            " | Received value: " + value +
                            " | Expected format: YYYY-MM-DD HH:MM:SS",
                            null, "DATA_INSERT");
                        throw new Exception("Data type mismatch: Column '" + columnName +
                            "' expects timestamp but got '" + value + "'");
                    }
                } else if (t.contains("date")) {
                    try {
                        ps.setDate(paramIndex, java.sql.Date.valueOf(value.trim()));
                    } catch (IllegalArgumentException e) {
                        log("ERROR",
                            "Date parse failure | Column: " + columnName +
                            " | Expected type: " + columnType +
                            " | Received value: " + value,
                            null, "DATA_INSERT");
                        throw new Exception("Data type mismatch: Column '" + columnName +
                            "' expects date but got '" + value + "'");
                    }
                } else if (t.contains("time")) {
                    try {
                        ps.setTime(paramIndex, java.sql.Time.valueOf(value.trim()));
                    } catch (IllegalArgumentException e) {
                        log("ERROR",
                            "Time parse failure | Column: " + columnName +
                            " | Expected type: " + columnType +
                            " | Received value: " + value,
                            null, "DATA_INSERT");
                        throw new Exception("Data type mismatch: Column '" + columnName +
                            "' expects time but got '" + value + "'");
                    }
                } else {
                    ps.setString(paramIndex, value);
                }
            } catch (NumberFormatException e) {
                log("ERROR",
                    "Number format parse failure | Column: " + columnName +
                    " | Expected type: " + columnType +
                    " | Received value: " + value,
                    null, "DATA_INSERT");
                throw new Exception("Data type mismatch: Column '" + columnName +
                    "' expects " + columnType + " but got '" + value + "'");
            }
        } else {
            ps.setString(paramIndex, value);
        }
    }

    // =========================================================================
    // RESOURCE MANAGEMENT
    // =========================================================================

    /**
     * Closes and nulls this.lookupConn.
     * Called unconditionally from the finally block in processFile().
     * Safe to call even if lookupConn was never successfully opened.
     */
    private void closeLookupConnection() {
        if (lookupConn != null) {
            try {
                lookupConn.close();
                log("INFO", "Lookup DB connection closed", null, "DB_CONNECT");
            } catch (Exception e) {
                log("ERROR",
                    "Failed to close lookup DB connection | Error: " + e.getMessage(),
                    null, "DB_CONNECT");
            } finally {
                lookupConn = null;
            }
        }
    }

    /** Silently closes any AutoCloseable (Connection, PreparedStatement, ResultSet). */
    private void closeQuietly(AutoCloseable c) {
        if (c != null) {
            try { c.close(); } catch (Exception ignored) {}
        }
    }

    // =========================================================================
    // UTILITY HELPERS
    // =========================================================================

    private String getField(String[] row, Map<String, Integer> headerIdx, String colName) {
        Integer idx = headerIdx.get(colName.toLowerCase());
        if (idx == null || idx >= row.length) return null;
        return row[idx];
    }

    private String trimToNull(String val) {
        if (val == null) return null;
        String trimmed = val.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}