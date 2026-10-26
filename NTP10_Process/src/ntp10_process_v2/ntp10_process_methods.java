package ntp10_process_v2;

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
 * Supports: 1) institution master CSVs (headers start with fim_) 2) user
 * address CSVs (all other headers)
 *
 * Architecture: Destination DB : 172.20.131.62:5432/ckyprodapdb
 * (re_mgmt.address) Lookup DB : 172.20.131.64:5432/ckyprodwbdb
 * (public.country_master, public.state_master) Same credentials are used for
 * both databases.
 *
 * Performance: - ONE lookup DB connection is opened per processFile() call,
 * before the transform loop, and closed unconditionally in the finally block. -
 * lookupCountryName / lookupStateName NEVER open a connection themselves. -
 * ConcurrentHashMap caches ensure each code is queried at most once per JVM
 * lifetime of this instance; cache hits are pure in-memory operations.
 *
 * Business rules implemented: - country ISO-2 -> full name via
 * public.country_master WHERE UPPER(iso_2)=? - state code -> full name via
 * public.state_master WHERE UPPER("key")=? - same_as_correspondence_address:
 * "on"->true, null/empty/other->false - updated_at: always current wall-clock
 * timestamp; source CSV value ignored - active: forced true for all inserted
 * rows - metadata: fim_FI_code->etl_fi_code,
 * fim_corres_state_others->fim_Reg_Off_state_others
 */
public class ntp10_process_methods {

	private enum CsvSourceType {
		INSTITUTION_MASTER, USER_ADDRESS
	}

	// -------------------------------------------------------------------------
	// Constants
	// -------------------------------------------------------------------------

	private static final String processName = "address-parser";
	private static final String processVersion = "1.0";
	private static final boolean debugEnabled = true;
	private static final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss SSS");
	private static final DateTimeFormatter DB_TS_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	/** Lookup DB coordinates — host .64, same credentials as destination. */
	private static final String LOOKUP_DB_URL = "jdbc:postgresql://172.20.131.64:5432/ckyprodwbdb";

	private static final int BATCH_SIZE = 200;

	/**
	 * Destination columns in insert order. Must stay aligned with
	 * buildUserAddressRecord() and buildInstitutionMasterRecords().
	 */
	private static final String[] DEST_COLUMNS = { "active", "created_at", "updated_at", "line1", "line2", "line3",
			"city", "country", "updated_by", "created_by", "pincode", "state", "address_type",
			"same_as_correspondence_address", "metadata" };

	// -------------------------------------------------------------------------
	// Per-instance caches — survive across processFile() calls on the same
	// instance so that repeated codes are never re-queried across files.
	// -------------------------------------------------------------------------

	private final Map<String, String> countryCache = new ConcurrentHashMap<>();
	private final Map<String, String> stateCache = new ConcurrentHashMap<>();

	/**
	 * Shared lookup DB connection for the duration of one processFile() call.
	 * Opened once before the transform loop; nulled and closed in finally. Must
	 * never be referenced outside processFile() or after its finally runs.
	 */
	private Connection lookupConn = null;

	// -------------------------------------------------------------------------
	// Logging
	// -------------------------------------------------------------------------

	private void log(String level, String message, String serverIP, String evtType) {
		if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
			return;
		}

		String timestamp = LocalDateTime.now().format(logFormat);

		StringBuilder lb = new StringBuilder(512);
		lb.append("Time:").append(timestamp).append(" [").append(capitalize(level)).append("] ")
				.append("Process_Name: ").append(processName).append(" ");

		if (serverIP != null && !serverIP.isEmpty()) {
			lb.append("Server_IP: ").append(serverIP).append(" ");
		}

		lb.append("Process_Version: ").append(processVersion).append(" Log_Level: EVT_LOG").append(" EVT_Type: ")
				.append(evtType != null ? evtType : "NA").append(" Description: ")
				.append(message != null ? message : "");

		System.out.println(lb.toString().trim());
	}

	private String capitalize(String s) {
		if (s == null || s.isEmpty()) {
			return s;
		}
		return s.substring(0, 1).toUpperCase() + s.substring(1).toLowerCase();
	}

	// =========================================================================
	// PUBLIC ENTRY POINTS
	// =========================================================================

	/**
	 * Backward-compatible 5-argument overload.
	 *
	 * Called by the existing scheduler / ntp10_process_main. Derives the lookup
	 * DB URL from the fixed constant (host .64) and reuses the same credentials
	 * supplied for the destination DB (host .62), since both databases share
	 * the same user and password.
	 */
	public boolean processFile(String pgURL, String pgUser, String pgPass, String csvFilePath, String destTable) {

		return processFile(pgURL, pgUser, pgPass, LOOKUP_DB_URL, pgUser, pgPass, csvFilePath, destTable);
	}

	/**
	 * Full 8-argument overload — explicit lookup DB credentials.
	 */
	public boolean processFile(String pgURL, String pgUser, String pgPass, String lookupDbURL, String lookupDbUser,
			String lookupDbPass, String csvFilePath, String destTable) {

		boolean status = false;

		try {
			log("INFO", "===== START | File: " + csvFilePath + " | Destination: " + destTable + " =====", null,
					"FILE_PROCESS");

			lookupConn = new PostgreSQLConnection().getConnection(lookupDbURL, lookupDbUser, lookupDbPass);

			if (lookupConn == null) {
				log("ERROR", "Unable to establish lookup DB connection | URL: " + lookupDbURL, null, "DB_CONNECT");
				return false;
			}
			log("INFO", "Lookup DB connection opened | URL: " + lookupDbURL, null, "DB_CONNECT");

			if (!validateTableExists(destTable, pgURL, pgUser, pgPass)) {
				log("ERROR", "Destination table not found: " + destTable, null, "VALIDATION");
				return false;
			}

			HashSet<String> destCols = getTableColumns(destTable, pgURL, pgUser, pgPass);

			for (String col : DEST_COLUMNS) {
				if (!destCols.contains(col.toLowerCase())) {
					log("ERROR",
							"Required column missing in destination table | Column: " + col + " | Table: " + destTable,
							null, "VALIDATION");
					return false;
				}
			}

			log("INFO", "Destination table validation passed | Table: " + destTable + " | Required columns verified: "
					+ DEST_COLUMNS.length, null, "VALIDATION");

			log("INFO", "Reading CSV file: " + csvFilePath, null, "FILE_PROCESS");

			String[] headers = new String[1];
			HashMap<String, String[]> rawData = readCSV(csvFilePath, headers);

			if (rawData == null || rawData.isEmpty()) {
				log("WARN", "No data rows found in CSV file: " + csvFilePath + " | Records returned: 0", null,
						"FILE_PROCESS");
				return false;
			}

			log("INFO", "CSV read complete | File: " + csvFilePath + " | Total rows: " + rawData.size(), null,
					"FILE_PROCESS");

			Map<String, Integer> headerIndex = buildHeaderIndex(headers[0].split("\\|", -1));

			log("DEBUG", "Header index map built | Column count: " + headerIndex.size(), null, "FILE_PROCESS");

			HashMap<String, String[]> transformedData = new LinkedHashMap<>();

			CsvSourceType sourceType = detectSourceType(headers[0].split("\\|", -1));

			log("INFO", "Source type detected | Mode: " + sourceType, null, "FILE_PROCESS");

			int transformSkipped = 0;

			for (Map.Entry<String, String[]> entry : rawData.entrySet()) {
				try {
					switch (sourceType) {
					case INSTITUTION_MASTER: {
						List<String[]> records = buildInstitutionMasterRecords(entry.getValue(), headerIndex);
						int dupIndex = 0;
						for (String[] rec : records) {
							transformedData.put(entry.getKey() + "_" + (dupIndex++), rec);
						}
						break;
					}
					case USER_ADDRESS: {
						String[] transformed = buildUserAddressRecord(entry.getValue(), headerIndex);
						transformedData.put(entry.getKey(), transformed);
						break;
					}
					default:
						throw new IllegalStateException("Unsupported CSV source type");
					}
				} catch (Exception ex) {
					transformSkipped++;
					log("WARN", "Skipping row due to transform error | Row: " + entry.getKey() + " | Error: "
							+ ex.getMessage(), null, "FILE_PROCESS");
				}
			}

			log("INFO", "Transform complete | Rows ready: " + transformedData.size() + " | Rows skipped: "
					+ transformSkipped, null, "FILE_PROCESS");

			if (transformedData.isEmpty()) {
				log("ERROR", "No rows available for insert after transform | File: " + csvFilePath, null,
						"FILE_PROCESS");
				return false;
			}

			status = insertDataToDestination(transformedData, destTable, pgURL, pgUser, pgPass);

			if (!status) {
				log("ERROR", "Insert failed | File: " + csvFilePath + " | Table: " + destTable, null, "DATA_INSERT");
			} else {
				log("INFO", "===== COMPLETE | File: " + csvFilePath + " | Table: " + destTable + " =====", null,
						"FILE_PROCESS");
			}

		} catch (Exception ex) {
			log("ERROR", "Unhandled exception in processFile | File: " + csvFilePath + " | Error: " + ex.getMessage(),
					null, "APP_ERROR");
			status = false;

		} finally {
			closeLookupConnection();
		}

		return status;
	}

	// =========================================================================
	// CSV READING
	// =========================================================================

	private HashMap<String, String[]> readCSV(String filePath, String[] headerOut) {

		HashMap<String, String[]> data = new LinkedHashMap<>();

		try (Reader reader = Files.newBufferedReader(Paths.get(filePath), StandardCharsets.UTF_8);

				CSVParser parser = CSVFormat.DEFAULT.builder().setDelimiter('|').setQuote('"').setEscape('\\')
						.setIgnoreEmptyLines(true).setAllowMissingColumnNames(true).setHeader()
						.setSkipHeaderRecord(true).build().parse(reader)) {

			List<String> headerNames = parser.getHeaderNames();
			if (headerNames == null || headerNames.isEmpty()) {
				log("WARN", "CSV header missing | File: " + filePath, null, "FILE_PROCESS");
				return null;
			}

			String[] headers = new String[headerNames.size()];
			for (int i = 0; i < headerNames.size(); i++) {
				headers[i] = headerNames.get(i).replace("\"", "").trim().toLowerCase();
			}

			headerOut[0] = String.join("|", headers);

			int rowNum = 0;

			for (CSVRecord record : parser) {
				String[] row = new String[headers.length];

				for (int i = 0; i < headers.length; i++) {
					String val = (i < record.size()) ? record.get(i) : null;

					if (val != null) {
						val = val.trim();

						if (val.startsWith("\"") && val.endsWith("\"") && val.length() >= 2) {
							val = val.substring(1, val.length() - 1);
						}

						val = val.replace("\"\"", "\"");

						if ("\\N".equalsIgnoreCase(val) || val.isEmpty() || "NULL".equalsIgnoreCase(val)) {
							val = null;
						}
					}

					row[i] = val;
				}

				data.put(String.valueOf(rowNum++), row);
			}

			log("INFO", "CSV read complete | File: " + filePath + " | Total rows: " + data.size(), null,
					"FILE_PROCESS");

		} catch (Exception ex) {

			log("ERROR", "Failed to read CSV file: " + filePath + " | Error: " + ex.getMessage(), null, "FILE_PROCESS");

			return null;
		}

		return data;
	}

	private CsvSourceType detectSourceType(String[] headers) {
		if (headers != null && headers.length > 0) {
			String first = headers[0];
			if (first != null && first.replace("\"", "").trim().toLowerCase().startsWith("fim_")) {
				return CsvSourceType.INSTITUTION_MASTER;
			}
		}
		return CsvSourceType.USER_ADDRESS;
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
	 */
	private String[] buildUserAddressRecord(String[] rawRow, Map<String, Integer> headerIdx) {

	    String[] record = new String[DEST_COLUMNS.length];

	    String nowTs = currentDbTimestamp();

	    record[0]  = "true"; // active
	    record[1]  = nowTs;  // created_at
	    record[2]  = nowTs;  // updated_at

	    record[3]  = trimToNull(getField(rawRow, headerIdx, "address1")); // line1
	    record[4]  = trimToNull(getField(rawRow, headerIdx, "address2")); // line2

	    record[5]  = buildLine3(
	            trimToNull(getField(rawRow, headerIdx, "address3")),
	            trimToNull(getField(rawRow, headerIdx, "address4"))
	    );

	    record[6]  = trimToNull(getField(rawRow, headerIdx, "city"));

	    record[7]  = lookupCountryName(
	            trimToNull(getField(rawRow, headerIdx, "country"))
	    );

	    record[8]  = null; // updated_by
	    record[9]  = null; // created_by

	    record[10] = trimToNull(getField(rawRow, headerIdx, "pincode"));

	    record[11] = lookupStateName(
	            trimToNull(getField(rawRow, headerIdx, "state_code"))
	    );

	    record[12] = null;      // address_type
	    record[13] = "false";   // same_as_correspondence_address

	    LinkedHashMap<String,String> metaMap = new LinkedHashMap<String,String>();

	    metaMap.put("email_id", "email_id");
	    metaMap.put("fax", "fax");
	    metaMap.put("phone1", "phone1");
	    metaMap.put("phone2", "phone2");
	    metaMap.put("phone3", "phone3");
	    metaMap.put("user_id", "user_id");
	    metaMap.put("village_code", "village_code");

	    record[14] = buildMetadataJson(rawRow, headerIdx, metaMap);

	    return record;
	}
	private String buildLine3(String address3, String address4) {
		address3 = trimToNull(address3);
		address4 = trimToNull(address4);
		if (address3 == null && address4 == null) {
			return null;
		}
		if (address3 == null) {
			return address4;
		}
		if (address4 == null) {
			return address3;
		}
		return address3 + " " + address4;
	}

	/**
	 * Builds two destination records (REGISTERED + CORRESPONDENCE) from one
	 * institution-master CSV row.
	 */
	private List<String[]> buildInstitutionMasterRecords(String[] rawRow, Map<String, Integer> headerIdx) {

		List<String[]> out = new ArrayList<>(2);

		String createdBy = trimToNull(getField(rawRow, headerIdx, "fim_created_by"));
		String updatedBy = trimToNull(getField(rawRow, headerIdx, "fim_created_by"));
		String permFlag = trimToNull(getField(rawRow, headerIdx, "fim_corres_perm_flag"));
		String sameAsCorr = deriveSameAsCorrespondenceAddress(permFlag);
		String nowTs = currentDbTimestamp();

		LinkedHashMap<String, String> metaMap = new LinkedHashMap<>();
		metaMap.put("fim_FI_code", "etl_fi_code");
		metaMap.put("fim_corres_state_others", "fim_Reg_Off_state_others");

		// ---- REGISTERED ----
		String[] registered = new String[DEST_COLUMNS.length];

		registered[0] = "true";
		registered[1] = nowTs; // created_at
		registered[2] = nowTs; // updated_at
		registered[3] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_addr1"));
		registered[4] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_addr2"));
		registered[5] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_addr3"));
		registered[6] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_city"));
		registered[7] = lookupCountryName(trimToNull(getField(rawRow, headerIdx, "fim_reg_off_country")));
		registered[8] = updatedBy; // updated_by
		registered[9] = createdBy; // created_by
		registered[10] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_pin"));
		registered[11] = lookupStateName(trimToNull(getField(rawRow, headerIdx, "fim_reg_off_state")));
		registered[12] = "REGISTERED";
		registered[13] = sameAsCorr;
		registered[14] = buildMetadataJson(rawRow, headerIdx, metaMap);
		out.add(registered);

		// ---- CORRESPONDENCE ----
		String[] correspondence = new String[DEST_COLUMNS.length];

		correspondence[0] = "true";
		correspondence[1] = nowTs; // created_at
		correspondence[2] = nowTs; // updated_at
		correspondence[3] = trimToNull(getField(rawRow, headerIdx, "fim_corres_addr1"));
		correspondence[4] = trimToNull(getField(rawRow, headerIdx, "fim_corres_addr2"));
		correspondence[5] = trimToNull(getField(rawRow, headerIdx, "fim_corres_addr3"));
		correspondence[6] = trimToNull(getField(rawRow, headerIdx, "fim_corres_city"));
		correspondence[7] = lookupCountryName(trimToNull(getField(rawRow, headerIdx, "fim_corres_country")));
		correspondence[8] = updatedBy; // updated_by
		correspondence[9] = createdBy; // created_by
		correspondence[10] = trimToNull(getField(rawRow, headerIdx, "fim_corres_pin"));
		correspondence[11] = lookupStateName(trimToNull(getField(rawRow, headerIdx, "fim_corres_state")));
		correspondence[12] = "CORRESPONDENCE";
		correspondence[13] = sameAsCorr;
		correspondence[14] = buildMetadataJson(rawRow, headerIdx, metaMap);
		out.add(correspondence);

		return out;
	}

	private String buildMetadataJson(String[] rawRow, Map<String, Integer> headerIdx,
			Map<String, String> sourceToJsonKeyMap) {

		StringBuilder json = new StringBuilder("{");
		boolean first = true;

		for (Map.Entry<String, String> entry : sourceToJsonKeyMap.entrySet()) {

			String val = trimToNull(getField(rawRow, headerIdx, entry.getKey()));

			// Requirement:
			// Always keep the key in metadata even when source is NULL
			if (val == null) {
				val = "";
			}

			if (!first) {
				json.append(",");
			}

			json.append("\"").append(escapeJson(entry.getValue())).append("\":\"").append(escapeJson(val)).append("\"");

			first = false;
		}

		json.append("}");
		return json.toString();
	}

	private String escapeJson(String val) {
		if (val == null) {
			return "";
		}
		return val.replace("\\", "\\\\").replace("\"", "\\\"");
	}

	// =========================================================================
	// LOOKUP METHODS
	// =========================================================================

	private String lookupCountryName(String iso2) {
		if (iso2 == null) {
			return null;
		}

		String key = iso2.trim().toUpperCase();
		if (key.isEmpty()) {
			return null;
		}

		String cached = countryCache.get(key);
		if (cached != null) {
			log("DEBUG", "Country cache hit | " + key + " -> " + cached, null, "LOOKUP");
			return cached;
		}

		if (lookupConn == null) {
			log("WARN", "lookupConn is null during country lookup | Code: " + key + " | Returning raw code", null,
					"LOOKUP");
			countryCache.put(key, key);
			return key;
		}

		String resolved = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = lookupConn.prepareStatement("SELECT country FROM public.country_master WHERE UPPER(iso_2) = ?");
			ps.setString(1, key);
			rs = ps.executeQuery();
			if (rs.next()) {
				resolved = trimToNull(rs.getString(1));
			}
			log("DEBUG", "Country lookup DB hit | " + key + " -> " + resolved, null, "LOOKUP");
		} catch (Exception ex) {
			log("WARN", "Country lookup failed | Code: " + key + " | Error: " + ex.getMessage(), null, "LOOKUP");
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

	private String lookupStateName(String stateKey) {
		if (stateKey == null) {
			return null;
		}

		String key = stateKey.trim().toUpperCase();
		if (key.isEmpty()) {
			return null;
		}

		String cached = stateCache.get(key);
		if (cached != null) {
			log("DEBUG", "State cache hit | " + key + " -> " + cached, null, "LOOKUP");
			return cached;
		}

		if (lookupConn == null) {
			log("WARN", "lookupConn is null during state lookup | Code: " + key + " | Returning raw code", null,
					"LOOKUP");
			stateCache.put(key, key);
			return key;
		}

		String resolved = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = lookupConn.prepareStatement("SELECT value FROM public.state_master WHERE UPPER(\"key\") = ?");
			ps.setString(1, key);
			rs = ps.executeQuery();
			if (rs.next()) {
				resolved = trimToNull(rs.getString(1));
			}
			log("DEBUG", "State lookup DB hit | " + key + " -> " + resolved, null, "LOOKUP");
		} catch (Exception ex) {
			log("WARN", "State lookup failed | Code: " + key + " | Error: " + ex.getMessage(), null, "LOOKUP");
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

	private String deriveSameAsCorrespondenceAddress(String permFlag) {
		return (permFlag != null && "on".equalsIgnoreCase(permFlag.trim())) ? "true" : "false";
	}

	private String currentDbTimestamp() {
		return LocalDateTime.now().format(DB_TS_FORMAT);
	}

	// =========================================================================
	// INSERT
	// =========================================================================

	private boolean insertDataToDestination(HashMap<String, String[]> transformedData, String destTable, String dbURL,
			String dbUser, String dbPass) {

		Connection conn = null;
		PreparedStatement ps = null;
		boolean status = false;

		int rowCount = 0;
		int inserted = 0;
		int skipped = 0;
		int unknown = 0;
		int totalRows = transformedData.size();
		long beforeCount = -1;
		long afterCount = -1;

		try {
			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

			if (conn == null) {
				log("ERROR", "Unable to establish destination DB connection for insert | Table: " + destTable, null,
						"DATA_INSERT");
				return false;
			}

			HashMap<String, String> columnTypes = getDestinationColumnTypes(destTable, DEST_COLUMNS, conn);

			if (columnTypes == null || columnTypes.isEmpty()) {
				log("ERROR", "Could not fetch destination column types | Table: " + destTable, null, "DATA_INSERT");
				return false;
			}

			try (PreparedStatement cntPs = conn.prepareStatement("SELECT COUNT(*) FROM " + destTable);
					ResultSet cntRs = cntPs.executeQuery()) {
				if (cntRs.next()) {
					beforeCount = cntRs.getLong(1);
				}
			}

			StringBuilder insertSQL = new StringBuilder("INSERT INTO ").append(destTable).append(" (id, ");

			for (int i = 0; i < DEST_COLUMNS.length; i++) {
				insertSQL.append(DEST_COLUMNS[i]);
				if (i < DEST_COLUMNS.length - 1) {
					insertSQL.append(", ");
				}
			}

			insertSQL.append(") VALUES (gen_random_uuid(), ");

			for (int i = 0; i < DEST_COLUMNS.length; i++) {
				insertSQL.append("?");
				if (i < DEST_COLUMNS.length - 1) {
					insertSQL.append(", ");
				}
			}

			insertSQL.append(") ON CONFLICT DO NOTHING");

			log("DEBUG", "Prepared insert query | Table: " + destTable + " | Query: " + insertSQL, null, "DATA_INSERT");

			conn.setAutoCommit(false);
			ps = conn.prepareStatement(insertSQL.toString());

			for (Map.Entry<String, String[]> entry : transformedData.entrySet()) {
				String[] record = entry.getValue();

				for (int i = 0; i < DEST_COLUMNS.length; i++) {
					String colName = DEST_COLUMNS[i].toLowerCase();
					String colType = columnTypes.get(colName);
					String value = (i < record.length) ? record[i] : null;
					setParameterByType(ps, i + 1, value, colType, colName);
				}

				ps.addBatch();
				rowCount++;

				if (rowCount % BATCH_SIZE == 0) {
					int[] results = ps.executeBatch();
					for (int r : results) {
						if (r == 1) {
							inserted++;
						} else if (r == 0) {
							skipped++;
						} else if (r == java.sql.Statement.SUCCESS_NO_INFO) {
							unknown++;
						}
					}
					log("DEBUG", "Mid-batch executed | Table: " + destTable + " | Rows so far: " + rowCount
							+ " | Total: " + totalRows, null, "DATA_INSERT");
				}
			}

			int[] results = ps.executeBatch();
			for (int r : results) {
				if (r == 1) {
					inserted++;
				} else if (r == 0) {
					skipped++;
				} else if (r == java.sql.Statement.SUCCESS_NO_INFO) {
					unknown++;
				}
			}

			conn.commit();

			try (PreparedStatement cntPs = conn.prepareStatement("SELECT COUNT(*) FROM " + destTable);
					ResultSet cntRs = cntPs.executeQuery()) {
				if (cntRs.next()) {
					afterCount = cntRs.getLong(1);
				}
			}

			status = true;

			log("INFO",
					"Insert Summary | Table: " + destTable + " | Total Rows (CSV): " + totalRows + " | Inserted (new): "
							+ inserted + " | Skipped (conflict): " + skipped + " | Unknown (driver): " + unknown
							+ " | Rows Before: " + beforeCount + " | Rows After: " + afterCount + " | Net Added: "
							+ (afterCount - beforeCount),
					null, "DATA_INSERT");

		} catch (Exception ex) {
			log("ERROR", "Insert failed | Table: " + destTable + " | Rows processed before failure: " + rowCount
					+ " | Error: " + ex.getMessage(), null, "DATA_INSERT");

			try {
				if (conn != null) {
					conn.rollback();
					log("INFO", "Transaction rolled back successfully | Table: " + destTable, null, "DATA_INSERT");
				}
			} catch (Exception rollbackEx) {
				log("ERROR", "Rollback failed | Table: " + destTable + " | Error: " + rollbackEx.getMessage(), null,
						"DATA_INSERT");
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
	// =========================================================================

	private boolean validateTableExists(String tableName, String dbURL, String dbUser, String dbPass) {

		Connection conn = null;
		boolean exists = false;

		try {
			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

			String schemaName = "public";
			String actualTableName = tableName;

			if (tableName.contains(".")) {
				String[] parts = tableName.split("\\.", 2);
				schemaName = parts[0].toLowerCase();
				actualTableName = parts[1].toLowerCase();
			} else {
				actualTableName = tableName.toLowerCase();
			}

			log("DEBUG", "Checking table existence | Schema: " + schemaName + " | Table: " + actualTableName, null,
					"VALIDATION");

			String sql = "SELECT 1 FROM information_schema.tables " + "WHERE table_schema = ? AND table_name = ?";

			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				ps.setString(1, schemaName);
				ps.setString(2, actualTableName);
				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						exists = true;
						log("INFO", "Table found | Schema: " + schemaName + " | Table: " + actualTableName, null,
								"VALIDATION");
					} else {
						log("WARN", "Table not found | Schema: " + schemaName + " | Table: " + actualTableName, null,
								"VALIDATION");
					}
				}
			}

		} catch (Exception ex) {
			log("ERROR",
					"Exception during table existence check | Table: " + tableName + " | Error: " + ex.getMessage(),
					null, "VALIDATION");
		} finally {
			closeQuietly(conn);
		}

		return exists;
	}

	private HashSet<String> getTableColumns(String tableName, String dbURL, String dbUser, String dbPass) {

		Connection conn = null;
		HashSet<String> columns = new HashSet<>();

		try {
			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

			String schemaName = null;
			String actualTableName = tableName;

			if (tableName.contains(".")) {
				String[] parts = tableName.split("\\.", 2);
				schemaName = parts[0].toLowerCase();
				actualTableName = parts[1].toLowerCase();
			} else {
				actualTableName = tableName.toLowerCase();
			}

			log("DEBUG", "Fetching column list | Schema: " + schemaName + " | Table: " + actualTableName, null,
					"VALIDATION");

			DatabaseMetaData meta = conn.getMetaData();
			try (ResultSet rs = meta.getColumns(null, schemaName, actualTableName, null)) {
				while (rs.next()) {
					columns.add(rs.getString("COLUMN_NAME").toLowerCase());
				}
			}

			log("INFO", "Column list fetched | Table: " + tableName + " | Total columns: " + columns.size(), null,
					"VALIDATION");

		} catch (Exception ex) {
			log("ERROR", "Exception fetching column list | Table: " + tableName + " | Error: " + ex.getMessage(), null,
					"VALIDATION");
		} finally {
			closeQuietly(conn);
		}

		return columns;
	}

	private HashMap<String, String> getDestinationColumnTypes(String tableName, String[] columns, Connection conn) {

		HashMap<String, String> columnTypes = new HashMap<>();

		try {
			DatabaseMetaData meta = conn.getMetaData();

			String schemaName = null;
			String actualTableName = tableName;

			if (tableName.contains(".")) {
				String[] parts = tableName.split("\\.", 2);
				schemaName = parts[0].toLowerCase();
				actualTableName = parts[1].toLowerCase();
			} else {
				actualTableName = tableName.toLowerCase();
			}

			for (String col : columns) {
				try (ResultSet rs = meta.getColumns(null, schemaName, actualTableName, col.toLowerCase())) {
					if (rs.next()) {
						String type = rs.getString("TYPE_NAME");
						columnTypes.put(col.toLowerCase(), type);
						log("DEBUG",
								"Column type resolved | Column: " + col + " | Type: " + type + " | Table: " + tableName,
								null, "VALIDATION");
					}
				}
			}

		} catch (Exception ex) {
			log("ERROR",
					"Failed to fetch destination column types | Table: " + tableName + " | Error: " + ex.getMessage(),
					null, "VALIDATION");
			return null;
		}

		return columnTypes;
	}

	// =========================================================================
	// PREPARED STATEMENT HELPER
	// =========================================================================

	private void setParameterByType(PreparedStatement ps, int paramIndex, String value, String columnType,
			String columnName) throws Exception {

		if (value == null) {
			if (columnType != null) {
				String t = columnType.toLowerCase();
				if (t.contains("int") || t.contains("serial")) {
					ps.setNull(paramIndex, java.sql.Types.BIGINT);
				} else if (t.contains("timestamp") || t.contains("date") || t.contains("time")) {
					ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
				} else if (t.contains("numeric") || t.contains("decimal") || t.contains("float")
						|| t.contains("double")) {
					ps.setNull(paramIndex, java.sql.Types.NUMERIC);
				} else if (t.contains("bool")) {
					ps.setNull(paramIndex, java.sql.Types.BOOLEAN);
				} else {
					ps.setNull(paramIndex, java.sql.Types.VARCHAR);
				}
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
				if (t.contains("int") || t.contains("serial")) {
					ps.setNull(paramIndex, java.sql.Types.BIGINT);
				} else if (t.contains("timestamp") || t.contains("date") || t.contains("time")) {
					ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
				} else if (t.contains("numeric") || t.contains("decimal") || t.contains("float")
						|| t.contains("double")) {
					ps.setNull(paramIndex, java.sql.Types.NUMERIC);
				} else if (t.contains("bool")) {
					ps.setNull(paramIndex, java.sql.Types.BOOLEAN);
				} else {
					ps.setString(paramIndex, value);
				}
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
								"Timestamp parse failure | Column: " + columnName + " | Expected type: " + columnType
										+ " | Received value: " + value + " | Expected format: YYYY-MM-DD HH:MM:SS",
								null, "DATA_INSERT");
						throw new Exception("Data type mismatch: Column '" + columnName
								+ "' expects timestamp but got '" + value + "'");
					}
				} else if (t.contains("date")) {
					try {
						ps.setDate(paramIndex, java.sql.Date.valueOf(value.trim()));
					} catch (IllegalArgumentException e) {
						log("ERROR", "Date parse failure | Column: " + columnName + " | Expected type: " + columnType
								+ " | Received value: " + value, null, "DATA_INSERT");
						throw new Exception(
								"Data type mismatch: Column '" + columnName + "' expects date but got '" + value + "'");
					}
				} else if (t.contains("time")) {
					try {
						ps.setTime(paramIndex, java.sql.Time.valueOf(value.trim()));
					} catch (IllegalArgumentException e) {
						log("ERROR", "Time parse failure | Column: " + columnName + " | Expected type: " + columnType
								+ " | Received value: " + value, null, "DATA_INSERT");
						throw new Exception(
								"Data type mismatch: Column '" + columnName + "' expects time but got '" + value + "'");
					}
				} else {
					ps.setString(paramIndex, value);
				}
			} catch (NumberFormatException e) {
				log("ERROR", "Number format parse failure | Column: " + columnName + " | Expected type: " + columnType
						+ " | Received value: " + value, null, "DATA_INSERT");
				throw new Exception("Data type mismatch: Column '" + columnName + "' expects " + columnType
						+ " but got '" + value + "'");
			}
		} else {
			ps.setString(paramIndex, value);
		}
	}

	// =========================================================================
	// RESOURCE MANAGEMENT
	// =========================================================================

	private void closeLookupConnection() {
		if (lookupConn != null) {
			try {
				lookupConn.close();
				log("INFO", "Lookup DB connection closed", null, "DB_CONNECT");
			} catch (Exception e) {
				log("ERROR", "Failed to close lookup DB connection | Error: " + e.getMessage(), null, "DB_CONNECT");
			} finally {
				lookupConn = null;
			}
		}
	}

	private void closeQuietly(AutoCloseable c) {
		if (c != null) {
			try {
				c.close();
			} catch (Exception ignored) {
			}
		}
	}

	// =========================================================================
	// UTILITY HELPERS
	// =========================================================================

	private String getField(String[] row, Map<String, Integer> headerIdx, String colName) {
		Integer idx = headerIdx.get(colName.toLowerCase());
		if (idx == null || idx >= row.length) {
			return null;
		}
		return row[idx];
	}

	private String trimToNull(String val) {
		if (val == null) {
			return null;
		}
		String trimmed = val.trim();
		return trimmed.isEmpty() ? null : trimmed;
	}
}