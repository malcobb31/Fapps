package ntp10_process_v3;

import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
 * ntp10_process_methods - CSV Transform and Update
 *
 * Supports:
 * 1) institution master CSVs (headers start with fim_)
 * 2) user address CSVs (all other headers)
 *
 * Lookup DB:
 * public.country_master, public.state_master
 *
 * Changes from old loader:
 * - Keeps the same CSV parsing and lookup logic
 * - Replaces INSERT with UPDATE
 * - Updates existing rows in re_mgmt.address_test
 * - user_address.user_created_date -> address_test.created_at
 * - same_as_correspondence_address for blank/null fim_corres_perm_flag is
 *   derived from field-by-field comparison
 *
 * NEW in this revision:
 * - REQUIREMENT 2: For USER_ADDRESS rows with address_type IN ('REGION','BRANCH'),
 *   country_code is populated from the CSV's raw "country" column ONLY when the
 *   existing destination row's country_code is currently NULL/blank. Implemented
 *   as a SQL-side CASE expression in the main UPDATE so it never clobbers an
 *   existing non-null value and does not require a pre-SELECT per row.
 * - REQUIREMENT 3: A separate, targeted backfill pass after the institution-master
 *   update. For destination rows where address_type IN ('REGISTERED','CORRESPONDENCE')
 *   AND country IS NULL, resolves the ISO code from the institution-master CSV
 *   through country_master and patches ONLY the country column on those rows.
 */
public class ntp10_process_methods {

	private enum CsvSourceType {
		INSTITUTION_MASTER, USER_ADDRESS
	}

	// -------------------------------------------------------------------------
	// Constants
	// -------------------------------------------------------------------------

	private static final String processName = "address-parser";
	private static final String processVersion = "1.2";
	private static final boolean debugEnabled = true;
	private static final java.time.format.DateTimeFormatter logFormat = java.time.format.DateTimeFormatter
			.ofPattern("dd-MM-yyyy HH:mm:ss SSS");
	private static final java.time.format.DateTimeFormatter DB_TS_FORMAT = java.time.format.DateTimeFormatter
			.ofPattern("yyyy-MM-dd HH:mm:ss");

	/** Lookup DB coordinates. */
	private static final String LOOKUP_DB_URL = "jdbc:postgresql://172.20.131.64:5432/ckyprodwbdb";

	private static final int BATCH_SIZE = 200;

	/** Address types eligible for the requirement-2 conditional country_code fill. */
	private static final HashSet<String> REGION_BRANCH_TYPES = new HashSet<String>(
			java.util.Arrays.asList("REGION", "BRANCH"));

	/**
	 * Destination columns in update order.
	 * Keep this aligned with getRecordValue().
	 *
	 * NOTE:
	 * This is a subset of the full address_test table.
	 * We are only updating the columns this parser owns.
	 */
	private static final String[] DEST_COLUMNS = { "active", "created_at", "updated_at", "line1", "line2", "line3",
			"city", "country", "country_code", "state", "state_code", "updated_by", "created_by", "pincode",
			"address_type", "same_as_correspondence_address", "metadata" };

	// Record indexes for DEST_COLUMNS
	private static final int IDX_ACTIVE = 0;
	private static final int IDX_CREATED_AT = 1;
	private static final int IDX_UPDATED_AT = 2;
	private static final int IDX_LINE1 = 3;
	private static final int IDX_LINE2 = 4;
	private static final int IDX_LINE3 = 5;
	private static final int IDX_CITY = 6;
	private static final int IDX_COUNTRY = 7;
	private static final int IDX_COUNTRY_CODE = 8;
	private static final int IDX_STATE = 9;
	private static final int IDX_STATE_CODE = 10;
	private static final int IDX_UPDATED_BY = 11;
	private static final int IDX_CREATED_BY = 12;
	private static final int IDX_PINCODE = 13;
	private static final int IDX_ADDRESS_TYPE = 14;
	private static final int IDX_SAME_AS_CORR = 15;
	private static final int IDX_METADATA = 16;

	// -------------------------------------------------------------------------
	// Per-instance caches
	// -------------------------------------------------------------------------

	private final Map<String, String> countryCache = new ConcurrentHashMap<String, String>();
	private final Map<String, String> stateCache = new ConcurrentHashMap<String, String>();

	/**
	 * Shared lookup DB connection for the duration of one processFile() call.
	 */
	private Connection lookupConn = null;

	// -------------------------------------------------------------------------
	// Logging
	// -------------------------------------------------------------------------

	private void log(String level, String message, String serverIP, String evtType) {
		if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
			return;
		}

		String timestamp = java.time.LocalDateTime.now().format(logFormat);

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

	public boolean processFile(String pgURL, String pgUser, String pgPass, String csvFilePath, String destTable) {
		return processFile(pgURL, pgUser, pgPass, LOOKUP_DB_URL, pgUser, pgPass, csvFilePath, destTable);
	}

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
					log("ERROR", "Required column missing in destination table | Column: " + col + " | Table: " + destTable,
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

			HashMap<String, String[]> transformedData = new LinkedHashMap<String, String[]>();

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
				log("ERROR", "No rows available for update after transform | File: " + csvFilePath, null,
						"FILE_PROCESS");
				return false;
			}

			status = updateDataToDestination(transformedData, destTable, pgURL, pgUser, pgPass, sourceType);

			if (!status) {
				log("ERROR", "Update failed | File: " + csvFilePath + " | Table: " + destTable, null, "DATA_UPDATE");
			} else {
				// REQUIREMENT 3: after the main institution-master update commits, run the
				// targeted country backfill for REGISTERED/CORRESPONDENCE rows that are
				// still NULL. Only attempted for institution-master files, since that CSV
				// is the source of the ISO codes for those two address types.
				if (sourceType == CsvSourceType.INSTITUTION_MASTER) {
					boolean backfillStatus = backfillMissingCountry(transformedData, destTable, pgURL, pgUser, pgPass);
					if (!backfillStatus) {
						log("WARN", "Country backfill pass reported failure | Table: " + destTable
								+ " | Main update already committed successfully, backfill is best-effort", null,
								"DATA_UPDATE");
					}
				}

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

		HashMap<String, String[]> data = new LinkedHashMap<String, String[]>();

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
		Map<String, Integer> index = new HashMap<String, Integer>();
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

		String createdAt = trimToNull(getField(rawRow, headerIdx, "user_created_date"));
		if (createdAt == null) {
			createdAt = nowTs;
		}

		record[IDX_ACTIVE] = "true";
		record[IDX_CREATED_AT] = createdAt;
		record[IDX_UPDATED_AT] = nowTs;

		record[IDX_LINE1] = trimToNull(getField(rawRow, headerIdx, "address1"));
		record[IDX_LINE2] = trimToNull(getField(rawRow, headerIdx, "address2"));
		record[IDX_LINE3] = buildLine3(trimToNull(getField(rawRow, headerIdx, "address3")),
				trimToNull(getField(rawRow, headerIdx, "address4")));

		record[IDX_CITY] = trimToNull(getField(rawRow, headerIdx, "city"));

		// NOTE: raw CSV "country" is kept in IDX_COUNTRY_CODE for use both as the
		// resolved-name lookup input AND (requirement 2) as the fallback raw value
		// written to address.country_code for REGION/BRANCH rows. See
		// getUpdateColumns()/updateDataToDestination() for how requirement 2 is
		// applied conditionally at the SQL layer.
		String rawCountryCode = trimToNull(getField(rawRow, headerIdx, "country"));
		record[IDX_COUNTRY_CODE] = rawCountryCode;
		record[IDX_COUNTRY] = lookupCountryName(rawCountryCode);

		record[IDX_STATE_CODE] = trimToNull(getField(rawRow, headerIdx, "state_code"));
		record[IDX_STATE] = lookupStateName(record[IDX_STATE_CODE]);

		record[IDX_UPDATED_BY] = null;
		record[IDX_CREATED_BY] = null;

		record[IDX_PINCODE] = trimToNull(getField(rawRow, headerIdx, "pincode"));

		record[IDX_ADDRESS_TYPE] = trimToNull(getField(rawRow, headerIdx, "address_type"));
		record[IDX_SAME_AS_CORR] = "false";

		LinkedHashMap<String, String> metaMap = new LinkedHashMap<String, String>();
		metaMap.put("email_id", "email_id");
		metaMap.put("fax", "fax");
		metaMap.put("phone1", "phone1");
		metaMap.put("phone2", "phone2");
		metaMap.put("phone3", "phone3");
		metaMap.put("user_id", "user_id");
		metaMap.put("village_code", "village_code");

		record[IDX_METADATA] = buildMetadataJson(rawRow, headerIdx, metaMap);

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

		List<String[]> out = new ArrayList<String[]>(2);

		String createdBy = trimToNull(getField(rawRow, headerIdx, "fim_created_by"));
		String updatedBy = trimToNull(getField(rawRow, headerIdx, "fim_created_by"));
		String permFlag = trimToNull(getField(rawRow, headerIdx, "fim_corres_perm_flag"));
		String sameAsCorr = deriveSameAsCorrespondenceAddress(permFlag, rawRow, headerIdx);
		String nowTs = currentDbTimestamp();

		LinkedHashMap<String, String> metaMap = new LinkedHashMap<String, String>();
		metaMap.put("fim_FI_code", "etl_fi_code");
		metaMap.put("fim_corres_state_others", "fim_Reg_Off_state_others");

		// ---- REGISTERED ----
		String[] registered = new String[DEST_COLUMNS.length];

		registered[IDX_ACTIVE] = "true";
		registered[IDX_CREATED_AT] = nowTs;
		registered[IDX_UPDATED_AT] = nowTs;
		registered[IDX_LINE1] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_addr1"));
		registered[IDX_LINE2] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_addr2"));
		registered[IDX_LINE3] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_addr3"));
		registered[IDX_CITY] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_city"));

		String regCountryCode = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_country"));
		registered[IDX_COUNTRY_CODE] = regCountryCode;
		registered[IDX_COUNTRY] = lookupCountryName(regCountryCode);

		registered[IDX_STATE_CODE] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_state"));
		registered[IDX_STATE] = lookupStateName(registered[IDX_STATE_CODE]);

		registered[IDX_UPDATED_BY] = updatedBy;
		registered[IDX_CREATED_BY] = createdBy;
		registered[IDX_PINCODE] = trimToNull(getField(rawRow, headerIdx, "fim_reg_off_pin"));
		registered[IDX_ADDRESS_TYPE] = "REGISTERED";
		registered[IDX_SAME_AS_CORR] = sameAsCorr;
		registered[IDX_METADATA] = buildMetadataJson(rawRow, headerIdx, metaMap);
		out.add(registered);

		// ---- CORRESPONDENCE ----
		String[] correspondence = new String[DEST_COLUMNS.length];

		correspondence[IDX_ACTIVE] = "true";
		correspondence[IDX_CREATED_AT] = nowTs;
		correspondence[IDX_UPDATED_AT] = nowTs;
		correspondence[IDX_LINE1] = trimToNull(getField(rawRow, headerIdx, "fim_corres_addr1"));
		correspondence[IDX_LINE2] = trimToNull(getField(rawRow, headerIdx, "fim_corres_addr2"));
		correspondence[IDX_LINE3] = trimToNull(getField(rawRow, headerIdx, "fim_corres_addr3"));
		correspondence[IDX_CITY] = trimToNull(getField(rawRow, headerIdx, "fim_corres_city"));

		String corCountryCode = trimToNull(getField(rawRow, headerIdx, "fim_corres_country"));
		correspondence[IDX_COUNTRY_CODE] = corCountryCode;
		correspondence[IDX_COUNTRY] = lookupCountryName(corCountryCode);

		correspondence[IDX_STATE_CODE] = trimToNull(getField(rawRow, headerIdx, "fim_corres_state"));
		correspondence[IDX_STATE] = lookupStateName(correspondence[IDX_STATE_CODE]);

		correspondence[IDX_UPDATED_BY] = updatedBy;
		correspondence[IDX_CREATED_BY] = createdBy;
		correspondence[IDX_PINCODE] = trimToNull(getField(rawRow, headerIdx, "fim_corres_pin"));
		correspondence[IDX_ADDRESS_TYPE] = "CORRESPONDENCE";
		correspondence[IDX_SAME_AS_CORR] = sameAsCorr;
		correspondence[IDX_METADATA] = buildMetadataJson(rawRow, headerIdx, metaMap);
		out.add(correspondence);

		return out;
	}

	private String buildMetadataJson(String[] rawRow, Map<String, Integer> headerIdx,
			Map<String, String> sourceToJsonKeyMap) {

		StringBuilder json = new StringBuilder("{");
		boolean first = true;

		for (Map.Entry<String, String> entry : sourceToJsonKeyMap.entrySet()) {

			String val = trimToNull(getField(rawRow, headerIdx, entry.getKey()));

			// Keep the key even when source is NULL
			if (val == null) {
				val = "";
			}

			if (!first) {
				json.append(",");
			}

			json.append("\"").append(escapeJson(entry.getValue())).append("\":\"")
					.append(escapeJson(val)).append("\"");

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

	private String deriveSameAsCorrespondenceAddress(String permFlag, String[] rawRow, Map<String, Integer> headerIdx) {
		String flag = trimToNull(permFlag);

		// New requirement: when flag is blank/null, compare the fields one by one.
		if (flag == null) {
			boolean same = equalsNormalized(getField(rawRow, headerIdx, "fim_corres_addr1"),
					getField(rawRow, headerIdx, "fim_reg_off_addr1"))
					&& equalsNormalized(getField(rawRow, headerIdx, "fim_corres_addr2"),
							getField(rawRow, headerIdx, "fim_reg_off_addr2"))
					&& equalsNormalized(getField(rawRow, headerIdx, "fim_corres_addr3"),
							getField(rawRow, headerIdx, "fim_reg_off_addr3"))
					&& equalsNormalized(getField(rawRow, headerIdx, "fim_corres_city"),
							getField(rawRow, headerIdx, "fim_reg_off_city"))
					&& equalsNormalized(getField(rawRow, headerIdx, "fim_corres_pin"),
							getField(rawRow, headerIdx, "fim_reg_off_pin"))
					&& equalsNormalized(getField(rawRow, headerIdx, "fim_corres_country"),
							getField(rawRow, headerIdx, "fim_reg_off_country"))
					&& equalsNormalized(getField(rawRow, headerIdx, "fim_corres_state"),
							getField(rawRow, headerIdx, "fim_reg_off_state"))
					&& equalsNormalized(getField(rawRow, headerIdx, "fim_corres_state_others"),
							getField(rawRow, headerIdx, "fim_reg_off_state_others"));

			return same ? "true" : "false";
		}

		// Existing flag behavior kept for non-empty values.
		return ("on".equalsIgnoreCase(flag) || "true".equalsIgnoreCase(flag) || "1".equals(flag)) ? "true" : "false";
	}

	private boolean equalsNormalized(String a, String b) {
		a = trimToNull(a);
		b = trimToNull(b);
		if (a == null && b == null) {
			return true;
		}
		if (a == null || b == null) {
			return false;
		}
		return a.equalsIgnoreCase(b);
	}

	private String currentDbTimestamp() {
		return java.time.LocalDateTime.now().format(DB_TS_FORMAT);
	}

	// =========================================================================
	// UPDATE
	// =========================================================================

	private boolean updateDataToDestination(HashMap<String, String[]> transformedData, String destTable, String dbURL,
			String dbUser, String dbPass, CsvSourceType sourceType) {

		Connection conn = null;
		PreparedStatement ps = null;
		boolean status = false;

		int rowCount = 0;
		int updated = 0;
		int skipped = 0;
		int unknown = 0;
		int totalRows = transformedData.size();
		long beforeCount = -1;
		long afterCount = -1;

		try {
			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

			if (conn == null) {
				log("ERROR", "Unable to establish destination DB connection for update | Table: " + destTable, null,
						"DATA_UPDATE");
				return false;
			}

			String[] updateColumns = getUpdateColumns(sourceType);
			HashMap<String, String> columnTypes = getDestinationColumnTypes(destTable, updateColumns, conn);

			if (columnTypes == null || columnTypes.isEmpty()) {
				log("ERROR", "Could not fetch destination column types | Table: " + destTable, null, "DATA_UPDATE");
				return false;
			}

			try (PreparedStatement cntPs = conn.prepareStatement("SELECT COUNT(*) FROM " + destTable);
					ResultSet cntRs = cntPs.executeQuery()) {
				if (cntRs.next()) {
					beforeCount = cntRs.getLong(1);
				}
			}

			// REQUIREMENT 2: for USER_ADDRESS updates, country_code is written through a
			// CASE expression instead of a plain "= ?" so that we only overwrite the
			// existing destination value when the row's address_type is REGION/BRANCH
			// AND its current country_code is NULL/blank. All other columns keep the
			// existing unconditional "= ?" behavior.
			boolean useConditionalCountryCode = (sourceType == CsvSourceType.USER_ADDRESS);

			StringBuilder updateSQL = new StringBuilder("UPDATE ").append(destTable).append(" SET ");

			for (int i = 0; i < updateColumns.length; i++) {
				String col = updateColumns[i];

				if (useConditionalCountryCode && "country_code".equalsIgnoreCase(col)) {
					updateSQL.append(col).append(" = CASE WHEN address_type IN ('REGION','BRANCH') ")
							.append("AND (country_code IS NULL OR country_code = '') THEN ? ELSE country_code END");
				} else {
					updateSQL.append(col).append(" = ?");
				}

				if (i < updateColumns.length - 1) {
					updateSQL.append(", ");
				}
			}

			updateSQL.append(" WHERE ");

			if (sourceType == CsvSourceType.INSTITUTION_MASTER) {
				updateSQL.append("address_type = ? AND metadata->>'etl_fi_code' = ?");
			} else {
				updateSQL.append("metadata->>'user_id' = ?");
			}

			log("DEBUG", "Prepared update query | Table: " + destTable + " | Query: " + updateSQL, null, "DATA_UPDATE");

			conn.setAutoCommit(false);
			ps = conn.prepareStatement(updateSQL.toString());

			for (Map.Entry<String, String[]> entry : transformedData.entrySet()) {
				String[] record = entry.getValue();

				for (int i = 0; i < updateColumns.length; i++) {
					String colName = updateColumns[i].toLowerCase();
					String colType = columnTypes.get(colName);
					String value = getRecordValue(colName, record);
					// Note: for the conditional country_code CASE expression above, the
					// parameter placeholder still needs a bound value (used only when the
					// CASE condition matches); we bind the CSV's raw country code here same
					// as before. When the condition doesn't match, the CASE falls back to
					// the existing country_code value and this bound param is ignored.
					setParameterByType(ps, i + 1, value, colType, colName);
				}

				int paramIndex = updateColumns.length + 1;

				if (sourceType == CsvSourceType.INSTITUTION_MASTER) {
					String addrType = getRecordValue("address_type", record);
					String fiCode = extractMetadataValue(getRecordValue("metadata", record), "etl_fi_code");
					ps.setString(paramIndex++, trimToNull(addrType));
					ps.setString(paramIndex++, trimToNull(fiCode));
				} else {
					String userId = extractMetadataValue(getRecordValue("metadata", record), "user_id");
					ps.setString(paramIndex++, trimToNull(userId));
				}

				ps.addBatch();
				rowCount++;

				if (rowCount % BATCH_SIZE == 0) {
					int[] results = ps.executeBatch();
					for (int r : results) {
						if (r == 1) {
							updated++;
						} else if (r == 0) {
							skipped++;
						} else if (r == java.sql.Statement.SUCCESS_NO_INFO) {
							unknown++;
						}
					}
					log("DEBUG", "Mid-batch executed | Table: " + destTable + " | Rows so far: " + rowCount + " | Total: "
							+ totalRows, null, "DATA_UPDATE");
				}
			}

			int[] results = ps.executeBatch();
			for (int r : results) {
				if (r == 1) {
					updated++;
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
					"Update Summary | Table: " + destTable + " | Total Rows (CSV): " + totalRows + " | Updated: "
							+ updated + " | Skipped (no match/conflict): " + skipped + " | Unknown (driver): "
							+ unknown + " | Rows Before: " + beforeCount + " | Rows After: " + afterCount
							+ " | Net Delta: " + (afterCount - beforeCount),
					null, "DATA_UPDATE");

		} catch (Exception ex) {
			log("ERROR",
					"Update failed | Table: " + destTable + " | Rows processed before failure: " + rowCount + " | Error: "
							+ ex.getMessage(),
					null, "DATA_UPDATE");

			try {
				if (conn != null) {
					conn.rollback();
					log("INFO", "Transaction rolled back successfully | Table: " + destTable, null, "DATA_UPDATE");
				}
			} catch (Exception rollbackEx) {
				log("ERROR", "Rollback failed | Table: " + destTable + " | Error: " + rollbackEx.getMessage(), null,
						"DATA_UPDATE");
			}

			status = false;

		} finally {
			closeQuietly(ps);
			closeQuietly(conn);
		}

		return status;
	}

	// =========================================================================
	// REQUIREMENT 3: COUNTRY BACKFILL FOR REGISTERED / CORRESPONDENCE
	// =========================================================================

	/**
	 * Targeted, best-effort remediation pass. Runs AFTER the main institution-master
	 * update has already committed successfully.
	 *
	 * For each transformed institution-master record (REGISTERED or CORRESPONDENCE),
	 * patches ONLY the destination "country" column, and ONLY on rows that currently
	 * satisfy:
	 *   address_type IN ('REGISTERED','CORRESPONDENCE') AND country IS NULL
	 *
	 * The resolved country name (record[IDX_COUNTRY], already resolved through
	 * country_master during transform) is used as the new value. Rows that don't
	 * have a resolvable country (i.e. record[IDX_COUNTRY] is null because the CSV's
	 * ISO code was blank) are skipped — there is nothing to backfill with.
	 *
	 * This method intentionally does NOT touch any other column, and intentionally
	 * does NOT run inside the same transaction/batch as the main update, so a
	 * failure here never rolls back or blocks the primary update that already
	 * succeeded.
	 */
	private boolean backfillMissingCountry(HashMap<String, String[]> transformedData, String destTable, String dbURL,
			String dbUser, String dbPass) {

		Connection conn = null;
		PreparedStatement ps = null;
		boolean status = false;

		int rowCount = 0;
		int patched = 0;
		int skippedNoCountry = 0;
		int skippedNoMatch = 0;

		String backfillSQL = "UPDATE " + destTable + " SET country = ? "
				+ "WHERE address_type IN ('REGISTERED','CORRESPONDENCE') "
				+ "AND country IS NULL "
				+ "AND address_type = ? AND metadata->>'etl_fi_code' = ?";

		try {
			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

			if (conn == null) {
				log("ERROR", "Unable to establish destination DB connection for country backfill | Table: "
						+ destTable, null, "DATA_UPDATE");
				return false;
			}

			log("DEBUG", "Prepared country backfill query | Table: " + destTable + " | Query: " + backfillSQL, null,
					"DATA_UPDATE");

			conn.setAutoCommit(false);
			ps = conn.prepareStatement(backfillSQL);

			for (Map.Entry<String, String[]> entry : transformedData.entrySet()) {
				String[] record = entry.getValue();

				String addrType = trimToNull(getRecordValue("address_type", record));
				if (!"REGISTERED".equalsIgnoreCase(addrType) && !"CORRESPONDENCE".equalsIgnoreCase(addrType)) {
					continue;
				}

				String resolvedCountry = trimToNull(getRecordValue("country", record));
				if (resolvedCountry == null) {
					// Nothing to backfill with (CSV had no usable ISO code for this row/type).
					skippedNoCountry++;
					continue;
				}

				String fiCode = extractMetadataValue(getRecordValue("metadata", record), "etl_fi_code");

				ps.setString(1, resolvedCountry);
				ps.setString(2, addrType);
				ps.setString(3, trimToNull(fiCode));
				ps.addBatch();
				rowCount++;

				if (rowCount % BATCH_SIZE == 0) {
					int[] results = ps.executeBatch();
					for (int r : results) {
						if (r == 1) {
							patched++;
						} else if (r == 0) {
							skippedNoMatch++;
						}
					}
					log("DEBUG", "Country backfill mid-batch executed | Table: " + destTable + " | Rows so far: "
							+ rowCount, null, "DATA_UPDATE");
				}
			}

			int[] results = ps.executeBatch();
			for (int r : results) {
				if (r == 1) {
					patched++;
				} else if (r == 0) {
					skippedNoMatch++;
				}
			}

			conn.commit();
			status = true;

			log("INFO",
					"Country Backfill Summary | Table: " + destTable + " | Candidate Rows: " + rowCount
							+ " | Patched (country was NULL, now set): " + patched
							+ " | Skipped (no match or country already non-null): " + skippedNoMatch
							+ " | Skipped (no resolvable ISO code in CSV): " + skippedNoCountry,
					null, "DATA_UPDATE");

		} catch (Exception ex) {
			log("ERROR", "Country backfill failed | Table: " + destTable + " | Rows processed before failure: "
					+ rowCount + " | Error: " + ex.getMessage(), null, "DATA_UPDATE");

			try {
				if (conn != null) {
					conn.rollback();
					log("INFO", "Country backfill transaction rolled back successfully | Table: " + destTable, null,
							"DATA_UPDATE");
				}
			} catch (Exception rollbackEx) {
				log("ERROR", "Country backfill rollback failed | Table: " + destTable + " | Error: "
						+ rollbackEx.getMessage(), null, "DATA_UPDATE");
			}

			status = false;

		} finally {
			closeQuietly(ps);
			closeQuietly(conn);
		}

		return status;
	}

	private String[] getUpdateColumns(CsvSourceType sourceType) {
		if (sourceType == CsvSourceType.INSTITUTION_MASTER) {
			// Keep created_at untouched for existing institution-master rows.
			return new String[] { "active", "updated_at", "line1", "line2", "line3", "city", "country",
					"country_code", "state", "state_code", "updated_by", "created_by", "pincode", "address_type",
					"same_as_correspondence_address", "metadata" };
		}

		// user_address rows
		return new String[] { "active", "created_at", "updated_at", "line1", "line2", "line3", "city", "country",
				"country_code", "state", "state_code", "pincode", "same_as_correspondence_address", "metadata" };
	}

	private String getRecordValue(String columnName, String[] record) {
		if ("active".equalsIgnoreCase(columnName)) {
			return record[IDX_ACTIVE];
		}
		if ("created_at".equalsIgnoreCase(columnName)) {
			return record[IDX_CREATED_AT];
		}
		if ("updated_at".equalsIgnoreCase(columnName)) {
			return record[IDX_UPDATED_AT];
		}
		if ("line1".equalsIgnoreCase(columnName)) {
			return record[IDX_LINE1];
		}
		if ("line2".equalsIgnoreCase(columnName)) {
			return record[IDX_LINE2];
		}
		if ("line3".equalsIgnoreCase(columnName)) {
			return record[IDX_LINE3];
		}
		if ("city".equalsIgnoreCase(columnName)) {
			return record[IDX_CITY];
		}
		if ("country".equalsIgnoreCase(columnName)) {
			return record[IDX_COUNTRY];
		}
		if ("country_code".equalsIgnoreCase(columnName)) {
			return record[IDX_COUNTRY_CODE];
		}
		if ("state".equalsIgnoreCase(columnName)) {
			return record[IDX_STATE];
		}
		if ("state_code".equalsIgnoreCase(columnName)) {
			return record[IDX_STATE_CODE];
		}
		if ("updated_by".equalsIgnoreCase(columnName)) {
			return record[IDX_UPDATED_BY];
		}
		if ("created_by".equalsIgnoreCase(columnName)) {
			return record[IDX_CREATED_BY];
		}
		if ("pincode".equalsIgnoreCase(columnName)) {
			return record[IDX_PINCODE];
		}
		if ("address_type".equalsIgnoreCase(columnName)) {
			return record[IDX_ADDRESS_TYPE];
		}
		if ("same_as_correspondence_address".equalsIgnoreCase(columnName)) {
			return record[IDX_SAME_AS_CORR];
		}
		if ("metadata".equalsIgnoreCase(columnName)) {
			return record[IDX_METADATA];
		}
		return null;
	}

	private String extractMetadataValue(String metadataJson, String key) {
		if (metadataJson == null || key == null) {
			return null;
		}

		String needle = "\"" + key + "\":\"";
		int start = metadataJson.indexOf(needle);
		if (start < 0) {
			return null;
		}

		start = start + needle.length();

		StringBuilder sb = new StringBuilder();
		boolean escape = false;

		for (int i = start; i < metadataJson.length(); i++) {
			char c = metadataJson.charAt(i);

			if (escape) {
				sb.append(c);
				escape = false;
				continue;
			}

			if (c == '\\') {
				escape = true;
				continue;
			}

			if (c == '"') {
				break;
			}

			sb.append(c);
		}

		String val = sb.toString().trim();
		return val.isEmpty() ? null : val;
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

			String sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";

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
			log("ERROR", "Exception during table existence check | Table: " + tableName + " | Error: " + ex.getMessage(),
					null, "VALIDATION");
		} finally {
			closeQuietly(conn);
		}

		return exists;
	}

	private HashSet<String> getTableColumns(String tableName, String dbURL, String dbUser, String dbPass) {

		Connection conn = null;
		HashSet<String> columns = new HashSet<String>();

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

		HashMap<String, String> columnTypes = new HashMap<String, String>();

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
						log("DEBUG", "Column type resolved | Column: " + col + " | Type: " + type + " | Table: "
								+ tableName, null, "VALIDATION");
					}
				}
			}

		} catch (Exception ex) {
			log("ERROR", "Failed to fetch destination column types | Table: " + tableName + " | Error: "
					+ ex.getMessage(), null, "VALIDATION");
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
				} else if (t.contains("numeric") || t.contains("decimal") || t.contains("float") || t.contains("double")) {
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
				} else if (t.contains("numeric") || t.contains("decimal") || t.contains("float") || t.contains("double")) {
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
						// Support date-only values like yyyy-MM-dd
						try {
							ps.setTimestamp(paramIndex, java.sql.Timestamp.valueOf(value.trim() + " 00:00:00"));
						} catch (IllegalArgumentException ex2) {
							log("ERROR",
									"Timestamp parse failure | Column: " + columnName + " | Expected type: " + columnType
											+ " | Received value: " + value + " | Expected format: YYYY-MM-DD HH:MM:SS",
									null, "DATA_UPDATE");
							throw new Exception("Data type mismatch: Column '" + columnName
									+ "' expects timestamp but got '" + value + "'");
						}
					}
				} else if (t.contains("date")) {
					try {
						ps.setDate(paramIndex, java.sql.Date.valueOf(value.trim()));
					} catch (IllegalArgumentException e) {
						if (value.trim().length() >= 10) {
							try {
								ps.setDate(paramIndex, java.sql.Date.valueOf(value.trim().substring(0, 10)));
							} catch (IllegalArgumentException ex2) {
								log("ERROR",
										"Date parse failure | Column: " + columnName + " | Expected type: "
												+ columnType + " | Received value: " + value,
										null, "DATA_UPDATE");
								throw new Exception("Data type mismatch: Column '" + columnName
										+ "' expects date but got '" + value + "'");
							}
						} else {
							log("ERROR",
									"Date parse failure | Column: " + columnName + " | Expected type: " + columnType
											+ " | Received value: " + value,
									null, "DATA_UPDATE");
							throw new Exception(
									"Data type mismatch: Column '" + columnName + "' expects date but got '" + value + "'");
						}
					}
				} else if (t.contains("time")) {
					try {
						ps.setTime(paramIndex, java.sql.Time.valueOf(value.trim()));
					} catch (IllegalArgumentException e) {
						log("ERROR", "Time parse failure | Column: " + columnName + " | Expected type: " + columnType
								+ " | Received value: " + value, null, "DATA_UPDATE");
						throw new Exception(
								"Data type mismatch: Column '" + columnName + "' expects time but got '" + value + "'");
					}
				} else {
					ps.setString(paramIndex, value);
				}
			} catch (NumberFormatException e) {
				log("ERROR", "Number format parse failure | Column: " + columnName + " | Expected type: " + columnType
						+ " | Received value: " + value, null, "DATA_UPDATE");
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