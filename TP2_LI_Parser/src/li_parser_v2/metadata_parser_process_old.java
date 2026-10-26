package li_parser_v2;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * ProcessData - PostgreSQL Data Parser and Transfer
 * 
 * Business Logic Flow: 1. Parse and validate user arguments 2. Validate source
 * and destination tables exist 3. Validate column structures 4. Fetch data from
 * source table 5. Transform data (direct columns + JSON metadata) 6. Insert
 * into destination table
 */
public class metadata_parser_process_old {

	PrintUtilMsg pu = new PrintUtilMsg();

	// Configuration flag for future enhancement
	private static final boolean SKIP_MISSING_COLUMNS = false; // Set to true to
																// skip missing
																// columns
																// instead of
																// stopping

	/**
	 * Main processing method
	 */
	public boolean parseAndTransferData(String dbURL, String dbUsername, String dbPassword, String sourceInput,
			String destTableName, String directColumns, int metadataCount, String destMetadataColumns,
			String sourceMetadataColumns, String jsonKeyNames) {

		boolean status = false;

		try {
			pu.sop("parseAndTransferData", "===== START CSV/DB PROCESSING =====");

			boolean isCSV = sourceInput != null && sourceInput.toLowerCase().endsWith(".csv");

			// ===== STEP 1: Parse arguments =====
			String[] directColArray = parseCommaSeparated(directColumns);
			String[] destMetaColArray = parseCommaSeparated(destMetadataColumns);
			String[] sourceMetaGroups = parseHashSeparated(sourceMetadataColumns);
			String[] jsonKeyGroups = parseHashSeparated(jsonKeyNames);

			List<String[]> sourceMetaColumnsList = new ArrayList<>();
			List<String[]> jsonKeyNamesList = new ArrayList<>();

			for (int i = 0; i < metadataCount; i++) {
				String[] sourceCols = parseCommaSeparated(sourceMetaGroups[i]);
				String[] jsonKeys = parseCommaSeparated(jsonKeyGroups[i]);

				if (sourceCols.length != jsonKeys.length) {
					pu.sop("parseAndTransferData", "ERROR: Metadata mismatch");
					return false;
				}

				sourceMetaColumnsList.add(sourceCols);
				jsonKeyNamesList.add(jsonKeys);
			}

			// ===== STEP 2: Validate destination table only =====
			if (!validateTableExists(destTableName, dbURL, dbUsername, dbPassword)) {
				pu.sop("parseAndTransferData", "ERROR: Destination table not found");
				return false;
			}

			HashSet<String> destTableColumns = getTableColumns(destTableName, dbURL, dbUsername, dbPassword);

			for (String col : directColArray) {
				if (!destTableColumns.contains(col.toLowerCase())) {
					pu.sop("parseAndTransferData", "ERROR: Direct column missing in destination: " + col);
					return false;
				}
			}

			for (String col : destMetaColArray) {
				if (!destTableColumns.contains(col.toLowerCase())) {
					pu.sop("parseAndTransferData", "ERROR: Metadata column missing in destination: " + col);
					return false;
				}
			}

			// ===== STEP 3: Fetch Data (CSV or DB) =====
			HashMap<String, String[]> sourceData;

			if (isCSV) {
				pu.sop("parseAndTransferData", "Reading from CSV: " + sourceInput);
				sourceData = readCSV(sourceInput);
			} else {
				pu.sop("parseAndTransferData", "Reading from DB: " + sourceInput);
				sourceData = new metadata_parser_source().fetchSourceData(sourceInput, directColArray,
						sourceMetaColumnsList, dbURL, dbUsername, dbPassword);
			}

			if (sourceData == null) {
				pu.sop("parseAndTransferData", "No data found (NULL)");
				return false;
			}

			if (sourceData.isEmpty()) {
				pu.sop("parseAndTransferData", "Empty file  skipping insert but marking SUCCESS");
				return true;
			}

			pu.sop("parseAndTransferData", "Records fetched: " + sourceData.size());

			// ===== STEP 4: Insert (UNCHANGED LOGIC) =====
			boolean inserted = insertDataToDestination(sourceData, destTableName, directColArray, destMetaColArray,
					sourceMetaColumnsList, jsonKeyNamesList, dbURL, dbUsername, dbPassword);

			if (!inserted) {
				pu.sop("parseAndTransferData", "Insert failed");
				return false;
			}

			pu.sop("parseAndTransferData", "SUCCESS");

			status = true;

		} catch (Exception ex) {
			System.out.println("EXCEPTION in parseAndTransferData: " + ex.toString());
			ex.printStackTrace();
			status = false;
		}

		return status;
	}

	private String[] parsePipeCsvLine(String line) {

		List<String> fields = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		boolean inQuotes = false;

		for (int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);

			if (c == '"') {
				// handle escaped double quote ""
				if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
					sb.append('"');
					i++; // skip next quote
				} else {
					inQuotes = !inQuotes;
				}
			} else if (c == '|' && !inQuotes) {
				fields.add(cleanValue(sb.toString()));
				sb.setLength(0);
			} else {
				sb.append(c);
			}
		}

		fields.add(cleanValue(sb.toString()));

		return fields.toArray(new String[0]);
	}

	private String cleanValue(String val) {
		if (val == null)
			return null;

		val = val.trim();

		if (val.equalsIgnoreCase("NULL") || val.isEmpty()) {
			return null;
		}

		return val;
	}

	private HashMap<String, String[]> readCSV(String filePath) {

		HashMap<String, String[]> data = new java.util.LinkedHashMap<>();

		try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(filePath))) {

			String headerLine = br.readLine();
			if (headerLine == null) {
				pu.sop("readCSV", "Empty CSV file");
				return null;
			}

			// Parse headers safely
			String[] headers = parsePipeCsvLine(headerLine);
			pu.sop("readCSV", "CSV Columns Found: " + headers.length);

			String line;
			int count = 0;

			while ((line = br.readLine()) != null) {

				String[] rawValues = parsePipeCsvLine(line);

				// STRICT VALIDATION
				if (rawValues.length != headers.length) {

					pu.sop("readCSV", " Skipping row " + count + " | expected=" + headers.length + " | got="
							+ rawValues.length + " | raw=" + line);

					count++;
					continue; // DO NOT PROCESS THIS ROW
				}

				// CLEAN ROW
				String[] finalRow = new String[headers.length];

				for (int i = 0; i < headers.length; i++) {
					finalRow[i] = rawValues[i];
				}

				data.put(String.valueOf(count), finalRow);
				count++;
			}

			pu.sop("readCSV", "Total CSV rows read: " + data.size());

		} catch (Exception e) {
			System.out.println("EXCEPTION in readCSV: " + e.toString());
			e.printStackTrace();
			return null;
		}

		return data;
	}

	/**
	 * Parse comma-separated string into array
	 */
	private String[] parseCommaSeparated(String input) {
		if (input == null || input.trim().isEmpty()) {
			return new String[0];
		}

		String[] parts = input.split(",");
		for (int i = 0; i < parts.length; i++) {
			parts[i] = parts[i].trim();
		}

		return parts;
	}

	/**
	 * Parse hash-separated string into array
	 */
	private String[] parseHashSeparated(String input) {
		if (input == null || input.trim().isEmpty()) {
			return new String[0];
		}

		String[] parts = input.split("#");
		for (int i = 0; i < parts.length; i++) {
			parts[i] = parts[i].trim();
		}

		return parts;
	}

	/**
	 * Validate if table exists
	 */
	private boolean validateTableExists(String tableName, String dbURL, String dbUser, String dbPass) {

		Connection conn = null;
		boolean exists = false;

		try {
			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

			String schemaName = "public";
			String actualTableName = tableName;

			if (tableName.contains(".")) {
				String[] parts = tableName.split("\\.");
				schemaName = parts[0].toLowerCase();
				actualTableName = parts[1].toLowerCase();
			} else {
				actualTableName = tableName.toLowerCase();
			}

			pu.sop("validateTableExists", "Checking table -> Schema: " + schemaName + ", Table: " + actualTableName);

			String sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";

			try (PreparedStatement ps = conn.prepareStatement(sql)) {
				ps.setString(1, schemaName);
				ps.setString(2, actualTableName);

				try (ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						exists = true;
						pu.sop("validateTableExists", " Table FOUND");
					} else {
						pu.sop("validateTableExists", " Table NOT FOUND");
					}
				}
			}

		} catch (Exception ex) {
			pu.sop("validateTableExists", "EXCEPTION: " + ex.toString());
			ex.printStackTrace();
			exists = false;
		} finally {
			try {
				if (conn != null)
					conn.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return exists;
	}

	/**
	 * Get all columns of a table
	 */
	private HashSet<String> getTableColumns(String tableName, String dbURL, String dbUser, String dbPass) {

		Connection conn = null;
		HashSet<String> columns = new HashSet<>();

		try {
			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
			DatabaseMetaData metaData = conn.getMetaData();

			// Handle schema-qualified table names (e.g.,
			// billing.gst_registration_raw)
			String schemaName = null;
			String actualTableName = tableName;

			if (tableName.contains(".")) {
				String[] parts = tableName.split("\\.");
				schemaName = parts[0].toLowerCase();
				actualTableName = parts[1].toLowerCase();
			} else {
				actualTableName = tableName.toLowerCase();
			}

			pu.sop("getTableColumns", "Getting columns - Schema: " + schemaName + ", Table: " + actualTableName);

			// PostgreSQL stores table names in lowercase by default
			ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, null);

			while (rs.next()) {
				String columnName = rs.getString("COLUMN_NAME");
				columns.add(columnName.toLowerCase());
				pu.sop("getTableColumns", "  Column: " + columnName);
			}

			rs.close();

		} catch (Exception ex) {
			System.out.println("EXCEPTION in getTableColumns: " + ex.toString());
			ex.printStackTrace();
			columns = null;
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return columns;
	}

	/**
	 * Validate data types can be converted to string
	 */
	private boolean validateDataTypes(String sourceTableName, String destTableName, String[] directColumns,
			String[] destMetaColumns, String dbURL, String dbUser, String dbPass) {

		Connection conn = null;
		boolean valid = true;

		try {
			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
			DatabaseMetaData metaData = conn.getMetaData();

			// Check direct columns in both tables
			for (String col : directColumns) {
				// Check source
				String sourceType = getColumnType(metaData, sourceTableName.toLowerCase(), col.toLowerCase());
				if (sourceType == null) {
					pu.sop("validateDataTypes", "✗ ERROR: Could not determine type for source column '" + col + "'");
					valid = false;
					continue;
				}

				// Check destination
				String destType = getColumnType(metaData, destTableName.toLowerCase(), col.toLowerCase());
				if (destType == null) {
					pu.sop("validateDataTypes", "✗ ERROR: Could not determine type for dest column '" + col + "'");
					valid = false;
					continue;
				}

				// Validate can convert to string
				if (!isConvertibleToString(sourceType)) {
					pu.sop("validateDataTypes", "✗ ERROR: Source column '" + col + "' type '" + sourceType
							+ "' cannot be converted to string");
					valid = false;
					continue;
				}

				if (!isConvertibleToString(destType)) {
					pu.sop("validateDataTypes", "✗ ERROR: Dest column '" + col + "' type '" + destType
							+ "' cannot be converted to string");
					valid = false;
					continue;
				}

				pu.sop("validateDataTypes", "  ✓ Column '" + col + "': " + sourceType + " -> " + destType);
			}

			// Check destination metadata columns (should be text/varchar/jsonb)
			for (String col : destMetaColumns) {
				String destType = getColumnType(metaData, destTableName.toLowerCase(), col.toLowerCase());
				if (destType == null) {
					pu.sop("validateDataTypes",
							"✗ ERROR: Could not determine type for dest metadata column '" + col + "'");
					valid = false;
					continue;
				}

				if (!isTextType(destType)) {
					pu.sop("validateDataTypes", "⚠ WARNING: Dest metadata column '" + col + "' type '" + destType
							+ "' is not text/varchar/jsonb - JSON may not fit");
				}

				pu.sop("validateDataTypes", "  ✓ Metadata column '" + col + "': " + destType);
			}

		} catch (Exception ex) {
			System.out.println("EXCEPTION in validateDataTypes: " + ex.toString());
			ex.printStackTrace();
			valid = false;
		} finally {
			try {
				if (conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return valid;
	}

	/**
	 * Get column data type
	 */
	private String getColumnType(DatabaseMetaData metaData, String tableName, String columnName) throws Exception {

		// Handle schema-qualified table names
		String schemaName = null;
		String actualTableName = tableName;

		if (tableName.contains(".")) {
			String[] parts = tableName.split("\\.");
			schemaName = parts[0].toLowerCase();
			actualTableName = parts[1].toLowerCase();
		} else {
			actualTableName = tableName.toLowerCase();
		}

		ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, columnName);

		String type = null;
		if (rs.next()) {
			type = rs.getString("TYPE_NAME");
		}

		rs.close();

		return type;
	}

	/**
	 * Check if data type can be converted to string
	 */
	private boolean isConvertibleToString(String dataType) {

		if (dataType == null) {
			return false;
		}

		String type = dataType.toLowerCase();

		// PostgreSQL types that can be converted to string
		return type.contains("char") || type.contains("text") || type.contains("int") || type.contains("numeric")
				|| type.contains("decimal") || type.contains("float") || type.contains("double")
				|| type.contains("real") || type.contains("bool") || type.contains("date") || type.contains("time")
				|| type.contains("json") || type.contains("uuid") || type.contains("serial") || type.contains("money");
	}

	/**
	 * Check if data type is text-based (for JSON storage)
	 */
	private boolean isTextType(String dataType) {

		if (dataType == null) {
			return false;
		}

		String type = dataType.toLowerCase();

		return type.contains("char") || type.contains("text") || type.contains("json");
	}

	/**
	 * Insert data into destination table
	 */
	private boolean insertDataToDestination(HashMap<String, String[]> sourceData, String destTableName,
			String[] directColumns, String[] destMetaColumns, List<String[]> sourceMetaColumnsList,
			List<String[]> jsonKeyNamesList, String dbURL, String dbUser, String dbPass) {

		Connection conn = null;
		PreparedStatement ps = null;
		boolean status = false;
		int batchSize = 200;
		int rowCount = 0;

		try {
			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);

			// Get column types for destination table
			HashMap<String, String> columnTypes = getDestinationColumnTypes(destTableName, directColumns,
					destMetaColumns, conn);

			if (columnTypes == null || columnTypes.size() == 0) {
				pu.sop("insertDataToDestination", "✗ ERROR: Could not fetch destination column types");
				return false;
			}

			// Build INSERT query (table name used as-is for INSERT, PostgreSQL
			// will handle schema)
			StringBuilder insertQuery = new StringBuilder("INSERT INTO " + destTableName + " (");

			// Add direct columns
			for (int i = 0; i < directColumns.length; i++) {
				insertQuery.append(directColumns[i]);
				if (i < directColumns.length - 1 || destMetaColumns.length > 0) {
					insertQuery.append(", ");
				}
			}

			// Add metadata columns
			for (int i = 0; i < destMetaColumns.length; i++) {
				insertQuery.append(destMetaColumns[i]);
				if (i < destMetaColumns.length - 1) {
					insertQuery.append(", ");
				}
			}

			insertQuery.append(") VALUES (");

			// Add placeholders
			int totalColumns = directColumns.length + destMetaColumns.length;
			for (int i = 0; i < totalColumns; i++) {
				insertQuery.append("?");
				if (i < totalColumns - 1) {
					insertQuery.append(", ");
				}
			}

			insertQuery.append(")");

			pu.sop("insertDataToDestination", "Insert Query: " + insertQuery.toString());

			conn.setAutoCommit(false);
			ps = conn.prepareStatement(insertQuery.toString());

			// Get column count for source data array
			int directColCount = directColumns.length;

			// Process each row
			boolean firstRowLogged = false;

			for (Map.Entry<String, String[]> entry : sourceData.entrySet()) {

				String[] record = entry.getValue();

				// ================= DEBUG FIRST ROW =================
				if (!firstRowLogged) {
					pu.sop("DEBUG_FIRST_ROW", java.util.Arrays.toString(record));

					for (int i = 0; i < directColumns.length; i++) {
						String val = (i < record.length) ? record[i] : "OUT_OF_RANGE";
						pu.sop("COLUMN_MAPPING", "Index " + i + " -> " + directColumns[i] + " = " + val);
					}

					firstRowLogged = true;
				}
				// ================= HARD VALIDATION =================
				if (!validateRow(record, directColumns, columnTypes)) {
					pu.sop("ROW_WARNING", "Validation failed, inserting anyway: " + java.util.Arrays.toString(record));
				}

				int paramIndex = 1;

				// ================= DIRECT COLUMNS =================
				for (int i = 0; i < directColumns.length; i++) {

					String columnName = directColumns[i].toLowerCase();
					String value = (i < record.length) ? record[i] : null;
					String columnType = columnTypes.get(columnName);

					setParameterByType(ps, paramIndex, value, columnType, columnName);
					paramIndex++;
				}

				// ================= METADATA =================
				int recordIndex = directColumns.length;

				for (int i = 0; i < destMetaColumns.length; i++) {

					String[] sourceCols = sourceMetaColumnsList.get(i);
					String[] jsonKeys = jsonKeyNamesList.get(i);

					StringBuilder jsonBuilder = new StringBuilder("{");

					for (int j = 0; j < sourceCols.length; j++) {

						String key = jsonKeys[j];
						String value = (recordIndex < record.length) ? record[recordIndex] : null;

						if (value != null) {
							value = value.replace("\\", "\\\\").replace("\"", "\\\"");
						}

						jsonBuilder.append("\"").append(key).append("\":");

						if (value != null) {
							jsonBuilder.append("\"").append(value).append("\"");
						} else {
							jsonBuilder.append("null");
						}

						if (j < sourceCols.length - 1) {
							jsonBuilder.append(",");
						}

						recordIndex++;
					}

					jsonBuilder.append("}");

					String columnName = destMetaColumns[i].toLowerCase();
					String columnType = columnTypes.get(columnName);

					if (columnType != null && columnType.toLowerCase().contains("jsonb")) {
						ps.setObject(paramIndex, jsonBuilder.toString(), java.sql.Types.OTHER);
					} else {
						ps.setString(paramIndex, jsonBuilder.toString());
					}

					paramIndex++;
				}

				ps.addBatch();
				rowCount++;

				if (rowCount % batchSize == 0) {
					ps.executeBatch();
					pu.sop("insertDataToDestination", "Batch executed - Rows processed: " + rowCount);
				}
			}

			// Execute remaining batch
			if (conn != null && ps != null) {
				ps.executeBatch();
				conn.commit();
				status = true;

				pu.sop("insertDataToDestination", "===== ALL DATA INSERTED SUCCESSFULLY =====");
				pu.sop("insertDataToDestination", "Total rows inserted: " + rowCount);
			}

		} catch (Exception ex) {
			System.out.println("EXCEPTION in insertDataToDestination: " + ex.toString());
			ex.printStackTrace();

			try {
				if (conn != null)
					conn.rollback();
			} catch (Exception e) {
				e.printStackTrace();
			}

			status = false;

		} finally {
			try {
				if (conn != null) {
					if (ps != null)
						ps.close();
					conn.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return status;
	}

	/**
	 * Get destination column types
	 */
	private HashMap<String, String> getDestinationColumnTypes(String tableName, String[] directColumns,
			String[] metaColumns, Connection conn) {

		HashMap<String, String> columnTypes = new HashMap<>();

		try {
			DatabaseMetaData metaData = conn.getMetaData();

			// Handle schema-qualified table names
			String schemaName = null;
			String actualTableName = tableName;

			if (tableName.contains(".")) {
				String[] parts = tableName.split("\\.");
				schemaName = parts[0].toLowerCase();
				actualTableName = parts[1].toLowerCase();
			} else {
				actualTableName = tableName.toLowerCase();
			}

			// Get all columns we need
			String[] allColumns = new String[directColumns.length + metaColumns.length];
			System.arraycopy(directColumns, 0, allColumns, 0, directColumns.length);
			System.arraycopy(metaColumns, 0, allColumns, directColumns.length, metaColumns.length);

			for (String colName : allColumns) {
				ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, colName.toLowerCase());

				if (rs.next()) {
					String dataType = rs.getString("TYPE_NAME");
					columnTypes.put(colName.toLowerCase(), dataType);
					pu.sop("getDestinationColumnTypes", "Column: " + colName + " -> Type: " + dataType);
				}

				rs.close();
			}

		} catch (Exception ex) {
			System.out.println("EXCEPTION in getDestinationColumnTypes: " + ex.toString());
			ex.printStackTrace();
			return null;
		}

		return columnTypes;
	}

	/**
	 * Set PreparedStatement parameter based on column type
	 */
	private void setParameterByType(PreparedStatement ps, int paramIndex, String value, String columnType,
			String columnName) throws Exception {

		if (value == null || value.trim().isEmpty() || value.equalsIgnoreCase("NULL")) {
			ps.setNull(paramIndex, java.sql.Types.NULL);
			return;
		}

		String val = trimToColumnSize(value.trim(), columnType);

		if (columnType == null) {
			ps.setString(paramIndex, val);
			return;
		}

		String type = columnType.toLowerCase();

		try {

			if (type.contains("int")) {
				if (!val.matches("-?\\d+")) {
					pu.sop("TYPE_FIX", "Invalid INT '" + val + "' -> NULL (" + columnName + ")");
					ps.setNull(paramIndex, java.sql.Types.INTEGER);
					return;
				}
				ps.setInt(paramIndex, Integer.parseInt(val));
			}

			else if (type.contains("bigint")) {
				if (!val.matches("-?\\d+")) {
					ps.setNull(paramIndex, java.sql.Types.BIGINT);
					return;
				}
				ps.setLong(paramIndex, Long.parseLong(val));
			}

			else if (type.contains("numeric") || type.contains("decimal")) {
				if (!val.matches("-?\\d+(\\.\\d+)?")) {
					ps.setNull(paramIndex, java.sql.Types.NUMERIC);
					return;
				}
				ps.setBigDecimal(paramIndex, new java.math.BigDecimal(val));
			}

			else if (type.contains("date")) {

				if (val.equals("0")) {
					ps.setNull(paramIndex, java.sql.Types.DATE);
					return;
				}

				if (!val.matches("\\d{4}-\\d{2}-\\d{2}")) {
					ps.setNull(paramIndex, java.sql.Types.DATE);
					return;
				}

				ps.setDate(paramIndex, java.sql.Date.valueOf(val));
			}

			else if (type.contains("timestamp")) {
				if (!val.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
					ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
					return;
				}
				ps.setTimestamp(paramIndex, java.sql.Timestamp.valueOf(val));
			}

			else if (type.contains("bool")) {
				ps.setBoolean(paramIndex, Boolean.parseBoolean(val));
			}

			else {
				ps.setString(paramIndex, val);
			}

		} catch (Exception e) {
			pu.sop("TYPE_FIX", "Failed parse '" + val + "' -> NULL (" + columnName + ")");
			ps.setNull(paramIndex, java.sql.Types.NULL);
		}

	}

	private boolean validateRow(String[] record, String[] directColumns, HashMap<String, String> columnTypes) {

		for (int i = 0; i < directColumns.length; i++) {

			String col = directColumns[i].toLowerCase();
			String val = (i < record.length) ? record[i] : null;
			String type = columnTypes.get(col);

			if (val == null || val.trim().isEmpty() || val.equalsIgnoreCase("NULL")) {
				continue;
			}

			val = val.trim();

			try {

				// INT / BIGINT
				if (type.contains("int")) {
					if (!val.matches("-?\\d+")) {
						pu.sop("ROW_VALIDATION", "Invalid INT at col=" + col + " val=" + val);
						return false;
					}
				}

				// DECIMAL
				else if (type.contains("numeric") || type.contains("decimal")) {
					if (!val.matches("-?\\d+(\\.\\d+)?")) {
						pu.sop("ROW_VALIDATION", "Invalid DECIMAL at col=" + col + " val=" + val);
						return false;
					}
				}

				// DATE
				else if (type.contains("date")) {

					if (val.equals("0")) {
						continue; // treat as NULL
					}

					if (!val.matches("\\d{4}-\\d{2}-\\d{2}")) {
						pu.sop("ROW_VALIDATION", "Invalid DATE at col=" + col + " val=" + val);
						return false;
					}
				}

				// TIMESTAMP
				else if (type.contains("timestamp")) {
					if (!val.matches("\\d{4}-\\d{2}-\\d{2}.*")) {
						pu.sop("ROW_VALIDATION", "Invalid TIMESTAMP at col=" + col + " val=" + val);
						return false;
					}
				}

			} catch (Exception e) {
				return false;
			}
		}

		return true;
	}

	private String trimToColumnSize(String value, String columnType) {

		if (value == null || columnType == null)
			return value;

		// Extract varchar size (varchar(1), varchar(10), etc.)
		try {
			if (columnType.toLowerCase().contains("varchar")) {
				int start = columnType.indexOf("(");
				int end = columnType.indexOf(")");

				if (start > 0 && end > start) {
					int size = Integer.parseInt(columnType.substring(start + 1, end));
					if (value.length() > size) {
						return value.substring(0, size);
					}
				}
			}
		} catch (Exception e) {
			// ignore safely
		}

		return value;
	}
}