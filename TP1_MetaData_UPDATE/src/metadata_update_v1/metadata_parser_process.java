//package metadata_update_v1;
//
//import java.sql.Connection;
//import java.sql.DatabaseMetaData;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//
///**
// * ProcessData - PostgreSQL Data Parser and Transfer
// * 
// * Business Logic Flow: 1. Parse and validate user arguments 2. Validate source
// * and destination tables exist 3. Validate column structures 4. Fetch data from
// * source table 5. Transform data (direct columns + JSON metadata) 6. Insert
// * into destination table
// */
//public class metadata_parser_process {
//
//	PrintUtilMsg pu = new PrintUtilMsg();
//
//	// Configuration flag for future enhancement
//	private static final boolean SKIP_MISSING_COLUMNS = false; // Set to true to
//																// skip missing
//																// columns
//																// instead of
//																// stopping
//
//	/**
//	 * Main processing method
//	 */
//	public boolean parseAndTransferData(String dbURL, String dbUsername, String dbPassword, String sourceInput,
//			String destTableName, String directColumns, int metadataCount, String destMetadataColumns,
//			String sourceMetadataColumns, String jsonKeyNames, String pkMapping) {
//
//		boolean status = false;
//
//		try {
//			pu.sop("parseAndTransferData", "===== START CSV/DB PROCESSING =====");
//
//			boolean isCSV = sourceInput != null && sourceInput.toLowerCase().endsWith(".csv");
//
//			// ===== STEP 1: Parse arguments =====
//			String[] directColArray = parseCommaSeparated(directColumns);
//			String[] destMetaColArray = parseCommaSeparated(destMetadataColumns);
//			String[] sourceMetaGroups = parseHashSeparated(sourceMetadataColumns);
//			String[] jsonKeyGroups = parseHashSeparated(jsonKeyNames);
//
//			List<String[]> sourceMetaColumnsList = new ArrayList<>();
//			List<String[]> jsonKeyNamesList = new ArrayList<>();
//
//			for (int i = 0; i < metadataCount; i++) {
//				String[] sourceCols = parseCommaSeparated(sourceMetaGroups[i]);
//				String[] jsonKeys = parseCommaSeparated(jsonKeyGroups[i]);
//
//				if (sourceCols.length != jsonKeys.length) {
//					pu.sop("parseAndTransferData", "ERROR: Metadata mismatch");
//					return false;
//				}
//
//				sourceMetaColumnsList.add(sourceCols);
//				jsonKeyNamesList.add(jsonKeys);
//			}
//
//			// ===== STEP 2: Validate destination table only =====
//			if (!validateTableExists(destTableName, dbURL, dbUsername, dbPassword)) {
//				pu.sop("parseAndTransferData", "ERROR: Destination table not found");
//				return false;
//			}
//
//			HashSet<String> destTableColumns = getTableColumns(destTableName, dbURL, dbUsername, dbPassword);
//
//			for (String col : directColArray) {
//				if (!destTableColumns.contains(col.toLowerCase())) {
//					pu.sop("parseAndTransferData", "ERROR: Direct column missing in destination: " + col);
//					return false;
//				}
//			}
//
//			for (String col : destMetaColArray) {
//				if (!destTableColumns.contains(col.toLowerCase())) {
//					pu.sop("parseAndTransferData", "ERROR: Metadata column missing in destination: " + col);
//					return false;
//				}
//			}
//
//			// ===== STEP 2b: Parse and validate PK mapping =====
//			// Format: "Csv_PK1,Csv_PK2 | Psql_PK1,Psql_PK2"
//			if (pkMapping == null || pkMapping.trim().isEmpty()) {
//				pu.sop("parseAndTransferData", "ERROR: pkMapping is required for UPDATE — none provided");
//				return false;
//			}
//
//			String[] pkSides = pkMapping.split("\\|", -1);
//			String[] csvPkCols  = parseCommaSeparated(pkSides[0]);
//			String[] psqlPkCols = pkSides.length > 1 ? parseCommaSeparated(pkSides[1]) : new String[0];
//
//			if (csvPkCols.length == 0 || psqlPkCols.length == 0) {
//				pu.sop("parseAndTransferData", "ERROR: pkMapping must specify both CSV and PSQL PK columns");
//				return false;
//			}
//			if (csvPkCols.length != psqlPkCols.length) {
//				pu.sop("parseAndTransferData", "ERROR: PK mapping side mismatch — CSV PKs: "
//						+ csvPkCols.length + ", PSQL PKs: " + psqlPkCols.length);
//				return false;
//			}
//
//			// Validate each PSQL PK column exists in destination.
//			// For metadata JSON PKs ("metadata:col4") validate only the base column ("metadata").
//			// For normal PKs ("customer_id") validate the column directly.
//			for (String pkCol : psqlPkCols) {
//				String colToValidate;
//				if (pkCol.contains(":")) {
//					// e.g. "metadata:col4" → check that "metadata" column exists
//					colToValidate = pkCol.split(":", 2)[0].trim().toLowerCase();
//					pu.sop("parseAndTransferData", "Metadata JSON PK detected: '" + pkCol
//							+ "' — validating base column '" + colToValidate + "'");
//				} else {
//					colToValidate = pkCol.toLowerCase();
//				}
//				if (!destTableColumns.contains(colToValidate)) {
//					pu.sop("parseAndTransferData", "ERROR: PK column missing in destination table: " + colToValidate);
//					return false;
//				}
//			}
//
//			// Resolve CSV PK column → index in record[].
//			// record[] layout: [directCols...] [metaGroup0Cols...] [metaGroup1Cols...] ...
//			// Search directColArray first; if not found, scan sourceMetaColumnsList offsets.
//			int[] csvPkIndices = new int[csvPkCols.length];
//			for (int i = 0; i < csvPkCols.length; i++) {
//				int idx = -1;
//
//				// 1. Search directColArray (positions 0 .. directColArray.length-1)
//				for (int j = 0; j < directColArray.length; j++) {
//					if (directColArray[j].equalsIgnoreCase(csvPkCols[i])) {
//						idx = j;
//						break;
//					}
//				}
//
//				// 2. If not found, search sourceMetaColumnsList (positions after direct cols)
//				if (idx == -1) {
//					int offset = directColArray.length;
//					outerSearch:
//					for (String[] metaCols : sourceMetaColumnsList) {
//						for (int j = 0; j < metaCols.length; j++) {
//							if (metaCols[j].equalsIgnoreCase(csvPkCols[i])) {
//								idx = offset + j;
//								break outerSearch;
//							}
//						}
//						offset += metaCols.length;
//					}
//				}
//
//				if (idx == -1) {
//					pu.sop("parseAndTransferData", "ERROR: CSV PK column '" + csvPkCols[i]
//							+ "' not found in directColumns or sourceMetaColumns — cannot resolve record index");
//					return false;
//				}
//				csvPkIndices[i] = idx;
//				pu.sop("parseAndTransferData", "PK Mapping: CSV[" + csvPkCols[i] + "] @ record[" + idx
//						+ "] → PSQL WHERE [" + psqlPkCols[i] + "]");
//			}
//
//			// ===== STEP 3: Fetch Data (CSV or DB) =====
//			HashMap<String, String[]> sourceData;
//
//			if (isCSV) {
//				pu.sop("parseAndTransferData", "Reading from CSV: " + sourceInput);
//				sourceData = readCSV(sourceInput);
//			} else {
//				pu.sop("parseAndTransferData", "Reading from DB: " + sourceInput);
//				sourceData = new metadata_parser_source().fetchSourceData(sourceInput, directColArray, sourceMetaColumnsList, dbURL,
//						dbUsername, dbPassword);
//			}
//
//			if (sourceData == null || sourceData.isEmpty()) {
//				pu.sop("parseAndTransferData", "No data found");
//				return false;
//			}
//
//			pu.sop("parseAndTransferData", "Records fetched: " + sourceData.size());
//
//			// ===== STEP 4: Update (INSERT replaced by UPDATE) =====
//			boolean inserted = updateDataToDestination(sourceData, destTableName, directColArray, destMetaColArray,
//					sourceMetaColumnsList, jsonKeyNamesList, dbURL, dbUsername, dbPassword,
//					csvPkIndices, psqlPkCols);
//
//			if (!inserted) {
//				pu.sop("parseAndTransferData", "Update failed");
//				return false;
//			}
//
//			pu.sop("parseAndTransferData", "SUCCESS");
//
//			status = true;
//
//		} catch (Exception ex) {
//			System.out.println("EXCEPTION in parseAndTransferData: " + ex.toString());
//			ex.printStackTrace();
//			status = false;
//		}
//
//		return status;
//	}
//	private boolean isPkColumn(String col, String[] psqlPkCols) {
//	    for (String pk : psqlPkCols) {
//	        if (!pk.contains(":") && pk.equalsIgnoreCase(col)) {
//	            return true;
//	        }
//	    }
//	    return false;
//	}
//	private HashMap<String, String[]> readCSV(String filePath) {
//
//	    HashMap<String, String[]> data = new java.util.LinkedHashMap<>();
//
//	    try (java.io.BufferedReader br = new java.io.BufferedReader(new java.io.FileReader(filePath))) {
//
//	        String headerLine = br.readLine();
//	        if (headerLine == null) {
//	            pu.sop("readCSV", "Empty CSV file");
//	            return null;
//	        }
//
//	        // Parse headers safely
//	        String[] headers = headerLine.split(",", -1);
//	        pu.sop("readCSV", "CSV Columns Found: " + headers.length);
//
//	        String line;
//	        int count = 0;
//
//	        while ((line = br.readLine()) != null) {
//
//	            java.util.List<String> valuesList = new java.util.ArrayList<>();
//	            StringBuilder sb = new StringBuilder();
//	            boolean inQuotes = false;
//
//	            for (char c : line.toCharArray()) {
//	                if (c == '"') {
//	                    inQuotes = !inQuotes;
//	                } else if (c == ',' && !inQuotes) {
//	                    valuesList.add(sb.toString());
//	                    sb.setLength(0);
//	                } else {
//	                    sb.append(c);
//	                }
//	            }
//	            valuesList.add(sb.toString());
//
//	            String[] rawValues = valuesList.toArray(new String[0]);
//
//	            // Normalize row length to header length
//	            String[] finalRow = new String[headers.length];
//
//	            for (int i = 0; i < headers.length; i++) {
//	                if (i < rawValues.length) {
//	                    finalRow[i] = rawValues[i];
//	                } else {
//	                    finalRow[i] = null;
//	                }
//	            }
//
//	            if (rawValues.length != headers.length) {
//	                pu.sop("readCSV", "Column mismatch at row " + count +
//	                        " | expected: " + headers.length +
//	                        " | got: " + rawValues.length);
//	            }
//
//	            data.put(String.valueOf(count), finalRow);
//	            count++;
//	        }
//
//	        pu.sop("readCSV", "Total CSV rows read: " + data.size());
//
//	    } catch (Exception e) {
//	        System.out.println("EXCEPTION in readCSV: " + e.toString());
//	        e.printStackTrace();
//	        return null;
//	    }
//
//	    return data;
//	}
//	/**
//	 * Parse comma-separated string into array
//	 */
//	private String[] parseCommaSeparated(String input) {
//		if (input == null || input.trim().isEmpty()) {
//			return new String[0];
//		}
//
//		String[] parts = input.split(",");
//		for (int i = 0; i < parts.length; i++) {
//			parts[i] = parts[i].trim();
//		}
//
//		return parts;
//	}
//
//	/**
//	 * Parse hash-separated string into array
//	 */
//	private String[] parseHashSeparated(String input) {
//		if (input == null || input.trim().isEmpty()) {
//			return new String[0];
//		}
//
//		String[] parts = input.split("#");
//		for (int i = 0; i < parts.length; i++) {
//			parts[i] = parts[i].trim();
//		}
//
//		return parts;
//	}
//
//	/**
//	 * Validate if table exists
//	 */
//	private boolean validateTableExists(String tableName, String dbURL, String dbUser, String dbPass) {
//
//	    Connection conn = null;
//	    boolean exists = false;
//
//	    try {
//	        conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
//
//	        String schemaName = "public";
//	        String actualTableName = tableName;
//
//	        if (tableName.contains(".")) {
//	            String[] parts = tableName.split("\\.");
//	            schemaName = parts[0].toLowerCase();
//	            actualTableName = parts[1].toLowerCase();
//	        } else {
//	            actualTableName = tableName.toLowerCase();
//	        }
//
//	        pu.sop("validateTableExists",
//	                "Checking table -> Schema: " + schemaName + ", Table: " + actualTableName);
//
//	        String sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?";
//
//	        try (PreparedStatement ps = conn.prepareStatement(sql)) {
//	            ps.setString(1, schemaName);
//	            ps.setString(2, actualTableName);
//
//	            try (ResultSet rs = ps.executeQuery()) {
//	                if (rs.next()) {
//	                    exists = true;
//	                    pu.sop("validateTableExists", " Table FOUND");
//	                } else {
//	                    pu.sop("validateTableExists", " Table NOT FOUND");
//	                }
//	            }
//	        }
//
//	    } catch (Exception ex) {
//	        pu.sop("validateTableExists", "EXCEPTION: " + ex.toString());
//	        ex.printStackTrace();
//	        exists = false;
//	    } finally {
//	        try {
//	            if (conn != null) conn.close();
//	        } catch (Exception e) {
//	            e.printStackTrace();
//	        }
//	    }
//
//	    return exists;
//	}
//	/**
//	 * Get all columns of a table
//	 */
//	private HashSet<String> getTableColumns(String tableName, String dbURL, String dbUser, String dbPass) {
//
//		Connection conn = null;
//		HashSet<String> columns = new HashSet<>();
//
//		try {
//			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
//			DatabaseMetaData metaData = conn.getMetaData();
//
//			// Handle schema-qualified table names (e.g.,
//			// billing.gst_registration_raw)
//			String schemaName = null;
//			String actualTableName = tableName;
//
//			if (tableName.contains(".")) {
//				String[] parts = tableName.split("\\.");
//				schemaName = parts[0].toLowerCase();
//				actualTableName = parts[1].toLowerCase();
//			} else {
//				actualTableName = tableName.toLowerCase();
//			}
//
//			pu.sop("getTableColumns", "Getting columns - Schema: " + schemaName + ", Table: " + actualTableName);
//
//			// PostgreSQL stores table names in lowercase by default
//			ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, null);
//
//			while (rs.next()) {
//				String columnName = rs.getString("COLUMN_NAME");
//				columns.add(columnName.toLowerCase());
//				pu.sop("getTableColumns", "  Column: " + columnName);
//			}
//
//			rs.close();
//
//		} catch (Exception ex) {
//			System.out.println("EXCEPTION in getTableColumns: " + ex.toString());
//			ex.printStackTrace();
//			columns = null;
//		} finally {
//			try {
//				if (conn != null) {
//					conn.close();
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//
//		return columns;
//	}
//
//	/**
//	 * Validate data types can be converted to string
//	 */
//	private boolean validateDataTypes(String sourceTableName, String destTableName, String[] directColumns,
//			String[] destMetaColumns, String dbURL, String dbUser, String dbPass) {
//
//		Connection conn = null;
//		boolean valid = true;
//
//		try {
//			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
//			DatabaseMetaData metaData = conn.getMetaData();
//
//			// Check direct columns in both tables
//			for (String col : directColumns) {
//				// Check source
//				String sourceType = getColumnType(metaData, sourceTableName.toLowerCase(), col.toLowerCase());
//				if (sourceType == null) {
//					pu.sop("validateDataTypes", "✗ ERROR: Could not determine type for source column '" + col + "'");
//					valid = false;
//					continue;
//				}
//
//				// Check destination
//				String destType = getColumnType(metaData, destTableName.toLowerCase(), col.toLowerCase());
//				if (destType == null) {
//					pu.sop("validateDataTypes", "✗ ERROR: Could not determine type for dest column '" + col + "'");
//					valid = false;
//					continue;
//				}
//
//				// Validate can convert to string
//				if (!isConvertibleToString(sourceType)) {
//					pu.sop("validateDataTypes", "✗ ERROR: Source column '" + col + "' type '" + sourceType
//							+ "' cannot be converted to string");
//					valid = false;
//					continue;
//				}
//
//				if (!isConvertibleToString(destType)) {
//					pu.sop("validateDataTypes", "✗ ERROR: Dest column '" + col + "' type '" + destType
//							+ "' cannot be converted to string");
//					valid = false;
//					continue;
//				}
//
//				pu.sop("validateDataTypes", "  ✓ Column '" + col + "': " + sourceType + " -> " + destType);
//			}
//
//			// Check destination metadata columns (should be text/varchar/jsonb)
//			for (String col : destMetaColumns) {
//				String destType = getColumnType(metaData, destTableName.toLowerCase(), col.toLowerCase());
//				if (destType == null) {
//					pu.sop("validateDataTypes",
//							"✗ ERROR: Could not determine type for dest metadata column '" + col + "'");
//					valid = false;
//					continue;
//				}
//
//				if (!isTextType(destType)) {
//					pu.sop("validateDataTypes", "⚠ WARNING: Dest metadata column '" + col + "' type '" + destType
//							+ "' is not text/varchar/jsonb - JSON may not fit");
//				}
//
//				pu.sop("validateDataTypes", "  ✓ Metadata column '" + col + "': " + destType);
//			}
//
//		} catch (Exception ex) {
//			System.out.println("EXCEPTION in validateDataTypes: " + ex.toString());
//			ex.printStackTrace();
//			valid = false;
//		} finally {
//			try {
//				if (conn != null) {
//					conn.close();
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//
//		return valid;
//	}
//
//	/**
//	 * Get column data type
//	 */
//	private String getColumnType(DatabaseMetaData metaData, String tableName, String columnName) throws Exception {
//
//		// Handle schema-qualified table names
//		String schemaName = null;
//		String actualTableName = tableName;
//
//		if (tableName.contains(".")) {
//			String[] parts = tableName.split("\\.");
//			schemaName = parts[0].toLowerCase();
//			actualTableName = parts[1].toLowerCase();
//		} else {
//			actualTableName = tableName.toLowerCase();
//		}
//
//		ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, columnName);
//
//		String type = null;
//		if (rs.next()) {
//			type = rs.getString("TYPE_NAME");
//		}
//
//		rs.close();
//
//		return type;
//	}
//
//	/**
//	 * Check if data type can be converted to string
//	 */
//	private boolean isConvertibleToString(String dataType) {
//
//		if (dataType == null) {
//			return false;
//		}
//
//		String type = dataType.toLowerCase();
//
//		// PostgreSQL types that can be converted to string
//		return type.contains("char") || type.contains("text") || type.contains("int") || type.contains("numeric")
//				|| type.contains("decimal") || type.contains("float") || type.contains("double")
//				|| type.contains("real") || type.contains("bool") || type.contains("date") || type.contains("time")
//				|| type.contains("json") || type.contains("uuid") || type.contains("serial") || type.contains("money");
//	}
//
//	/**
//	 * Check if data type is text-based (for JSON storage)
//	 */
//	private boolean isTextType(String dataType) {
//
//		if (dataType == null) {
//			return false;
//		}
//
//		String type = dataType.toLowerCase();
//
//		return type.contains("char") || type.contains("text") || type.contains("json");
//	}
//
//	/**
//	 * Update data in destination table using batch UPDATE.
//	 *
//	 * Parameter binding order per row:
//	 *   1. SET  — all directColumns values  (positional from record[])
//	 *   2. SET  — all destMetaColumns values (JSON built from sourceMetaColumnsList)
//	 *   3. WHERE — each psqlPkCol value     (looked up via csvPkIndices into record[])
//	 *
//	 * Rows where ANY PK value is NULL or empty are skipped without failing the batch.
//	 */
//	private boolean updateDataToDestination(HashMap<String, String[]> sourceData, String destTableName,
//			String[] directColumns, String[] destMetaColumns, List<String[]> sourceMetaColumnsList,
//			List<String[]> jsonKeyNamesList, String dbURL, String dbUser, String dbPass,
//			int[] csvPkIndices, String[] psqlPkCols) {
//
//		Connection conn = null;
//		PreparedStatement ps = null;
//		boolean status = false;
//		int batchSize = 200;
//		int rowCount = 0;
//		int skippedCount = 0;
//
//		try {
//			conn = new PostgreSQLConnection().getConnection(dbURL, dbUser, dbPass);
//
//			// ── Fetch column types for SET columns (direct + meta) ──────────────
//			HashMap<String, String> columnTypes = getDestinationColumnTypes(destTableName, directColumns,
//					destMetaColumns, conn);
//
//			if (columnTypes == null || columnTypes.size() == 0) {
//				pu.sop("updateDataToDestination", "ERROR: Could not fetch destination column types");
//				return false;
//			}
//
//			// ── Fetch types for WHERE PK columns not already present ─────────────
//			// Skip metadata JSON PKs ("metadata:col4") — they resolve to text via ->>'',
//			// so no DB column type lookup is needed or valid for them.
//			DatabaseMetaData dbMeta = conn.getMetaData();
//			String pkSchemaName = null;
//			String pkActualTable = destTableName;
//			if (destTableName.contains(".")) {
//				String[] tParts = destTableName.split("\\.");
//				pkSchemaName   = tParts[0].toLowerCase();
//				pkActualTable  = tParts[1].toLowerCase();
//			} else {
//				pkActualTable  = destTableName.toLowerCase();
//			}
//
//			for (String pkCol : psqlPkCols) {
//				if (pkCol.contains(":")) {
//					// metadata:col4 — always bound as text; no DB lookup required
//					continue;
//				}
//				String pkColLower = pkCol.toLowerCase();
//				if (!columnTypes.containsKey(pkColLower)) {
//					try (ResultSet crs = dbMeta.getColumns(null, pkSchemaName, pkActualTable, pkColLower)) {
//						if (crs.next()) {
//							String fetchedType = crs.getString("TYPE_NAME");
//							columnTypes.put(pkColLower, fetchedType);
//							pu.sop("updateDataToDestination",
//									"Fetched PK WHERE column type: " + pkCol + " -> " + fetchedType);
//						} else {
//							pu.sop("updateDataToDestination",
//									"WARN: Could not resolve DB type for PK column: " + pkCol + " — will bind as VARCHAR");
//						}
//					}
//				}
//			}
//
//			// ── Build UPDATE query ───────────────────────────────────────────────
//			// Format: UPDATE tbl SET c1=?, c2=?, ..., meta1=?, meta2=? WHERE pk1=? AND pk2=?
//			StringBuilder updateQuery = new StringBuilder("UPDATE ").append(destTableName).append(" SET ");
//
//			// SET: direct columns
//			boolean first = true;
//
//			for (int i = 0; i < directColumns.length; i++) {
//			    if (isPkColumn(directColumns[i], psqlPkCols)) continue;
//
//			    if (!first) updateQuery.append(", ");
//			    updateQuery.append(directColumns[i]).append(" = ?");
//			    first = false;
//			}
//
//			for (int i = 0; i < destMetaColumns.length; i++) {
//			    if (!first) updateQuery.append(", ");
//			    updateQuery.append(destMetaColumns[i]).append(" = ?");
//			    first = false;
//			}
//
//			// WHERE: PK columns
//			// Normal PK:   customer_id      → customer_id = ?
//			// Metadata PK: metadata:col4    → metadata->>'col4' = ?
//			updateQuery.append(" WHERE ");
//			for (int i = 0; i < psqlPkCols.length; i++) {
//				String pkCol = psqlPkCols[i];
//				if (pkCol.contains(":")) {
//					// Split "metadata:col4" → base="metadata", key="col4"
//					String[] jsonParts = pkCol.split(":", 2);
//					String baseCol = jsonParts[0].trim();
//					String jsonKey = jsonParts[1].trim();
//					updateQuery.append(baseCol).append("->>'").append(jsonKey).append("' = ?");
//				} else {
//					updateQuery.append(pkCol).append(" = ?");
//				}
//				if (i < psqlPkCols.length - 1) {
//					updateQuery.append(" AND ");
//				}
//			}
//
//			pu.sop("updateDataToDestination", "Update Query: " + updateQuery.toString());
//
//			conn.setAutoCommit(false);
//			ps = conn.prepareStatement(updateQuery.toString());
//
//			int directColCount = directColumns.length;
//
//			// ── Process each row ─────────────────────────────────────────────────
//			for (Map.Entry<String, String[]> entry : sourceData.entrySet()) {
//				String[] record = entry.getValue();
//
//				// PK NULL/EMPTY guard — skip row entirely if any PK value is absent
//				boolean skipRow = false;
//				for (int i = 0; i < csvPkIndices.length; i++) {
//					int pkIdx = csvPkIndices[i];
//					String pkValue = (pkIdx < record.length) ? record[pkIdx] : null;
//					if (pkValue == null || pkValue.trim().isEmpty()) {
//						pu.sop("updateDataToDestination", "SKIP row [" + entry.getKey()
//								+ "]: NULL/empty PK value for PSQL column '" + psqlPkCols[i] + "'");
//						skipRow = true;
//						break;
//					}
//				}
//				if (skipRow) {
//					skippedCount++;
//					continue;
//				}
//
//				int paramIndex = 1;
//
//				for (int i = 0; i < directColumns.length; i++) {
//				    if (isPkColumn(directColumns[i], psqlPkCols)) continue;
//
//				    String columnName = directColumns[i].toLowerCase();
//				    String value      = (i < record.length) ? record[i] : null;
//				    String columnType = columnTypes.get(columnName);
//
//				    setParameterByType(ps, paramIndex, value, columnType, columnName);
//				    paramIndex++;
//				}
//
//				// ── BIND SET: metadata JSON columns ────────────────────────────
//				int recordIndex = directColCount;
//				for (int i = 0; i < destMetaColumns.length; i++) {
//					String[] sourceCols = sourceMetaColumnsList.get(i);
//					String[] jsonKeys   = jsonKeyNamesList.get(i);
//
//					StringBuilder jsonBuilder = new StringBuilder("{");
//					for (int j = 0; j < sourceCols.length; j++) {
//						String key   = jsonKeys[j];
//						String value = (recordIndex < record.length) ? record[recordIndex] : null;
//
//						String escapedValue = value;
//						if (escapedValue != null) {
//							escapedValue = escapedValue.replace("\\", "\\\\").replace("\"", "\\\"");
//						}
//
//						jsonBuilder.append("\"").append(key).append("\":");
//						if (escapedValue != null) {
//							jsonBuilder.append("\"").append(escapedValue).append("\"");
//						} else {
//							jsonBuilder.append("null");
//						}
//						if (j < sourceCols.length - 1) {
//							jsonBuilder.append(",");
//						}
//						recordIndex++;
//					}
//					jsonBuilder.append("}");
//
//					String columnName = destMetaColumns[i].toLowerCase();
//					String columnType = columnTypes.get(columnName);
//
//					if (columnType != null && columnType.toLowerCase().contains("jsonb")) {
//						ps.setObject(paramIndex, jsonBuilder.toString(), java.sql.Types.OTHER);
//					} else {
//						ps.setString(paramIndex, jsonBuilder.toString());
//					}
//					paramIndex++;
//				}
//
//				// ── BIND WHERE: PK columns ──────────────────────────────────────
//				// Normal PK   → setParameterByType (uses DB column type)
//				// Metadata PK → setString (metadata->>'key' always returns text)
//				for (int i = 0; i < psqlPkCols.length; i++) {
//					String pkCol  = psqlPkCols[i];
//					int    csvIdx = csvPkIndices[i];
//					String pkValue = (csvIdx < record.length) ? record[csvIdx] : null;
//
//					if (pkCol.contains(":")) {
//						// JSON path extraction always returns TEXT — bind as string
//						ps.setString(paramIndex, pkValue);
//					} else {
//						String psqlPkColLower = pkCol.toLowerCase();
//						String columnType     = columnTypes.get(psqlPkColLower);
//						setParameterByType(ps, paramIndex, pkValue, columnType, psqlPkColLower);
//					}
//					paramIndex++;
//				}
//
//				ps.addBatch();
//				rowCount++;
//
//				if (rowCount % batchSize == 0) {
//					int[] results = ps.executeBatch();
//
//					for (int r : results) {
//					    if (r == 0) {
//					        pu.sop("updateDataToDestination", 
//					            "WARN: No row updated (PK not found)");
//					    }
//					}
//					pu.sop("updateDataToDestination", "Batch executed - Rows processed: " + rowCount);
//				}
//			}
//
//			// Execute remaining batch
//			if (conn != null && ps != null) {
//				int[] results = ps.executeBatch();
//
//				for (int r : results) {
//				    if (r == 0) {
//				        pu.sop("updateDataToDestination", 
//				            "WARN: No row updated (PK not found)");
//				    }
//				}
//				conn.commit();
//				status = true;
//
//				pu.sop("updateDataToDestination", "===== ALL DATA UPDATED SUCCESSFULLY =====");
//				pu.sop("updateDataToDestination", "Total rows updated : " + rowCount);
//				pu.sop("updateDataToDestination", "Total rows skipped (null PK): " + skippedCount);
//			}
//
//		} catch (Exception ex) {
//			System.out.println("EXCEPTION in updateDataToDestination: " + ex.toString());
//			ex.printStackTrace();
//
//			try {
//				if (conn != null) conn.rollback();
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//
//			status = false;
//
//		} finally {
//			try {
//				if (conn != null) {
//					if (ps != null) ps.close();
//					conn.close();
//				}
//			} catch (Exception e) {
//				e.printStackTrace();
//			}
//		}
//
//		return status;
//	}
//
//	/**
//	 * Get destination column types
//	 */
//	private HashMap<String, String> getDestinationColumnTypes(String tableName, String[] directColumns,
//			String[] metaColumns, Connection conn) {
//
//		HashMap<String, String> columnTypes = new HashMap<>();
//
//		try {
//			DatabaseMetaData metaData = conn.getMetaData();
//
//			// Handle schema-qualified table names
//			String schemaName = null;
//			String actualTableName = tableName;
//
//			if (tableName.contains(".")) {
//				String[] parts = tableName.split("\\.");
//				schemaName = parts[0].toLowerCase();
//				actualTableName = parts[1].toLowerCase();
//			} else {
//				actualTableName = tableName.toLowerCase();
//			}
//
//			// Get all columns we need
//			String[] allColumns = new String[directColumns.length + metaColumns.length];
//			System.arraycopy(directColumns, 0, allColumns, 0, directColumns.length);
//			System.arraycopy(metaColumns, 0, allColumns, directColumns.length, metaColumns.length);
//
//			for (String colName : allColumns) {
//				ResultSet rs = metaData.getColumns(null, schemaName, actualTableName, colName.toLowerCase());
//
//				if (rs.next()) {
//					String dataType = rs.getString("TYPE_NAME");
//					columnTypes.put(colName.toLowerCase(), dataType);
//					pu.sop("getDestinationColumnTypes", "Column: " + colName + " -> Type: " + dataType);
//				}
//
//				rs.close();
//			}
//
//		} catch (Exception ex) {
//			System.out.println("EXCEPTION in getDestinationColumnTypes: " + ex.toString());
//			ex.printStackTrace();
//			return null;
//		}
//
//		return columnTypes;
//	}
//
//	/**
//	 * Set PreparedStatement parameter based on column type
//	 */
//	private void setParameterByType(PreparedStatement ps, int paramIndex, String value, String columnType,
//			String columnName) throws Exception {
//
//		// Only treat actual NULL as NULL, not empty strings or whitespace
//		if (value == null) {
//			// Handle null values based on type
//			if (columnType != null) {
//				String type = columnType.toLowerCase();
//				if (type.contains("int") || type.contains("serial")) {
//					ps.setNull(paramIndex, java.sql.Types.BIGINT);
//				} else if (type.contains("timestamp") || type.contains("date") || type.contains("time")) {
//					ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
//				} else if (type.contains("numeric") || type.contains("decimal") || type.contains("float")
//						|| type.contains("double")) {
//					ps.setNull(paramIndex, java.sql.Types.NUMERIC);
//				} else if (type.contains("bool")) {
//					ps.setNull(paramIndex, java.sql.Types.BOOLEAN);
//				} else {
//					ps.setNull(paramIndex, java.sql.Types.VARCHAR);
//				}
//			} else {
//				ps.setNull(paramIndex, java.sql.Types.VARCHAR);
//			}
//			return;
//		}
//
//		// For empty strings or whitespace, insert as-is for string types
//		// For numeric/date types, treat empty string as NULL
//		if (value.trim().isEmpty()) {
//			if (columnType != null) {
//				String type = columnType.toLowerCase();
//
//				// For string types, insert the blank value as-is
//				if (type.contains("char") || type.contains("text")) {
//					ps.setString(paramIndex, value);
//					return;
//				}
//
//				// For non-string types, treat blank as NULL
//				if (type.contains("int") || type.contains("serial")) {
//					ps.setNull(paramIndex, java.sql.Types.BIGINT);
//				} else if (type.contains("timestamp") || type.contains("date") || type.contains("time")) {
//					ps.setNull(paramIndex, java.sql.Types.TIMESTAMP);
//				} else if (type.contains("numeric") || type.contains("decimal") || type.contains("float")
//						|| type.contains("double")) {
//					ps.setNull(paramIndex, java.sql.Types.NUMERIC);
//				} else if (type.contains("bool")) {
//					ps.setNull(paramIndex, java.sql.Types.BOOLEAN);
//				} else {
//					ps.setString(paramIndex, value);
//				}
//			} else {
//				// If type unknown, insert blank as-is
//				ps.setString(paramIndex, value);
//			}
//			return;
//		}
//
//		// Set value based on column type
//		if (columnType != null) {
//			String type = columnType.toLowerCase();
//
//			try {
//				if (type.contains("bigint") || type.contains("int8") || type.contains("bigserial")) {
//					ps.setLong(paramIndex, Long.parseLong(value.trim()));
//				} else if (type.contains("int") || type.contains("serial")) {
//					ps.setInt(paramIndex, Integer.parseInt(value.trim()));
//				} else if (type.contains("numeric") || type.contains("decimal")) {
//					ps.setBigDecimal(paramIndex, new java.math.BigDecimal(value.trim()));
//				} else if (type.contains("float") || type.contains("double") || type.contains("real")) {
//					ps.setDouble(paramIndex, Double.parseDouble(value.trim()));
//				} else if (type.contains("bool")) {
//					ps.setBoolean(paramIndex, Boolean.parseBoolean(value.trim()));
//				} else if (type.contains("timestamp")) {
//					// Try to parse timestamp
//					try {
//						java.sql.Timestamp ts = java.sql.Timestamp.valueOf(value.trim());
//						ps.setTimestamp(paramIndex, ts);
//					} catch (IllegalArgumentException e) {
//						// Log detailed error for timestamp parsing failure
//						pu.sop("setParameterByType", "✗ ERROR: Cannot parse value '" + value + "' for column '"
//								+ columnName + "' as timestamp");
//						pu.sop("setParameterByType", "  Column type in destination: " + columnType);
//						pu.sop("setParameterByType",
//								"  Expected format: 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD HH:MM:SS.nnnnnnnnn'");
//						pu.sop("setParameterByType",
//								"  ⚠ This indicates a data type mismatch between source and destination tables");
//
//						// Throw exception to stop processing
//						throw new Exception("Data type mismatch: Column '" + columnName
//								+ "' expects timestamp but got '" + value + "'");
//					}
//				} else if (type.contains("date")) {
//					try {
//						java.sql.Date date = java.sql.Date.valueOf(value.trim());
//						ps.setDate(paramIndex, date);
//					} catch (IllegalArgumentException e) {
//						pu.sop("setParameterByType", "✗ ERROR: Cannot parse value '" + value + "' for column '"
//								+ columnName + "' as date");
//						throw new Exception(
//								"Data type mismatch: Column '" + columnName + "' expects date but got '" + value + "'");
//					}
//				} else if (type.contains("time")) {
//					try {
//						java.sql.Time time = java.sql.Time.valueOf(value.trim());
//						ps.setTime(paramIndex, time);
//					} catch (IllegalArgumentException e) {
//						pu.sop("setParameterByType", "✗ ERROR: Cannot parse value '" + value + "' for column '"
//								+ columnName + "' as time");
//						throw new Exception(
//								"Data type mismatch: Column '" + columnName + "' expects time but got '" + value + "'");
//					}
//				} else {
//					// Default to string - insert value as-is (including blank
//					// strings)
//					ps.setString(paramIndex, value);
//				}
//			} catch (NumberFormatException e) {
//				pu.sop("setParameterByType", "✗ ERROR: Cannot parse value '" + value + "' for column '" + columnName
//						+ "' as " + columnType);
//				throw new Exception("Data type mismatch: Column '" + columnName + "' expects " + columnType
//						+ " but got '" + value + "'");
//			}
//		} else {
//			// If type is unknown, set as string (including blank strings)
//			ps.setString(paramIndex, value);
//		}
//	}
//}