package ntp19_process_v1;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ntp19_process_methods {
	private static final String PROCESS_NAME = "NTP19";
	private static final String PROCESS_VERSION = "1.0.0";
	private static final int MAX_PARALLEL_FILES = 5;
	private static final int DEFAULT_BATCH_SIZE = 1000;
	private static final Charset FILE_CHARSET = StandardCharsets.UTF_8;
	private static final String DEFAULT_CREATED_BY = "SYSTEM";
	private static final String DEFAULT_UPDATED_BY = "SYSTEM";
	private static final long DEFAULT_VERSION = 0L;
	private static final ZoneOffset IST_OFFSET = ZoneOffset.ofHoursMinutes(5, 30);
	private static final String CERT_BASE_DIR = "/Data/CKYC_FILES/PUBLICKEY";
	private static final String CERT_FILE_SUFFIX = "_pub.cer";

	private final String jdbcUrl;
	private final String dbUser;
	private final String dbPassword;
	private final String delimiter;
	private final String processedDirPath;
	private final String serverIp;
	private final boolean debugEnabled = false;
	private final DateTimeFormatter logFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

	private volatile String reportingEntityCodeColumn;
	private final Map<String, CertInfo> certCache = new HashMap<String, CertInfo>();

	public ntp19_process_methods(String jdbcUrl, String dbUser, String dbPassword, String delimiter,
			String processedDirPath, String serverIp) {
		this.jdbcUrl = jdbcUrl;
		this.dbUser = dbUser;
		this.dbPassword = dbPassword;
		this.delimiter = (delimiter == null || delimiter.isEmpty()) ? "|" : delimiter;
		this.processedDirPath = processedDirPath;
		this.serverIp = serverIp;
	}

	public void process(File input) throws Exception {
		log("INFO", "Process started", serverIp, "PROCESS_START");

		List<File> files = collectFiles(input);
		if (files.isEmpty()) {
			log("INFO", "No CSV files found to process", serverIp, "DIR_SUMMARY");
			logDirectorySummary(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
			log("INFO", "Process completed", serverIp, "PROCESS_END");
			return;
		}

		File processedDir = processedDirPath == null || processedDirPath.trim().length() == 0
				? new File(input.isDirectory() ? input : input.getParentFile(), "processed")
				: new File(processedDirPath);
		ensureDirectory(processedDir);

		int poolSize = Math.min(MAX_PARALLEL_FILES, files.size());
		ExecutorService executor = Executors.newFixedThreadPool(poolSize);
		List<Future<FileSummary>> futures = new ArrayList<Future<FileSummary>>();

		try {
			for (int i = 0; i < files.size(); i++) {
				final File file = files.get(i);
				futures.add(executor.submit(new Callable<FileSummary>() {
					public FileSummary call() {
						return processSingleFile(file, processedDir);
					}
				}));
			}
		} finally {
			executor.shutdown();
		}

		long totalFiles = files.size();
		long completedFiles = 0L;
		long retainedFiles = 0L;
		long emptyFiles = 0L;
		long headerOnlyFiles = 0L;
		long errorFiles = 0L;
		long totalRows = 0L;
		long inserted = 0L;
		long skipped = 0L;
		long dbFailed = 0L;
		long rowErrors = 0L;
		long batchFailures = 0L;
		long blankLines = 0L;

		for (Future<FileSummary> future : futures) {
			FileSummary summary;
			try {
				summary = future.get();
			} catch (InterruptedException ie) {
				Thread.currentThread().interrupt();
				throw ie;
			} catch (ExecutionException ee) {
				Throwable root = rootCause(ee);
				log("ERROR",
						"Fatal error waiting for file result: " + (root == null ? ee.getMessage() : root.getMessage()),
						serverIp, "FATAL_ERROR");
				continue;
			}

			if (summary == null) {
				continue;
			}

			if (summary.moved) {
				completedFiles++;
			} else {
				retainedFiles++;
			}

			if ("EMPTY".equalsIgnoreCase(summary.status)) {
				emptyFiles++;
			}
			if ("HEADER_ONLY".equalsIgnoreCase(summary.status)) {
				headerOnlyFiles++;
			}
			if ("ERROR".equalsIgnoreCase(summary.status) || "PARTIAL".equalsIgnoreCase(summary.status)) {
				errorFiles++;
			}

			totalRows += summary.dataRows;
			inserted += summary.inserted;
			skipped += summary.skipped;
			dbFailed += summary.dbFailed;
			rowErrors += summary.rowErrors;
			batchFailures += summary.batchFailures;
			blankLines += summary.blankLines;
		}

		logDirectorySummary(totalFiles, completedFiles, retainedFiles, emptyFiles, headerOnlyFiles, errorFiles,
				totalRows, inserted, skipped, dbFailed, rowErrors, batchFailures, blankLines);
		log("INFO", "Process completed", serverIp, "PROCESS_END");
	}

	private List<File> collectFiles(File input) {
		List<File> files = new ArrayList<File>();
		if (input == null) {
			return files;
		}
		if (input.isFile()) {
			if (isCsv(input)) {
				files.add(input);
			}
			return files;
		}

		File[] list = input.listFiles();
		if (list == null) {
			return files;
		}

		Arrays.sort(list, new Comparator<File>() {
			public int compare(File a, File b) {
				return a.getName().compareToIgnoreCase(b.getName());
			}
		});

		for (int i = 0; i < list.length; i++) {
			File f = list[i];
			if (f != null && f.isFile() && isCsv(f)) {
				files.add(f);
			}
		}
		return files;
	}

	private boolean isCsv(File f) {
		String name = f == null ? null : f.getName();
		return name != null && name.toLowerCase(Locale.ENGLISH).endsWith(".csv");
	}

	private void ensureDirectory(File dir) throws IOException {
		if (dir == null) {
			return;
		}
		if (!dir.exists()) {
			Files.createDirectories(dir.toPath());
		}
	}

	private FileSummary processSingleFile(File file, File processedDir) {
		FileSummary summary = new FileSummary();
		summary.fileName = file.getName();
		summary.effectiveBatchSize = DEFAULT_BATCH_SIZE;

		log("INFO", "File start: " + file.getName(), serverIp, "FILE_START");

		Connection conn = null;
		PreparedStatement ps = null;
		BufferedReader reader = null;
		boolean success = false;

		try {
			conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPassword);
			conn.setAutoCommit(false);

			reader = new BufferedReader(new InputStreamReader(new FileInputStream(file), FILE_CHARSET));
			String headerLine = readNextLogicalRecord(reader);
			if (headerLine == null) {
				summary.status = "EMPTY";
				summary.moved = moveToDirectory(file, processedDir);
				if (summary.moved) {
					log("INFO", "File moved: " + file.getName(), serverIp, "FILE_MOVED");
				}
				logFileSummary(summary);
				success = true;
				return summary;
			}

			List<String> header = parseCsvLine(headerLine, delimiter.charAt(0));
			if (header.isEmpty()) {
				summary.status = "EMPTY";
				summary.moved = moveToDirectory(file, processedDir);
				if (summary.moved) {
					log("INFO", "File moved: " + file.getName(), serverIp, "FILE_MOVED");
				}
				logFileSummary(summary);
				success = true;
				return summary;
			}

			Map<String, Integer> headerIndex = buildHeaderIndex(header);
			if (!isSupportedHeader(headerIndex)) {
				throw new IllegalArgumentException("Unsupported CSV header for file " + file.getName());
			}

			if (isHeaderOnlyFile(reader)) {
				summary.status = "HEADER_ONLY";
				summary.moved = moveToDirectory(file, processedDir);
				if (summary.moved) {
					log("INFO", "File moved: " + file.getName(), serverIp, "FILE_MOVED");
				}
				logFileSummary(summary);
				success = true;
				return summary;
			}

			ps = conn.prepareStatement(buildInsertSql());

			List<RowData> batch = new ArrayList<RowData>(DEFAULT_BATCH_SIZE);
			String record;
			while ((record = readNextLogicalRecord(reader)) != null) {
				if (isBlankRecord(record)) {
					summary.blankLines++;
					continue;
				}

				summary.dataRows++;
				List<String> values = parseCsvLine(record, delimiter.charAt(0));
				RowData row = buildRow(conn, headerIndex, values);

				if (row.skipReason != null) {
					summary.skipped++;
					summary.rowErrors++;
					continue;
				}

				batch.add(row);
				if (batch.size() >= summary.effectiveBatchSize) {
					flushBatch(conn, ps, batch, summary);
					batch.clear();
				}
			}

			if (!batch.isEmpty()) {
				flushBatch(conn, ps, batch, summary);
				batch.clear();
			}

			conn.commit();
			success = true;

			if (summary.dbFailed == 0 && summary.rowErrors == 0 && summary.batchFailures == 0) {
				summary.status = "SUCCESS";
				summary.moved = moveToDirectory(file, processedDir);
				if (summary.moved) {
					log("INFO", "File moved: " + file.getName(), serverIp, "FILE_MOVED");
				}
			} else {
				summary.status = "PARTIAL";
				summary.moved = false;
			}

			logFileSummary(summary);
		} catch (Exception ex) {
			summary.status = "ERROR";
			summary.moved = false;
			summary.errorMessage = rootCause(ex) == null ? ex.getMessage() : rootCause(ex).getMessage();
			log("ERROR", "Fatal error in file " + file.getName() + ": " + summary.errorMessage, serverIp,
					"FATAL_ERROR");
			rollbackQuietly(conn);
			logFileSummary(summary);
		} finally {
			closeQuietly(ps);
			closeQuietly(reader);
			closeQuietly(conn);
			if (!success && summary.status == null) {
				summary.status = "ERROR";
			}
			if (!success && summary.errorMessage == null && "ERROR".equalsIgnoreCase(summary.status)) {
				summary.errorMessage = "Unknown fatal error";
			}
		}

		return summary;
	}

	private boolean isSupportedHeader(Map<String, Integer> headerIndex) {
		return headerIndex.containsKey(normalizeHeader("upcd_certificate_name"))
				|| headerIndex.containsKey(normalizeHeader("file_name"))
				|| headerIndex.containsKey(normalizeHeader("upcd_expiry_date"))
				|| headerIndex.containsKey(normalizeHeader("valid_till"));
	}

	private boolean isHeaderOnlyFile(BufferedReader reader) throws IOException {
		if (reader == null) {
			return true;
		}
		reader.mark(1024 * 1024);
		String next;
		while ((next = readNextLogicalRecord(reader)) != null) {
			if (!isBlankRecord(next)) {
				reader.reset();
				return false;
			}
		}
		reader.reset();
		return true;
	}

	private UUID resolveReportingEntityId(Connection conn, String fiCode) throws Exception {

		if (fiCode == null || fiCode.trim().isEmpty()) {
			return null;
		}

		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement("SELECT id FROM re_mgmt.reporting_entity WHERE fi_code = ?");
			ps.setString(1, fiCode.trim());

			rs = ps.executeQuery();

			if (rs.next()) {
				return (UUID) rs.getObject(1);
			}

			return null;

		} finally {
			closeQuietly(rs);
			closeQuietly(ps);
		}
	}

	private String buildInsertSql() {
		return "INSERT INTO re_mgmt.re_ip_whitelisting_public_key "
				+ "(public_key_id, file_name, valid_from, valid_till, reporting_entity_id, created_at,"
				+ " updated_at, created_by, updated_by, version, active, public_key_fingerprint, certificate_hex, key_usage_type) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	}

	private RowData buildRow(Connection conn, Map<String, Integer> headerIndex, List<String> values) throws Exception {
		RowData row = new RowData();

		String certificateName = firstNonBlank(getByHeader(headerIndex, values, "upcd_certificate_name", "file_name"),
				getByHeader(headerIndex, values, "file_name", "upcd_certificate_name"));

		if (isNullLike(certificateName)) {
			row.skipReason = "file_name missing";
			return row;
		}

		String fiCode = firstNonBlank(getByHeader(headerIndex, values, "upcd_fi_code", "reporting_entity_id"),
				getByHeader(headerIndex, values, "reporting_entity_id", "upcd_fi_code"));

		if (isNullLike(fiCode)) {
			row.skipReason = "reporting_entity_id missing";
			return row;
		}

		row.fileName = certificateName;
		row.publicKeyId = null;

		String createdAtRaw = firstNonBlank(getByHeader(headerIndex, values, "created_at"),
				getByHeader(headerIndex, values, "upcd_uploaded_date"));

		String validTillRaw = firstNonBlank(getByHeader(headerIndex, values, "valid_till"),
				getByHeader(headerIndex, values, "upcd_expiry_date"));

		row.validFrom = buildDateOnlyAsIstMidnight(createdAtRaw);
		row.validTill = buildDateOnlyAsIstMidnight(validTillRaw);

		row.reportingEntityId = resolveReportingEntityId(conn, fiCode);
		if (row.reportingEntityId == null && looksLikeUuid(fiCode)) {
			row.reportingEntityId = safeUuid(fiCode);
		}

		Timestamp createdAt = parseNullableTimestamp(
				firstNonBlank(getByHeader(headerIndex, values, "upcd_uploaded_date", "created_at"),
						getByHeader(headerIndex, values, "created_at", "upcd_uploaded_date")));
		if (createdAt == null) {
			createdAt = new Timestamp(System.currentTimeMillis());
		}
		row.createdAt = createdAt;

		Timestamp updatedAt = parseNullableTimestamp(
				firstNonBlank(getByHeader(headerIndex, values, "upcd_updated_time", "updated_at"),
						getByHeader(headerIndex, values, "updated_at", "upcd_updated_time")));
		if (updatedAt == null) {
			updatedAt = createdAt;
		}
		row.updatedAt = updatedAt;

		String createdBy = firstNonBlank(getByHeader(headerIndex, values, "upcd_uploaded_userid", "created_by"),
				getByHeader(headerIndex, values, "created_by", "upcd_uploaded_userid"));
		if (isNullLike(createdBy)) {
			createdBy = DEFAULT_CREATED_BY;
		}
		row.createdBy = createdBy;

		row.updatedBy = DEFAULT_UPDATED_BY;
		row.version = Long.valueOf(DEFAULT_VERSION);
		row.active = Boolean.TRUE;

		CertInfo certInfo = loadCertificateInfo(fiCode);
		row.publicKeyFingerprint = certInfo == null ? null : certInfo.fingerprint;
		row.certificateHex = certInfo == null ? null : certInfo.certificateHex;
		row.keyUsageType = "ENCRYPTION";

		return row;
	}

	private Timestamp buildDateOnlyAsIstMidnight(String value) {
		value = normalizeNullable(value);
		if (value == null) {
			return null;
		}

		LocalDate date = extractLocalDate(value);

		if (date == null) {
			return null;
		}

		return Timestamp.from(date.atStartOfDay(IST_OFFSET).toInstant());
	}

	private LocalDate extractLocalDate(String value) {
		value = normalizeNullable(value);
		if (value == null) {
			return null;
		}

		int space = value.indexOf(' ');
		int t = value.indexOf('T');
		int cut = -1;

		if (space >= 0 && t >= 0) {
			cut = Math.min(space, t);
		} else if (space >= 0) {
			cut = space;
		} else if (t >= 0) {
			cut = t;
		}

		if (cut > 0) {
			value = value.substring(0, cut);
		}

		try {
			return LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		} catch (Exception ex) {
			return null;
		}
	}

	private void bindRow(PreparedStatement ps, RowData row) throws Exception {
		int idx = 1;
		ps.setString(idx++, row.publicKeyId);
		ps.setString(idx++, row.fileName);

		if (row.validFrom == null) {
			ps.setTimestamp(idx++, null);
		} else {
			ps.setTimestamp(idx++, row.validFrom);
		}

		if (row.validTill == null) {
			ps.setTimestamp(idx++, null);
		} else {
			ps.setTimestamp(idx++, row.validTill);
		}

		if (row.reportingEntityId == null) {
			ps.setObject(idx++, null);
		} else {
			ps.setObject(idx++, row.reportingEntityId);
		}

		ps.setTimestamp(idx++, row.createdAt);
		ps.setTimestamp(idx++, row.updatedAt);
		ps.setString(idx++, row.createdBy);
		ps.setString(idx++, row.updatedBy);

		if (row.version == null) {
			ps.setObject(idx++, null);
		} else {
			ps.setLong(idx++, row.version.longValue());
		}

		if (row.active == null) {
			ps.setObject(idx++, null);
		} else {
			ps.setBoolean(idx++, row.active.booleanValue());
		}

		ps.setString(idx++, row.publicKeyFingerprint);
		ps.setString(idx++, row.certificateHex);
		ps.setString(idx++, row.keyUsageType);
	}

	private String getByHeader(Map<String, Integer> headerIndex, List<String> values, String... names) {
		if (headerIndex == null || values == null || names == null) {
			return null;
		}
		for (int n = 0; n < names.length; n++) {
			String name = names[n];
			Integer idx = headerIndex.get(normalizeHeader(name));
			if (idx != null && idx.intValue() >= 0 && idx.intValue() < values.size()) {
				return normalizeNullable(values.get(idx.intValue()));
			}
		}
		return null;
	}

	private Map<String, Integer> buildHeaderIndex(List<String> header) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		if (header == null) {
			return map;
		}
		for (int i = 0; i < header.size(); i++) {
			String key = normalizeHeader(header.get(i));
			if (key != null && key.length() > 0 && !map.containsKey(key)) {
				map.put(key, Integer.valueOf(i));
			}
		}
		return map;
	}

	private String firstNonBlank(String... values) {
		if (values == null) {
			return null;
		}
		for (int i = 0; i < values.length; i++) {
			String v = normalizeNullable(values[i]);
			if (v != null) {
				return v;
			}
		}
		return null;
	}

	private String normalizeHeader(String value) {
		if (value == null) {
			return null;
		}
		return value.trim().toLowerCase(Locale.ENGLISH);
	}

	private String normalizeNullable(String value) {
		if (value == null) {
			return null;
		}
		String trimmed = value.trim();
		if (trimmed.length() == 0) {
			return null;
		}
		if ("\\N".equals(trimmed) || "NULL".equalsIgnoreCase(trimmed)) {
			return null;
		}
		if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
			trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
			if (trimmed.length() == 0) {
				return null;
			}
			if ("\\N".equals(trimmed) || "NULL".equalsIgnoreCase(trimmed)) {
				return null;
			}
		}
		return trimmed;
	}

	private boolean isNullLike(String value) {
		return normalizeNullable(value) == null;
	}

	private boolean looksLikeUuid(String value) {
		if (isNullLike(value)) {
			return false;
		}
		String v = value.trim();
		return v.matches("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");
	}

	private UUID safeUuid(String value) {
		if (isNullLike(value)) {
			return null;
		}
		try {
			return UUID.fromString(value.trim());
		} catch (Exception ex) {
			return null;
		}
	}

	private Timestamp parseNullableTimestamp(String value) {
		value = normalizeNullable(value);
		if (value == null) {
			return null;
		}

		List<String> offsetPatterns = Arrays.asList("yyyy-MM-dd HH:mm:ss.SSS Z", "yyyy-MM-dd HH:mm:ss Z",
				"yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd HH:mm:ssXXX");

		for (int i = 0; i < offsetPatterns.size(); i++) {
			try {
				DateTimeFormatter fmt = DateTimeFormatter.ofPattern(offsetPatterns.get(i), Locale.ENGLISH);
				OffsetDateTime odt = OffsetDateTime.parse(value, fmt);
				return Timestamp.from(odt.toInstant());
			} catch (DateTimeParseException ignore) {
			}
		}

		List<String> plainPatterns = Arrays.asList("yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss",
				"yyyy/MM/dd HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ss.SSS",
				"yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd");

		for (int i = 0; i < plainPatterns.size(); i++) {
			String pattern = plainPatterns.get(i);
			try {
				if ("yyyy-MM-dd".equals(pattern)) {
					LocalDate ld = LocalDate.parse(value, DateTimeFormatter.ofPattern(pattern));
					return Timestamp.valueOf(ld.atStartOfDay());
				}
				LocalDateTime ldt = LocalDateTime.parse(value, DateTimeFormatter.ofPattern(pattern));
				return Timestamp.valueOf(ldt);
			} catch (DateTimeParseException ignore) {
			}
		}

		try {
			return Timestamp.valueOf(value.replace('T', ' '));
		} catch (Exception ex) {
			return null;
		}
	}

	private void flushBatch(Connection conn, PreparedStatement ps, List<RowData> batch, FileSummary summary)
			throws Exception {
		if (batch == null || batch.isEmpty()) {
			return;
		}

		Savepoint sp = null;
		try {
			sp = conn.setSavepoint();
			for (int i = 0; i < batch.size(); i++) {
				bindRow(ps, batch.get(i));
				ps.addBatch();
			}
			int[] counts = ps.executeBatch();
			int insertedInBatch = 0;
			for (int i = 0; i < counts.length; i++) {
				if (counts[i] >= 0 || counts[i] == Statement.SUCCESS_NO_INFO) {
					insertedInBatch++;
				}
			}
			summary.inserted += insertedInBatch;
		} catch (BatchUpdateException bue) {
			summary.batchFailures++;
			log("ERROR", "Batch failure in file " + summary.fileName + ": " + rootCause(bue).getMessage(), serverIp,
					"BATCH_FAILURE");
			rollbackQuietly(conn, sp);
			fallbackBatchRowByRow(conn, ps, batch, summary);
		} catch (SQLException sqle) {
			summary.batchFailures++;
			log("ERROR", "Batch failure in file " + summary.fileName + ": " + rootCause(sqle).getMessage(), serverIp,
					"BATCH_FAILURE");
			rollbackQuietly(conn, sp);
			fallbackBatchRowByRow(conn, ps, batch, summary);
		} finally {
			try {
				ps.clearBatch();
			} catch (Exception ignore) {
			}
		}
	}

	private void fallbackBatchRowByRow(Connection conn, PreparedStatement ps, List<RowData> batch, FileSummary summary)
			throws Exception {
		if (batch == null || batch.isEmpty()) {
			return;
		}
		for (int i = 0; i < batch.size(); i++) {
			RowData row = batch.get(i);
			Savepoint rowSp = null;
			try {
				rowSp = conn.setSavepoint();
				bindRow(ps, row);
				ps.executeUpdate();
				summary.inserted++;
			} catch (SQLException sqle) {
				rollbackQuietly(conn, rowSp);
				String state = sqle.getSQLState();
				if (state != null && (state.startsWith("23") || state.startsWith("22") || state.startsWith("28")
						|| state.startsWith("40"))) {
					summary.dbFailed++;
					summary.skipped++;
					log("WARN", "Row rejected by DB in file " + summary.fileName + " (sqlstate=" + state + "): "
							+ sqle.getMessage(), serverIp, "ROW_DB_REJECTED");
				} else {
					throw sqle;
				}
			}
		}
	}

	private CertInfo loadCertificateInfo(String fiCode) {
		fiCode = normalizeNullable(fiCode);
		if (fiCode == null) {
			return null;
		}

		synchronized (certCache) {
			CertInfo cached = certCache.get(fiCode);
			if (cached != null) {
				return cached;
			}
		}

		CertInfo info = readCertificateInfo(fiCode);
		synchronized (certCache) {
			certCache.put(fiCode, info);
		}
		return info;
	}

	private CertInfo readCertificateInfo(String fiCode) {
		File certFile = new File(CERT_BASE_DIR, fiCode + CERT_FILE_SUFFIX);
		if (!certFile.exists() || !certFile.isFile()) {
			return null;
		}

		byte[] rawBytes;
		try {
			rawBytes = Files.readAllBytes(certFile.toPath());
		} catch (Exception ex) {
			log("ERROR", "Unable to read certificate file for " + fiCode + ": " + rootCause(ex).getMessage(), serverIp,
					"CERT_READ_ERROR");
			return null;
		}

		String certificateHex = toLowerHex(rawBytes);
		try {
			byte[] certBytesForParse = rawBytes;
			if (isPemCertificate(rawBytes)) {
				certBytesForParse = pemToDer(rawBytes);
			}

			CertificateFactory cf = CertificateFactory.getInstance("X.509");
			X509Certificate cert;
			try (InputStream in = new ByteArrayInputStream(certBytesForParse)) {
				cert = (X509Certificate) cf.generateCertificate(in);
			}

			byte[] spki = cert.getPublicKey().getEncoded();
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			String fingerprint = toLowerHex(md.digest(spki));
			return new CertInfo(fingerprint, certificateHex);
		} catch (Exception ex) {
			log("ERROR", "Unable to parse certificate for " + fiCode + ": " + rootCause(ex).getMessage(), serverIp,
					"CERT_PARSE_ERROR");
			return new CertInfo(null, certificateHex);
		}
	}

	private boolean isPemCertificate(byte[] raw) {
		if (raw == null || raw.length == 0) {
			return false;
		}
		String prefix = new String(raw, 0, Math.min(raw.length, 64), StandardCharsets.US_ASCII);
		return prefix.contains("-----BEGIN CERTIFICATE-----");
	}

	private byte[] pemToDer(byte[] raw) {
		String text = new String(raw, StandardCharsets.US_ASCII);
		StringBuilder body = new StringBuilder();
		String[] lines = text.split("\\r?\\n");
		for (int i = 0; i < lines.length; i++) {
			String line = lines[i].trim();
			if (line.length() == 0) {
				continue;
			}
			if (line.startsWith("-----BEGIN") || line.startsWith("-----END")) {
				continue;
			}
			body.append(line);
		}
		return Base64.getDecoder().decode(body.toString().replaceAll("\\s+", ""));
	}

	private String toLowerHex(byte[] bytes) {
		if (bytes == null) {
			return null;
		}
		char[] out = new char[bytes.length * 2];
		final char[] hex = "0123456789abcdef".toCharArray();
		for (int i = 0; i < bytes.length; i++) {
			int v = bytes[i] & 0xFF;
			out[i * 2] = hex[v >>> 4];
			out[i * 2 + 1] = hex[v & 0x0F];
		}
		return new String(out);
	}

	private boolean moveToDirectory(File source, File targetDir) {
		if (source == null || targetDir == null) {
			return false;
		}
		try {
			ensureDirectory(targetDir);
			Path sourcePath = source.toPath();
			Path targetPath = new File(targetDir, source.getName()).toPath();
			Files.move(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
			return true;
		} catch (Exception ex) {
			log("ERROR", "Failed to move file " + source.getName() + ": " + rootCause(ex).getMessage(), serverIp,
					"FILE_MOVE_ERROR");
			return false;
		}
	}

	private String readNextLogicalRecord(BufferedReader reader) throws IOException {
		if (reader == null) {
			return null;
		}
		StringBuilder sb = new StringBuilder(1024);
		String line;
		boolean inQuotes = false;
		boolean sawAny = false;
		while ((line = reader.readLine()) != null) {
			sawAny = true;
			if (sb.length() > 0) {
				sb.append('\n');
			}
			sb.append(line);
			inQuotes = hasUnclosedQuotes(sb);
			if (!inQuotes) {
				break;
			}
		}
		if (!sawAny) {
			return null;
		}
		return sb.toString();
	}

	private boolean hasUnclosedQuotes(CharSequence cs) {
		if (cs == null) {
			return false;
		}
		boolean inQuotes = false;
		for (int i = 0; i < cs.length(); i++) {
			char c = cs.charAt(i);
			if (c == '"') {
				if (inQuotes && i + 1 < cs.length() && cs.charAt(i + 1) == '"') {
					i++;
				} else {
					inQuotes = !inQuotes;
				}
			}
		}
		return inQuotes;
	}

	private boolean isBlankRecord(String record) {
		return record == null || record.trim().length() == 0;
	}

	private List<String> parseCsvLine(String line, char delimiter) {
		if (line == null) {
			return Collections.emptyList();
		}
		List<String> values = new ArrayList<String>();
		StringBuilder cur = new StringBuilder();
		boolean inQuotes = false;
		for (int i = 0; i < line.length(); i++) {
			char c = line.charAt(i);
			if (inQuotes) {
				if (c == '"') {
					if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
						cur.append('"');
						i++;
					} else {
						inQuotes = false;
					}
				} else {
					cur.append(c);
				}
			} else {
				if (c == '"') {
					inQuotes = true;
				} else if (c == delimiter) {
					values.add(normalizeNullable(cur.toString()));
					cur.setLength(0);
				} else {
					cur.append(c);
				}
			}
		}
		values.add(normalizeNullable(cur.toString()));
		return values;
	}

	private void logFileSummary(FileSummary summary) {
		StringBuilder sb = new StringBuilder(256);
		sb.append("file=").append(summary.fileName).append(" status=").append(summary.status).append(" data_rows=")
				.append(summary.dataRows).append(" inserted=").append(summary.inserted).append(" skipped=")
				.append(summary.skipped).append(" db_failed=").append(summary.dbFailed).append(" row_errors=")
				.append(summary.rowErrors).append(" batch_failures=").append(summary.batchFailures)
				.append(" blank_lines=").append(summary.blankLines).append(" effective_batch_size=")
				.append(summary.effectiveBatchSize).append(" moved=").append(summary.moved);
		log("INFO", sb.toString(), serverIp, "FILE_SUMMARY");
		if (summary.errorMessage != null && summary.status != null && "ERROR".equalsIgnoreCase(summary.status)) {
			log("ERROR", "File error detail for " + summary.fileName + ": " + summary.errorMessage, serverIp,
					"FILE_ERROR");
		}
	}

	private void logDirectorySummary(long totalFiles, long completedFiles, long retainedFiles, long emptyFiles,
			long headerOnlyFiles, long errorFiles, long totalRows, long inserted, long skipped, long dbFailed,
			long rowErrors, long batchFailures, long blankLines) {
		StringBuilder sb = new StringBuilder(256);
		sb.append("total_files=").append(totalFiles).append(" completed_files=").append(completedFiles)
				.append(" retained_files=").append(retainedFiles).append(" empty_files=").append(emptyFiles)
				.append(" header_only_files=").append(headerOnlyFiles).append(" error_files=").append(errorFiles)
				.append(" total_rows=").append(totalRows).append(" inserted=").append(inserted).append(" skipped=")
				.append(skipped).append(" db_failed=").append(dbFailed).append(" row_errors=").append(rowErrors)
				.append(" batch_failures=").append(batchFailures).append(" blank_lines=").append(blankLines);
		log("INFO", sb.toString(), serverIp, "DIR_SUMMARY");
	}

	private void closeQuietly(AutoCloseable c) {
		if (c == null) {
			return;
		}
		try {
			c.close();
		} catch (Exception ignore) {
		}
	}

	private void rollbackQuietly(Connection conn) {
		if (conn == null) {
			return;
		}
		try {
			conn.rollback();
		} catch (Exception ignore) {
		}
	}

	private void rollbackQuietly(Connection conn, Savepoint sp) {
		if (conn == null || sp == null) {
			return;
		}
		try {
			conn.rollback(sp);
		} catch (Exception ignore) {
		}
	}

	private Throwable rootCause(Throwable t) {
		if (t == null) {
			return null;
		}
		Throwable cur = t;
		while (cur.getCause() != null && cur.getCause() != cur) {
			cur = cur.getCause();
		}
		return cur;
	}

	private String capitalize(String value) {
		if (value == null || value.length() == 0) {
			return "";
		}
		String lower = value.toLowerCase(Locale.ENGLISH);
		return Character.toUpperCase(lower.charAt(0)) + lower.substring(1);
	}

	private void log(String level, String message, String serverIP, String evtType) {
		if ("DEBUG".equalsIgnoreCase(level) && !debugEnabled) {
			return;
		}

		String timestamp = LocalDateTime.now().format(logFormat);
		StringBuilder logBuilder = new StringBuilder(512);
		logBuilder.append("Time:").append(timestamp).append(" [").append(capitalize(level)).append("] ")
				.append("Process_Name: ").append(PROCESS_NAME).append(" ");

		if (serverIP != null && !serverIP.isEmpty()) {
			logBuilder.append("Server_IP: ").append(serverIP).append(" ");
		}

		logBuilder.append("Process_Version: ").append(PROCESS_VERSION).append(" ").append("Log_Level: EVT_LOG ")
				.append(" EVT_Type: ").append(evtType != null ? evtType : "NA").append(" Description: ")
				.append(message != null ? message : "");

		System.out.println(logBuilder.toString().trim());
	}

	private static class RowData {
		String publicKeyId;
		String fileName;
		Timestamp validFrom;
		Timestamp validTill;
		UUID reportingEntityId;
		Timestamp createdAt;
		Timestamp updatedAt;
		String createdBy;
		String updatedBy;
		Long version;
		Boolean active;
		String publicKeyFingerprint;
		String certificateHex;
		String keyUsageType;
		String skipReason;
	}

	private static class FileSummary {
		String fileName;
		String status;
		long dataRows;
		long inserted;
		long skipped;
		long dbFailed;
		long rowErrors;
		long batchFailures;
		long blankLines;
		long effectiveBatchSize;
		boolean moved;
		String errorMessage;
	}

	private static class CertInfo {
		final String fingerprint;
		final String certificateHex;

		CertInfo(String fingerprint, String certificateHex) {
			this.fingerprint = fingerprint;
			this.certificateHex = certificateHex;
		}
	}

}
