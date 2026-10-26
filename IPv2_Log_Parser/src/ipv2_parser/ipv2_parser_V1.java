package ipv2_parser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.*;

public class ipv2_parser_V1 {

	private static final DateTimeFormatter INPUT_TS = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	private static final DateTimeFormatter OUTPUT_TS = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm");

	// Precompiled regex (IMPORTANT for large logs)
	private static final Pattern FILE_START_PATTERN = Pattern.compile("file:(.*?) tableName:(.*)");

	private static final Pattern FILE_END_PATTERN = Pattern.compile("File:(.*?) tableName:(.*)");

	private static final Pattern SUCCESS_PATTERN = Pattern.compile("File=(.*)");

	private static final Pattern FILE_FOUND_PATTERN = Pattern.compile("Files_Found:(\\d+)");

	private static final Pattern TOTAL_FILES_PATTERN = Pattern.compile("Total_Files:(\\d+)");

	private static final Pattern THREAD_PATTERN = Pattern.compile("Thread_Count:(\\d+)");

	private static final Pattern RESOURCE_PATTERN = Pattern.compile("Memory: ([0-9.]+)% CPU Load: ([0-9.]+)%");

	static class FileExecution {

		int sno;
		String tableName;
		String fileName;
		long fileSizeMB;

		LocalDateTime startTime;
		LocalDateTime endTime;

		boolean success = false;
		boolean failed = false;
		String errorMessage = "";

		long durationSeconds = 0;
	}

	public static void process(String inputCsv, String outputCsv) throws Exception {

		long totalFileAccumulated = 0;
		int currentFileCount = 0;
		int threadCount = 0;
		double cpuPeak = 0;
		double memoryPeak = 0;
		int skippedLines = 0;

		Map<String, FileExecution> fileMap = new LinkedHashMap<>();
		Map<String, String> activeFilePerTable = new HashMap<>();

		try (BufferedReader br = Files.newBufferedReader(Paths.get(inputCsv), StandardCharsets.UTF_8)) {

			String line;

			while ((line = br.readLine()) != null) {

				if (line.trim().isEmpty()) {
					skippedLines++;
					continue;
				}

				String[] cols = parseCSVLine(line);
				if (cols.length < 7) {
					skippedLines++;
					continue;
				}

				String timestampStr = cols[0].replace("\"", "").trim();

				if (timestampStr.equalsIgnoreCase("Time Stamp")) {
					continue;
				}

				LocalDateTime timestamp;
				try {
					timestamp = LocalDateTime.parse(timestampStr, INPUT_TS);
				} catch (Exception e) {
					skippedLines++;
					continue;
				}

				String evtType = cols[5].replace("\"", "").trim();
				String desc = cols[6].replace("\"", "").trim();

				switch (evtType) {

				case "FILE_DISCOVERY":
					Matcher m1 = FILE_FOUND_PATTERN.matcher(desc);
					if (m1.find())
						currentFileCount = Integer.parseInt(m1.group(1));
					break;

				case "FILE_GROUPING":
					Matcher m2 = TOTAL_FILES_PATTERN.matcher(desc);
					if (m2.find())
						totalFileAccumulated += Integer.parseInt(m2.group(1));
					break;

				case "PARALLEL_START":
					Matcher m3 = THREAD_PATTERN.matcher(desc);
					if (m3.find())
						threadCount = Integer.parseInt(m3.group(1));
					break;

				case "RESOURCE_MONITOR":
					Matcher m4 = RESOURCE_PATTERN.matcher(desc);
					if (m4.find()) {
						double mem = Double.parseDouble(m4.group(1));
						double cpu = Double.parseDouble(m4.group(2));
						cpuPeak = Math.max(cpuPeak, cpu);
						memoryPeak = Math.max(memoryPeak, mem);
					}
					break;

				case "FILE_START":
					handleFileStart(desc, timestamp, fileMap, activeFilePerTable);
					break;

				case "MYSQL_LOAD_SUCCESS":
					handleSuccess(desc, fileMap);
					break;

				case "MYSQL_LOAD_ERROR":
				case "ERROR":
					handleError(desc, fileMap, activeFilePerTable);
					break;

				case "FILE_END":
					handleFileEnd(desc, timestamp, fileMap);
					break;
				}
			}
		}

		writeFinalCSV(fileMap, outputCsv, currentFileCount, totalFileAccumulated, threadCount, cpuPeak, memoryPeak);

		System.out.println("Skipped malformed lines: " + skippedLines);
	}

	private static void handleFileStart(String desc, LocalDateTime ts, Map<String, FileExecution> fileMap,
			Map<String, String> activeFilePerTable) {

		Matcher m = FILE_START_PATTERN.matcher(desc);

		if (m.find()) {

			String fileName = m.group(1).trim();
			String tableName = m.group(2).trim();

			String key = tableName + "|" + fileName;

			if (fileMap.containsKey(key)) {
				return; // prevent duplicate overwrite
			}

			FileExecution fe = new FileExecution();
			fe.tableName = tableName;
			fe.fileName = fileName;
			fe.startTime = ts;
			fe.fileSizeMB = getFileSizeSafe(fileName);

			fileMap.put(key, fe);

			activeFilePerTable.put(tableName, key);
		}
	}

	private static void handleSuccess(String desc, Map<String, FileExecution> fileMap) {

		Matcher m = SUCCESS_PATTERN.matcher(desc);

		if (m.find()) {

			String fileName = m.group(1).trim();

			for (FileExecution fe : fileMap.values()) {
				if (fe.fileName.equals(fileName)) {
					fe.success = true;
					fe.failed = false;
					break;
				}
			}
		}
	}

	private static void handleError(String desc, Map<String, FileExecution> fileMap,
			Map<String, String> activeFilePerTable) {

		for (String key : activeFilePerTable.values()) {

			FileExecution fe = fileMap.get(key);

			if (fe != null && !fe.success && fe.endTime == null) {
				fe.failed = true;
				fe.errorMessage = desc;
				break;
			}
		}
	}

	private static void handleFileEnd(String desc, LocalDateTime ts, Map<String, FileExecution> fileMap) {

		Matcher m = FILE_END_PATTERN.matcher(desc);

		if (m.find()) {

			String fileName = m.group(1).trim();
			String tableName = m.group(2).trim();

			String key = tableName + "|" + fileName;

			FileExecution fe = fileMap.get(key);

			if (fe != null) {

				fe.endTime = ts;

				if (fe.startTime != null) {
					fe.durationSeconds = ChronoUnit.SECONDS.between(fe.startTime, fe.endTime);
				}

				if (!fe.success) {
					fe.failed = true;
				}
			}
		}
	}

	private static long getFileSizeSafe(String fileName) {
		try {
			Path p = Paths.get(fileName);
			if (Files.exists(p))
				return Files.size(p) / (1024 * 1024);
		} catch (Exception ignored) {
		}
		return 0;
	}

	private static void writeFinalCSV(Map<String, FileExecution> fileMap, String outputCsv, int currentFileCount,
			long totalFileAccumulated, int threadCount, double cpuPeak, double memoryPeak) throws Exception {

		try (BufferedWriter bw = Files.newBufferedWriter(Paths.get(outputCsv), StandardCharsets.UTF_8,
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {

			bw.write("SNO,TableName,FileName,FileSizeMB,Current_File_Count,Total_File_Accumulated,");
			bw.write("File_Start,File_End,Duration_sec,Status,Thread_Count,CPU_Peak,Memory_Peak,Error_Message");
			bw.newLine();

			for (FileExecution fe : fileMap.values()) {

				String status = fe.success ? "SUCCESS" : fe.failed ? "FAIL" : "UNKNOWN";

				bw.write(String.join(",", "", safe(fe.tableName), safe(fe.fileName), String.valueOf(fe.fileSizeMB),
						String.valueOf(currentFileCount), String.valueOf(totalFileAccumulated),
						formatTime(fe.startTime), formatTime(fe.endTime), String.valueOf(fe.durationSeconds), status,
						String.valueOf(threadCount), String.valueOf(cpuPeak), String.valueOf(memoryPeak),
						"\"" + safe(fe.errorMessage) + "\""));
				bw.newLine();
			}
		}
	}

	private static String formatTime(LocalDateTime t) {
		return (t == null) ? "" : t.format(OUTPUT_TS);
	}

	private static String safe(String s) {
		return s == null ? "" : s.replace(",", " ");
	}

	private static String[] parseCSVLine(String line) {

		List<String> result = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		boolean inQuotes = false;

		for (char c : line.toCharArray()) {

			if (c == '"') {
				inQuotes = !inQuotes;
			} else if (c == ',' && !inQuotes) {
				result.add(sb.toString());
				sb.setLength(0);
			} else {
				sb.append(c);
			}
		}

		result.add(sb.toString());
		return result.toArray(new String[0]);
	}

	public static void main(String[] args) throws Exception {

	}

}