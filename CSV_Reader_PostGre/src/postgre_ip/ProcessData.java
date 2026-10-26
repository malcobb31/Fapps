package postgre_ip;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ProcessData {

    private static final String LOGGER_DB_HOST = "127.0.0.1";
    private static final int LOGGER_DB_PORT = 3306;
    private static final String LOGGER_DB_NAME = "pview";
    private static final String LOGGER_DB_USER = "root";
    private static final String LOGGER_DB_PASS = "admin@321";
    private static final String LOGGER_TABLE = "automation_alpha3_log_reader";
    private static final String SCHEDULE_TABLE = "airtel_ep_ftp_schedule_details";

    private static final String PROCESS_NAME = "CSV_TO_PSQL";
    private static final String PROCESS_VERSION = "1.0.0";

    private static final DateTimeFormatter LOG_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void processAllInDirectory(String dirPath, String headerFlag, String delimiter, String dbHost, String dbPort, String dbName, String dbUser, String dbPass) {
        try {
            List<Path> files = Files.list(Paths.get(dirPath))
                .filter(p -> p.getFileName().toString().startsWith("raw_") && p.getFileName().toString().endsWith(".csv"))
                .collect(Collectors.toList());

            for (Path csvPath : files) {
                process(csvPath.toString(), headerFlag, delimiter, dbHost, dbPort, dbName, dbUser, dbPass);
            }
        } catch (IOException e) {
            System.err.println("Failed to scan directory: " + e.getMessage());
        }
    }

    public static boolean process(String csvFile, String headerFlag, String delimiter, String dbHost, String dbPort, String dbName, String dbUser, String dbPass) {
        boolean header = headerFlag.equalsIgnoreCase("yes");

        String resolvedTable = resolveTableFromCsvName(csvFile);
        if (resolvedTable == null || resolvedTable.isEmpty()) {
            logEvent("CRITICAL", "TABLE_RESOLVE_FAIL", "CSV_File:" + csvFile + "# Status:InvalidNaming or Schedule Mapping Missing");
            return false;
        }

        logEvent("INFO", "P_START", "CSV_File:" + csvFile + "# ResolvedTable:" + resolvedTable + "# Host:" + dbHost);

        if (!Files.exists(Paths.get(csvFile))) {
            System.err.println("CSV file does not exist: " + csvFile);
            logEvent("CRITICAL", "CSV_NOT_FOUND", "CSV_File:" + csvFile + "# Status:NotFound");
            logEvent("INFO", "P_END", "CSV_File:" + csvFile + "# Status:Terminated");
            return false;
        }

        try {
            if (!tableExists(dbHost, dbPort, dbName, dbUser, dbPass, resolvedTable)) {
                logEvent("MAJOR", "TABLE_CREATION", "Table:" + resolvedTable + "# Action:Auto-Create");
                createTableFromCSV(resolvedTable, csvFile, delimiter, dbHost, dbPort, dbName, dbUser, dbPass);
                logEvent("MAJOR", "TABLE_CREATED", "Table:" + resolvedTable + "# Status:Created");
            }

            logEvent("INFO", "IMPORT_START", "CSV_File:" + csvFile + "# Table:" + resolvedTable + "# Header:" + header);
            importCSVUsingPsql(csvFile, delimiter, header, resolvedTable, dbHost, dbPort, dbName, dbUser, dbPass);
            logEvent("INFO", header ? "IMPORT_WH_END" : "IMPORT_WOH_END", "CSV_File:" + csvFile + "# Import:Success");

        } catch (Exception e) {
            logEvent("CRITICAL", "ERR_IMPORT_FAILED", "Exception:" + e.getMessage().replace("\n", " "));
            e.printStackTrace();
            return false;
        }

        logEvent("INFO", "P_END", "Process:Complete# File:" + csvFile);
        return true;
    }

    private static String resolveTableFromCsvName(String csvFile) {
        try (Connection conn = DriverManager.getConnection(
                "jdbc:mysql://" + LOGGER_DB_HOST + ":" + LOGGER_DB_PORT + "/" + LOGGER_DB_NAME,
                LOGGER_DB_USER, LOGGER_DB_PASS)) {
            String fileName = Paths.get(csvFile).getFileName().toString();
            String[] parts = fileName.split("_");
            if (parts.length < 5) return null;
            String id = parts[4];
            String sql = "SELECT table_name FROM " + SCHEDULE_TABLE + " WHERE sno = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, id);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Failed to resolve table name from schedule DB: " + e.getMessage());
        }
        return null;
    }

    private static void logEvent(String logLevel, String evtType, String description) {
        String timestamp = LocalDateTime.now().format(LOG_FORMAT);
        String logMsg = String.format("Time:%s [Debug] Process_Name: %s Server_IP:127.0.0.1 Process_Version: %s Log_Level: %s EVT_Log EVT_Type:%s Description: %s",
                timestamp, PROCESS_NAME, PROCESS_VERSION, logLevel, evtType, description);
        System.out.println(logMsg);

        try (Connection conn = DriverManager.getConnection(
                "jdbc:mysql://" + LOGGER_DB_HOST + ":" + LOGGER_DB_PORT + "/" + LOGGER_DB_NAME,
                LOGGER_DB_USER, LOGGER_DB_PASS)) {
            String sql = "INSERT INTO " + LOGGER_TABLE + " (Time_Stamp, Process_Name, Process_Version, Log_Level, EVT_Type, Description) VALUES (?, ?, ?, ?, ?, ?)";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, timestamp);
                stmt.setString(2, PROCESS_NAME);
                stmt.setString(3, PROCESS_VERSION);
                stmt.setString(4, logLevel);
                stmt.setString(5, evtType);
                stmt.setString(6, description);
                stmt.executeUpdate();
            }
        } catch (SQLException e) {
            System.err.println("Failed to log event: " + e.getMessage());
        }
    }

    private static boolean tableExists(String host, String port, String dbName, String user, String pass, String tableName) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder("psql", "-h", host, "-p", port, "-U", user, "-d", dbName,
                "-tAc", "SELECT 1 FROM information_schema.tables WHERE table_name = '" + tableName + "'");
        pb.environment().put("PGPASSWORD", pass);
        Process process = pb.start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = reader.readLine();
        process.waitFor();
        return line != null && line.trim().equals("1");
    }

    private static void createTableFromCSV(String tableName, String csvFile, String delimiter, String host, String port, String dbName, String user, String pass) throws IOException, InterruptedException {
        String headerLine = Files.lines(Paths.get(csvFile)).findFirst().orElseThrow(null);
        String[] columns = headerLine.split(Pattern.quote(delimiter));
        StringBuilder sql = new StringBuilder("CREATE TABLE \"" + tableName + "\" (");
        for (int i = 0; i < columns.length; i++) {
        	String columnName = columns[i].trim()
        	        .replaceAll("[^a-zA-Z0-9_]", "_") // replace non-alphanum with _
        	        .replaceAll("^_+", "")            // remove leading underscores
        	        .replaceAll("_+$", "");           // remove trailing underscores

            sql.append("\"" + columnName + "\" text");
            if (i != columns.length - 1) sql.append(", ");
        }
        sql.append(");");

        ProcessBuilder pb = new ProcessBuilder("psql", "-h", host, "-p", port, "-U", user, "-d", dbName, "-c", sql.toString());
        pb.environment().put("PGPASSWORD", pass);
        pb.inheritIO();
        pb.start().waitFor();
    }

    private static void importCSVUsingPsql(String csvFile, String delimiter, boolean header, String tableName,
                                           String host, String port, String dbName, String user, String pass) throws IOException, InterruptedException {
        String copyCmd = String.format(
                "COPY \"%s\" FROM '%s' WITH (FORMAT csv, DELIMITER '%s'%s);",
                tableName, csvFile.replace("\\", "\\\\"), delimiter, header ? ", HEADER" : "");

        ProcessBuilder pb = new ProcessBuilder("psql", "-h", host, "-p", port, "-U", user, "-d", dbName, "-c", copyCmd);
        pb.environment().put("PGPASSWORD", pass);
        pb.inheritIO();
        pb.start().waitFor();
    }
}