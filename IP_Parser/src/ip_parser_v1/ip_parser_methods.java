package ip_parser_v1;

import java.sql.*;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ip_parser_methods {

    // Holds per-file or per-table info
    public static class FileTableDetail {
        public String fileName = "";
        public String tableName = "";
        public LocalDateTime startTime;
        public LocalDateTime endTime;
        public long durationSeconds;
        public int rawCount = -1;
        public double fileSizeMB = -1;
        public int threadId = 0;
    }

    // Holds per-process summary
    public static class ProcessSummary {
        public String processName;
        public String processVersion;
        public String serverIP;
        public LocalDateTime startTime;
        public LocalDateTime endTime;
        public long durationSeconds;
        public List<FileTableDetail> details = new ArrayList<>();
        public int totalThreads = 0;
        public int totalFiles = 0;
        public int totalTables = 0;
        public int successCount = 0;
        public int failCount = 0;
        public int migratedTables = 0;
    }

    public void parseLogs(String sourceTable, String destTable, String dbUrl, String dbUser, String dbPass) {
        Map<String, ProcessSummary> processes = new LinkedHashMap<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {

            // Fetch all logs ordered by timestamp
            String selectSQL = "SELECT Time_Stamp, Process_Name, Process_Version, Server_IP, EVT_Type, Description " +
                               "FROM " + sourceTable + " ORDER BY Time_Stamp";
            try (PreparedStatement ps = conn.prepareStatement(selectSQL);
                 ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    String processName = rs.getString("Process_Name");
                    String processVersion = rs.getString("Process_Version");
                    String serverIP = rs.getString("Server_IP");
                    String evtType = rs.getString("EVT_Type");
                    String desc = rs.getString("Description");
                    LocalDateTime ts = rs.getTimestamp("Time_Stamp").toLocalDateTime();

                    ProcessSummary process = processes.getOrDefault(processName, new ProcessSummary());
                    process.processName = processName;
                    process.processVersion = processVersion;
                    process.serverIP = serverIP;

                    // Process start
                    if (evtType.contains("PROCESS_START") || evtType.equalsIgnoreCase("START")) {
                        process.startTime = ts;
                    }

                    // Process end
                    if (evtType.contains("PROCESS_END") || evtType.equalsIgnoreCase("End")) {
                        process.endTime = ts;
                        if (process.startTime != null)
                            process.durationSeconds = Duration.between(process.startTime, process.endTime).getSeconds();
                    }

                    // Detect thread/file/table start
                    if (evtType.contains("Thread_Start") || evtType.contains("TABLE_START") || evtType.contains("FILE_START")) {
                        FileTableDetail detail = new FileTableDetail();
                        if (desc.contains("file:") || desc.contains("File:")) {
                            detail.fileName = extractValue(desc, "file");
                        } else if (desc.contains("tableName:")) {
                            detail.tableName = extractValue(desc, "tableName");
                        }
                        detail.startTime = ts;
                        process.details.add(detail);
                        process.totalThreads++;
                    }

                    // Detect thread/file/table end
                    if (evtType.contains("Thread_End") || evtType.contains("TABLE_END") || evtType.contains("FILE_END")) {
                        String name = "";
                        if (desc.contains("file:") || desc.contains("File:")) name = extractValue(desc, "file");
                        else if (desc.contains("tableName:")) name = extractValue(desc, "tableName");

                        for (int i = process.details.size() - 1; i >= 0; i--) {
                            FileTableDetail f = process.details.get(i);
                            if (f.endTime == null && (f.fileName.equals(name) || f.tableName.equals(name))) {
                                f.endTime = ts;
                                f.durationSeconds = Duration.between(f.startTime, f.endTime).getSeconds();
                                break;
                            }
                        }
                    }

                    // Raw count info
                    if (evtType.contains("rawCount")) {
                        String table = extractValue(desc, "tableName");
                        try {
                        	
                            int raw = Integer.parseInt(extractValue(desc, "rawCount"));
                            for (int i = process.details.size() - 1; i >= 0; i--) {
                                FileTableDetail f = process.details.get(i);
                                if ((f.tableName.equals(table) || f.fileName.equals(table)) && f.rawCount == -1) {
                                    f.rawCount = raw;
                                    break;
                                }
                            }
                        } catch (NumberFormatException e) {
                            System.out.println("Invalid rawCount in description: " + desc);
                        }
                    }
//                    else if (evtType.contains("Total_Records"))
//                    {
//                    	String table = extractValue(desc, "tableName");
//                        try {
//                        	
//                            int raw = Integer.parseInt(extractValue(desc, "Total_Records"));
//                            for (int i = process.details.size() - 1; i >= 0; i--) {
//                                FileTableDetail f = process.details.get(i);
//                                if ((f.tableName.equals(table) || f.fileName.equals(table)) && f.rawCount == -1) {
//                                    f.rawCount = raw;
//                                    break;
//                                }
//                            }
//                        } catch (NumberFormatException e) {
//                            System.out.println("Invalid rawCount in description: " + desc);
//                        }
//                    }

                    // CSV file size info (robust)
                    if (evtType.contains("CSV_DETAILS")) {
                        String descr = desc;

                        // Extract file name
                        String file = null;
                        Pattern filePattern = Pattern.compile("Original[ _]CSV\\s*:\\s*([^|]+)", Pattern.CASE_INSENSITIVE);
                        Matcher fileMatcher = filePattern.matcher(descr);
                        if (fileMatcher.find()) {
                            file = fileMatcher.group(1).trim();
                            int lastSlash = file.lastIndexOf('/');
                            if (lastSlash != -1) file = file.substring(lastSlash + 1);
                        }

                        // Extract file size
                        double sizeMB = 0.0;
                        Pattern sizePattern = Pattern.compile("File[ _]Size\\s*:\\s*([\\d.]+)\\s*([KMG]?B)?", Pattern.CASE_INSENSITIVE);
                        Matcher sizeMatcher = sizePattern.matcher(descr);
                        if (sizeMatcher.find()) {
                            try {
                                sizeMB = Double.parseDouble(sizeMatcher.group(1));
                                String unit = sizeMatcher.group(2);
                                if (unit != null) {
                                    unit = unit.toUpperCase();
                                    switch (unit) {
                                        case "KB": sizeMB /= 1024; break;
                                        case "GB": sizeMB *= 1024; break;
                                    }
                                }
                            } catch (NumberFormatException e) {
                                System.out.println("Invalid File_Size in description: " + descr);
                            }
                        }

                        // Extract Total_Records to store in Raw_Count
                        int totalRecords = 0;
                        Pattern totalPattern = Pattern.compile("Total[_ ]Records\\s*:\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
                        Matcher totalMatcher = totalPattern.matcher(descr);
                        if (totalMatcher.find()) {
                            try {
                                totalRecords = Integer.parseInt(totalMatcher.group(1));
                            } catch (NumberFormatException e) {
                                System.out.println("Invalid Total_Records in description: " + descr);
                            }
                        }

                        // Update file detail object
                        for (int i = process.details.size() - 1; i >= 0; i--) {
                            FileTableDetail f = process.details.get(i);
                            if (file != null && f.fileName != null && file.equals(f.fileName.trim())) {
                                f.fileSizeMB = sizeMB;
                                f.rawCount = totalRecords; // <-- set Raw_Count to Total_Records
                                break;
                            }
                        }

                        System.out.println("Parsed file: " + file + " | Size (MB): " + sizeMB + " | Total_Records: " + totalRecords);
                    }


                    // Success/Fail detection
                    if (evtType.contains("Thread_Status")) {
                        if (desc.toLowerCase().contains("success")) {
                            process.successCount++;
                        } else if (desc.toLowerCase().contains("fail")) {
                            process.failCount++;
                        }
                    }

                    processes.put(processName, process);
                }
            }

            // Insert into destination table
            String insertSQL = "INSERT INTO " + destTable +
                    " (Process_Name, Process_Version, Server_IP, Process_Start, Process_End, Process_Duration, " +
                    "File_Name, Table_Name, File_Start, File_End, File_Duration, Raw_Count, File_Size_MB, " +
                    "Threads_Opened, Total_Files, Total_Tables, Success_Count, Fail_Count, Migrated_Tables) " +
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

            try (PreparedStatement ps = conn.prepareStatement(insertSQL)) {
                for (ProcessSummary p : processes.values()) {
                    int files = 0, tables = 0;
                    for (FileTableDetail f : p.details) {
                        ps.setString(1, p.processName);
                        ps.setString(2, p.processVersion);
                        ps.setString(3, p.serverIP);
                        ps.setString(4, p.startTime != null ? p.startTime.toString() : null);
                        ps.setString(5, p.endTime != null ? p.endTime.toString() : null);
                        ps.setLong(6, p.durationSeconds);
                        ps.setString(7, f.fileName);
                        ps.setString(8, f.tableName);
                        ps.setString(9, f.startTime != null ? f.startTime.toString() : null);
                        ps.setString(10, f.endTime != null ? f.endTime.toString() : null);
                        ps.setLong(11, f.durationSeconds);
                        ps.setObject(12, f.rawCount >= 0 ? f.rawCount : null);
                        ps.setObject(13, f.fileSizeMB >= 0 ? f.fileSizeMB : null);
                        ps.setInt(14, p.totalThreads);
                        ps.setInt(15, f.fileName.isEmpty() ? 0 : 1);
                        ps.setInt(16, f.tableName.isEmpty() ? 0 : 1);
                        ps.setInt(17, p.successCount);
                        ps.setInt(18, p.failCount);
                        ps.setInt(19, p.details.size());
                        ps.addBatch();

                        if (!f.fileName.isEmpty()) files++;
                        if (!f.tableName.isEmpty()) tables++;
                    }

                    ps.executeBatch();

                    // Insert summary row
                    ps.setString(1, p.processName + " (Summary)");
                    ps.setString(2, p.processVersion);
                    ps.setString(3, p.serverIP);
                    ps.setString(4, p.startTime != null ? p.startTime.toString() : null);
                    ps.setString(5, p.endTime != null ? p.endTime.toString() : null);
                    ps.setLong(6, p.durationSeconds);
                    ps.setString(7, null);
                    ps.setString(8, null);
                    ps.setString(9, null);
                    ps.setString(10, null);
                    ps.setLong(11, 0);
                    ps.setObject(12, null);
                    ps.setObject(13, null);
                    ps.setInt(14, p.totalThreads);
                    ps.setInt(15, files);
                    ps.setInt(16, tables);
                    ps.setInt(17, p.successCount);
                    ps.setInt(18, p.failCount);
                    ps.setInt(19, p.details.size());
                    ps.addBatch();
                    ps.executeBatch();
                }
            }

            System.out.println("Parsing and insertion complete.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Original extractValue method for other fields
    private static String extractValue(String desc, String key) {
        try {
            String pattern = key + ":";
            int idx = desc.indexOf(pattern);
            if (idx < 0) return "";
            String s = desc.substring(idx + pattern.length()).trim();
            if (s.contains("#")) s = s.split("#")[0];
            else if (s.contains(" ")) s = s.split(" ")[0];
            return s;
        } catch (Exception e) {
            return "";
        }
    }

    public static void main(String[] args) {
       
    }
}




//package ip_parser_v1;
//
//import java.sql.*;
//import java.time.Duration;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.*;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//public class ip_parser_methods {
//
//    // Holds per-table or per-file info
//    public static class FileTableDetail {
//        public String fileName = "";
//        public String tableName = "";
//        public LocalDateTime startTime;
//        public LocalDateTime endTime;
//        public long durationSeconds;
//        public int rawCount = -1;
//        public double fileSizeMB = -1;
//        public int threadId = 0;
//    }
//
//    // Holds per-process summary
//    public static class ProcessSummary {
//        public String processName;
//        public String processVersion;
//        public String serverIP;
//        public LocalDateTime startTime;
//        public LocalDateTime endTime;
//        public long durationSeconds;
//        public List<FileTableDetail> details = new ArrayList<>();
//        public int totalThreads = 0;
//        public int totalFiles = 0;
//        public int totalTables = 0;
//        public int successCount = 0;
//        public int failCount = 0;
//        public int migratedTables = 0;
//    }
//
//    public void parseLogs(String sourceTable, String destTable, String dbUrl, String dbUser, String dbPass) {
//        Map<String, ProcessSummary> processes = new LinkedHashMap<>();
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//
//        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass)) {
//
//            // Fetch all logs ordered by time
//            String selectSQL = "SELECT Time_Stamp, Process_Name, Process_Version, Server_IP, EVT_Type, Description " +
//                               "FROM " + sourceTable + " ORDER BY Time_Stamp";
//            try (PreparedStatement ps = conn.prepareStatement(selectSQL);
//                 ResultSet rs = ps.executeQuery()) {
//
//                while (rs.next()) {
//                    String processName = rs.getString("Process_Name");
//                    String processVersion = rs.getString("Process_Version");
//                    String serverIP = rs.getString("Server_IP");
//                    String evtType = rs.getString("EVT_Type");
//                    String desc = rs.getString("Description");
//                    LocalDateTime ts = rs.getTimestamp("Time_Stamp").toLocalDateTime();
//
//                    ProcessSummary process = processes.getOrDefault(processName, new ProcessSummary());
//                    process.processName = processName;
//                    process.processVersion = processVersion;
//                    process.serverIP = serverIP;
//
//                    // Process start
//                    if (evtType.contains("PROCESS_START") || evtType.equalsIgnoreCase("START")) {
//                        process.startTime = ts;
//                    }
//                    // Process end
//                    if (evtType.contains("PROCESS_END") || evtType.equalsIgnoreCase("End")) {
//                        process.endTime = ts;
//                        if (process.startTime != null)
//                            process.durationSeconds = Duration.between(process.startTime, process.endTime).getSeconds();
//                    }
//
//                    // Detect thread/file/table start
//                    if (evtType.contains("Thread_Start") || evtType.contains("TABLE_START") || evtType.contains("FILE_START")) {
//                        FileTableDetail detail = new FileTableDetail();
//                        if (desc.contains("file:") || desc.contains("File:")) {
//                            detail.fileName = extractValue(desc, "file");
//                        } else if (desc.contains("tableName:")) {
//                            detail.tableName = extractValue(desc, "tableName");
//                        }
//                        detail.startTime = ts;
//                        process.details.add(detail);
//                        process.totalThreads++;
//                    }
//
//                    // Detect thread/file/table end
//                    if (evtType.contains("Thread_End") || evtType.contains("TABLE_END") || evtType.contains("FILE_END")) {
//                        String name = "";
//                        if (desc.contains("file:") || desc.contains("File:")) name = extractValue(desc, "file");
//                        else if (desc.contains("tableName:")) name = extractValue(desc, "tableName");
//
//                        for (int i = process.details.size() - 1; i >= 0; i--) {
//                            FileTableDetail f = process.details.get(i);
//                            if (f.endTime == null && (f.fileName.equals(name) || f.tableName.equals(name))) {
//                                f.endTime = ts;
//                                f.durationSeconds = Duration.between(f.startTime, f.endTime).getSeconds();
//                                break;
//                            }
//                        }
//                    }
//
//                    // Raw count info
//                    if (evtType.contains("rawCount")) {
//                        String table = extractValue(desc, "tableName");
//                        int raw = Integer.parseInt(extractValue(desc, "rawCount"));
//                        for (int i = process.details.size() - 1; i >= 0; i--) {
//                            FileTableDetail f = process.details.get(i);
//                            if ((f.tableName.equals(table) || f.fileName.equals(table)) && f.rawCount == -1) {
//                                f.rawCount = raw;
//                                break;
//                            }
//                        }
//                    }
//
//                    // CSV file size info
//                    if (evtType.contains("CSV_DETAILS")) {
//                        String descr = "Original_CSV: /home/pview/khushant/pra_csv/process_NA_NA_NA_2_2025_09_19_19_28_03_20250924_144345.csv"
//                                        + " | Total_Records: 2 | File_Size: 2 MB";
//
//                        // Extract file name
//                        String file = null;
//                        Pattern filePattern = Pattern.compile("Original[ _]CSV\\s*:\\s*([^|\\s]+/?(?:[^|]+)?)", Pattern.CASE_INSENSITIVE);
//                        Matcher fileMatcher = filePattern.matcher(descr);  // use 'descr' here
//                        if (fileMatcher.find()) {
//                            file = fileMatcher.group(1).trim();
//                            int lastSlash = file.lastIndexOf('/');
//                            if (lastSlash != -1) file = file.substring(lastSlash + 1);
//                        }
//
//                        // Extract file size
//                        double sizeMB = 0.0;
//                        Pattern sizePattern = Pattern.compile("File[ _]Size\\s*:\\s*([\\d.]+)\\s*([KMG]?B)?", Pattern.CASE_INSENSITIVE);
//                        Matcher sizeMatcher = sizePattern.matcher(descr);  // use 'descr' here
//                        if (sizeMatcher.find()) {
//                            String sizeStr = sizeMatcher.group(1);
//                            String unit = sizeMatcher.group(2);
//
//                            sizeMB = Double.parseDouble(sizeStr);
//
//                            if (unit != null) {
//                                unit = unit.toUpperCase();
//                                switch (unit) {
//                                    case "KB": sizeMB /= 1024; break;
//                                    case "GB": sizeMB *= 1024; break;
//                                }
//                            }
//                        }
//
//                        // Update file detail object
//                        for (int i = process.details.size() - 1; i >= 0; i--) {
//                            FileTableDetail f = process.details.get(i);
//                            if (file != null && f.fileName != null && file.equals(f.fileName.trim())) {
//                                f.fileSizeMB = sizeMB;
//                                break;
//                            }
//                        }
//
//                        System.out.println("Parsed file: " + file + " | Size (MB): " + sizeMB);
//                    }
//
//
//                    // Success/Fail detection
//                    if (evtType.contains("Thread_Status")) {
//                        if (desc.toLowerCase().contains("success")) {
//                            process.successCount++;
//                        } else if (desc.toLowerCase().contains("fail")) {
//                            process.failCount++;
//                        }
//                    }
//
//                    processes.put(processName, process);
//                }
//            }
//
//            // Insert into destination table
//            String insertSQL = "INSERT INTO " + destTable +
//                " (Process_Name, Process_Version, Server_IP, Process_Start, Process_End, Process_Duration, " +
//                "File_Name, Table_Name, File_Start, File_End, File_Duration, Raw_Count, File_Size_MB, " +
//                "Threads_Opened, Total_Files, Total_Tables, Success_Count, Fail_Count, Migrated_Tables) " +
//                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
//
//            try (PreparedStatement ps = conn.prepareStatement(insertSQL)) {
//                for (ProcessSummary p : processes.values()) {
//                    int files = 0, tables = 0;
//                    for (FileTableDetail f : p.details) {
//                        ps.setString(1, p.processName);
//                        ps.setString(2, p.processVersion);
//                        ps.setString(3, p.serverIP);
//                        ps.setString(4, p.startTime != null ? p.startTime.toString() : null);
//                        ps.setString(5, p.endTime != null ? p.endTime.toString() : null);
//                        ps.setLong(6, p.durationSeconds);
//                        ps.setString(7, f.fileName);
//                        ps.setString(8, f.tableName);
//                        ps.setString(9, f.startTime != null ? f.startTime.toString() : null);
//                        ps.setString(10, f.endTime != null ? f.endTime.toString() : null);
//                        ps.setLong(11, f.durationSeconds);
//                        ps.setObject(12, f.rawCount >= 0 ? f.rawCount : null);
//                        ps.setObject(13, f.fileSizeMB >= 0 ? f.fileSizeMB : null);
//                        ps.setInt(14, p.totalThreads);
//                        ps.setInt(15, f.fileName.isEmpty() ? 0 : 1);
//                        ps.setInt(16, f.tableName.isEmpty() ? 0 : 1);
//                        ps.setInt(17, p.successCount);
//                        ps.setInt(18, p.failCount);
//                        ps.setInt(19, p.details.size());
//                        ps.addBatch();
//
//                        if (!f.fileName.isEmpty()) files++;
//                        if (!f.tableName.isEmpty()) tables++;
//                    }
//
//                    ps.executeBatch();
//
//                    // Insert summary row
//                    ps.setString(1, p.processName + " (Summary)");
//                    ps.setString(2, p.processVersion);
//                    ps.setString(3, p.serverIP);
//                    ps.setString(4, p.startTime != null ? p.startTime.toString() : null);
//                    ps.setString(5, p.endTime != null ? p.endTime.toString() : null);
//                    ps.setLong(6, p.durationSeconds);
//                    ps.setString(7, null);
//                    ps.setString(8, null);
//                    ps.setString(9, null);
//                    ps.setString(10, null);
//                    ps.setLong(11, 0);
//                    ps.setObject(12, null);
//                    ps.setObject(13, null);
//                    ps.setInt(14, p.totalThreads);
//                    ps.setInt(15, files);
//                    ps.setInt(16, tables);
//                    ps.setInt(17, p.successCount);
//                    ps.setInt(18, p.failCount);
//                    ps.setInt(19, p.details.size());
//                    ps.addBatch();
//                    ps.executeBatch();
//                }
//            }
//
//            System.out.println("Parsing and insertion complete.");
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    private static String extractValue(String desc, String key) {
//        try {
//            String pattern = key + ":";
//            int idx = desc.indexOf(pattern);
//            if (idx < 0) return "";
//            String s = desc.substring(idx + pattern.length()).trim();
//            if (s.contains("#")) s = s.split("#")[0];
//            else if (s.contains(" ")) s = s.split(" ")[0];
//            return s;
//        } catch (Exception e) {
//            return "";
//        }
//    }
//
//    public static void main(String[] args) {
//        // Example usage:
//        // new ip_parser_methods().parseLogs("postgre_agent_logs", "parsed_summary", dbUrl, dbUser, dbPass);
//    }
//}
