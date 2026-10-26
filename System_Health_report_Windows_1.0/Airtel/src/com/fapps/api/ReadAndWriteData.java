package com.fapps.api;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import com.fapps.db.SNORangeFinder;
import com.fapps.util.PrintUtilMsg;

public class ReadAndWriteData {

    private HashMap<String, String> kpi_count_data = new HashMap<String, String>();

    PrintUtilMsg pu = new PrintUtilMsg();

    protected String currentTime() {
        String current_time = "";

        try {
            SimpleDateFormat sdf_date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Calendar c1 = Calendar.getInstance();

            Date date = new Date();
            c1.setTime(date);

            current_time = sdf_date.format(c1.getTime());

        } catch (Exception E) {
            System.out.println("currentTime:: Exception while computing current date");
            E.printStackTrace();
        }

        return current_time;
    }

    private String[] binarySearch(String binarySearch, String date, String sourceTable,
            String db_url, String db_name, String db_pass, String date_format) {

        String[] snoRangeReturn = new String[4];

        try {

            pu.sop("getAPILogData", "Is Binary Search? [" + binarySearch + "]");

            if (binarySearch.equalsIgnoreCase("yes")) {

                String current_month = "";
                String next_month = "";
                String year = "";
                String from_date = "";
                String to_date = "";
                String[] dates = new String[2];

                int[] snoRange = null;
                int[] snoRangeFinal = new int[2];

                boolean hashExist = false;

                String repId = "120";
                String[] pull_data_details = null;
                String from_date_final = "";
                String to_date_final = "";

                pull_data_details = date.split("~~");

                String[] dates_details = pull_data_details[1].split("#");

                if (dates_details.length > 1) {
                    hashExist = true;
                }

                pu.sop("Status Date Interval", "Interval Status is " + hashExist);

                if (pull_data_details[2].toLowerCase().equals("monthly")
                        || (pull_data_details[2].toLowerCase().equals("daily") && hashExist == true)) {

                    if (hashExist == false) {

                        pu.sop("Trend Type", "Monthly only " + hashExist);

                        current_month = pull_data_details[1].substring(5, 7);
                        pu.sop("Monthly", "Month is " + current_month);

                        if (Integer.parseInt(current_month) == 12) {
                            next_month = "01";
                        } else {
                            next_month = "" + (Integer.parseInt(current_month) + 1);
                        }

                        if (next_month.length() == 1) {
                            next_month = "0" + next_month;
                        }

                        pu.sop("Monthly", "Final Next Month is " + next_month);

                        from_date = pull_data_details[1].substring(0, 8) + "01";
                        pu.sop("Monthly", "From data is " + from_date);

                        from_date_final = from_date;

                        if (Integer.parseInt(current_month) == 12) {
                            year = "" + ((Integer.parseInt(
                                    pull_data_details[1].substring(0, 4))) + 1);

                            to_date = year + "-" + next_month + "-01";

                        } else {
                            to_date = from_date.replace(current_month, next_month);
                        }

                        pu.sop("Monthly", "To data is " + to_date);

                        to_date_final = pull_data_details[1];

                    } else if (hashExist == true
                            && pull_data_details[2].toLowerCase().equals("monthly")) {

                        pu.sop("Trend Type", "Monthly Interval " + hashExist);

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Date shared_date = sdf.parse(dates_details[0]);

                        Calendar c = Calendar.getInstance();
                        c.setTime(shared_date);
                        c.add(Calendar.MONTH, -Integer.parseInt(dates_details[1]));

                        Date from_month = c.getTime();
                        String from_year_month = sdf.format(from_month);

                        pu.sop("Getting Require month and year",
                                "Year is " + from_year_month);

                        from_date_final = from_year_month.substring(0, 8) + "01";
                        pu.sop("Final From Date", "From Date " + from_date_final);

                        to_date_final = dates_details[0].substring(0, 8) + "01";
                        pu.sop("Final To Date", "To Date " + to_date_final);

                    } else {

                        pu.sop("Trend Type", "Date Interval " + hashExist);

                        Date shared_date = new SimpleDateFormat("yyyy-MM-dd")
                                .parse(dates_details[0]);

                        int numberOfDays = (Integer.parseInt(dates_details[1])) - 1;

                        pu.sop("Number of days", "Days are: " + numberOfDays);

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        to_date_final = sdf.format(shared_date);

                        System.out.println("To date is " + to_date_final);

                        Calendar c = Calendar.getInstance();
                        c.setTime(shared_date);
                        c.add(Calendar.DATE, -numberOfDays);

                        Date from_fetched_date = c.getTime();

                        from_date_final = sdf.format(from_fetched_date);

                        System.out.println("From date is " + from_date_final);
                    }

                    dates[0] = from_date_final;
                    dates[1] = to_date_final;

                    snoRange = new SNORangeFinder().getStartEndIdOfTable(
                            sourceTable,
                            db_url,
                            db_name,
                            db_pass);

                    pu.sop("TABLE MIN MAX VALUE",
                            "MIN [" + snoRange[0] + "], MAX [" + snoRange[1] + "]");

                    int[] snoRangeToFromDate = null;

                    for (int i = 0; i < dates.length; i++) {

                        snoRangeToFromDate = new SNORangeFinder().getStartAndEndId(
                                dates[i],
                                snoRange[0],
                                snoRange[1],
                                sourceTable,
                                "DATE_FORMAT(" + pull_data_details[0] + ",'%Y-%m-%d')",
                                "daily",
                                repId,
                                false,
                                "sno",
                                db_url,
                                db_name,
                                db_pass,
                                date_format);

                        pu.sop("getAPILogData", "Table Name [" + sourceTable + "]");
                        pu.sop("getAPILogData",
                                "FOR DATE [" + dates[i] + "], MinSno["
                                        + snoRangeToFromDate[0] + "], MaxSno["
                                        + snoRangeToFromDate[1] + "]");

                        pu.sop("getAPILogData", "Date [" + date + "]");

                        if (i == 0) {
                            snoRangeFinal[0] = snoRangeToFromDate[0];
                        } else {
                            snoRangeFinal[1] = snoRangeToFromDate[1];
                        }
                    }

                    snoRangeReturn[0] = "" + snoRangeFinal[0];
                    snoRangeReturn[1] = "" + snoRangeFinal[1];
                    snoRangeReturn[2] = from_date_final;
                    snoRangeReturn[3] = to_date_final;

                } else {

                    snoRange = new SNORangeFinder().getStartEndIdOfTable(
                            sourceTable,
                            db_url,
                            db_name,
                            db_pass);

                    pu.sop("TABLE MIN MAX VALUE",
                            "MIN [" + snoRange[0] + "], MAX [" + snoRange[1] + "]");

                    snoRange = new SNORangeFinder().getStartAndEndId(
                            pull_data_details[1],
                            snoRange[0],
                            snoRange[1],
                            sourceTable,
                            "DATE_FORMAT(" + pull_data_details[0] + ",'" + date_format + "')",
                            pull_data_details[2].toLowerCase(),
                            repId,
                            false,
                            "sno",
                            db_url,
                            db_name,
                            db_pass,
                            date_format);

                    pu.sop("getAPILogData", "Table Name [" + sourceTable + "]");
                    pu.sop("getAPILogData",
                            "MinSno[" + snoRange[0] + "], MaxSno[" + snoRange[1] + "]");
                    pu.sop("getAPILogData", "Date [" + date + "]");

                    snoRangeReturn[0] = "" + snoRange[0];
                    snoRangeReturn[1] = "" + snoRange[1];
                    snoRangeReturn[2] = "NA";
                    snoRangeReturn[3] = "NA";
                }
            }

        } catch (Exception ex) {

            System.out.println("EXCEPTION JAR: Join_Left: " + ex.toString());
            ex.printStackTrace();
        }

        return snoRangeReturn;
    }

    public ArrayList<HashMap<String, String>> fileRead(String txtFilePath, String fileName)
            throws IOException {

        ArrayList<HashMap<String, String>> data =
                new ArrayList<HashMap<String, String>>();

        HashMap<String, String> driveHM = new HashMap<String, String>();
        HashMap<String, String> infoHM = new HashMap<String, String>();
        HashMap<String, String> to10HM = new HashMap<String, String>();
        HashMap<String, String> CpuHM = new HashMap<String, String>();

        HashMap<String, String> info_key = new HashMap<String, String>();

        info_key.put("Host Name", "Host Name");
        info_key.put("OS Name", "OS Name");
        info_key.put("OS Version", "OS Version");
        info_key.put("OS Manufacturer", "OS Manufacturer");
        info_key.put("OS Configuration", "OS Configuration");
        info_key.put("OS Build Type", "OS Build Type");
        info_key.put("Registered Owner", "Registered Owner");
        info_key.put("Registered Organization", "Registered Organization");
        info_key.put("Product ID", "Product ID");
        info_key.put("Original Install Date", "Original Install Date");
        info_key.put("System Boot Time", "System Boot Time");
        info_key.put("System Manufacturer", "System Manufacturer");
        info_key.put("System Model", "System Model");
        info_key.put("System Type", "System Type");
        info_key.put("Processor(s)", "Processor(s)");
        info_key.put("BIOS Version", "BIOS Version");
        info_key.put("Windows Directory", "Windows Directory");
        info_key.put("System Directory", "System Directory");
        info_key.put("Boot Device", "Boot Device");
        info_key.put("System Locale", "System Locale");
        info_key.put("Input Locale", "Input Locale");
        info_key.put("Time Zone", "Time Zone");
        info_key.put("Total Physical Memory", "Total Physical Memory");
        info_key.put("Available Physical Memory", "Available Physical Memory");
        info_key.put("Virtual Memory: Max Size", "Virtual Memory: Max Size");
        info_key.put("Virtual Memory: Available", "Virtual Memory: Available");
        info_key.put("Virtual Memory: In Use", "Virtual Memory: In Use");
        info_key.put("Page File Location(s)", "Page File Location(s)");
        info_key.put("Domain", "Domain");
        info_key.put("Logon Server", "Logon Server");
        info_key.put("Hotfix(s)", "Hotfix(s)");
        info_key.put("Network Card(s)", "Network Card(s)");
        info_key.put("Hyper-V Requirements", "Hyper-V Requirements");

        File file = null;
        BufferedReader br = null;

        int pcount = 0;

        try {

            System.out.println("File name is In Function");

            file = new File(txtFilePath + fileName);
            br = new BufferedReader(new FileReader(file));

            String st;
            String used_free = "";
            String processes_10 = "";
            String cpu_5 = "";
            String lastKeyMatched = "";

            boolean key_found = false;

            int found = 0;
            int system_status = 0;

            int cpu_load_status = 0;
            int top_10_status = 0;
            int cpu_utilization_status = 0;
            int cpu_utilization_status_dash = 0;

            while ((st = br.readLine()) != null) {

                pcount++;

                // =========================
                // DRIVE DATA
                // =========================

                if (system_status == 0) {

                    if (st.startsWith("C ")) {

                        used_free = st.replaceAll(" ", "#");

                        char[] st_arr = used_free.toCharArray();
                        used_free = "";

                        found = 0;

                        for (int i = 0; i < st_arr.length; i++) {

                            if (st_arr[i] == '#') {

                                if (found == 0) {
                                    used_free = used_free + ",";
                                    found = 1;
                                }

                            } else {

                                used_free = used_free + st_arr[i];
                                found = 0;
                            }
                        }

                        driveHM.put("C", used_free);

                        System.out.println(used_free);
                    }

                    if (st.startsWith("D ")) {

                        used_free = st.replaceAll(" ", "#");

                        char[] st_arr = used_free.toCharArray();
                        used_free = "";

                        found = 0;

                        for (int i = 0; i < st_arr.length; i++) {

                            if (st_arr[i] == '#') {

                                if (found == 0) {
                                    used_free = used_free + ",";
                                    found = 1;
                                }

                            } else {

                                used_free = used_free + st_arr[i];
                                found = 0;
                            }
                        }

                        driveHM.put("D", used_free);

                        System.out.println(used_free);
                    }

                    if (st.startsWith("E ")) {

                        used_free = st.replaceAll(" ", "#");

                        char[] st_arr = used_free.toCharArray();
                        used_free = "";

                        found = 0;

                        for (int i = 0; i < st_arr.length; i++) {

                            if (st_arr[i] == '#') {

                                if (found == 0) {
                                    used_free = used_free + ",";
                                    found = 1;
                                }

                            } else {

                                used_free = used_free + st_arr[i];
                                found = 0;
                            }
                        }

                        driveHM.put("E", used_free);

                        System.out.println(used_free);
                    }

                    if (st.startsWith("F ")) {

                        used_free = st.replaceAll(" ", "#");

                        char[] st_arr = used_free.toCharArray();
                        used_free = "";

                        found = 0;

                        for (int i = 0; i < st_arr.length; i++) {

                            if (st_arr[i] == '#') {

                                if (found == 0) {
                                    used_free = used_free + ",";
                                    found = 1;
                                }

                            } else {

                                used_free = used_free + st_arr[i];
                                found = 0;
                            }
                        }

                        driveHM.put("F", used_free);

                        System.out.println(used_free);
                    }
                }

                // =========================
                // SYSTEM INFORMATION
                // =========================

                if (st.contains("System Information")) {
                    system_status = 1;
                }

                if (system_status == 1) {

                    key_found = false;
                    st = st.trim();

                    if (st.length() > 0 && !st.contains("System Information")) {

                        for (Entry<String, String> set : info_key.entrySet()) {

                            if (st.contains(set.getKey())) {

                                lastKeyMatched = set.getKey();

                                st = st.replace(set.getKey() + ":", "");
                                st = st.trim();

                                key_found = true;

                                infoHM.put(set.getKey(), st);

                                break;
                            }
                        }
                    }

                    if (key_found == false) {

                        if (st.contains("CPU Load")) {

                            system_status = 2;
                            cpu_load_status = 1;

                        } else if (!lastKeyMatched.isEmpty()
                                && infoHM.containsKey(lastKeyMatched)) {

                            String previousValue = infoHM.get(lastKeyMatched);

                            if (previousValue == null) {
                                previousValue = "";
                            }

                            if (st.length() > 0) {
                                infoHM.put(
                                        lastKeyMatched,
                                        previousValue + ", " + st
                                );
                            }
                        }
                    }

                    if (st.contains("CPU Load :")) {

                        System.out.println("Contains CPU Load " + st);

                        system_status = 2;
                        cpu_load_status = 1;
                    }
                }

                // =========================
                // CPU LOAD
                // =========================

                if (cpu_load_status == 1) {

                    st = st.trim();

                    if (st.length() > 0 && !st.contains("CPU Load :")) {

                        if (st.contains("LoadPercentage :")) {

                            String[] loadParts = st.split(":");

                            if (loadParts.length > 1) {

                                String loadPercentage =
                                        loadParts[1].trim();

                                System.out.println(
                                        "System Info: CPU Load: key is: LoadPercentage and value is "
                                                + loadPercentage
                                );

                                infoHM.put(
                                        "LoadPercentage",
                                        loadPercentage
                                );

                                cpu_load_status = 2;
                            }
                        }
                    }
                }

                // =========================
                // TOP 10 MEMORY PROCESSES
                // =========================

                if (st.contains("process with high memory utilization")) {
                    top_10_status = 1;
                }

                if (top_10_status == 1) {

                    found = 0;
                    processes_10 = "";
                    st = st.trim();

                    if (!st.contains("ProcessName")
                            && !st.contains("------")
                            && !st.contains("process with high memory utilization")
                            && st.length() > 0
                            && !st.contains("Memory Compression")
                            && !st.contains("Returing to main batch file")
                            && !st.contains("Returning to main batch file")
                            && !st.contains("process with High CPU utilization")) {

                        processes_10 = st.replaceAll(" ", "#");

                        char[] st_arr = processes_10.toCharArray();
                        processes_10 = "";

                        for (int k = 0; k < st_arr.length; k++) {

                            if (st_arr[k] == '#') {

                                if (found == 0) {
                                    processes_10 = processes_10 + ",";
                                    found = 1;
                                }

                            } else {

                                processes_10 = processes_10 + st_arr[k];
                                found = 0;
                            }
                        }
                    }

                    if (st.contains("process with High CPU utilization")) {
                        top_10_status = 2;
                    }

                    try {

                        String[] processParts =
                                processes_10.split(",");

                        if (processParts.length >= 2
                                && processParts[0].length() > 0) {

                            to10HM.put(
                                    processParts[0] + "#####" + pcount,
                                    processParts[1]
                            );
                        }

                    } catch (Exception e) {
                        // Ignore malformed top-process line
                    }
                }

                // =========================
                // CPU UTILIZATION PROCESSES
                // =========================

                if (st.contains("process with High CPU utilization")) {

                    top_10_status = 2;
                    cpu_utilization_status = 1;
                }

                if (cpu_utilization_status == 1) {

                    found = 0;
                    cpu_5 = "";
                    st = st.trim();

                    if (st.contains("----")) {
                        cpu_utilization_status_dash = 1;
                    }

                    if (cpu_utilization_status_dash == 1) {

                        if (st.length() > 0
                                && !st.contains("process with High CPU utilization")
                                && !st.contains("Name")
                                && !st.contains("----")
                                && st.contains("%")) {

                            cpu_5 = st.replaceAll(" ", "#");

                            char[] st_arr = cpu_5.toCharArray();
                            cpu_5 = "";

                            for (int k = 0; k < st_arr.length; k++) {

                                if (st_arr[k] == '#') {

                                    if (found == 0) {
                                        cpu_5 = cpu_5 + "==";
                                        found = 1;
                                    }

                                } else {

                                    cpu_5 = cpu_5 + st_arr[k];
                                    found = 0;
                                }
                            }
                        }

                        try {

                            String[] cpuParts =
                                    cpu_5.split("==");

                            if (cpuParts.length > 0
                                    && !cpuParts[0].isEmpty()) {

                                CpuHM.put(
                                        cpuParts[0] + "#####" + pcount,
                                        cpu_5
                                );
                            }

                        } catch (Exception ee) {
                            // Ignore malformed CPU line
                        }
                    }
                }
            }

        } catch (Exception ee) {

            System.out.println(
                    "Exception while reading file " + ee.getMessage()
            );

            ee.printStackTrace();

        } finally {

            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                    // Ignore close exception
                }
            }
        }

        // =========================
        // PRINT PARSED DATA
        // =========================

        for (Entry<String, String> drv : driveHM.entrySet()) {

            System.out.println(
                    "Drive:::: Key is "
                            + drv.getKey()
                            + " and value is "
                            + drv.getValue()
            );
        }

        System.out.println(
                "========================================================================================================================================="
        );

        for (Entry<String, String> inf : infoHM.entrySet()) {

            System.out.println(
                    "Info:::: Key is "
                            + inf.getKey()
                            + " and value is "
                            + inf.getValue()
            );
        }

        System.out.println(
                "========================================================================================================================================="
        );

        for (Entry<String, String> top : to10HM.entrySet()) {

            System.out.println(
                    "Top:::: Key is "
                            + top.getKey()
                            + " and value is "
                            + top.getValue()
            );
        }

        System.out.println(
                "========================================================================================================================================="
        );

        for (Entry<String, String> cpu : CpuHM.entrySet()) {

            System.out.println(
                    "CPU:::: Key is "
                            + cpu.getKey()
                            + " and value is "
                            + cpu.getValue()
            );
        }

        data.add(driveHM);
        data.add(infoHM);
        data.add(to10HM);
        data.add(CpuHM);

        return data;
    }

    // ========================================================================
    // INFORMATION TABLE
    // ========================================================================

    public boolean insertIntoInformationTable(
            String inputDate,
            String timestamp,
            int batch,
            String db_url,
            String db_name,
            String db_pass,
            String destTable,
            ArrayList<HashMap<String, String>> data_ar,
            String host_ip) {

        boolean status = false;

        Connection conn = null;
        PreparedStatement ps = null;

        try {

            String $query =
                    "insert into " + destTable + "("
                            + "input_date,"
                            + "device_time,"
                            + "c_used,"
                            + "c_free,"
                            + "d_used,"
                            + "d_free,"
                            + "e_used,"
                            + "e_free,"
                            + "f_used,"
                            + "f_free,"
                            + "host_name,"
                            + "os_name,"
                            + "os_version,"
                            + "os_manufacturer,"
                            + "os_configuration,"
                            + "os_build_type,"
                            + "registered_owner,"
                            + "registered_organization,"
                            + "product_id,"
                            + "original_install_date,"
                            + "system_boot_time,"
                            + "system_manufacturer,"
                            + "system_model,"
                            + "system_type,"
                            + "processor,"
                            + "bios_version,"
                            + "window_directory,"
                            + "system_directory,"
                            + "boot_device,"
                            + "system_locale,"
                            + "input_locale,"
                            + "time_zone,"
                            + "total_physical_memory,"
                            + "available_physical_memory,"
                            + "virtual_memory_max_size,"
                            + "virtual_memory_available,"
                            + "virtual_memory_in_use,"
                            + "page_file_location,"
                            + "domain,"
                            + "logon_server,"
                            + "hotfix,"
                            + "network_card,"
                            + "hyper_v_requirement,"
                            + "cpu_load,"
                            + "host_ip)"
                            + " values(?,?,?,?,?"
                            + ",?,?,?,?,?"
                            + ",?,?,?,?,?"
                            + ",?,?,?,?,?"
                            + ",?,?,?,?,?"
                            + ",?,?,?,?,?"
                            + ",?,?,?,?,?"
                            + ",?,?,?,?,?"
                            + ",?,?,?,?,?)";

            conn = new com.fapps.db.MySQLConnection().getConnection(
                    db_url,
                    db_name,
                    db_pass
            );

            if (conn == null) {

                System.out.println(
                        "EXCEPTION JAR: insertIntoInformationTable: Database connection is NULL"
                );

                return false;
            }

            ps = conn.prepareStatement($query);

            HashMap<String, String> drive = data_ar.get(0);
            HashMap<String, String> info = data_ar.get(1);

            double dValue = 0;

            int batch_count = 0;

            ps.setString(1, inputDate);
            ps.setString(2, timestamp);

            // C used
            try {

                if (drive != null && drive.get("C") != null) {

                    try {
                        dValue = Double.parseDouble(
                                drive.get("C").split(",")[1]
                        );
                    } catch (Exception eee) {
                        dValue = 0;
                    }

                    ps.setString(3, "" + dValue);

                } else {
                    ps.setString(3, "0");
                }

            } catch (Exception e) {

                ps.setString(3, "0");

                System.out.println(
                        "Exception while fetching C used drive data: "
                                + e.getMessage()
                );
            }

            // C free
            try {

                if (drive != null && drive.get("C") != null) {

                    try {
                        dValue = Double.parseDouble(
                                drive.get("C").split(",")[2]
                        );
                    } catch (Exception eee) {
                        dValue = 0;
                    }

                    ps.setString(4, "" + dValue);

                } else {
                    ps.setString(4, "0");
                }

            } catch (Exception e) {

                ps.setString(4, "0");

                System.out.println(
                        "Exception while fetching C free drive data: "
                                + e.getMessage()
                );
            }

            // D used
            try {

                if (drive != null && drive.get("D") != null) {

                    try {
                        dValue = Double.parseDouble(
                                drive.get("D").split(",")[1]
                        );
                    } catch (Exception eee) {
                        dValue = 0;
                    }

                    ps.setString(5, "" + dValue);

                } else {
                    ps.setString(5, "0");
                }

            } catch (Exception e) {

                ps.setString(5, "0");

                System.out.println(
                        "Exception while fetching D Used drive data: "
                                + e.getMessage()
                );
            }

            // D free
            try {

                if (drive != null && drive.get("D") != null) {

                    try {
                        dValue = Double.parseDouble(
                                drive.get("D").split(",")[2]
                        );
                    } catch (Exception eee) {
                        dValue = 0;
                    }

                    ps.setString(6, "" + dValue);

                } else {
                    ps.setString(6, "0");
                }

            } catch (Exception e) {

                ps.setString(6, "0");

                System.out.println(
                        "Exception while fetching D Free drive data: "
                                + e.getMessage()
                );
            }

            // E used
            try {

                if (drive != null && drive.get("E") != null) {

                    try {
                        dValue = Double.parseDouble(
                                drive.get("E").split(",")[1]
                        );
                    } catch (Exception eee) {
                        dValue = 0;
                    }

                    ps.setString(7, "" + dValue);

                } else {
                    ps.setString(7, "0");
                }

            } catch (Exception e) {

                ps.setString(7, "0");

                System.out.println(
                        "Exception while fetching E Used drive data: "
                                + e.getMessage()
                );
            }

            // E free
            try {

                if (drive != null && drive.get("E") != null) {

                    try {
                        dValue = Double.parseDouble(
                                drive.get("E").split(",")[2]
                        );
                    } catch (Exception eee) {
                        dValue = 0;
                    }

                    ps.setString(8, "" + dValue);

                } else {
                    ps.setString(8, "0");
                }

            } catch (Exception e) {

                ps.setString(8, "0");

                System.out.println(
                        "Exception while fetching E Free drive data: "
                                + e.getMessage()
                );
            }

            // F used
            try {

                if (drive != null && drive.get("F") != null) {

                    try {
                        dValue = Double.parseDouble(
                                drive.get("F").split(",")[1]
                        );
                    } catch (Exception eee) {
                        dValue = 0;
                    }

                    ps.setString(9, "" + dValue);

                } else {
                    ps.setString(9, "0");
                }

            } catch (Exception e) {

                ps.setString(9, "0");

                System.out.println(
                        "Exception while fetching F Used drive data: "
                                + e.getMessage()
                );
            }

            // F free
            try {

                if (drive != null && drive.get("F") != null) {

                    try {
                        dValue = Double.parseDouble(
                                drive.get("F").split(",")[2]
                        );
                    } catch (Exception eee) {
                        dValue = 0;
                    }

                    ps.setString(10, "" + dValue);

                } else {
                    ps.setString(10, "0");
                }

            } catch (Exception e) {

                ps.setString(10, "0");

                System.out.println(
                        "Exception while fetching F Free drive data: "
                                + e.getMessage()
                );
            }

            // Host Name
            try {
                ps.setString(
                        11,
                        info != null && info.get("Host Name") != null
                                ? info.get("Host Name")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(11, "0");
            }

            // OS Name
            try {
                ps.setString(
                        12,
                        info != null && info.get("OS Name") != null
                                ? info.get("OS Name")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(12, "0");
            }

            // OS Version
            try {
                ps.setString(
                        13,
                        info != null && info.get("OS Version") != null
                                ? info.get("OS Version")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(13, "0");
            }

            // OS Manufacturer
            try {
                ps.setString(
                        14,
                        info != null && info.get("OS Manufacturer") != null
                                ? info.get("OS Manufacturer")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(14, "0");
            }

            // OS Configuration
            try {
                ps.setString(
                        15,
                        info != null && info.get("OS Configuration") != null
                                ? info.get("OS Configuration")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(15, "0");
            }

            // OS Build Type
            try {
                ps.setString(
                        16,
                        info != null && info.get("OS Build Type") != null
                                ? info.get("OS Build Type")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(16, "0");
            }

            // Registered Owner
            try {
                ps.setString(
                        17,
                        info != null && info.get("Registered Owner") != null
                                ? info.get("Registered Owner")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(17, "0");
            }

            // Registered Organization
            try {
                ps.setString(
                        18,
                        info != null && info.get("Registered Organization") != null
                                ? info.get("Registered Organization")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(18, "0");
            }

            // Product ID
            try {
                ps.setString(
                        19,
                        info != null && info.get("Product ID") != null
                                ? info.get("Product ID")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(19, "0");
            }

            // Original Install Date
            try {
                ps.setString(
                        20,
                        info != null && info.get("Original Install Date") != null
                                ? info.get("Original Install Date")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(20, "0");
            }

            // System Boot Time
            try {
                ps.setString(
                        21,
                        info != null && info.get("System Boot Time") != null
                                ? info.get("System Boot Time")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(21, "0");
            }

            // System Manufacturer
            try {
                ps.setString(
                        22,
                        info != null && info.get("System Manufacturer") != null
                                ? info.get("System Manufacturer")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(22, "0");
            }

            // System Model
            try {
                ps.setString(
                        23,
                        info != null && info.get("System Model") != null
                                ? info.get("System Model")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(23, "0");
            }

            // System Type
            try {
                ps.setString(
                        24,
                        info != null && info.get("System Type") != null
                                ? info.get("System Type")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(24, "0");
            }

            // Processor
            try {
                ps.setString(
                        25,
                        info != null && info.get("Processor(s)") != null
                                ? info.get("Processor(s)")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(25, "0");
            }

            // BIOS
            try {
                ps.setString(
                        26,
                        info != null && info.get("BIOS Version") != null
                                ? info.get("BIOS Version")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(26, "0");
            }

            // Windows Directory
            try {
                ps.setString(
                        27,
                        info != null && info.get("Windows Directory") != null
                                ? info.get("Windows Directory")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(27, "0");
            }

            // System Directory
            try {
                ps.setString(
                        28,
                        info != null && info.get("System Directory") != null
                                ? info.get("System Directory")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(28, "0");
            }

            // Boot Device
            try {
                ps.setString(
                        29,
                        info != null && info.get("Boot Device") != null
                                ? info.get("Boot Device")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(29, "0");
            }

            // System Locale
            try {
                ps.setString(
                        30,
                        info != null && info.get("System Locale") != null
                                ? info.get("System Locale")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(30, "0");
            }

            // Input Locale
            try {
                ps.setString(
                        31,
                        info != null && info.get("Input Locale") != null
                                ? info.get("Input Locale")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(31, "0");
            }

            // Time Zone
            try {
                ps.setString(
                        32,
                        info != null && info.get("Time Zone") != null
                                ? info.get("Time Zone")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(32, "0");
            }

            // Total Physical Memory
            try {
                ps.setString(
                        33,
                        info != null && info.get("Total Physical Memory") != null
                                ? info.get("Total Physical Memory")
                                        .replace("MB", "")
                                        .replace(",", "")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(33, "0");
            }

            // Available Physical Memory
            try {
                ps.setString(
                        34,
                        info != null && info.get("Available Physical Memory") != null
                                ? info.get("Available Physical Memory")
                                        .replace("MB", "")
                                        .replace(",", "")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(34, "0");
            }

            // Virtual Memory Max
            try {
                ps.setString(
                        35,
                        info != null && info.get("Virtual Memory: Max Size") != null
                                ? info.get("Virtual Memory: Max Size")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(35, "0");
            }

            // Virtual Memory Available
            try {
                ps.setString(
                        36,
                        info != null && info.get("Virtual Memory: Available") != null
                                ? info.get("Virtual Memory: Available")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(36, "0");
            }

            // Virtual Memory In Use
            try {
                ps.setString(
                        37,
                        info != null && info.get("Virtual Memory: In Use") != null
                                ? info.get("Virtual Memory: In Use")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(37, "0");
            }

            // Page File
            try {
                ps.setString(
                        38,
                        info != null && info.get("Page File Location(s)") != null
                                ? info.get("Page File Location(s)")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(38, "0");
            }

            // Domain
            try {
                ps.setString(
                        39,
                        info != null && info.get("Domain") != null
                                ? info.get("Domain")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(39, "0");
            }

            // Logon Server
            try {
                ps.setString(
                        40,
                        info != null && info.get("Logon Server") != null
                                ? info.get("Logon Server")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(40, "0");
            }

            // Hotfix
            try {
                ps.setString(
                        41,
                        info != null && info.get("Hotfix(s)") != null
                                ? info.get("Hotfix(s)")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(41, "0");
            }

            // Network Card
            try {
                ps.setString(
                        42,
                        info != null && info.get("Network Card(s)") != null
                                ? info.get("Network Card(s)")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(42, "0");
            }

            // Hyper-V
            try {
                ps.setString(
                        43,
                        info != null && info.get("Hyper-V Requirements") != null
                                ? info.get("Hyper-V Requirements")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(43, "0");
            }

            // CPU Load
            try {
                ps.setString(
                        44,
                        info != null && info.get("LoadPercentage") != null
                                ? info.get("LoadPercentage")
                                : "0"
                );
            } catch (Exception e) {
                ps.setString(44, "0");
            }

            // Host IP
            try {
                ps.setString(45, host_ip);
            } catch (Exception e) {
                ps.setString(45, "NA");
            }

            ps.addBatch();

            batch_count++;

            if (batch > 0 && batch_count % batch == 0) {

                pu.sop(
                        "Insert query after batch addition",
                        ps.toString()
                );

                ps.executeBatch();
                conn.commit();

                pu.sop(
                        "Batch limit reached",
                        "Row count:[" + batch_count + "]"
                );
            }

            if (batch_count > 0) {

                ps.executeBatch();
                conn.commit();
            }

            status = true;

        } catch (Exception ex) {

            System.out.println(
                    "EXCEPTION JAR: insertIntoInformationTable: "
                            + ex.toString()
            );

            status = false;
            ex.printStackTrace();

            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (Exception rollbackEx) {
                rollbackEx.printStackTrace();
            }

        } finally {

            try {

                if (ps != null) {
                    ps.close();
                }

                if (conn != null) {
                    conn.close();
                }

            } catch (Exception e) {
                // Ignore close exception
            }
        }

        return status;
    }

    // ========================================================================
    // TOP PROCESS TABLE
    // ========================================================================

    public boolean insertIntoTopProcessTable(
            String inputDate,
            String timestamp,
            int batch,
            String db_url,
            String db_name,
            String db_pass,
            String destTable,
            ArrayList<HashMap<String, String>> data_ar,
            String host_ip) {

        boolean status = false;

        Connection conn = null;
        PreparedStatement ps = null;

        try {

            String $query =
                    "insert into " + destTable + "("
                            + "input_date,"
                            + "device_time,"
                            + "process_name,"
                            + "used_ram,"
                            + "host_ip,"
                            + "host_name)"
                            + " values(?,?,?,?,?,?)";

            conn = new com.fapps.db.MySQLConnection().getConnection(
                    db_url,
                    db_name,
                    db_pass
            );

            if (conn == null) {

                System.out.println(
                        "EXCEPTION JAR: insertIntoTopProcessTable: Database connection is NULL"
                );

                return false;
            }

            ps = conn.prepareStatement($query);

            HashMap<String, String> info = data_ar.get(1);
            HashMap<String, String> top = data_ar.get(2);

            int batch_count = 0;

            for (Entry<String, String> set : top.entrySet()) {

                ps.setString(1, inputDate);
                ps.setString(2, timestamp);

                try {
                    ps.setString(
                            3,
                            set.getKey().split("#####")[0]
                    );
                } catch (Exception e) {
                    ps.setString(3, "0");
                }

                try {
                    ps.setString(
                            4,
                            set.getValue()
                                    .replace(".", ",")
                                    .split(",")[0]
                    );
                } catch (Exception e) {
                    ps.setString(4, "0");
                }

                try {
                    ps.setString(5, host_ip);
                } catch (Exception e) {
                    ps.setString(5, "NA");
                }

                try {

                    if (info != null
                            && info.get("Host Name") != null) {

                        ps.setString(
                                6,
                                info.get("Host Name")
                        );

                    } else {

                        ps.setString(6, "NA");
                    }

                } catch (Exception e) {

                    ps.setString(6, "NA");

                    System.out.println(
                            "Exception while fetching TOP data: Host_Name: "
                                    + e.getMessage()
                    );
                }

                ps.addBatch();

                batch_count++;

                if (batch > 0 && batch_count % batch == 0) {

                    pu.sop(
                            "Insert query after batch addition",
                            ps.toString()
                    );

                    ps.executeBatch();
                    conn.commit();

                    pu.sop(
                            "Batch limit reached",
                            "Row count:[" + batch_count + "]"
                    );
                }
            }

            if (batch_count > 0) {

                ps.executeBatch();
                conn.commit();
                status = true;

            } else {

                System.out.println(
                        "No top process records found."
                );

                status = true;
            }

        } catch (Exception ex) {

            System.out.println(
                    "EXCEPTION JAR: insertIntoTopProcessTable: "
                            + ex.toString()
            );

            status = false;
            ex.printStackTrace();

            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (Exception rollbackEx) {
                rollbackEx.printStackTrace();
            }

        } finally {

            try {

                if (ps != null) {
                    ps.close();
                }

                if (conn != null) {
                    conn.close();
                }

            } catch (Exception e) {
                // Ignore close exception
            }
        }

        return status;
    }

    // ========================================================================
    // CPU UTILIZATION TABLE
    // ========================================================================

    public boolean insertIntoCpuUtilizationTable(
            String inputDate,
            String timestamp,
            int batch,
            String db_url,
            String db_name,
            String db_pass,
            String destTable,
            ArrayList<HashMap<String, String>> data_ar,
            String host_ip) {

        boolean status = false;

        Connection conn = null;
        PreparedStatement ps = null;

        try {

            String $query =
                    "insert into " + destTable + "("
                            + "input_date,"
                            + "device_time,"
                            + "process_name,"
                            + "cpu,"
                            + "memory,"
                            + "host_ip,"
                            + "host_name,"
                            + "description)"
                            + " values(?,?,?,?,?,?,?,?)";

            conn = new com.fapps.db.MySQLConnection().getConnection(
                    db_url,
                    db_name,
                    db_pass
            );

            if (conn == null) {

                System.out.println(
                        "EXCEPTION JAR: insertIntoCpuUtilizationTable: Database connection is NULL"
                );

                return false;
            }

            ps = conn.prepareStatement($query);

            HashMap<String, String> info = data_ar.get(1);
            HashMap<String, String> cpu = data_ar.get(3);

            int batch_count = 0;

            for (Entry<String, String> set : cpu.entrySet()) {

                ps.setString(1, inputDate);
                ps.setString(2, timestamp);

                // Process name
                try {

                    ps.setString(
                            3,
                            set.getKey().split("#####")[0]
                    );

                } catch (Exception e) {

                    ps.setString(3, "0");
                }

                // CPU + memory
                String[] cpuParts = set.getValue().split("==");

                int cpuIndex = -1;

                try {

                    for (int loop = 0; loop < cpuParts.length; loop++) {

                        if (cpuParts[loop].contains("%")) {

                            ps.setString(
                                    4,
                                    cpuParts[loop].replace("%", "")
                            );

                            cpuIndex = loop;

                            break;
                        }
                    }

                    if (cpuIndex == -1) {
                        ps.setString(4, "0");
                    }

                } catch (Exception e) {

                    ps.setString(4, "0");
                }

                // Memory
                try {

                    if (cpuIndex >= 0
                            && cpuIndex + 1 < cpuParts.length) {

                        ps.setString(
                                5,
                                cpuParts[cpuIndex + 1]
                                        .replace(",", "")
                                        .replace("K", "")
                        );

                    } else {

                        ps.setString(5, "0");
                    }

                } catch (Exception e) {

                    ps.setString(5, "0");
                }

                // Host IP
                try {

                    ps.setString(6, host_ip);

                } catch (Exception e) {

                    ps.setString(6, "NA");
                }

                // Host Name
                try {

                    if (info != null
                            && info.get("Host Name") != null) {

                        ps.setString(
                                7,
                                info.get("Host Name")
                        );

                    } else {

                        ps.setString(7, "NA");
                    }

                } catch (Exception e) {

                    ps.setString(7, "NA");

                    System.out.println(
                            "Exception while fetching CPU data: Host_Name: "
                                    + e.getMessage()
                    );
                }

                // Description
                try {

                    if (cpuIndex >= 0
                            && cpuIndex + 1 < cpuParts.length) {

                        String memoryMarker =
                                cpuParts[cpuIndex + 1] + "==K";

                        String description =
                                set.getValue()
                                        .split(java.util.regex.Pattern
                                                .quote(memoryMarker), 2)
                                        .length > 1
                                                ? set.getValue()
                                                        .split(java.util.regex.Pattern
                                                                .quote(memoryMarker), 2)[1]
                                                        .replace("==K", "")
                                                        .replaceAll("==", " ")
                                                        .trim()
                                                : "";

                        ps.setString(
                                8,
                                description
                        );

                    } else {

                        ps.setString(8, "");
                    }

                } catch (Exception e) {

                    ps.setString(8, "");
                }

                ps.addBatch();

                batch_count++;

                if (batch > 0 && batch_count % batch == 0) {

                    pu.sop(
                            "Insert query after batch addition",
                            ps.toString()
                    );

                    ps.executeBatch();
                    conn.commit();

                    pu.sop(
                            "Batch limit reached",
                            "Row count:[" + batch_count + "]"
                    );
                }
            }

            if (batch_count > 0) {

                ps.executeBatch();
                conn.commit();
                status = true;

            } else {

                System.out.println(
                        "No CPU utilization records found."
                );

                status = true;
            }

        } catch (Exception ex) {

            System.out.println(
                    "EXCEPTION JAR: insertIntoCpuUtilizationTable: "
                            + ex.toString()
            );

            status = false;
            ex.printStackTrace();

            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (Exception rollbackEx) {
                rollbackEx.printStackTrace();
            }

        } finally {

            try {

                if (ps != null) {
                    ps.close();
                }

                if (conn != null) {
                    conn.close();
                }

            } catch (Exception e) {
                // Ignore close exception
            }
        }

        return status;
    }
}