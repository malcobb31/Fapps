package opd_parser_v3;

import java.sql.Connection;
import java.sql.Statement;

public class OPD_Source {

    PrintUtilMsg pu = new PrintUtilMsg();

    // ============================================================
    // STEP 0 — DELETE EXISTING MONTH DATA
    // ============================================================
    public boolean deleteMonthData(String copyTable, String processMonth,
                                   String dbURL, String dbName, String dbPass) {

        Connection conn = null;
        Statement stmt = null;

        try {
            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement();

            String firstDay = processMonth + "-01";

            String sql = "DELETE FROM " + copyTable +
                    " WHERE VISIT_DATE >= '" + firstDay + "'" +
                    " AND VISIT_DATE <= LAST_DAY('" + firstDay + "')";

            int rows = stmt.executeUpdate(sql);
            conn.commit();

            pu.sop("deleteMonthData", "Deleted rows: " + rows);
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            try { if (conn != null) conn.rollback(); } catch (Exception ignored) {}
            return false;
        } finally {
            try { if (stmt != null) stmt.close(); if (conn != null) conn.close(); } catch (Exception ignored) {}
        }
    }

    // ============================================================
    // STEP 1 — COPY DATA + DEFAULT OPD_TYPE
    // ============================================================
    public boolean copyDataForDate(String sourceTable, String copyTable,
                                   String processMonth, String processDate,
                                   String dbURL, String dbName, String dbPass) {

        Connection conn = null;
        Statement stmt = null;

        try {
            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement();

            String firstDay = processMonth + "-01";

            String sql = "INSERT INTO " + copyTable + " (" +
                    "sno, PATIENT_TYPE, REGISTERED_DATE, APPOINTMENT_TYPE, MRD_NO," +
                    "PATIENT_NAME, LOCATION_CODE, LOCATION, AGE, SEX," +
                    "SPECIALITY_CODE, SPECIALITY_NAME, TREATMENT_CODE, TREATMENT_NAME," +
                    "ADDRESS1, ADDRESS2, AREA, WARD, CITY, PINCODE," +
                    "state, country, DOCTOR_CD, DOCTOR_NAME, OPTO_NAME," +
                    "APPOINTMENT_SOURCE, PATIENT_CATEGORY, PATIENT_SUBCATEGORY," +
                    "APPT_CreatedUser, REG_CreatedUser, VISIT_DATE, REGISTERED_DATETIME," +
                    "REMARKS, MAILID, MAXID, PatientDepartment, PatientDepartmentName," +
                    "CONTACT_DETAILS, CONTACT_DETAILS1, source, facebook_status," +
                    "google_adwords_status, website_status, whatsapp_status," +
                    "campaign_name, cid, campaign_list, rule_name, filter_query," +
                    "LOCATION_NAME, cluster, Region, OPD_TYPE" +
                    ") SELECT " +
                    "sno, PATIENT_TYPE, REGISTERED_DATE, APPOINTMENT_TYPE, MRD_NO," +
                    "PATIENT_NAME, LOCATION_CODE, LOCATION, AGE, SEX," +
                    "SPECIALITY_CODE, SPECIALITY_NAME, TREATMENT_CODE, TREATMENT_NAME," +
                    "ADDRESS1, ADDRESS2, AREA, WARD, CITY, PINCODE," +
                    "state, country, DOCTOR_CD, DOCTOR_NAME, OPTO_NAME," +
                    "APPOINTMENT_SOURCE, PATIENT_CATEGORY, PATIENT_SUBCATEGORY," +
                    "APPT_CreatedUser, REG_CreatedUser, VISIT_DATE, REGISTERED_DATETIME," +
                    "REMARKS, MAILID, MAXID, PatientDepartment, PatientDepartmentName," +
                    "CONTACT_DETAILS, CONTACT_DETAILS1, source, facebook_status," +
                    "google_adwords_status, website_status, whatsapp_status," +
                    "campaign_name, cid, campaign_list, rule_name, filter_query," +
                    "LOCATION_NAME, cluster, Region, 'Natural OPD' " +
                    "FROM " + sourceTable +
                    " WHERE date(VISIT_DATE) >= '" + firstDay + "'" +
                    " AND date(VISIT_DATE) <= '" + processDate + "'";

            int rows = stmt.executeUpdate(sql);
            conn.commit();

            pu.sop("copyData", "Copied rows: " + rows);
            return true;

        } catch (Exception e) {
            e.printStackTrace();
            try { if (conn != null) conn.rollback(); } catch (Exception ignored) {}
            return false;
        } finally {
            try { if (stmt != null) stmt.close(); if (conn != null) conn.close(); } catch (Exception ignored) {}
        }
    }

    // ============================================================
    // COMMON EXECUTOR
    // ============================================================
    private int executeUpdate(String label, String sql,
                              String dbURL, String dbName, String dbPass) {

        Connection conn = null;
        Statement stmt = null;
        int rows = -1;

        try {
            pu.sop(label, "SQL: " + sql);

            conn = new MySQLConnection().getConnection(dbURL, dbName, dbPass);
            stmt = conn.createStatement();
            rows = stmt.executeUpdate(sql);
            conn.commit();

            pu.sop(label, "Rows affected: " + rows);

        } catch (Exception e) {
            e.printStackTrace();
            try { if (conn != null) conn.rollback(); } catch (Exception ignored) {}
            rows = -1;
        } finally {
            try { if (stmt != null) stmt.close(); if (conn != null) conn.close(); } catch (Exception ignored) {}
        }

        return rows;
    }

    // ============================================================
    // RULES (ALL CLEAN)
    // ============================================================

    public int applyOldOpd(String table, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " SET OPD_TYPE='Old OPD'" +
                " WHERE VISIT_DATE >= '" + f + "' AND VISIT_DATE <= '" + d + "'" +
                " AND APPOINTMENT_TYPE='Reg' AND PATIENT_TYPE!='New'";
        return executeUpdate("RULE-1 Old OPD", sql, u, n, p);
    }

    public int applyAttendantActivity(String table, String free, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " t INNER JOIN " + free +
                " f ON t.MRD_NO=f.MRD_NO SET t.OPD_TYPE='Attendant Activity'" +
                " WHERE t.VISIT_DATE >= '" + f + "' AND t.VISIT_DATE <= '" + d + "'";
        return executeUpdate("RULE-2 Attendant", sql, u, n, p); // change the whole scan to visit_date  
    }

    public int applyCampOutReach(String table, String camp, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " t SET OPD_TYPE='Camp (Out Reach)'" +
                " WHERE t.VISIT_DATE >= '" + f + "' AND t.VISIT_DATE <= '" + d + "'" +
                " AND (EXISTS (SELECT 1 FROM " + camp + " c WHERE c.MRD_NO=t.MRD_NO AND c.opd_visit='Y')" +
                " OR t.PATIENT_CATEGORY='OUT - REACH CAMP')";
        return executeUpdate("RULE-3 Camp OR", sql, u, n, p); // why to scan whole table
    }

    public int applyCampInHouse(String table, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " SET OPD_TYPE='Camp (In House)'" +
                " WHERE VISIT_DATE >= '" + f + "' AND VISIT_DATE <= '" + d + "'" +
                " AND PATIENT_CATEGORY='Camp Patient'";
        return executeUpdate("RULE-4 Camp IH", sql, u, n, p);
    }

    public int applySurgicalCoupon(String table, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " SET OPD_TYPE='Surgical Coupon'" +
                " WHERE VISIT_DATE >= '" + f + "' AND VISIT_DATE <= '" + d + "'" +
                " AND PATIENT_CATEGORY='MARKETING ACTIVITY'" +
                " AND PATIENT_SUBCATEGORY='SURGICAL COUPON PATIENTS'";
        return executeUpdate("RULE-5 Surgical", sql, u, n, p);
    }

    public int applyReferral(String table, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " SET OPD_TYPE='Referral'" +
                " WHERE VISIT_DATE >= '" + f + "' AND VISIT_DATE <= '" + d + "'" +
                " AND PATIENT_CATEGORY='MARKETING ACTIVITY'" +
                " AND PATIENT_SUBCATEGORY IN ('REFERRAL ARMY CLINIC','MAX ATTENDENT ACTIVITIES'," +
                "'Lasik Referral Program','REFERENCE FROM AGENT','INVITATION CARD ACTIVITY'," +
                "'IN CENTER COUPON ACTIVITY','REFERRAL THROUGH DOCTORS'," +
                "'REFERRAL MARKETING','REFERRAL FROM OPTICAL')";
        return executeUpdate("RULE-6 Referral", sql, u, n, p);
    }

    public int applyCorporateOpd(String table, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " SET OPD_TYPE='Corporate OPD'" +
                " WHERE VISIT_DATE >= '" + f + "' AND VISIT_DATE <= '" + d + "'" +
                " AND PATIENT_CATEGORY IN ('CORPORATE (CASH)','CORPORATE (CASHLESS)')";
        return executeUpdate("RULE-7 Corporate", sql, u, n, p);
    }

    public int applySeniorCitizenActivity(String table, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " SET OPD_TYPE='Senior Citizen Activity'" +
                " WHERE VISIT_DATE >= '" + f + "' AND VISIT_DATE <= '" + d + "'" +
                " AND PATIENT_CATEGORY='MARKETING ACTIVITY'" +
                " AND PATIENT_SUBCATEGORY='SENIOR CITIZEN ACTIVITY'";
        return executeUpdate("RULE-8 Senior", sql, u, n, p);
    }

    public int applyNewOpdEchs(String table, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " SET OPD_TYPE='New OPD ECHS'" +
                " WHERE VISIT_DATE >= '" + f + "' AND VISIT_DATE <= '" + d + "'" +
                " AND PATIENT_CATEGORY='EX SERVICEMAN'";
        return executeUpdate("RULE-9 ECHS", sql, u, n, p);
    }

    public int applyNewOpdCghs(String table, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " SET OPD_TYPE='New OPD CGHS'" +
                " WHERE VISIT_DATE >= '" + f + "' AND VISIT_DATE <= '" + d + "'" +
                " AND PATIENT_CATEGORY='CGHS'";
        return executeUpdate("RULE-10 CGHS", sql, u, n, p);
    }

    public int applyNewOpdDghs(String table, String m, String d, String u, String n, String p) {
        String f = m + "-01";
        String sql = "UPDATE " + table + " SET OPD_TYPE='New OPD DGHS'" +
                " WHERE VISIT_DATE >= '" + f + "' AND VISIT_DATE <= '" + d + "'" +
                " AND PATIENT_CATEGORY='DGHS'";
        return executeUpdate("RULE-11 DGHS", sql, u, n, p);
    }
}