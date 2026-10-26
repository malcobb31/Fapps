package rest_api_v3;

import java.sql.*;

public class rest_api_DBConnection {

    private static String dbUrl = "jdbc:mysql://localhost:3306/your_db_name";
    private static String dbUser = "root";
    private static String dbPassword = "password";

    // ===== INSERT REPOSITORY DETAILS =====
    public static void insertEnterpriseDetail(String enterpriseName, int enterpriseId) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            String sql = "INSERT INTO Enterprise_detail (enterprise_name, enterprise_id) VALUES (?, ?)";
            ps = conn.prepareStatement(sql);
            ps.setString(1, enterpriseName);
            ps.setInt(2, enterpriseId);
            ps.executeUpdate();
            System.out.println("[DB] Enterprise_detail inserted: " + enterpriseName + ", ID=" + enterpriseId);
        } catch (SQLException e) {
            System.err.println("[DB ERROR] insertEnterpriseDetail: " + e.getMessage());
        } finally {
            closeResources(conn, ps, null);
        }
    }

    // ===== INSERT BUCKET/USER DETAILS =====
    public static void insertEnterpriseUserDetail(String enterpriseName, String username, String password,
                                                  String bucketName, String bucketId, String userQuota,
                                                  String displayName, String alias,
                                                  String bucketS3Path, String bucketDiskPath) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            String sql = "INSERT INTO Enterprise_user_details " +
                    "(enterprise_name, user_name, password, bucket_name, bucket_id, user_quota, display_name, alias, bucket_s3_path, bucket_disk_path) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            ps = conn.prepareStatement(sql);
            ps.setString(1, enterpriseName);
            ps.setString(2, username);
            ps.setString(3, password);
            ps.setString(4, bucketName);
            ps.setString(5, bucketId);
            ps.setString(6, userQuota);
            ps.setString(7, displayName);
            ps.setString(8, alias);
            ps.setString(9, bucketS3Path);
            ps.setString(10, bucketDiskPath);

            ps.executeUpdate();
            System.out.println("[DB] Enterprise_user_details inserted: " + bucketName);
        } catch (SQLException e) {
            System.err.println("[DB ERROR] insertEnterpriseUserDetail: " + e.getMessage());
        } finally {
            closeResources(conn, ps, null);
        }
    }

    // ===== UPDATE S3 PATH AFTER FOLDER CREATION =====
    public static void updateEnterpriseFolder(int enterpriseId, String s3Path) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
            String sql = "UPDATE Enterprise_detail SET s3_path=? WHERE enterprise_id=?";
            ps = conn.prepareStatement(sql);
            ps.setString(1, s3Path);
            ps.setInt(2, enterpriseId);
            ps.executeUpdate();
            System.out.println("[DB] Enterprise_detail updated s3_path: " + s3Path);
        } catch (SQLException e) {
            System.err.println("[DB ERROR] updateEnterpriseFolder: " + e.getMessage());
        } finally {
            closeResources(conn, ps, null);
        }
    }

    // ===== UTILITY: CLOSE RESOURCES =====
    private static void closeResources(Connection conn, Statement st, ResultSet rs) {
        try { if (rs != null) rs.close(); } catch (Exception e) {}
        try { if (st != null) st.close(); } catch (Exception e) {}
        try { if (conn != null) conn.close(); } catch (Exception e) {}
    }
}
