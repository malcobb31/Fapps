package rest_api_job;

import java.sql.*;

public class rest_api_DBConnection {
    private static final String URL = "jdbc:mysql://localhost:3306/your_db";
    private static final String USER = "root";
    private static final String PASS = "password";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASS);
    }

    public static boolean enterpriseExists(String enterpriseName) {
        try(Connection conn = getConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM Enterprise_detail WHERE enterprise_name=?")) {
            ps.setString(1, enterpriseName);
            ResultSet rs = ps.executeQuery();
            return rs.next();
        } catch(SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static int getEnterpriseId(String enterpriseName) {
        try(Connection conn = getConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT enterprise_id FROM Enterprise_detail WHERE enterprise_name=?")) {
            ps.setString(1, enterpriseName);
            ResultSet rs = ps.executeQuery();
            if(rs.next()) return rs.getInt("enterprise_id");
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public static void insertEnterprise(String enterpriseName, int enterpriseId, String s3Path, String diskPath) {
        try(Connection conn = getConnection();
            PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO Enterprise_detail (enterprise_name, enterprise_id, s3_path, disk_path) VALUES (?,?,?,?)")) {
            ps.setString(1, enterpriseName);
            ps.setInt(2, enterpriseId);
            ps.setString(3, s3Path);
            ps.setString(4, diskPath);
            ps.executeUpdate();
            System.out.println("Inserted enterprise: " + enterpriseName);
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    // ------------------- NEW METHOD -------------------
    public static void insertEnterpriseUserDetail(
            String enterpriseName,
            String userName,
            String password,
            String bucketName,
            String bucketId,      // stored as STRING
            String userQuota,
            String displayName,
            String alias,
            String bucketS3Path,
            String bucketDiskPath) {

        String sql = "INSERT INTO Enterprise_user_detail (" +
                     "enterprise_name, user_name, password, bucket_name, bucket_id, " +
                     "user_quota, display_name, alias, bucket_s3_path, bucket_disk_path) " +
                     "VALUES (?,?,?,?,?,?,?,?,?,?)";

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, enterpriseName);
            ps.setString(2, userName);
            ps.setString(3, password);
            ps.setString(4, bucketName);
            ps.setString(5, bucketId);   // bucket_id as String
            ps.setString(6, userQuota);
            ps.setString(7, displayName);
            ps.setString(8, alias);
            ps.setString(9, bucketS3Path);
            ps.setString(10, bucketDiskPath);

            ps.executeUpdate();
            System.out.println("Inserted user details for enterprise: " + enterpriseName +
                               " | bucket_id: " + bucketId);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
