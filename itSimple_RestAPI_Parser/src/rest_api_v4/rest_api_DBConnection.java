package rest_api_v4;

import java.sql.*;

import org.json.JSONObject;

public class rest_api_DBConnection {
    private static final String URL = "jdbc:mysql://localHost:3306/pview";
    private static final String USER = "root";
    private static final String PASS = "";

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASS);
    }

    public static boolean enterpriseExists(String enterpriseName) {
        try(Connection conn = getConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT * FROM backup_process_enterprise_detail WHERE enterprise_name=?")) {
            ps.setString(1, enterpriseName);
            ResultSet rs = ps.executeQuery();
            return rs.next();
        } catch(SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
    public static boolean userExists(String enterpriseName, String userName) {
        String sql = "SELECT 1 FROM backup_process_enterprise_user_detail WHERE enterprise_name = ? AND user_name = ?";
        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, enterpriseName);
            ps.setString(2, userName);
            ResultSet rs = ps.executeQuery();
            return rs.next();
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    public static int getEnterpriseId(String enterpriseName) {
        try(Connection conn = getConnection();
            PreparedStatement ps = conn.prepareStatement("SELECT enterprise_id FROM backup_process_enterprise_detail WHERE enterprise_name=?")) {
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
                "INSERT INTO backup_process_enterprise_detail (enterprise_name, enterprise_id, s3_path, disk_path) VALUES (?,?,?,?)")) {
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
    public static boolean bucketExists(String enterpriseName, String userName, String displayName) {
        boolean exists = false;
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            conn = getConnection(); // Your existing method to get DB connection
            String sql = "SELECT 1 FROM backup_process_enterprise_user_detail WHERE enterprise_name = ? AND user_name = ? AND display_name = ?";
            ps = conn.prepareStatement(sql);
            ps.setString(1, enterpriseName);
            ps.setString(2, userName);
            ps.setString(3, displayName);

            rs = ps.executeQuery();
            if (rs.next()) {
                exists = true;
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try { if (rs != null) rs.close(); } catch (Exception ignored) {}
            try { if (ps != null) ps.close(); } catch (Exception ignored) {}
            try { if (conn != null) conn.close(); } catch (Exception ignored) {}
        }

        return exists;
    }

    public static boolean user_status() {
        String getMaxIdSQL = "SELECT MAX(id) AS max_id FROM user_info";
        String updateSQL = "UPDATE user_info SET user_status = 'a' WHERE id = ?";
        
        try (Connection conn = getConnection();
             PreparedStatement psMax = conn.prepareStatement(getMaxIdSQL);
             ResultSet rs = psMax.executeQuery()) {
            
            if (rs.next()) {
                long maxId = rs.getLong("max_id");
                
                if (maxId == 0) {
                    System.out.println("No users found.");
                    return false;
                }
                
                try (PreparedStatement psUpdate = conn.prepareStatement(updateSQL)) {
                    psUpdate.setLong(1, maxId);
                    int rowsAffected = psUpdate.executeUpdate();
                    
                    System.out.println("Updated user_status for id = " + maxId);
                    return rowsAffected > 0;
                }
            }
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return false;
    }
    
    public static boolean updateTargetSMC(String enterpriseName, String userName, String targetSmcId, String targetSmcName) {
        String sql = "UPDATE backup_process_enterprise_user_detail " +
                     "SET Target_SMC_id = ?, Target_SMC_name = ? " +
                     "WHERE enterprise_name = ? AND user_name = ?";

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            ps.setString(1, targetSmcId);
            ps.setString(2, targetSmcName);
            ps.setString(3, enterpriseName);
            ps.setString(4, userName);

            return ps.executeUpdate() > 0;

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }
    public static boolean UserVolumeQuota(String enterpriseName,int enterpriseID , String userName, String volQuota, String Date) {
        String sql = "INSERT INTO backup_process_volume_details " 
                     + "(enterprise_name,enterprise_id,user_name,total_volume, used_volume,"
                     + " available_volume,date)" 
                    + "VALUES (?,?,?,?,?,?,?)";

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
        	
        	ps.setString(1, enterpriseName);
        	ps.setInt(2, enterpriseID);
            ps.setString(3, userName);
            ps.setString(4, volQuota);
            ps.setString(5, volQuota);
            ps.setString(6, volQuota);
            ps.setString(7, "2025-11-01");
           
            return ps.executeUpdate() > 0;

        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static JSONObject getTargetSMCByEnterprise(String enterprise) throws Exception {
        Connection con = getConnection();
        String sql = "SELECT Target_SMC_id, Target_SMC_name FROM backup_process_enterprise_user_detail " +
                     "WHERE enterprise_name=? AND Target_SMC_id IS NOT NULL LIMIT 1";

        PreparedStatement ps = con.prepareStatement(sql);
        ps.setString(1, enterprise);

        ResultSet rs = ps.executeQuery();

        if (rs.next()) {
            JSONObject obj = new JSONObject();
            obj.put("Target_SMC_id", rs.getString("Target_SMC_id"));
            obj.put("Target_SMC_name", rs.getString("Target_SMC_name"));
            return obj;
        }
        return null;
    }


    public static JSONObject fetchExistingSMC(String containerName, String enterpriseName) {
        JSONObject result = null;

        String query = "SELECT Target_SMC_id, Target_SMC_name FROM backup_process_enterprise_user_detail WHERE Target_SMC_name=? AND enterprise_name=?";

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(query)) {

            ps.setString(1, containerName);
            ps.setString(2, enterpriseName);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                result = new JSONObject();
                result.put("id", rs.getString("id"));
                result.put("name", rs.getString("name"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static JSONObject getExistingTargetSMC(String enterpriseName, String userName) {
        JSONObject result = null;

        String query = "SELECT Target_SMC_id, Target_SMC_name FROM backup_process_enterprise_user_detail WHERE enterprise_name=? AND user_name=?";

        try (Connection conn = getConnection();
             PreparedStatement ps = conn.prepareStatement(query)) {

            ps.setString(1, enterpriseName);
            ps.setString(2, userName);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                result = new JSONObject();
                result.put("Target_SMC_id", rs.getString("Target_SMC_id"));
                result.put("Target_SMC_name", rs.getString("Target_SMC_name"));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


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
    	String repo_name= enterpriseName+ "-"+ userName;
        String sql = "INSERT INTO backup_process_enterprise_user_detail (" +
                     "enterprise_name, user_name, password, bucket_name, bucket_id, " +
                     "user_quota, display_name, alias, bucket_s3_path, bucket_disk_path,user_repository_name) " +
                     "VALUES (?,?,?,?,?,?,?,?,?,?,?)";

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
            ps.setString(11, repo_name);

            ps.executeUpdate();
            System.out.println("Inserted user details for enterprise: " + enterpriseName +
                               " | bucket_id: " + bucketId);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
}
