package opd_parser_final;
import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLConnection {
    public Connection getConnection(String DB_URL, String USER, String PASS) {
        Connection conn = null;
        try {
            // Use 5.1.x driver - compatible with Java 7
            Class.forName("com.mysql.jdbc.Driver");
            
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            conn.setAutoCommit(false);
            return conn;
        } catch (ClassNotFoundException e) {
            System.err.println(" MySQL JDBC Driver not found!");
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            System.err.println(" Database connection failed!");
            e.printStackTrace();
            return null;
        }
    }
}



