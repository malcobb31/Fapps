package tp4_parser;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * PostgreSQLConnection - Database Connection Handler
 * 
 * Establishes connection to PostgreSQL database
 */
public class PostgreSQLConnection {

    /**
     * Get PostgreSQL database connection
     * 
     * @param DB_URL - Database URL (jdbc:postgresql://host:port/database)
     * @param USER - Database username
     * @param PASS - Database password
     * @return Connection object or null if failed
     */
    public Connection getConnection(String DB_URL, String USER, String PASS) {
        Connection conn = null;
        
        try {
            // Explicitly load PostgreSQL driver
            Class.forName("org.postgresql.Driver");

            // Create connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            conn.setAutoCommit(false);
            
            return conn;

        } catch (ClassNotFoundException e) {
            System.err.println("✗ PostgreSQL JDBC Driver not found!");
            System.err.println("EXCEPTION: " + e.getMessage());
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            System.err.println("✗ Database connection failed!");
            System.err.println("EXCEPTION: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}