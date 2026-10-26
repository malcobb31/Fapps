package tp3_update;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * PostgreSQLConnection - Database Connection Handler
 *
 * Establishes connection to PostgreSQL database
 */
public class PostgreSQLConnection {

    PrintUtilMsg pu = new PrintUtilMsg();

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
            pu.log("INFO",
                   "DB_CONNECT_START - db_url: " + DB_URL + " db_user: " + USER,
                   "DB_CONNECT");

            // Explicitly load PostgreSQL driver
            Class.forName("org.postgresql.Driver");

            // Create connection
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            conn.setAutoCommit(false);

            pu.log("INFO",
                   "DB_CONNECT_SUCCESS - db_url: " + DB_URL + " auto_commit: false",
                   "DB_CONNECT");

            return conn;

        } catch (ClassNotFoundException e) {
            pu.log("ERROR",
                   "DB_CONNECT_FAILED - reason: postgresql_jdbc_driver_not_found detail: " + e.getMessage(),
                   "DB_CONNECT");
            return null;
        } catch (Exception e) {
            pu.log("ERROR",
                   "DB_CONNECT_FAILED - db_url: " + DB_URL + " reason: " + e.getMessage(),
                   "DB_CONNECT");
            return null;
        }
    }
}
