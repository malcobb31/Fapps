package csv_to_config;

import java.io.File;
import java.sql.*;
import com.mysql.jdbc.Driver;

public class csv_to_config_methods {

    public void run(String csvPath,
                    String mysqlUrl,
                    String user,
                    String password,
                    String tempTable,
                    String configTable) throws Exception {

        Connection conn = null;
        Statement stmt = null;

        try {

//            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(mysqlUrl, user, password);
            conn.setAutoCommit(false);

            stmt = conn.createStatement();

            System.out.println("Connected to MySQL");

            createTempTable(stmt, tempTable, configTable);

            addColumnsIfMissing(conn, tempTable);

            loadCSV(stmt, csvPath, tempTable);

            String primaryKey = getPrimaryKey(conn, configTable);

            System.out.println("Primary Key Detected: " + primaryKey);

            generateChecksum(stmt, tempTable);
            generateChecksum(stmt, configTable);

            updateDelta(stmt, tempTable, configTable, primaryKey);

            conn.commit();

        } catch (Exception e) {

            if (conn != null) {
                conn.rollback();
            }

            throw e;
        }
        finally {

            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
        }
    }

    // create temp table if not exists
    private void createTempTable(Statement stmt,
                                 String tempTable,
                                 String configTable) throws SQLException {

        String sql = "CREATE TABLE IF NOT EXISTS " + tempTable +
                " LIKE " + configTable;

        stmt.execute(sql);

        System.out.println("Temp table verified");
    }

    // add checksum and status columns if missing
    private void addColumnsIfMissing(Connection conn,
                                     String tempTable) throws SQLException {

        String checkColumn =
                "SELECT COUNT(*) FROM information_schema.COLUMNS " +
                "WHERE TABLE_NAME=? AND COLUMN_NAME='checksum'";

        PreparedStatement ps = conn.prepareStatement(checkColumn);
        ps.setString(1, tempTable);

        ResultSet rs = ps.executeQuery();
        rs.next();

        if (rs.getInt(1) == 0) {

            Statement stmt = conn.createStatement();

            String alter =
                    "ALTER TABLE " + tempTable +
                    " ADD COLUMN checksum VARCHAR(32) DEFAULT '0'," +
                    " ADD COLUMN status TINYINT DEFAULT 0";

            stmt.execute(alter);

            stmt.close();

            System.out.println("Checksum and Status columns added");
        }

        rs.close();
        ps.close();
    }

    // load csv into temp table
    private void loadCSV(Statement stmt,
                         String csvPath,
                         String tempTable) throws SQLException {

        File file = new File(csvPath);

        String path = file.getAbsolutePath().replace("\\", "\\\\");

        String sql =
                "LOAD DATA LOCAL INFILE '" + path + "' " +
                "INTO TABLE " + tempTable + " " +
                "FIELDS TERMINATED BY ',' " +
                "ENCLOSED BY '\"' " +
                "LINES TERMINATED BY '\\n' " +
                "IGNORE 1 ROWS";

        stmt.execute(sql);

        System.out.println("CSV Loaded into temp table");
    }

    // detect primary key dynamically
    private String getPrimaryKey(Connection conn,
                                 String table) throws SQLException {

        String sql =
                "SELECT COLUMN_NAME " +
                "FROM information_schema.KEY_COLUMN_USAGE " +
                "WHERE TABLE_NAME=? " +
                "AND CONSTRAINT_NAME='PRIMARY'";

        PreparedStatement ps = conn.prepareStatement(sql);
        ps.setString(1, table);

        ResultSet rs = ps.executeQuery();

        if (rs.next()) {

            String pk = rs.getString(1);

            rs.close();
            ps.close();

            return pk;
        }

        rs.close();
        ps.close();

        throw new RuntimeException("Primary key not found!");
    }

    // generate md5 checksum
    private void generateChecksum(Statement stmt,
                                  String table) throws SQLException {

        String sql =
                "UPDATE " + table + " SET checksum = MD5(CONCAT_WS('|'," +
                "sno, dtid, date_time, sample_date, Times, Lat, Drive_Long," +
                "UL_Mbps, DL_Mbps, RTT_ms, Jitter_ms, Packet_Loss_Pct," +
                "network_type, rat_type, ta, cqi, rssi, asu, cqi_table_index," +
                "rsrp, rsrq, rssnr, mcc, mnc, tac, pci, cgi, earfcn," +
                "eutran_cell_id, enodeb, bandwidth, mobile_network_operator," +
                "mobile_no, ue_speed_kmph, neighbour_network_measurements," +
                "neighbour_cell_info, site_id, sector_geojson, Drive_Type," +
                "floor_img_path))";

        int rows = stmt.executeUpdate(sql);

        System.out.println("Checksum generated for " + rows + " rows in " + table);
    }

    // update delta records and mark matched
    private void updateDelta(Statement stmt,
                             String tempTable,
                             String configTable,
                             String pk) throws SQLException {

        System.out.println("Updating changed rows...");

        String update =
                "UPDATE " + configTable + " c " +
                "JOIN " + tempTable + " t " +
                "ON c." + pk + "=t." + pk + " " +
                "SET c.checksum=t.checksum, " +
                "c.status=1, " +
                "c.dtid=t.dtid, " +
                "c.date_time=t.date_time, " +
                "c.sample_date=t.sample_date, " +
                "c.Times=t.Times, " +
                "c.Lat=t.Lat, " +
                "c.Drive_Long=t.Drive_Long, " +
                "c.UL_Mbps=t.UL_Mbps, " +
                "c.DL_Mbps=t.DL_Mbps " +
                "WHERE c.checksum<>t.checksum";

        int updated = stmt.executeUpdate(update);

        System.out.println("Rows updated: " + updated);

        System.out.println("Marking matched rows...");

        String match =
                "UPDATE " + tempTable + " t " +
                "JOIN " + configTable + " c " +
                "ON c." + pk + "=t." + pk + " " +
                "SET t.status=1 " +
                "WHERE t.checksum=c.checksum";

        int matched = stmt.executeUpdate(match);

        System.out.println("Rows matched: " + matched);
    }
}
