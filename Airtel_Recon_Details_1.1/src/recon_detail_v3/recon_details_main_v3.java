package recon_detail_v3;

import java.sql.Connection;

public class recon_details_main_v3 {

    static PrintUtilMsg log = new PrintUtilMsg();

    public static void main(String[] args) {

        if (args.length < 11) {
            System.out.println("Usage: java ReconMain <db_url> <user> <pass> <target_table> <date> <ipm_table> <ipm_sno_col> <ipm_date_col> <url_table> <url_sno_col> <url_date_col>");
            return;
        }
        
        String dbUrl     = args[0];
        String user      = args[1];
        String pass      = args[2];
        String tableName = args[3];
        String date      = args[4];

        String ipmTable      = args[5];
        String ipmSnoColumn  = args[6];
        String ipmDateColumn = args[7];

        String urlTable      = args[8];
        String urlSnoColumn  = args[9];
        String urlDateColumn = args[10];

        Connection conn = null;

        try {
            log.sop("MAIN", "Starting Recon Job for date: " + date);

            MySQLConnection db = new MySQLConnection();
            conn = db.getConnection(dbUrl, user, pass);

            if (conn == null) {
                log.sop("MAIN", "DB CONNECTION FAILED");
                return;
            }

            recon_details_methods service =
                    new recon_details_methods(
                            conn, tableName, date,
                            ipmTable, ipmSnoColumn, ipmDateColumn,
                            urlTable, urlSnoColumn, urlDateColumn,
                            dbUrl, user, pass
                    );

            service.process();

            conn.commit(); //  commit only if NO exception

            log.sop("MAIN", "Recon Job Completed Successfully");

        } catch (Exception e) {

            log.sop("MAIN", "ERROR OCCURRED: " + e.getMessage());

            try {
                if (conn != null) {
                    conn.rollback();   //  THIS WAS MISSING
                    log.sop("MAIN", "ROLLBACK DONE");
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            e.printStackTrace();
        }
    }
}