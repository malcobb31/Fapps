package final_ip_v8;

public class final_ip_v8_main {
    public static void main(String[] args) throws Exception {

        System.out.println("Length of arguments: " + args.length);

        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        final_ip_v8_Methods obj = new final_ip_v8_Methods();

        if (args.length == 22) {

            obj.processAllInDirectory(
                args[0],   // directory containing CSVs
                args[1],   // processed directory
                args[2],   // header yes/no
                args[3],   // delimiter
                args[4],   // source db url
                args[5],   // source db user
                args[6],   // source db pass
                args[7],   // server IP
                args[8],   // postgres host
                args[9],   // postgres port
                args[10],  // postgres db name
                args[11],  // postgres user
                args[12],  // postgres pass
                args[13],  // destination SQL URL
                args[14],  // destination SQL user
                args[15],  // destination SQL pass
                args[16],  // number of tables to pick
                args[17],  // config table name
                args[18],  // max CPU utilization
                args[19],  // max memory utilization
                args[20],  // bulk number
                args[21]   // db type (sql / psql)
            ); //  FIXED: missing semicolon added here

            System.out.println("============================================================");
            System.out.println("============= Directory scan & import complete =============");
            System.out.println("============================================================");

        } else {

            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println("Usage:");
            System.out.println(
                "<dirPath> <processedDirPath> <headerFlag> <delimiter> " +
                "<dbUrl> <dbUser> <dbPass> <serverIP> <pdbHost> <pdbPort> <pdbName> <pdbUser> <pdbPass> " +
                "<dest_sql_url> <dest_sql_user> <dest_sql_pass> <numTablesToPickStr> <config_table> " +
                "<MAX_CPU_UTILIZATION> <MAX_MEMORY_UTILIZATION> <bulkNumberStr> <dbTypeStr>"
            );
        }
    }
}