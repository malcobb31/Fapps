package preprocess_ip;

import preprocess_ip.ProcessData;

public class ftp_postgre_main {
    public static void main(String[] args) {
        System.out.println("Length of arguments: " + args.length);
        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        if (args.length == 7) {
            // Call the method that scans directory and processes all raw_*.csv files
        	ProcessData.process(
                args[0],  // directory containing CSVs
                args[1],  // dir for processed file
                args[2],  // header yes/no
                args[3],  // delimeter
                args[4],  // sqldburl
                args[5],  // sdbuser
                args[6]  // sdbpass
                
            );

            System.out.println("============================================================");
            System.out.println("============= Directory scan & import complete =============");
            System.out.println("============================================================");
        } else
            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println("Usage:");
            System.out.println("<csv_directory> <dbUrl> <dbUser> <dbPass> <destin_dir> <Tablename> <server_ip>");
        }
    }

