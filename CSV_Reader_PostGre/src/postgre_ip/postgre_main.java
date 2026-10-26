package postgre_ip;

public class postgre_main {
    public static void main(String[] args) {
        System.out.println("Length of arguments: " + args.length);
        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        if (args.length == 8) {
            // Call the method that scans directory and processes all raw_*.csv files
            ProcessData.processAllInDirectory(
                args[0], // directory containing CSVs
                args[1], // header yes/no
                args[2], // delimiter
                args[3], // dbHost
                args[4], // dbPort
                args[5], // dbName
                args[6], // dbUser
                args[7]  // dbPass
            );

            System.out.println("============================================================");
            System.out.println("============= Directory scan & import complete =============");
            System.out.println("============================================================");
        } else {
            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println("Usage:");
            System.out.println("<csv_directory> <header_yes_no> <delimiter> <dbHost> <dbPort> <dbName> <dbUser> <dbPass>");
        }
    }
}
