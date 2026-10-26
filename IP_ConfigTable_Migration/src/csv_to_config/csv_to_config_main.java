package csv_to_config;

public class csv_to_config_main {

    public static void main(String[] args) throws Exception {

        System.out.println("Length of arguments: " + args.length);

        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        csv_to_config_methods obj = new csv_to_config_methods();

        // Expected Arguments:
        // 0 -> CSV File Path
        // 1 -> MySQL URL
        // 2 -> DB User
        // 3 -> DB Password
        // 4 -> Temp Table Name
        // 5 -> Config Table Name

        if (args.length == 6) {

            obj.run(
                    args[0],  // csv file
                    args[1],  // db url
                    args[2],  // db user
                    args[3],  // db password
                    args[4],  // temp table
                    args[5]   // config table
            );

            System.out.println("================================================");
            System.out.println("========== CHECKSUM SYNC COMPLETED ==============");
            System.out.println("================================================");

        } else {

            System.out.println("================ INVALID ARGUMENTS ================");
            System.out.println("Usage:");
            System.out.println(
                    "<csvPath> <mysqlUrl> <mysqlUser> <mysqlPass> <tempTable> <configTable>"
            );
        }
    }
}
