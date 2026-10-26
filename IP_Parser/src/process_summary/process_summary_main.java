package process_summary;



public class process_summary_main {
    public static void main(String[] args) throws Exception {
        System.out.println("Length of arguments: " + args.length);
        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        process_summary_methods parser = new process_summary_methods();
        		

        if (args.length == 5) {
            parser.parseLogs(
                args[0], // input_table name
                args[1], // output_table_name
                args[2], // db_url
                args[3], // db_user
                args[4] // db_pass
                  // Server_ip
            );

            System.out.println("============================================================");
            System.out.println("============= Log parsing & summary complete ==============");
            System.out.println("============================================================");
        } else {
            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println("Usage:");
            System.out.println("<file_path> <table_name> <db_url> <db_user> <db_pass> <Server_ip>");
        }
    }
}
