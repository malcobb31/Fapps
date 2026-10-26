package binlog_reader_v1;

public class binlog_main_v1 {

    public static void main(String[] args) {

        try {

            System.out.println("Total Arguments Received: " + args.length);

            for (int i = 0; i < args.length; i++) {
                System.out.println("Arg[" + i + "] : " + args[i]);
            }

            if (args.length != 7) {

                System.out.println("\nUsage:");
                System.out.println(
                        "java -cp binlog_reader.jar binlog_reaeder_v1.binlog_main_v1 " +
                        "<mysqlbinlog_path> <binlog_file> <start_datetime> <stop_datetime> " +
                        "<mysql_url> <mysql_user> <mysql_pass>"
                );

                System.exit(1);
            }

            binlog_methods parserService = new binlog_methods();

            parserService.startBinlogReader(
                    args[0],
                    args[1],
                    args[2],
                    args[3],
                    args[4],
                    args[5],
                    args[6]
            );

            System.out.println("\n====================================================");
            System.out.println("=========== BINLOG PARSING COMPLETED ===============");
            System.out.println("====================================================");

        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}