package ftp_beta_v2;

public class ftp_beta_main_v2 {

    private static final int EXPECTED_ARGS = 10;

    public static void main(String[] args) throws Exception {

        System.out.println("Length of arguments: " + args.length);

        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        ftp_beta_methods obj = new ftp_beta_methods();

        if (args.length == EXPECTED_ARGS) {

            obj.processFTP(
                    args[0],
                    args[1],
                    args[2],
                    args[3],
                    args[4],
                    args[5],
                    args[6],
                    args[7],
                    args[8],
                    args[9]
            );

            System.out.println("============================================================");
            System.out.println("==================== SFTP PROCESS COMPLETE =================");
            System.out.println("============================================================");

        } else {

            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println(
                "<db_url> <db_user> <db_pass> <input_date> <no_of_threads> " +
                "<config_table> <ftp_server_ip> <ftp_server_user> " +
                "<ftp_server_pass> <ftp_server_port>"
            );
        }
    }
}