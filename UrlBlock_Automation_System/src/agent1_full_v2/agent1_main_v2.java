package agent1_full_v2;

public class agent1_main_v2 {

    public static void main(String[] args) {

        System.out.println("========================================");
        System.out.println("Starting NSLOOKUP Validation Job");
        System.out.println("========================================");

        try {

            if (args.length != 9) {
                System.out.println("===================EXCEPTION JAR=============================");
                System.out.println("Usage:");
                System.out.println("ssh_ip ssh_port ssh_user ssh_pass db_url db_user db_pass table_name domain");
                System.exit(1);
            }

            //  DO NOT print passwords
            System.out.println("SSH_IP       : " + args[0]);
            System.out.println("SSH_PORT     : " + args[1]);
            System.out.println("SSH_USER     : " + args[2]);
            System.out.println("DB_URL       : " + args[4]);
            System.out.println("DB_USER      : " + args[5]);
            System.out.println("TABLE_NAME   : " + args[7]);
            System.out.println("DOMAIN       : " + args[8]);

            boolean result = agent1_methods_v2.processNslookupValidation(
                    args[0], // ssh_ip
                    args[1], // ssh_port
                    args[2], // ssh_user
                    args[3], // ssh_pass
                    args[4], // db_url
                    args[5], // db_user
                    args[6], // db_pass
                    args[7], // table_name
                    args[8]  // domain
            );

            System.out.println("========================================");

            if (result) {
                System.out.println("=============Execution Successful============================");
                System.exit(0); //  success exit code
            } else {
                System.out.println("=============Execution Failed================================");
                System.exit(2); //  failure exit code
            }

        } catch (Exception e) {

            System.out.println("=============FATAL ERROR====================================");
            System.out.println("Exception: " + e.getMessage());
            System.exit(3);
        }
    }
}