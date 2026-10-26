package ep_beta_v1;

public class ep_beta_main_v1 {

    public static void main(String[] args) throws Exception {

        System.out.println("Length of arguments: " + args.length);

        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        ep_beta_methods obj = new ep_beta_methods();

        if (args.length == 9) {

            obj.process(
                args[0], // db_url
                args[1], // db_user
                args[2], // db_pass
                args[3], // input_date
                args[4], // config_table
                args[5], // ssh_ip
                args[6], // ssh_user
                args[7], // ssh_pass
                Integer.parseInt(args[8]) // ssh_port
            );

            System.out.println("============================================================");
            System.out.println("===================== PROCESS COMPLETE =====================");
            System.out.println("============================================================");

        } else {

            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println("Usage:");

            System.out.println(
                "<db_url> <db_user> <db_pass> <input_date> " +
                "<config_table> <ssh_ip> <ssh_user> <ssh_pass> <ssh_port>"
            );
        }
    }
}