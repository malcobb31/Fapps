package agent2_ssh;

public class agent2_main {

    public static void main(String[] args) {

        System.out.println("length of arguments : " + args.length);

        if (args.length == 8) {

            agent2_methods.processSSHCheck(
                    args[0], // ssh_ip
                    args[1], // ssh_port
                    args[2], // ssh_user
                    args[3], // ssh_pass
                    args[4], // db_url
                    args[5], // db_user
                    args[6], // db_pass
                    args[7]  // table_name
            );

        } else {
            System.out.println("Usage:");
            System.out.println("ssh_ip ssh_port ssh_user ssh_pass db_url db_user db_pass table_name");
        }
    }
}