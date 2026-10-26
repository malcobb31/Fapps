package ip_preprocess_v4;

public class Ip_preprocess_main_v4 {

    public static void main(String[] args) {

        if (args.length != 7) {
            System.out.println("Usage:");
            System.out.println("<csv_directory> <dbUrl> <dbUser> <dbPass> <destin_dir> <Tablename> <server_ip>");
            return;
        }

        System.out.println("Length of arguments: " + args.length);

        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        Ip_preprocess_methods_v4.process(
                args[0],
                args[1],
                args[2],
                args[3],
                args[4],
                args[5],
                args[6]
        );
    }
}
