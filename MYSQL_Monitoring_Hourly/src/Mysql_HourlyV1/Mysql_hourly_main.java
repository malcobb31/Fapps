package Mysql_HourlyV1;

public class Mysql_hourly_main {

    public static void main(String[] args) {

        System.out.println("Length of arguments: " + args.length);
        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        // Expected args:
        // 0 -> dbUrl
        // 1 -> dbUser
        // 2 -> dbPass
        // 3 -> serverIP
        if (args.length == 4) {

            Mysql_hourly_methods.main(
                    args[0], // dbUrl
                    args[1], // dbUser
                    args[2], // dbPass
                    args[3]  // serverIP
            );

            System.out.println("============================================================");
            System.out.println("============= MySQL Monitoring Completed ===================");
            System.out.println("============================================================");

        } else {
            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println("Usage:");
            System.out.println("<dbUrl> <dbUser> <dbPass> <serverIP>");
        }
    }
}
