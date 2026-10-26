package Mysql_DailyV1;

public class Mysql_daily_main_v1 {

    public static void main(String[] args) {

        System.out.println("Length of arguments: " + args.length);
        for (int i = 0; i < args.length; i++) {
            System.out.println("-------------- " + args[i] + " ------------");
        }

        // Expected args:
        // 0 -> dbUrl
        // 1 -> dbUser
        // 2 -> dbPass
        // 3 -> serverIP
        if (args.length == 4) {

            Mysql_daily_methods.main(
                    args[0], //dbUrl
                    args[1], // dbUser
                    args[2], // dbPass
                    args[3]  // serverIP
            );

            System.out.println("============================================================");
            System.out.println("============= MySQL Daily Monitoring Completed =============");
            System.out.println("============================================================");

        } else {
            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println("Usage:");
            System.out.println("<dbUrl> <dbUser> <dbPass> <serverIP>");
        }
    }
}