package nslookup;

public class nslookup_main {

    public static void main(String[] args) {

        System.out.println("length of arguments : " + args.length + "\n\n ");

        for (String string : args) {
            System.out.println("--------------" + string + "------------");
        }

        if (args.length == 9) {

            if (nslookup_methods.processNslookupValidation(
                    args[0], // ssh_ip
                    args[1], // ssh_port  
                    args[2], // ssh_user
                    args[3], // ssh_pass
                    args[4], // db_url
                    args[5], // db_user
                    args[6], // db_pass
                    args[7], // table_name
                    args[8]  // domain
            )) {

                System.out.println("============================================================");
                System.out.println("=============Execution Successful============================");
                System.out.println("============================================================");

            } else {

                System.out.println("============================================================");
                System.out.println("=============Execution Failed================================");
                System.out.println("============================================================");
            }

        } else {

            System.out.println("===================EXCEPTION JAR=============================");
            System.out.println("Usage:");
            System.out.println("ssh_ip ssh_port ssh_user ssh_pass db_url db_user db_pass table_name domain");
        }
    }
}