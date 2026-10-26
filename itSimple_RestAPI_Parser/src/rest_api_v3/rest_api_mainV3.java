package rest_api_v3;

import rest_api_v3.rest_api_methods;

public class rest_api_mainV3 {
    public static void main(String[] args) {
//        System.out.println("Length of arguments: " + args.length + "\n\n");
        for (String string : args) {
//            System.out.println("-------------- " + string + " ------------");
        }

        if (args.length == 2) {
            try {
                new rest_api_methods().run(
                    args[0], // id
                    args[1] // path
                   
                );
//                System.out.println("============================================================");
//                System.out.println("========= GET API execution completed successfully =========");
//                System.out.println("============================================================");
            } catch (Exception e) {
//                System.out.println("======================================================");
//                System.out.println("========= GET API execution failed ====================");
//                System.out.println("======================================================");
//                e.printStackTrace();
            }
        } else {
            System.out.println("===================EXCEPTION JAR==================================");
            System.out.println("EXCEPTION JAR: Please enter parameters in required order : ");
            System.out.println("id , path");
        }
    }
}
