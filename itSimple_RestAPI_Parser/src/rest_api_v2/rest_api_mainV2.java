package rest_api_v2;

import rest_api_v2.rest_api_methods;

public class rest_api_mainV2 {
    public static void main(String[] args) {
//        System.out.println("Length of arguments: " + args.length + "\n\n");
        for (String string : args) {
//            System.out.println("-------------- " + string + " ------------");
        }

        if (args.length == 8) {
            try {
                new rest_api_methods().run(
                    args[0], // enterpriseName
                    args[1], // username
                    args[2], // password
                    args[3], // confirmPassword
                    args[4], // accessKey
                    args[5], // secretKey
                    args[6], // bucketName
                    args[7]  // userQuota
                );
//                System.out.println("============================================================");
//                System.out.println("========= GET API execution completed successfully =========");
//                System.out.println("============================================================");
//                System.out.println("1");
            } catch (Exception e) {
//                System.out.println("======================================================");
//                System.out.println("========= GET API execution failed ====================");
//                System.out.println("======================================================");
//                e.printStackTrace();
//                System.out.println("0");
            }
        } else {
//            System.out.println("===================EXCEPTION JAR==================================");
//            System.out.println("EXCEPTION JAR: Please enter parameters in required order : ");
//            System.out.println("enterpriseName username password confirmPassword accessKey secretKey bucketName userQuota");
            System.out.println("Failure Wrong Parameter, Code: 6");
        }
    }
}
