package rest_api_job;

import rest_api_job.rest_api_methods;

public class rest_job_api_main {
    public static void main(String[] args) {
        System.out.println("Length of arguments: " + args.length + "\n\n");
        for (String string : args) {
            System.out.println("-------------- " + string + " ------------");
        }

        if (args.length == 0) {
        
            
            try {
                new rest_api_methods().run();
                System.out.println("============================================================");
                System.out.println("========= GET API execution completed successfully =========");
                System.out.println("============================================================");
            } catch (Exception e) {
                System.out.println("======================================================");
                System.out.println("========= GET API execution failed ====================");
                System.out.println("======================================================");
                e.printStackTrace();
            }
        } else {
            System.out.println("===================EXCEPTION JAR==================================");
            System.out.println("EXCEPTION JAR: Please enter parameters in required order : ");
            System.out.println("enterpriseName username password confirmPassword accessKey secretKey bucketName userQuota");
        }
    }
}
