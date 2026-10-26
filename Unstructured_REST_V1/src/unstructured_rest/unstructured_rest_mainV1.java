package unstructured_rest;

import unstructured_rest.unstructured_restV1;

public class unstructured_rest_mainV1 {

    public static void main(String[] args) {

        System.out.println("length of arguments : " + args.length + "\n\n ");

        for (String string : args) {
            System.out.println("--------------" + string + "------------");
        }

        if (args.length == 1) {

            try {
                unstructured_restV1 rest = new unstructured_restV1();

                rest.run(
                        args[0]              // csvPath
                       
                );

                System.out.println("============================================================");
                System.out.println("============= FOLDER CREATION COMPLETED ====================");
                System.out.println("============================================================");

            } catch (Exception e) {
                System.out.println("======================================================");
                System.out.println("============= FOLDER CREATION FAILED =================");
                System.out.println("======================================================");
                e.printStackTrace();
            }

        } else {
            System.out.println("=================== EXCEPTION JAR ============================");
            System.out.println(
                "Please enter parameters in required order:\n" +
                "csvPath baseUrl dbName username password objectStoragePk bucketId"
            );
        }
    }
}
