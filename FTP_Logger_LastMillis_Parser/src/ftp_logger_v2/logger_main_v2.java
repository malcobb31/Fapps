package ftp_logger_v2;

import ftp_logger_v2.ProcessData;

public class logger_main_v2 {

    public static void main(String[] args) throws Exception {

        System.out.println("Length of arguments: " + args.length);

        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        if (args.length == 8) {

            ProcessData.process(
                args[0],  // inputFileName
                args[1],  // inputFolder
                args[2],  // outputFolder
                args[3],  // dbUrl
                args[4],  // dbUser
                args[5],  // dbPass
                args[6],  // tableName
                args[7]   // sftpOutputFilepath
            );

            System.out.println("============================================================");
            System.out.println("============= Directory scan & import complete =============");
            System.out.println("============================================================");

        } else {

            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println("Usage:");
            System.out.println(
                "<inputFileName> <inputFolder> <outputFolder> " +
                "<dbUrl> <dbUser> <dbPass> <tableName> <sftpOutputFilepath>"
            );
        }
    }
}