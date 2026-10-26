package eyeq_parser;

public class eyeq_parser_main_v1 {

    public static void main(String[] args) throws Exception {

        System.out.println("Length of arguments: " + args.length);
        for (String arg : args) {
            System.out.println("-------------- " + arg + " ------------");
        }

        eyeq_parser_v1 parser = new eyeq_parser_v1();

        if (args.length == 2) {

            parser.downloadAndConvert(
                    args[0], // URL
                    args[1]  // output CSV path
            );

            System.out.println("============================================================");
            System.out.println("============= URL TXT download + CSV complete ===============");
            System.out.println("============================================================");

        } else {
            System.out.println("=================== INVALID ARGUMENTS ======================");
            System.out.println("Usage:");
            System.out.println("<txt_url> <output_csv_path>");
        }
    }
}
