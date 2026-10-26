//package url_parserV1;
//
//import url_parserV1.url_parser_methods;
//
//public class url_parser_mainV1 {
//
//	 public static void main(String[] args) throws Exception {
//	        System.out.println("Length of arguments: " + args.length);
//	        for (String arg : args) {
//	            System.out.println("-------------- " + arg + " ------------");
//	        }
//
//	        url_parser_methods parser = new url_parser_methods();
//	        		
//
//	        if (args.length == 3) {
//	            parser.run(
//	                args[0], // job ids
//	                args[1], // username
//	                args[2] // password
//	      
//	                  
//	            );
//
//	            System.out.println("============================================================");
//	            System.out.println("============= Process complete ==============");
//	            System.out.println("============================================================");
//	        } else {
//	            System.out.println("=================== INVALID ARGUMENTS ======================");
//	            System.out.println("Usage:");
//	            System.out.println("<job_id> <username> <password>");
//	        }
//	    }
//}
