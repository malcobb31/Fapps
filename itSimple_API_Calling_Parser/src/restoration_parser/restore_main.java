package restoration_parser;
import restoration_parser.restore_methods;

public class restore_main {
	public static void main( String[] args ){
//		System.out.println("length of arguments : "+args.length+"\n\n ");
		for (String string : args) {
//			System.out.println("--------------"+string+"------------");
		}
		if( args.length == 8 ) {
			//int batch=Integer.parseInt(args[3]);
			if( restore_methods.processVeeamSessionData(
												args[0],	//base url
												args[1],	//username
												args[2],	//password
												args[3],	//Destination Table
												args[4],	//db_URL
												args[5],    //db_user											    
												args[6],    //db_pass											    
												args[7]))   //db_name
			{
//				System.out.println("============================================================");
				//System.out.println("=============Source data successfully processed==============");
//				System.out.println("============================================================");
			}else{	
//				System.out.println("======================================================");
				//System.out.println("=============Source data execution failed===================");
//				System.out.println("======================================================");
			}
		}else{
//			System.out.println("===================EXCEPTION JAR==================================");
//			System.out.println("EXCEPTION JAR: Please enter parameters in required order : Check_Data_Insertion_In_Table");
		}
	}
}