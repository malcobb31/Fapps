package com.fapps.main;
import com.fapps.api.ProcessData;

public class Main {
	public static void main( String[] args ){
		System.out.println("length of arguments : "+args.length+"\n\n ");
		for (String string : args) {
			System.out.println("--------------"+string+"------------");
		}
		if( args.length == 9 ) {
			//int batch=Integer.parseInt(args[3]);
			if( new ProcessData().addSourceData(args[0],	//Config server Table
												args[1],	//Config site Table
												args[2],	//input date
												args[3],	//Destination Table
												args[4],	//category
												args[5],    //db_URL											    
												args[6],    //db_User											    
												args[7],    //db_Pass
												args[8]))   //db_name
			{
				System.out.println("============================================================");
				//System.out.println("=============Source data successfully processed==============");
				System.out.println("============================================================");
			}else{	
				System.out.println("======================================================");
				//System.out.println("=============Source data execution failed===================");
				System.out.println("======================================================");
			}
		}else{
			System.out.println("===================EXCEPTION JAR==================================");
			System.out.println("EXCEPTION JAR: Please enter parameters in required order : Check_Data_Insertion_In_Table");
		}
	}
}