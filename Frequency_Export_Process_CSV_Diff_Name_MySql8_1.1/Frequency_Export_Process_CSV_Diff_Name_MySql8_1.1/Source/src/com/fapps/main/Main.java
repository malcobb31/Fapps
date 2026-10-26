package com.fapps.main;

import com.fapps.api.ProcessData;

public class Main {
	public static void main( String[] args ){
		System.out.println("length of arguments : "+args.length+"\n\n ");
		for (String string : args) {
			System.out.println("--------------"+string+"------------");
		}
		if( args.length == 12 ) {
			int batch=Integer.parseInt(args[5]);
			if( new ProcessData().addSourceData(args[0],   //source Table
												args[1],   //date
												args[2],   //schedule table
												args[3],   //Thread Limit
												args[4],   //file Path
												args[5],	   	 //batch											    
												args[6],    //db_URL											    
												args[7],    //db_User											    
												args[8],    //db_Pass
												args[9],     //db_name
												args[10],    //host
												args[11]))   //Event Table
			{
				System.out.println("============================================================");
				System.out.println("=============Source data successfully processed==============");
				System.out.println("============================================================");
			}
			else
			{	
				System.out.println("=====================FAILED=================================");
				System.out.println("=============Source data execution failed===================");
				System.out.println("=====================FAILED=================================");
			}
						
		}
		else
		{
			System.out.println("===================EXCEPTION JAR==================================");
			System.out.println("EXCEPTION JAR: Please enter parameters in required order : Frequency_Export_Process_CSV_1.0");
		}
	}
}