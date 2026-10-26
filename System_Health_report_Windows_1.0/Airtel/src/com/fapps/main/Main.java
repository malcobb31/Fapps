package com.fapps.main;

import com.fapps.api.ProcessData;


public class Main {
	
	
	
	public static void main( String[] args )
	{
		//LoggerClient logClient = new LoggerClient();
		System.out.println("length of arguments : "+args.length+"\n\n ");
		for (String string : args) {
			System.out.println("--------------"+string+"------------");
		}
			if( args.length == 9) {
			int batch=Integer.parseInt(args[3]);
			if( new ProcessData().addSourceData(args[0],   //Info table:- system_health_information
												args[1],    //XLSX file path only without file name
												args[2],    //file name xlsx
												batch,    //batch 
												args[4],    //DB URL
												args[5],    //DB User Name
												args[6],    //DB Passowrd
												args[7],    //top 10 table:  system_health_top_10_processes
												args[8]))   //CPU Utilization Table : : system_health_cpu_utlization 
			//if( new ProcessData().addSourceData())
												

					
					
					
					
							
			{
				System.out.println("============================================================");
				System.out.println("=============Source data successfully inserted==============");
				System.out.println("============================================================");
			}
			else
			{	
				System.out.println("=====================FAILED=================================");
				System.out.println("=============Source data insertion failed===================");
				System.out.println("=====================FAILED=================================");
			}
						
		}
		else
		{
			System.out.println("===================EXCEPTION JAR==================================");
			System.out.println("EXCEPTION JAR: Please enter parameters in required order : System_Health_report_Windows_1.0");
			//logClient.ulog("Error", "{\"Alarm\": \"Main\", \"Error\": \"Please enter parameters in required order\"}","MAIN_ERROR" ,"LightStorm", "Alarm",true);
		}
	}
}