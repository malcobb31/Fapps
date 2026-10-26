package com.fapps.api;

import java.util.ArrayList;
import java.util.HashMap;

public class ProcessData {

	com.fapps.util.PrintUtilMsg pu = new com.fapps.util.PrintUtilMsg();

	
	
	
	public boolean addSourceData(String infoTable,  String filepath,String fileName,int batch, String db_URL, String db_Name,
							String db_Pass, String top10ProcessTable, String cpuUtilizationTable) {
	
	
			
		
		
		boolean status = false;
		
		
		String host_ip = ""; 				
		ArrayList<HashMap<String, String>> rawData = null;
		
				
		String[] fileNameSplit = null;
		fileNameSplit = fileName.split("_");
		
		host_ip = fileNameSplit[1].replaceAll("-", ".");
		host_ip = host_ip.replaceAll("#", ":");
		
		System.out.println("Host IP is: "+host_ip);
		
		//String timestamp = fileNameSplit[1]+"-"+fileNameSplit[2]+"-"+fileNameSplit[3]+" "+fileNameSplit[4]+":"+fileNameSplit[5].substring(0,2)+":00";
		String timestamp = fileNameSplit[2]+"-"+fileNameSplit[3]+"-"+fileNameSplit[4]+" "+fileNameSplit[5]+":"+fileNameSplit[6].substring(0,2)+":00";
		System.out.println("File timestamp is: "+timestamp);
		String currentTime = ""; 
		
		try {	
		
			
			//xlsxFileNameSplit = xlsxFileName.split("_"); 
			//timestamp = xlsxFileNameSplit[3];
			
			//String[] date_split = inputdate.split("MINUTE");	
			
			System.out.println("Calling function fileRead ");
			
			ReadAndWriteData RW = new ReadAndWriteData();
			
			
			currentTime = RW.currentTime();
						
				
				try {
					
					rawData = RW.fileRead(filepath, fileName);
					
					if(rawData!=null) {
						
						boolean informationStatus = RW.insertIntoInformationTable(
						        currentTime,
						        timestamp,
						        batch,
						        db_URL,
						        db_Name,
						        db_Pass,
						        infoTable,
						        rawData,
						        host_ip
						);

						boolean topProcessStatus = RW.insertIntoTopProcessTable(
						        currentTime,
						        timestamp,
						        batch,
						        db_URL,
						        db_Name,
						        db_Pass,
						        top10ProcessTable,
						        rawData,
						        host_ip
						);

						boolean cpuUtilizationStatus = RW.insertIntoCpuUtilizationTable(
						        currentTime,
						        timestamp,
						        batch,
						        db_URL,
						        db_Name,
						        db_Pass,
						        cpuUtilizationTable,
						        rawData,
						        host_ip
						);

						status = informationStatus && topProcessStatus && cpuUtilizationStatus;
						}else {
						System.out.println("System_Health_report_Windows_1.0:::: No data found for the file ");
					}
					
				}catch(Exception e) {
				    System.out.println("System_Health_report_Windows_1.0:::: Exception while processing file");
				    e.printStackTrace();
				    status = false;
				}
				
				
				
			
			
					
	
		}catch(Exception ex)
		{
			System.out.println("EXCEPTION JAR: System_Health_report_Windows_1.0: "+ex.toString());
			ex.printStackTrace();
		
			status = false;
		//ex.printStackTrace();
		}return status;
	}

	
}