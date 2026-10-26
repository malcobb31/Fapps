package li_parser_v1;

import java.util.Map;

public class PrintUtilMsg{
	public static String imsiToDis = "";
	public static String msgType   = "";
	public void sop( String tagName, String msg ){
		String className                  = null;
		int lineNumFromWhereThisFunCalled = 0;
		try{
			throw new Exception("");
		}catch(Exception e)
		{
			className  = e.getStackTrace()[1].getClassName();
			lineNumFromWhereThisFunCalled = e.getStackTrace()[1].getLineNumber();
		}
		System.out.println( this.getDateTime() + "\t["+tagName+"][" + className + "]\t[" + lineNumFromWhereThisFunCalled + "]\t" + msg );
	}

	private String getDateTime() {
		java.text.DateFormat dateFormat = new java.text.SimpleDateFormat("dd-MM-yyyy HH:mm:ss SSS");
		java.util.Date date             = new java.util.Date();
		return dateFormat.format(date);
	}
	
	public void eventLog(String level, String evtType, String desc) {

	    String log = "Time:" + getDateTime()
	            + " [" + level + "]"
	            + " Process_Name: IP"
	            + " Server_IP: 172.20.131.100"
	            + " Process_Version: 7.3.0"
	            + " Log_Level: EVT_LOG"
	            + " EVT_Type: " + evtType
	            + " Description: " + desc;

	    System.out.println(log);
	}


	public void printMap( String TAG, Map<String, String> map ){
		for( Map.Entry<String,String> entry : map.entrySet() ){
			this.sop( TAG, "possible event ["+entry.getKey()+"], possible action ["+entry.getValue()+"]" );
		}
	}

}