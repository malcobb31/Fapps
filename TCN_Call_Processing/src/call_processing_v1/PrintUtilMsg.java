package call_processing_v1;

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


	public void printMap( String TAG, Map<String, String> map ){
		for( Map.Entry<String,String> entry : map.entrySet() ){
			this.sop( TAG, "possible event ["+entry.getKey()+"], possible action ["+entry.getValue()+"]" );
		}
	}

	// public String normalizePhoneNumber(String phoneNumber) {
	//     if (phoneNumber == null || phoneNumber.trim().isEmpty()) {
	//         return phoneNumber;
	//     }
	
	//     // Remove all spaces, + symbols, and hyphens
	//     String cleaned = phoneNumber.replaceAll("[\\s+\\-]", "");
	
	//     // Get last 10 digits
	//     if (cleaned.length() >= 10) {
	//         return cleaned.substring(cleaned.length() - 10);
	//     }
	
	//     // If less than 10 digits, return as-is
	//     return cleaned;
	// }

}

