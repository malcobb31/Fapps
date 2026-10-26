package process_summary;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class process_summary_methods {

    public void parseLogs(String sourceTable, String destTable, String dbUrl, String dbUser, String dbPass) {
//        String query =
//            "INSERT INTO " + destTable + " (" +
//            "Table_Name," +
//            "EP_Start, EP_End, EP_Duration_Sec," +
//            "FP_Start, FP_End, FP_Duration_Sec," +
//            "IP_Start, IP_End, IP_Duration_Sec" +
//            ") " +
//            "SELECT " +
//            "t.Table_Name, " +
//
//            // EP
//            "MAX(CASE WHEN t.Process_Name LIKE 'EP%' THEN t.Process_Start END) AS EP_Start, " +
//            "MAX(CASE WHEN t.Process_Name LIKE 'EP%' THEN t.Process_End END) AS EP_End, " +
//            "TIMESTAMPDIFF(SECOND, " +
//            "MAX(CASE WHEN t.Process_Name LIKE 'EP%' THEN t.Process_Start END), " +
//            "MAX(CASE WHEN t.Process_Name LIKE 'EP%' THEN t.Process_End END)" +
//            ") AS EP_Duration_Sec, " +
//
//            // FP
//            "MAX(CASE WHEN t.Process_Name LIKE 'FP%' THEN t.Process_Start END) AS FP_Start, " +
//            "MAX(CASE WHEN t.Process_Name LIKE 'FP%' THEN t.Process_End END) AS FP_End, " +
//            "TIMESTAMPDIFF(SECOND, " +
//            "MAX(CASE WHEN t.Process_Name LIKE 'FP%' THEN t.Process_Start END), " +
//            "MAX(CASE WHEN t.Process_Name LIKE 'FP%' THEN t.Process_End END)" +
//            ") AS FP_Duration_Sec, " +
//
//            // IP (IP)
//            "MAX(CASE WHEN t.Process_Name LIKE 'IP%' THEN t.Process_Start END) AS IP_Start, " +
//            "MAX(CASE WHEN t.Process_Name LIKE 'IP%' THEN t.Process_End END) AS IP_End, " +
//            "TIMESTAMPDIFF(SECOND, " +
//            "MAX(CASE WHEN t.Process_Name LIKE 'IP%' THEN t.Process_Start END), " +
//            "MAX(CASE WHEN t.Process_Name LIKE 'IP%' THEN t.Process_End END)" +
//            ") AS IP_Duration_Sec " +
//
//            "FROM " + sourceTable + " t " +
//            "WHERE t.Table_Name IS NOT NULL AND t.Table_Name <> '' " +
//            "GROUP BY t.Table_Name;";
    	String query =
    		    "INSERT INTO " + destTable + " (" +
    		    "Table_Name," +
    		    "EP_Start, EP_End, EP_Duration_Sec," +
    		    "FP_Start, FP_End, FP_Duration_Sec," +
    		    "IP_Start, IP_End, IP_Duration_Sec," +
    		    "Total_Row_Count," +
    		    "Total_File_Size_MB" +
    		    ") " +
    		    "SELECT " +
    		    "t.Table_Name, " +

    		    // EP
    		    "MAX(CASE WHEN t.Process_Name LIKE 'EP%' THEN t.Process_Start END), " +
    		    "MAX(CASE WHEN t.Process_Name LIKE 'EP%' THEN t.Process_End END), " +
    		    "TIMESTAMPDIFF(SECOND, " +
    		    "MAX(CASE WHEN t.Process_Name LIKE 'EP%' THEN t.Process_Start END), " +
    		    "MAX(CASE WHEN t.Process_Name LIKE 'EP%' THEN t.Process_End END)), " +

    		    // FP
    		    "MAX(CASE WHEN t.Process_Name LIKE 'FP%' THEN t.Process_Start END), " +
    		    "MAX(CASE WHEN t.Process_Name LIKE 'FP%' THEN t.Process_End END), " +
    		    "TIMESTAMPDIFF(SECOND, " +
    		    "MAX(CASE WHEN t.Process_Name LIKE 'FP%' THEN t.Process_Start END), " +
    		    "MAX(CASE WHEN t.Process_Name LIKE 'FP%' THEN t.Process_End END)), " +

    		    // IP
    		    "MAX(CASE WHEN t.Process_Name LIKE 'IP%' THEN t.Process_Start END), " +
    		    "MAX(CASE WHEN t.Process_Name LIKE 'IP%' THEN t.Process_End END), " +
    		    "TIMESTAMPDIFF(SECOND, " +
    		    "MAX(CASE WHEN t.Process_Name LIKE 'IP%' THEN t.Process_Start END), " +
    		    "MAX(CASE WHEN t.Process_Name LIKE 'IP%' THEN t.Process_End END)), " +

    		    //  NEW FIELDS
    		    "COALESCE(SUM(t.Raw_Count),0) AS Total_Row_Count, " +
    		    "COALESCE(SUM(t.File_Size_MB),0) AS Total_File_Size_MB " +

    		    "FROM " + sourceTable + " t " +
    		    "WHERE t.Table_Name IS NOT NULL AND t.Table_Name <> '' " +
    		    "GROUP BY t.Table_Name;";


        try (Connection conn = DriverManager.getConnection(dbUrl, dbUser, dbPass);
             Statement stmt = conn.createStatement()) {

            int rows = stmt.executeUpdate(query);
            System.out.println(" Process summary generated successfully for " + rows + " tables.");

        } catch (SQLException e) {
            System.err.println(" Error while generating process summary:");
            e.printStackTrace();
        }
    }
}
