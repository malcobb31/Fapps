package api_parser;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLConnection {
	 public Connection getConnection(String DB_URL,String USER,String PASS){
			// JDBC driver name and database URL
			   //final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
			  // final String DB_URL = "jdbc:mysql://localhost:3306/pviewidea?useSSL=false";
			   //final String DB_URL = "jdbc:mysql://localhost:3306/pview3";

			   //  Database credentials
			   //final String USER = "mxchange";
			  // final String USER = "root";
			 //  final String PASS = "";
			   //final String PASS = "";
			   Connection conn = null;
			   //System.out.println("Inside get connection.");
			   try{
				   conn = DriverManager.getConnection(DB_URL, USER, PASS);
				   conn.setAutoCommit( false );
				   return conn;
			   }catch(Exception e){
				   e.printStackTrace();
				   return null;
			   }
}
}
