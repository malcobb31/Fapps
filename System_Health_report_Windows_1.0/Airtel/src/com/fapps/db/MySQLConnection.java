package com.fapps.db;

import java.sql.Connection;
import java.sql.DriverManager;

public class MySQLConnection {

    public Connection getConnection(
            String DB_URL,
            String USER,
            String PASS) {

        Connection conn = null;

        try {

            String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";

            Class.forName(JDBC_DRIVER);

            String finalUrl = DB_URL;

            if (!finalUrl.toLowerCase().contains("useSSL=")) {
                finalUrl += (finalUrl.contains("?") ? "&" : "?")
                        + "useSSL=false";
            }

            if (!finalUrl.toLowerCase().contains("allowPublicKeyRetrieval=")) {
                finalUrl += (finalUrl.contains("?") ? "&" : "?")
                        + "allowPublicKeyRetrieval=true";
            }

            System.out.println("Connecting to MySQL: " + finalUrl);
            System.out.println("MySQL User: " + USER);

            conn = DriverManager.getConnection(
                    finalUrl,
                    USER,
                    PASS
            );

            conn.setAutoCommit(false);

            System.out.println("MySQL connection successful.");

            return conn;

        } catch (Exception e) {

            System.out.println("MySQL connection failed.");
            e.printStackTrace();

            return null;
        }
    }
}