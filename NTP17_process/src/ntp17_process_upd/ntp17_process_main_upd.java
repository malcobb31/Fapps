package ntp17_process_upd;

import java.sql.Connection;
import java.sql.DriverManager;

public class ntp17_process_main_upd {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: java SignupUserMain <csvFile> <jdbcUrl> <dbUser> <dbPass> [debugEnabled]");
            return;
        }

        String csvFile = args[0];
        String jdbcUrl = args[1];
        String dbUser = args[2];
        String dbPass = args[3];
        boolean debugEnabled = args.length > 4 ? Boolean.parseBoolean(args[4]) : true;

        String processName = "SignupUserParser";
        String processVersion = "1.0.0";
        String serverIP = extractHostFromJdbcUrl(jdbcUrl);

        ntp17_process_methods methods = new ntp17_process_methods(processName, processVersion, debugEnabled);

        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL driver not found: " + e.getMessage());
            return;
        }

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(jdbcUrl, dbUser, dbPass);
            conn.setAutoCommit(false);

            methods.process(csvFile, conn, serverIP);

            conn.commit();
        } catch (Exception e) {
            if (conn != null) {
                try {
                    conn.rollback();
                } catch (Exception ignored) {
                }
            }
            System.out.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    private static String extractHostFromJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) return null;

        try {
            String prefix = "jdbc:postgresql://";
            int idx = jdbcUrl.indexOf(prefix);
            if (idx >= 0) {
                String rest = jdbcUrl.substring(idx + prefix.length());
                int slash = rest.indexOf('/');
                if (slash > 0) {
                    String hostPort = rest.substring(0, slash);
                    int colon = hostPort.indexOf(':');
                    return colon > 0 ? hostPort.substring(0, colon) : hostPort;
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }
}