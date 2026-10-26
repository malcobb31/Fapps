package audit_process;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;

public class TableAuditMain {

    public static void main(String[] args) {

        if (args.length < 6) {
            System.out.println(
                    "Usage: java TableAuditMain <dbUrl> <dbUser> <dbPass> <dbName> <auditDate> <masterTable>");
            System.exit(1);
        }

        String dbUrl = args[0].trim();
        String dbUser = args[1].trim();
        String dbPass = args[2];
        String dbName = args[3].trim();
        String auditDate = args[4].trim();
        String masterTable = args[5].trim();

        if (dbUrl.isEmpty()) {
            System.out.println("ERROR: dbUrl must not be empty.");
            System.exit(1);
        }
        if (dbUser.isEmpty()) {
            System.out.println("ERROR: dbUser must not be empty.");
            System.exit(1);
        }
        if (dbName.isEmpty()) {
            System.out.println("ERROR: dbName must not be empty.");
            System.exit(1);
        }
        if (masterTable.isEmpty()) {
            System.out.println("ERROR: masterTable must not be empty.");
            System.exit(1);
        }

        try {
            LocalDate.parse(auditDate);
        } catch (DateTimeParseException e) {
            System.out.println("ERROR: Invalid auditDate. Expected format yyyy-MM-dd.");
            System.exit(1);
        }

        try {
            TableAuditMethods methods = new TableAuditMethods();
            methods.runAudit(dbUrl, dbUser, dbPass, dbName, auditDate, masterTable);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}