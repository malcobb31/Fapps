package ktp10_upd_audit_process_v1;

/**
 * ktp10_audit_process_main
 *
 * Lightweight entry point for the KYC audit-log insert parser.
 *
 * Reads already-transformed kyc_linked_re-shaped CSV files and inserts
 * one audit row per CSV row into either:
 *
 *     kyc.kyc_individual_audit_log   (ckyc_no starts with 1-6)
 *     kyc.kyc_legal_audit_log        (ckyc_no starts with 7-9)
 *
 * This class only parses CLI arguments, builds the two DB configurations
 * (APP DB + KYC DB), and delegates all real work to
 * {@link ktp10_audit_process_methods}.
 *
 * CLI arguments (positional):
 *
 *   0  inputDir
 *   1  processedDir
 *   2  headerFlag
 *   3  delimiter
 *   4  appDbHost
 *   5  appDbPort
 *   6  appDbName
 *   7  appDbUser
 *   8  appDbPassword
 *   9  kycDbHost
 *   10 kycDbPort
 *   11 kycDbName
 *   12 kycDbUser
 *   13 kycDbPassword
 *   14 serverIP (optional)
 */
public class ktp10_upd_audit_process_main {

    public static void main(String[] args) {

    	ktp10_upd_audit_process_methods methods = new ktp10_upd_audit_process_methods();

        try {

            if (args.length < 14) {
                System.err.println(
                        "Usage: ktp10_audit_process_main "
                                + "<inputDir> <processedDir> <headerFlag> <delimiter> "
                                + "<appDbHost> <appDbPort> <appDbName> <appDbUser> <appDbPassword> "
                                + "<kycDbHost> <kycDbPort> <kycDbName> <kycDbUser> <kycDbPassword> "
                                + "[serverIP]");
                System.exit(1);
                return;
            }

            String inputDir      = args[0];
            String processedDir  = args[1];
            String headerFlag    = args[2];
            String delimiter     = args[3];

            String appDbHost     = args[4];
            String appDbPort     = args[5];
            String appDbName     = args[6];
            String appDbUser     = args[7];
            String appDbPassword = args[8];

            String kycDbHost     = args[9];
            String kycDbPort     = args[10];
            String kycDbName     = args[11];
            String kycDbUser     = args[12];
            String kycDbPassword = args[13];

            String serverIP = args.length > 14 ? args[14] : "";

            methods.processAllInDirectory(
                    inputDir,
                    processedDir,
                    headerFlag,
                    delimiter,
                    appDbHost,
                    appDbPort,
                    appDbName,
                    appDbUser,
                    appDbPassword,
                    kycDbHost,
                    kycDbPort,
                    kycDbName,
                    kycDbUser,
                    kycDbPassword,
                    serverIP);

        } catch (Exception e) {
            System.err.println("Fatal error in ktp10_audit_process_main: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}