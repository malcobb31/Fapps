package ktp10_insert_process_v1;

/**
 * kyc_linked_re_insert_main
 *
 * Lightweight entry point for the kyc.kyc_linked_re insert parser.
 *
 * Responsibilities of this class (kept intentionally thin):
 *   - parse command-line arguments
 *   - construct / initialize ktp10_insert_process_methods
 *   - kick off processing
 *   - handle top-level exceptions cleanly
 *
 * All real work (Hikari pool setup, file discovery, psql CLI execution,
 * staging import, final insert, JSONB handling, column-name transformation,
 * logging, etc.) lives in ktp10_insert_process_methods, following the same
 * split used by the existing final_ip_v9 parser.
 *
 * Expected CLI arguments (positional):
 *   0  dirPath              - input directory containing the already-mapped CSV files
 *   1  processedDirPath     - directory to move successfully processed files into
 *   2  headerFlag           - "true"/"yes" if CSVs have a header row (expected: true)
 *   3  delimiter            - CSV delimiter (expected: "|")
 *   4  pdbHost              - PostgreSQL host
 *   5  pdbPort              - PostgreSQL port
 *   6  pdbName              - PostgreSQL database name
 *   7  pdbUser              - PostgreSQL user
 *   8  pdbPass              - PostgreSQL password
 *   9  serverIP             - server IP used only for log tagging (optional, can be empty)
 */
public class ktp10_insert_process_main {

    public static void main(String[] args) {

        ktp10_insert_process_methods methods = new ktp10_insert_process_methods();

        try {

            if (args.length < 9) {
                System.err.println(
                        "Usage: kyc_linked_re_insert_main <dirPath> <processedDirPath> <headerFlag> <delimiter> "
                                + "<pdbHost> <pdbPort> <pdbName> <pdbUser> <pdbPass> [serverIP]");
                System.exit(1);
                return;
            }

            String dirPath          = args[0];
            String processedDirPath = args[1];
            String headerFlag       = args[2];
            String delimiter        = args[3];
            String pdbHost          = args[4];
            String pdbPort          = args[5];
            String pdbName          = args[6];
            String pdbUser          = args[7];
            String pdbPass          = args[8];
            String serverIP         = args.length > 9 ? args[9] : "";

            methods.processAllInDirectory(
                    dirPath,
                    processedDirPath,
                    headerFlag,
                    delimiter,
                    pdbHost,
                    pdbPort,
                    pdbName,
                    pdbUser,
                    pdbPass,
                    serverIP);

        } catch (Exception e) {
            System.err.println("Fatal error in kyc_linked_re_insert_main: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}