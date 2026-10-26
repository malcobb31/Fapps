package ktp13_process_v1;

import java.io.File;

public class ktp13_process_main {
	public static void main(String[] args) {
		if (args == null || args.length < 6) {
			System.err.println(
					"Usage: java KTP13_main <sourcePath> <destinationDir> <jdbcUrl> <dbUser> <dbPassword> <targetTable> [batchSize] [serverIP]");
			System.exit(1);
		}
		String sourcePath = args[0];
		String destinationDir = args[1];
		String jdbcUrl = args[2];
		String dbUser = args[3];
		String dbPassword = args[4];
		String targetTable = args[5];
		int batchSize = 50000;
		String serverIP = null;
		if (args.length >= 7) {
			try {
				batchSize = Integer.parseInt(args[6]);
			} catch (Exception ex) {
				System.err.println("Invalid batch size: " + args[6]);
				System.exit(1);
			}
		}
		if (args.length >= 8) {
			serverIP = args[7];
		}
		File src = new File(sourcePath);
		if (!src.exists()) {
			System.err.println("Source path does not exist: " + sourcePath);
			System.exit(1);
		}
		File dest = new File(destinationDir);
		if (!dest.exists()) {
			if (!dest.mkdirs()) {
				System.err.println("Unable to create destination directory: " + destinationDir);
				System.exit(1);
			}
		}
		ktp13_process_methods methods = new ktp13_process_methods();
		methods.execute(sourcePath, destinationDir, jdbcUrl, dbUser, dbPassword, targetTable, batchSize, serverIP);
	}
}