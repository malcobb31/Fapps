package tp3_update;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * FileManager - File Lifecycle Utility for TP3 CSV Pipeline
 *
 * Manages safe, atomic file movement between pipeline directories using Java NIO.
 * File movement is ONLY triggered externally after a confirmed successful DB commit.
 *
 * Directory contract:
 *   meta_dest      - Input directory: CSV files awaiting processing
 *   meta_processed - Success directory: CSV files that completed successfully
 *
 * Safety guarantees:
 *   - Files.move with REPLACE_EXISTING is atomic on most OS/FS combinations.
 *   - Destination directory is created if it does not exist.
 *   - Any permission or I/O failure is caught, logged, and returned as false.
 *   - On failure, source file is NOT touched (no partial movement).
 *
 * Idempotency:
 *   - isAlreadyProcessed() checks for the file's presence in meta_processed
 *     before processing begins, preventing duplicate ingestion.
 */
public class FileManager {

    PrintUtilMsg pu = new PrintUtilMsg();

    // =========================================================================
    // Public API
    // =========================================================================

    /**
     * Idempotency guard: check whether a file with the same name already exists
     * in the processed directory. If it does, the file has already been ingested
     * and should NOT be processed again.
     *
     * @param csvFilePath  - Full path to the CSV file in meta_dest
     * @param processedDir - Path to the meta_processed directory
     * @return true if the file already exists in processedDir (caller should SKIP), false otherwise
     */
    public boolean isAlreadyProcessed(String csvFilePath, String processedDir) {
        String fileName = null;
        try {
            fileName = Paths.get(csvFilePath).getFileName().toString();
            Path destPath = Paths.get(processedDir, fileName);
            boolean exists = Files.exists(destPath);

            if (exists) {
                pu.log("WARN",
                       "IDEMPOTENCY_CHECK - file_name: " + fileName
                       + " status: already_exists_in_processed_dir"
                       + " processed_path: " + destPath.toAbsolutePath()
                       + " action: skipping_to_prevent_duplicate_ingestion",
                       "FILE_IDEMPOTENCY");
            } else {
                pu.log("INFO",
                       "IDEMPOTENCY_CHECK - file_name: " + fileName + " status: not_yet_processed action: proceeding",
                       "FILE_IDEMPOTENCY");
            }

            return exists;

        } catch (Exception ex) {
            // Fail-open: if the check itself fails, allow processing to proceed and log the issue.
            pu.log("WARN",
                   "IDEMPOTENCY_CHECK_FAILED - file_name: " + fileName
                   + " reason: " + ex.getMessage()
                   + " action: allowing_processing_to_proceed",
                   "FILE_IDEMPOTENCY");
            return false;
        }
    }

    /**
     * Move a successfully processed CSV file from meta_dest to meta_processed.
     *
     * Must only be called AFTER:
     *   1. All DB batches have been committed successfully.
     *   2. No exceptions occurred during processing.
     *
     * Behavior:
     *   SUCCESS - file moved atomically; original is removed from source.
     *   FAILURE - file remains untouched in source; caller receives false.
     *
     * If a file with the same name already exists in processedDir, it is overwritten
     * (REPLACE_EXISTING). This handles retry scenarios after a partial move failure.
     *
     * @param csvFilePath  - Full path to the source CSV file in meta_dest
     * @param processedDir - Destination directory path (meta_processed)
     * @return true if file moved successfully, false on any failure
     */
    public boolean moveToProcessed(String csvFilePath, String processedDir) {
        String fileName = null;
        try {
            Path sourcePath = Paths.get(csvFilePath);
            fileName = sourcePath.getFileName().toString();
            Path destPath = Paths.get(processedDir, fileName);

            pu.log("INFO",
                   "FILE_MOVE_START - file_name: " + fileName
                   + " source: " + sourcePath.toAbsolutePath()
                   + " destination: " + destPath.toAbsolutePath(),
                   "FILE_MOVE");

            // Ensure destination directory exists (creates all parent dirs if needed)
            Files.createDirectories(Paths.get(processedDir));

            // Atomic move; overwrites destination if it already exists
            Files.move(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);

            pu.log("INFO",
                   "FILE_MOVE_SUCCESS - file_name: " + fileName
                   + " final_location: " + destPath.toAbsolutePath(),
                   "FILE_MOVE");
            return true;

        } catch (java.nio.file.AccessDeniedException ex) {
            pu.log("ERROR",
                   "FILE_MOVE_FAILED - file_name: " + fileName
                   + " reason: permission_denied source_remains_at: " + csvFilePath,
                   "FILE_MOVE");
            return false;

        } catch (java.nio.file.NoSuchFileException ex) {
            pu.log("ERROR",
                   "FILE_MOVE_FAILED - file_name: " + fileName + " reason: source_file_not_found",
                   "FILE_MOVE");
            return false;

        } catch (Exception ex) {
            pu.log("ERROR",
                   "FILE_MOVE_FAILED - file_name: " + fileName
                   + " reason: " + ex.getMessage()
                   + " source_remains_at: " + csvFilePath,
                   "FILE_MOVE");
            return false;
        }
    }

    /**
     * Move a failed CSV file to an error quarantine directory.
     *
     * Optional: called when the caller wants to quarantine bad files rather than
     * leaving them in meta_dest indefinitely.
     * File movement only happens on explicit caller request — NOT automatically on failure.
     *
     * @param csvFilePath - Full path to the source CSV file
     * @param errorDir    - Destination error directory path
     * @return true if file moved successfully, false on any failure
     */
    public boolean moveToError(String csvFilePath, String errorDir) {
        String fileName = null;
        try {
            Path sourcePath = Paths.get(csvFilePath);
            fileName = sourcePath.getFileName().toString();
            Path destPath = Paths.get(errorDir, fileName);

            pu.log("INFO",
                   "FILE_QUARANTINE_START - file_name: " + fileName
                   + " source: " + sourcePath.toAbsolutePath()
                   + " error_dir: " + destPath.toAbsolutePath(),
                   "FILE_QUARANTINE");

            Files.createDirectories(Paths.get(errorDir));
            Files.move(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING);

            pu.log("INFO",
                   "FILE_QUARANTINE_SUCCESS - file_name: " + fileName
                   + " error_location: " + destPath.toAbsolutePath(),
                   "FILE_QUARANTINE");
            return true;

        } catch (java.nio.file.AccessDeniedException ex) {
            pu.log("ERROR",
                   "FILE_QUARANTINE_FAILED - file_name: " + fileName + " reason: permission_denied",
                   "FILE_QUARANTINE");
            return false;

        } catch (java.nio.file.NoSuchFileException ex) {
            pu.log("ERROR",
                   "FILE_QUARANTINE_FAILED - file_name: " + fileName + " reason: source_file_not_found",
                   "FILE_QUARANTINE");
            return false;

        } catch (Exception ex) {
            pu.log("ERROR",
                   "FILE_QUARANTINE_FAILED - file_name: " + fileName + " reason: " + ex.getMessage(),
                   "FILE_QUARANTINE");
            return false;
        }
    }
}
