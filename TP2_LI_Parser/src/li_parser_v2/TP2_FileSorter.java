package li_parser_v2;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * TP2_FileSorter - Natural Timestamp Sort for CSV Files
 *
 * Extracts the last 6 underscore-separated parts of the filename (before
 * extension) as YYYY_MM_DD_HH_MM_SS and sorts ascending (earliest first).
 *
 * Example filename:
 *   raw_NA_NA_NA_696_2026_01_23_16_08_27.csv
 *   → extracted key: 2026_01_23_16_08_27
 */
public class TP2_FileSorter {

    private static final TP2_Logger logger = new TP2_Logger();

    /**
     * Pattern: captures the last 6 numeric segments (YYYY_MM_DD_HH_MM_SS) that
     * appear at the end of the filename stem (before the extension).
     * Handles filenames with any prefix token count.
     */
    private static final Pattern TIMESTAMP_PATTERN =
        Pattern.compile("(\\d{4})_(\\d{2})_(\\d{2})_(\\d{2})_(\\d{2})_(\\d{2})$");

    /**
     * Sort an array of CSV files by the timestamp embedded in their names.
     * Files whose timestamps cannot be parsed are placed LAST (stable relative
     * order among themselves) to avoid silently dropping them.
     *
     * @param files Array of files to sort (modified in-place)
     */
    public void sortByTimestamp(File[] files) {
        if (files == null || files.length <= 1) {
            return;
        }

        Arrays.sort(files, Comparator.comparing(this::extractTimestampKey));

        logger.info(TP2_Logger.EVT_FILE_DETECTED,
            "Files sorted by timestamp. Order after sort:");
        for (int i = 0; i < files.length; i++) {
            logger.info(TP2_Logger.EVT_FILE_DETECTED,
                "  [" + (i + 1) + "] " + files[i].getName()
                + " → key=" + extractTimestampKey(files[i]));
        }
    }

    /**
     * Extract a sortable timestamp key string from a filename.
     *
     * Strategy:
     *  1. Strip file extension.
     *  2. Find the LAST occurrence of the pattern YYYY_MM_DD_HH_MM_SS.
     *  3. Return as a plain concatenated numeric string (lexicographically
     *     sortable because all parts are zero-padded to fixed widths).
     *
     * On failure returns "99999999999999" so unparseable files sort last.
     */
    String extractTimestampKey(File file) {
        String stem = file.getName();
        int dotIdx = stem.lastIndexOf('.');
        if (dotIdx > 0) {
            stem = stem.substring(0, dotIdx);
        }

        Matcher m = TIMESTAMP_PATTERN.matcher(stem);
        if (m.find()) {
            // Concatenate all 6 groups → e.g. "20260123160827"
            return m.group(1) + m.group(2) + m.group(3)
                 + m.group(4) + m.group(5) + m.group(6);
        }

        logger.warn(TP2_Logger.EVT_FILE_DETECTED,
            "Could not extract timestamp from filename: " + file.getName()
            + " — placing at end of sort order");
        return "99999999999999";
    }
}
