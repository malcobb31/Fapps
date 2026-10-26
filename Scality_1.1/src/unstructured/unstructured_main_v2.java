package unstructured;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class unstructured_main_v2 {

    private static final String S3_ENDPOINT = "http://172.20.161.11";
    private static final String REGION = "us-east-1";

    private static final String ACCESS_KEY = "HPZBCYK10H24VCNRKKM7";
    private static final String SECRET_KEY = "hNz4QcK6TWQUmCuqfUSr+DBHXC2kHo93Fhq5xqLm";

    private static final String SOURCE_BUCKET = "dc-bulkarch-bucket1";
    private static final String DEST_BUCKET   = "testincdata";
    private static final String DEST_PREFIX   = "dc-bulkarch-bucket1/";

    private static final String INPUT_CSV = "files_to_copy.csv";
    private static final int THREAD_COUNT = 8;

    private static final Logger logger =
            Logger.getLogger(unstructured_main_v2.class.getName());

    static {
        Logger root = Logger.getLogger("");
        for (Handler h : root.getHandlers()) {
            root.removeHandler(h);
        }

        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleLogFormatter());
        handler.setLevel(Level.INFO);

        root.addHandler(handler);
        root.setLevel(Level.INFO);
    }

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        AtomicInteger total = new AtomicInteger();
        AtomicInteger copied = new AtomicInteger();
        AtomicInteger skipped = new AtomicInteger();
        AtomicInteger verified = new AtomicInteger();
        AtomicInteger checksumFailed = new AtomicInteger();

        logger.info("Incremental copy started (CSV + Parallel + SHA-256)");

        AmazonS3 s3 = createS3Client();

        List<String> sourceKeys;
        try {
            sourceKeys = loadCSV(INPUT_CSV);
        } catch (Exception e) {
            logger.severe("Failed to read CSV: " + e.getMessage());
            return;
        }

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        CompletionService<Void> cs = new ExecutorCompletionService<>(pool);

        for (String sourceKey : sourceKeys) {
            cs.submit(() -> {

                total.incrementAndGet();
                String destKey = DEST_PREFIX + sourceKey;

                if (destinationExists(s3, DEST_BUCKET, destKey)) {
                    skipped.incrementAndGet();
                    logger.info("SKIPPED : " + sourceKey);
                    return null;
                }

                s3.copyObject(SOURCE_BUCKET, sourceKey, DEST_BUCKET, destKey);
                copied.incrementAndGet();

                String srcHash = sha256(
                        s3.getObject(SOURCE_BUCKET, sourceKey).getObjectContent()
                );
                String dstHash = sha256(
                        s3.getObject(DEST_BUCKET, destKey).getObjectContent()
                );

                if (srcHash.equals(dstHash)) {
                    verified.incrementAndGet();
                    logger.info("VERIFIED : " + sourceKey);
                } else {
                    checksumFailed.incrementAndGet();
                    logger.severe("CHECKSUM FAILED : " + sourceKey);
                }

                return null;
            });
        }

        try {
            for (int i = 0; i < sourceKeys.size(); i++) {
                cs.take();
            }
        } catch (InterruptedException ignored) {}

        pool.shutdown();

        logger.info("========== JOB SUMMARY ==========");
        logger.info("Total files   : " + total.get());
        logger.info("Copied        : " + copied.get());
        logger.info("Skipped       : " + skipped.get());
        logger.info("Verified      : " + verified.get());
        logger.info("Checksum fail : " + checksumFailed.get());
        logger.info("Time (sec)    : " +
                (System.currentTimeMillis() - startTime) / 1000);
    }

    // ================= UTIL =================

    private static List<String> loadCSV(String path) throws Exception {
        List<String> keys = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            boolean headerSkipped = false;
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                if (!headerSkipped) {
                    headerSkipped = true;
                    if (line.equalsIgnoreCase("file_name")) continue;
                }

                if (line.startsWith(SOURCE_BUCKET + "/")) {
                    line = line.substring((SOURCE_BUCKET + "/").length());
                }
                keys.add(line);
            }
        }
        return keys;
    }

    private static AmazonS3 createS3Client() {
        BasicAWSCredentials creds =
                new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);

        ClientConfiguration config = new ClientConfiguration();
        config.setSignerOverride("AWSS3V4SignerType");

        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                S3_ENDPOINT, REGION))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(config)
                .withCredentials(new AWSStaticCredentialsProvider(creds))
                .build();
    }

    private static boolean destinationExists(
            AmazonS3 s3, String bucket, String key) {
        try {
            s3.getObjectMetadata(bucket, key);
            return true;
        } catch (AmazonS3Exception e) {
            return e.getStatusCode() != 404;
        }
    }

    private static String sha256(InputStream is) throws Exception {
        MessageDigest digest =
                MessageDigest.getInstance("SHA-256");
        byte[] buffer = new byte[8192];
        int read;
        try (InputStream in = is) {
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : digest.digest()) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}

/**
 *  EXPLICIT logging formatter (NO CLASS CLASH)
 */
class SimpleLogFormatter extends java.util.logging.Formatter {

    private final SimpleDateFormat sdf =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public String format(LogRecord r) {
        return String.format(
                "%s | %s | %s | %s%n",
                sdf.format(new Date(r.getMillis())),
                r.getLevel(),
                r.getLoggerName(),
                r.getMessage()
        );
    }
}
