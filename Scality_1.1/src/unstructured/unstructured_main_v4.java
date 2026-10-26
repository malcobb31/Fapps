package unstructured;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

public class unstructured_main_v4 {

    private static final String S3_ENDPOINT = "http://172.20.161.11";
    private static final String REGION = "us-east-1";

    private static final String ACCESS_KEY = "HPZBCYK10H24VCNRKKM7";
    private static final String SECRET_KEY = "hNz4QcK6TWQUmCuqfUSr+DBHXC2kHo93Fhq5xqLm";

    private static final String SOURCE_BUCKET = "dc-bulkarch-bucket1";
    private static final String DEST_BUCKET   = "testincdata";
    private static final String DEST_PREFIX   = "dc-bulkarch-bucket1/";

    private static final String INPUT_CSV = "files_to_copy.csv";
    private static final int THREAD_COUNT = 24;

    private static final Logger logger =
            Logger.getLogger(unstructured_main_v4.class.getName());

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

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        AtomicInteger total    = new AtomicInteger();
        AtomicInteger copied   = new AtomicInteger();
        AtomicInteger skipped  = new AtomicInteger();
        AtomicInteger verified = new AtomicInteger();
        AtomicInteger failed   = new AtomicInteger();

        logger.info("Incremental copy started (PER-FILE FOLDER MODE)");

        AmazonS3 s3 = createS3Client();

        List<String> sourceKeys = loadCSV(INPUT_CSV);
        logger.info("CSV files loaded : " + sourceKeys.size());

        // Preload destination keys ONCE
        Set<String> destKeys = preloadDestinationKeys(s3);
        logger.info("Destination keys loaded : " + destKeys.size());

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        CompletionService<Void> cs = new ExecutorCompletionService<>(pool);

        for (String sourceKey : sourceKeys) {
            cs.submit(() -> {

                total.incrementAndGet();

                // -------------------------------
                // PER FILE FOLDER LOGIC
                // -------------------------------
                String fileName =
                        sourceKey.substring(sourceKey.lastIndexOf('/') + 1);

                int dot = fileName.lastIndexOf('.');
                String folderName =
                        (dot > 0) ? fileName.substring(0, dot) : fileName;

                String destKey =
                        DEST_PREFIX + folderName + "/" + fileName;
                // -------------------------------

                if (destKeys.contains(destKey)) {
                    skipped.incrementAndGet();
                    return null;
                }

                try {
                    s3.copyObject(
                            SOURCE_BUCKET,
                            sourceKey,
                            DEST_BUCKET,
                            destKey);

                    copied.incrementAndGet();

                    ObjectMetadata srcMeta =
                            s3.getObjectMetadata(SOURCE_BUCKET, sourceKey);
                    ObjectMetadata dstMeta =
                            s3.getObjectMetadata(DEST_BUCKET, destKey);

                    if (srcMeta.getContentLength() ==
                        dstMeta.getContentLength()) {

                        int v = verified.incrementAndGet();
                        if (v % 1000 == 0) {
                            logger.info("VERIFIED so far : " + v);
                        }

                    } else {
                        failed.incrementAndGet();
                        logger.severe("SIZE MISMATCH : " + sourceKey);
                    }

                } catch (Exception e) {
                    failed.incrementAndGet();
                    logger.severe(
                            "FAILED : " + sourceKey + " | " + e.getMessage());
                }

                return null;
            });
        }

        for (int i = 0; i < sourceKeys.size(); i++) {
            cs.take();
        }

        pool.shutdown();

        long timeTaken =
                (System.currentTimeMillis() - startTime) / 1000;

        logger.info("========== JOB SUMMARY ==========");
        logger.info("Total files   : " + total.get());
        logger.info("Copied        : " + copied.get());
        logger.info("Skipped       : " + skipped.get());
        logger.info("Verified      : " + verified.get());
        logger.info("Failed        : " + failed.get());
        logger.info("Time (sec)    : " + timeTaken);
        logger.info("Throughput    : " +
                (total.get() / Math.max(1, timeTaken)) + " files/sec");
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
                    line = line.substring(
                            (SOURCE_BUCKET + "/").length());
                }

                keys.add(line);
            }
        }
        return keys;
    }

    private static Set<String> preloadDestinationKeys(AmazonS3 s3) {

        Set<String> keys = ConcurrentHashMap.newKeySet();

        ObjectListing listing =
                s3.listObjects(DEST_BUCKET, DEST_PREFIX);

        do {
            for (S3ObjectSummary s : listing.getObjectSummaries()) {
                keys.add(s.getKey());
            }
            listing = s3.listNextBatchOfObjects(listing);
        } while (listing.isTruncated());

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
                .withCredentials(
                        new AWSStaticCredentialsProvider(creds))
                .build();
    }
}
