package dell_ecs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

public class dell_ecs_main {

    // ================= ECS CONFIG =================
    private static final String S3_ENDPOINT = "http://192.168.90.163:80";
    private static final String REGION = "us-east-1";

    private static final String ACCESS_KEY = "AKIA5071D3A94B4DABD6";
    private static final String SECRET_KEY = "7w/OjXG1UEsikGbSeFTFQu01z7WkygPQqfjdrPpq";
    // ==============================================

    // ================= JOB CONFIG =================
    private static final String SOURCE_BUCKET = "uat_testbed_bucket";
    private static final String DEST_BUCKET   = "uat_testbed_bucket_02";

    // folder name in destination
    private static final String DEST_PREFIX  = SOURCE_BUCKET + "/";

    private static final int PICK_FILES = 10;
    private static final int THREAD_COUNT = 5;
    private static final int MAX_RETRY = 3;

    private static final boolean VERIFY_SIZE = true;
    // ==============================================

    private static final Logger logger = Logger.getLogger(dell_ecs_main.class.getName());

    static {
        Logger root = Logger.getLogger("");
        for (Handler h : root.getHandlers()) root.removeHandler(h);

        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleLogFormatter());
        handler.setLevel(Level.INFO);

        root.addHandler(handler);
        root.setLevel(Level.INFO);
    }

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        AtomicInteger totalPicked = new AtomicInteger();
        AtomicInteger copied = new AtomicInteger();
        AtomicInteger verified = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger skipped = new AtomicInteger();

        logger.info("===============================================");
        logger.info("ECS SAMPLE COPY JOB STARTED");
        logger.info("Source Bucket : " + SOURCE_BUCKET);
        logger.info("Dest Bucket   : " + DEST_BUCKET);
        logger.info("Dest Folder   : " + DEST_PREFIX);
        logger.info("Pick Files    : " + PICK_FILES);
        logger.info("Threads       : " + THREAD_COUNT);
        logger.info("Endpoint      : " + S3_ENDPOINT);
        logger.info("===============================================");

        AmazonS3 s3 = createS3Client();

        // 1) Pick first 10 files from source bucket
        List<String> pickedKeys = pickFirstNKeys(s3, SOURCE_BUCKET, PICK_FILES);

        if (pickedKeys.isEmpty()) {
            logger.warning("No objects found in source bucket. Exiting...");
            return;
        }

        logger.info("Picked objects count: " + pickedKeys.size());
        for (String k : pickedKeys) {
            logger.info("PICKED -> " + k);
        }

        ExecutorService pool = Executors.newFixedThreadPool(THREAD_COUNT);
        CompletionService<Void> cs = new ExecutorCompletionService<>(pool);

        for (String sourceKey : pickedKeys) {
            cs.submit(() -> {

                totalPicked.incrementAndGet();

                String destKey = DEST_PREFIX + sourceKey;

                // skip if already exists
                if (doesObjectExist(s3, DEST_BUCKET, destKey)) {
                    skipped.incrementAndGet();
                    logger.info("SKIPPED (already exists): " + destKey);
                    return null;
                }

                try {
                    boolean ok = copyWithRetry(s3, sourceKey, destKey);

                    if (!ok) {
                        failed.incrementAndGet();
                        logger.severe("FAILED COPY (after retry): " + sourceKey);
                        return null;
                    }

                    copied.incrementAndGet();
                    logger.info("COPIED OK: " + sourceKey + " -> " + destKey);

                    if (VERIFY_SIZE) {
                        ObjectMetadata srcMeta = s3.getObjectMetadata(SOURCE_BUCKET, sourceKey);
                        ObjectMetadata dstMeta = s3.getObjectMetadata(DEST_BUCKET, destKey);

                        if (srcMeta.getContentLength() == dstMeta.getContentLength()) {
                            verified.incrementAndGet();
                            logger.info("VERIFIED OK: " + sourceKey + " | size=" + srcMeta.getContentLength());
                        } else {
                            failed.incrementAndGet();
                            logger.severe("VERIFY FAILED (SIZE MISMATCH): " + sourceKey +
                                    " | src=" + srcMeta.getContentLength() +
                                    " dst=" + dstMeta.getContentLength());
                        }
                    }

                } catch (Exception e) {
                    failed.incrementAndGet();
                    logger.severe("FAILED: " + sourceKey + " | " + e.getMessage());
                }

                return null;
            });
        }

        // wait all
        for (int i = 0; i < pickedKeys.size(); i++) {
            cs.take();
        }

        pool.shutdown();
        pool.awaitTermination(30, TimeUnit.MINUTES);

        long timeTakenSec = (System.currentTimeMillis() - startTime) / 1000;

        logger.info("===============================================");
        logger.info("JOB SUMMARY");
        logger.info("Picked     : " + pickedKeys.size());
        logger.info("Copied     : " + copied.get());
        logger.info("Skipped    : " + skipped.get());
        logger.info("Verified   : " + verified.get());
        logger.info("Failed     : " + failed.get());
        logger.info("Time(sec)  : " + timeTakenSec);
        logger.info("===============================================");
    }

    // ================= FUNCTIONS =================

    private static List<String> pickFirstNKeys(AmazonS3 s3, String bucket, int limit) {

        List<String> keys = new ArrayList<>();

        String token = null;
        do {
            ListObjectsV2Request req = new ListObjectsV2Request()
                    .withBucketName(bucket)
                    .withContinuationToken(token)
                    .withMaxKeys(1000);

            ListObjectsV2Result res = s3.listObjectsV2(req);

            for (S3ObjectSummary obj : res.getObjectSummaries()) {
                if (obj.getKey().endsWith("/")) continue; // ignore folder marker
                keys.add(obj.getKey());
                if (keys.size() >= limit) break;
            }

            token = res.getNextContinuationToken();

            if (keys.size() >= limit) break;

        } while (token != null);

        return keys;
    }

    private static boolean copyWithRetry(AmazonS3 s3, String sourceKey, String destKey) {

        for (int attempt = 1; attempt <= MAX_RETRY; attempt++) {
            try {
                CopyObjectRequest req = new CopyObjectRequest(SOURCE_BUCKET, sourceKey, DEST_BUCKET, destKey);
                s3.copyObject(req);
                return true;

            } catch (Exception e) {
                logger.warning("COPY FAILED attempt(" + attempt + "): " + sourceKey + " | " + e.getMessage());
                try { Thread.sleep(1000L * attempt); } catch (InterruptedException ignored) {}
            }
        }
        return false;
    }

    private static boolean doesObjectExist(AmazonS3 s3, String bucket, String key) {
        try {
            s3.getObjectMetadata(bucket, key);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static AmazonS3 createS3Client() {

        BasicAWSCredentials creds = new BasicAWSCredentials(ACCESS_KEY, SECRET_KEY);

        ClientConfiguration config = new ClientConfiguration();
        config.setSignerOverride("AWSS3V4SignerType");

        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(S3_ENDPOINT, REGION))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(config)
                .withCredentials(new AWSStaticCredentialsProvider(creds))
                .build();
    }

    static class SimpleLogFormatter extends java.util.logging.Formatter {
        @Override
        public String format(LogRecord record) {
            return String.format("%s [%s] %s%n",
                    LocalDateTime.now(),
                    record.getLevel(),
                    record.getMessage());
        }
    }

}
