package unstructured;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import java.io.InputStream;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;

public class unstructured_main_v1 {

    private static final String S3_ENDPOINT = "http://172.20.161.11";
    private static final String REGION = "us-east-1";
    private static final String ACCESS_KEY = "HPZBCYK10H24VCNRKKM7";
    private static final String SECRET_KEY = "hNz4QcK6TWQUmCuqfUSr+DBHXC2kHo93Fhq5xqLm";
    private static final String SOURCE_BUCKET = "dc-bulkarch-bucket1";
    private static final String DEST_BUCKET = "testincdata";
    private static final String DEST_PREFIX = "dc-bulkarch-bucket1/";
    private static final int MAX_COPY = 1000000000;

    private static final Logger logger = Logger.getLogger(unstructured_main_v1.class.getName());

    static {
        Logger root = Logger.getLogger("");
        for (Handler h : root.getHandlers()) {
            root.removeHandler(h);
        }

        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new Formatter() {
            private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
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
        });

        handler.setLevel(Level.INFO);
        root.addHandler(handler);
        root.setLevel(Level.INFO);
    }

    public static void main(String[] args) {

        long startTime = System.currentTimeMillis();

        int totalScanned = 0;
        int copied = 0;
        int skipped = 0;
        int verified = 0;
        int failedChecksum = 0;

        logger.info("Incremental copy job started with SHA-256 validation");

        AmazonS3 s3 = createS3Client();

        ListObjectsV2Request listReq = new ListObjectsV2Request()
                .withBucketName(SOURCE_BUCKET);

        ListObjectsV2Result result;

        try {
            do {
                result = s3.listObjectsV2(listReq);

                for (S3ObjectSummary obj : result.getObjectSummaries()) {

                    if (copied >= MAX_COPY) {
                        logSummary(totalScanned, copied, skipped, verified, failedChecksum, startTime);
                        return;
                    }

                    totalScanned++;

                    String sourceKey = obj.getKey();
                    String destKey = DEST_PREFIX + sourceKey;

                    String sourcePath = "s3://" + SOURCE_BUCKET + "/" + sourceKey;
                    String destPath   = "s3://" + DEST_BUCKET + "/" + destKey;

                    if (destinationExists(s3, DEST_BUCKET, destKey)) {
                        skipped++;
                        logger.info("SKIPPED      : " + sourcePath);
                        continue;
                    }

                    s3.copyObject(new CopyObjectRequest(
                            SOURCE_BUCKET, sourceKey,
                            DEST_BUCKET, destKey
                    ));

                    copied++;
                    logger.info("COPIED");
                    logger.info("SOURCE      : " + sourcePath);
                    logger.info("DESTINATION : " + destPath);

                    String srcHash = sha256(
                            s3.getObject(SOURCE_BUCKET, sourceKey).getObjectContent()
                    );
                    String dstHash = sha256(
                            s3.getObject(DEST_BUCKET, destKey).getObjectContent()
                    );

                    logger.info("SRC SHA-256 : " + srcHash);
                    logger.info("DST SHA-256 : " + dstHash);

                    if (srcHash.equals(dstHash)) {
                        verified++;
                        logger.info("VERIFIED    : SHA-256 MATCH");
                    } else {
                        failedChecksum++;
                        logger.severe("SHA-256 MISMATCH");
                    }
                }

                listReq.setContinuationToken(result.getNextContinuationToken());

            } while (result.isTruncated());

            logSummary(totalScanned, copied, skipped, verified, failedChecksum, startTime);

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Job failed: " + e.getMessage(), e);
            logSummary(totalScanned, copied, skipped, verified, failedChecksum, startTime);
        }
    }

    private static void logSummary(
            int total, int copied, int skipped,
            int verified, int failed, long startTime) {

        long durationSec = (System.currentTimeMillis() - startTime) / 1000;

        logger.info("========== JOB SUMMARY ==========");
        logger.info("Total files scanned  : " + total);
        logger.info("Files copied         : " + copied);
        logger.info("Files skipped        : " + skipped);
        logger.info("Files verified       : " + verified);
        logger.info("Checksum failures    : " + failed);
        logger.info("Execution time (sec) : " + durationSec);
        logger.info("=================================");
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

    private static boolean destinationExists(AmazonS3 s3, String bucket, String key) {
        try {
            s3.getObjectMetadata(bucket, key);
            return true;
        } catch (AmazonS3Exception e) {
            return e.getStatusCode() != 404;
        }
    }

    private static String sha256(InputStream is) throws Exception {

        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] buffer = new byte[8192];
        int read;

        while ((read = is.read(buffer)) != -1) {
            digest.update(buffer, 0, read);
        }
        is.close();

        byte[] hash = digest.digest();
        StringBuilder sb = new StringBuilder();

        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
