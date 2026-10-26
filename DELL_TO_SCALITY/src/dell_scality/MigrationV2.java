package dell_scality;

import java.io.*;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;

public class MigrationV2 {

    private static final Logger logger = Logger.getLogger(MigrationV2.class.getName());

    private static final String PROCESS_NAME = "MIGRATION";
    private static final String SERVER_IP = "172.20.131.100";
    private static final String PROCESS_VERSION = "1.0.1";

    private static final int MAX_FILES = 1;
    private static final int THREADS = 1;

    static {
        try {
            Logger root = Logger.getLogger("");
            for (Handler h : root.getHandlers())
                root.removeHandler(h);

            ConsoleHandler handler = new ConsoleHandler();
            handler.setFormatter(new Formatter() {
                @Override
                public String format(LogRecord record) {
                    String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                            .format(new Date(record.getMillis()));

                    return "Time:" + time +
                            " [Debug] Process_Name: " + PROCESS_NAME +
                            " Server_IP: " + SERVER_IP +
                            " Process_Version: " + PROCESS_VERSION +
                            " Log_Level: EVT_LOG  EVT_Type: " +
                            record.getMessage() + "\n";
                }
            });

            logger.addHandler(handler);
            logger.setUseParentHandlers(false);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ==============================
    // S3 CLIENT CREATION (V1 STYLE)
    // ==============================
    public static AmazonS3 createClient(String accessKey, String secretKey, String endpoint) {

        BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration config = new ClientConfiguration();
        config.setMaxConnections(1000);

        //  REQUIRED FOR YOUR SCALITY (FROM V1)
        config.setSignerOverride("S3SignerType");

        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(config)
                .withCredentials(new AWSStaticCredentialsProvider(creds))
                .build();
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            String s = Integer.toHexString(0xff & b);
            if (s.length() == 1)
                hex.append('0');
            hex.append(s);
        }
        return hex.toString();
    }

    private static void migrateFile(AmazonS3 source,
                                    AmazonS3 destination,
                                    String sourceBucket,
                                    String destBucket,
                                    String sourceKey,
                                    TransferManager tm,
                                    AtomicInteger success,
                                    AtomicInteger failed,
                                    AtomicInteger skipped) {

        String destKey = sourceKey;
        File tempFile = null;

        try {

            if (sourceKey.endsWith("/")) {
                skipped.incrementAndGet();
                logger.info("SKIPPED Description: FolderMarker=" + sourceKey);
                return;
            }

            for (int attempt = 1; attempt <= 3; attempt++) {

                try {

                    logger.info("COPY_START Description: File=" + sourceKey + " Attempt=" + attempt);

                    // DOWNLOAD + SHA256
                    S3Object sourceObject = source.getObject(sourceBucket, sourceKey);

                    MessageDigest sourceDigest = MessageDigest.getInstance("SHA-256");

                    tempFile = File.createTempFile("mig_", ".tmp");

                    try (DigestInputStream dis =
                                 new DigestInputStream(sourceObject.getObjectContent(), sourceDigest);
                         FileOutputStream fos = new FileOutputStream(tempFile)) {

                        byte[] buffer = new byte[16 * 1024];
                        int read;
                        while ((read = dis.read(buffer)) != -1) {
                            fos.write(buffer, 0, read);
                        }
                    }

                    sourceObject.close();

                    String sourceSha = bytesToHex(sourceDigest.digest());
                    logger.info("SHA256_SOURCE Description: File=" + sourceKey + " Hash=" + sourceSha);

                    // UPLOAD USING FILE (SAFE FOR SCALITY)
                    Upload upload = tm.upload(destBucket, destKey, tempFile);
                    upload.waitForCompletion();

                    // VERIFY DESTINATION SHA
                    S3Object destObject = destination.getObject(destBucket, destKey);

                    MessageDigest destDigest = MessageDigest.getInstance("SHA-256");

                    try (DigestInputStream dis =
                                 new DigestInputStream(destObject.getObjectContent(), destDigest)) {

                        byte[] buffer = new byte[16 * 1024];
                        while (dis.read(buffer) != -1) { }
                    }

                    destObject.close();

                    String destSha = bytesToHex(destDigest.digest());
                    logger.info("SHA256_DEST Description: File=" + sourceKey + " Hash=" + destSha);

                    if (!sourceSha.equals(destSha))
                        throw new RuntimeException("SHA256_MISMATCH");

                    success.incrementAndGet();
                    logger.info("SUCCESS Description: File=" + sourceKey + " IntegrityVerified=true");

                    tempFile.delete();
                    return;

                } catch (Exception e) {

                    if (attempt == 3) {
                        failed.incrementAndGet();
                        logger.info("FAILED Description: File=" + sourceKey +
                                " Reason=" + e.getMessage());
                    }

                    if (tempFile != null && tempFile.exists())
                        tempFile.delete();
                }
            }

        } catch (Exception ex) {

            failed.incrementAndGet();
            logger.info("ERROR Description: File=" + sourceKey +
                    " Reason=" + ex.getMessage());

            if (tempFile != null && tempFile.exists())
                tempFile.delete();
        }
    }

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        AmazonS3 source = createClient(
                "PROTEAN",
                "nLNB6hdI17PDkL3YvsM5FdKcmHv7wVWOuuRdAp1c",
                "http://192.168.90.163");

        AmazonS3 destination = createClient(
                "HPZBCYK10H24VCNRKKM7",
                "hNz4QcK6TWQUmCuqfUSr+DBHXC2kHo93Fhq5xqLm",
                "http://172.20.161.11");

        String sourceBucket = "dc_bulkarch_bucket2";
        String destBucket = "dc_bulkarch_bucket2";

        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        TransferManager tm = TransferManagerBuilder.standard()
                .withS3Client(destination)
                .withExecutorFactory(() -> Executors.newFixedThreadPool(THREADS))
                .withMultipartUploadThreshold(64L * 1024 * 1024)
                .withMinimumUploadPartSize(32L * 1024 * 1024)
                .build();

        AtomicInteger processed = new AtomicInteger();
        AtomicInteger success = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        AtomicInteger skipped = new AtomicInteger();

        logger.info("START Description: Migration Started | MaxFiles=" +
                MAX_FILES + " | Threads=" + THREADS);

        Files.lines(Paths.get("files.csv"))
                .limit(MAX_FILES)
                .forEach(key -> {

                    String sourceKey = key.trim();
                    if (sourceKey.isEmpty())
                        return;

                    processed.incrementAndGet();

                    executor.submit(() ->
                            migrateFile(source, destination,
                                    sourceBucket, destBucket,
                                    sourceKey, tm,
                                    success, failed, skipped));
                });

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        tm.shutdownNow(false);

        long endTime = System.currentTimeMillis();
        double totalSeconds = (endTime - startTime) / 1000.0;

        double avgSpeed = processed.get() == 0 ? 0 :
                processed.get() / totalSeconds;

        logger.info("END Description: Migration Completed");

        logger.info("SUMMARY Description: Processed=" + processed.get()
                + " Success=" + success.get()
                + " Failed=" + failed.get()
                + " Skipped=" + skipped.get()
                + " TotalTime=" + String.format("%.2f", totalSeconds) + " sec"
                + " AvgSpeed=" + String.format("%.2f", avgSpeed) + " files/sec");
    }
}