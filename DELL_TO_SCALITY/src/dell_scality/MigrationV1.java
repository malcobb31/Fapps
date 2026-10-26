package dell_scality;

import java.io.*;
import java.nio.file.*;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.*;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.*;

public class MigrationV1 {

	private static final Logger logger = Logger.getLogger(MigrationV1.class.getName());

	private static final int MAX_FILES = 1;
	private static final int THREADS = 1;

	
	static {
		try {

			Logger root = Logger.getLogger("");
			for (Handler h : root.getHandlers())
				root.removeHandler(h);

			FileHandler fh = new FileHandler("migration.log", true);
			fh.setFormatter(new SimpleFormatter());

			logger.addHandler(fh);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	public static AmazonS3 createClient(String accessKey, String secretKey, String endpoint) {

		BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);

		ClientConfiguration config = new ClientConfiguration();

		// CRITICAL FOR SPEED
		config.setMaxConnections(1000);
		config.setSignerOverride("S3SignerType");

		return AmazonS3ClientBuilder.standard()
				.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
				.withPathStyleAccessEnabled(true).withClientConfiguration(config)
				.withCredentials(new AWSStaticCredentialsProvider(creds)).build();
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

	// CREATE DESTINATION FOLDER
	private static void createFolderIfNotExists(AmazonS3 s3, String bucket, String prefix) {

		if (!s3.doesObjectExist(bucket, prefix)) {

			ObjectMetadata metadata = new ObjectMetadata();
			metadata.setContentLength(0);

			PutObjectRequest request = new PutObjectRequest(bucket, prefix, new ByteArrayInputStream(new byte[0]),
					metadata);

			s3.putObject(request);
			logger.info("Created folder -> " + prefix);
		}
	}

	
	private static void migrateFile(AmazonS3 source, AmazonS3 destination, String sourceBucket, String destBucket,
			String prefix, String sourceKey, TransferManager tm, AtomicInteger success, AtomicInteger failed,
			AtomicInteger skipped) {

		String destKey = prefix + sourceKey;

		try {

			// Skip folder markers
			if (sourceKey.endsWith("/")) {
				skipped.incrementAndGet();
				return;
			}

			if (destination.doesObjectExist(destBucket, destKey)) {
				skipped.incrementAndGet();
				logger.info("SKIPPED -> " + destKey);
				return;
			}

			for (int attempt = 1; attempt <= 3; attempt++) {

				try {

					logger.info("COPY -> " + sourceKey);

					S3Object sourceObject = source.getObject(sourceBucket, sourceKey);

					MessageDigest digest = MessageDigest.getInstance("SHA-256");

					DigestInputStream digestStream = new DigestInputStream(sourceObject.getObjectContent(), digest);

					Upload upload = tm.upload(
							new PutObjectRequest(destBucket, destKey, digestStream, sourceObject.getObjectMetadata()));

					upload.waitForCompletion();

					String sourceSha = bytesToHex(digest.digest());

					digestStream.close();
					sourceObject.close();

					// VERIFY DESTINATION
					S3Object destObject = destination.getObject(destBucket, destKey);

					MessageDigest destDigest = MessageDigest.getInstance("SHA-256");

					InputStream destStream = new DigestInputStream(destObject.getObjectContent(), destDigest);

					byte[] buffer = new byte[16 * 1024];
					while (destStream.read(buffer) != -1) {
					}

					String destSha = bytesToHex(destDigest.digest());

					destStream.close();
					destObject.close();

					if (!sourceSha.equals(destSha))
						throw new RuntimeException("SHA mismatch");

					success.incrementAndGet();
					logger.info("SUCCESS -> " + destKey);
					return;

				} catch (Exception e) {

					if (attempt == 3) {

						failed.incrementAndGet();

						logger.severe("FAILED -> " + sourceKey + " | Reason: " + e.getMessage());
					}
				}
			}

		} catch (Exception ex) {

			failed.incrementAndGet();
			logger.severe("ERROR -> " + sourceKey);
		}
	}

	public static void main(String[] args) throws Exception {

		long startTime = System.currentTimeMillis();

		AmazonS3 source = createClient("PROTEAN", "nLNB6hdI17PDkL3YvsM5FdKcmHv7wVWOuuRdAp1c",
				"http://192.168.90.163");

		AmazonS3 destination = createClient("HPZBCYK10H24VCNRKKM7", "hNz4QcK6TWQUmCuqfUSr+DBHXC2kHo93Fhq5xqLm",
				"http://172.20.161.11");

		String sourceBucket = "dc_bulkarch_bucket2";
		String destBucket = "dc_bulkarch_bucket2";
		String prefix = "pview/";

		createFolderIfNotExists(destination, destBucket, prefix);

		ExecutorService executor = Executors.newFixedThreadPool(THREADS);

		TransferManager tm = TransferManagerBuilder.standard().withS3Client(destination)
				.withExecutorFactory(() -> Executors.newFixedThreadPool(THREADS))
				.withMultipartUploadThreshold(64L * 1024 * 1024).withMinimumUploadPartSize(32L * 1024 * 1024).build();

		AtomicInteger processed = new AtomicInteger();
		AtomicInteger success = new AtomicInteger();
		AtomicInteger failed = new AtomicInteger();
		AtomicInteger skipped = new AtomicInteger();

		logger.info("=========== MIGRATION STARTED ===========");

		// READ CSV
		Files.lines(Paths.get("files.csv")).limit(MAX_FILES).forEach(key -> {

			String sourceKey = key.trim();

			if (sourceKey.isEmpty())
				return;

			processed.incrementAndGet();

			executor.submit(() -> migrateFile(source, destination, sourceBucket, destBucket, prefix, sourceKey, tm,
					success, failed, skipped));
		});

		executor.shutdown();
		executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
		tm.shutdownNow(false);

		long endTime = System.currentTimeMillis();
		long seconds = (endTime - startTime) / 1000;

		logger.info("=========== JOB COMPLETE ===========");
		logger.info("Processed : " + processed.get());
		logger.info("Success   : " + success.get());
		logger.info("Skipped   : " + skipped.get());
		logger.info("Failed    : " + failed.get());
		logger.info("Total Time : " + seconds + " seconds (" + (seconds / 60) + " minutes)");
	}
}

/*
 * AmazonS3 source = createClient( "AKIA5071D3A94B4DABD6",
 * "7w/OjXG1UEsikGbSeFTFQu01z7WkygPQqfjdrPpq", "http://192.168.90.163" );
 * 
 * AmazonS3 destination = createClient( "HPZBCYK10H24VCNRKKM7",
 * "hNz4QcK6TWQUmCuqfUSr+DBHXC2kHo93Fhq5xqLm", "http://172.20.161.11" );
 */