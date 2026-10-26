package dell_scality;

import java.util.*;
import java.util.logging.*;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.*;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

public class Delete {

    private static final Logger logger = Logger.getLogger(Delete.class.getName());

    static {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new SimpleFormatter());
        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
    }

    //  ECS Compatible Client (Signature V2)
    public static AmazonS3 createClient(String accessKey, String secretKey, String endpoint) {

        BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);

        ClientConfiguration config = new ClientConfiguration();
        config.setProtocol(Protocol.HTTP);
        config.setMaxConnections(200);

        //  REQUIRED for many Dell ECS clusters
        config.setSignerOverride("S3SignerType");

        return AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(endpoint, null))
                .withPathStyleAccessEnabled(true)
                .disableChunkedEncoding()
                .withClientConfiguration(config)
                .withCredentials(new AWSStaticCredentialsProvider(creds))
                .build();
    }

    public static void main(String[] args) {

        AmazonS3 s3 = createClient(
                "HPZBCYK10H24VCNRKKM7",
                "hNz4QcK6TWQUmCuqfUSr+DBHXC2kHo93Fhq5xqLm",
                "http://172.20.161.11"
        );

        String bucketName = "test";     //  REAL bucket
        String parentPrefix = "pview/"; //  folder inside bucket

        logger.warning("=========== SAFE FOLDER CLEANUP STARTED ===========");

        long start = System.currentTimeMillis();

        ListObjectsRequest request = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix(parentPrefix)
                .withDelimiter("/"); // lists subfolders only

        ObjectListing listing = s3.listObjects(request);

        for (String prefix : listing.getCommonPrefixes()) {

            logger.warning("DELETING folder -> " + prefix);

            deleteFolderCompletely(s3, bucketName, prefix);
        }

        long end = System.currentTimeMillis();

        logger.warning("=========== CLEANUP COMPLETE ===========");
        logger.warning("Total Time: " + ((end - start) / 1000.0) + " seconds");
    }

    private static void deleteFolderCompletely(AmazonS3 s3, String bucket, String prefix) {

        ObjectListing listing = s3.listObjects(bucket, prefix);

        int totalDeleted = 0;
        long startTime = System.currentTimeMillis();

        do {

            List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();

            for (S3ObjectSummary obj : listing.getObjectSummaries()) {
                keys.add(new DeleteObjectsRequest.KeyVersion(obj.getKey()));
            }

            if (!keys.isEmpty()) {

                DeleteObjectsRequest deleteRequest =
                        new DeleteObjectsRequest(bucket).withKeys(keys);

                DeleteObjectsResult result = s3.deleteObjects(deleteRequest);

                totalDeleted += result.getDeletedObjects().size();

                double seconds =
                        (System.currentTimeMillis() - startTime) / 1000.0;

                double speed = totalDeleted / Math.max(seconds, 1);

                logger.info("Deleted: " + totalDeleted +
                        " | Speed: " + String.format("%.2f", speed) + " objects/sec");
            }

            listing = s3.listNextBatchOfObjects(listing);

        } while (listing.isTruncated());

        logger.warning("TOTAL DELETED from " + prefix + " = " + totalDeleted);
    }
}
