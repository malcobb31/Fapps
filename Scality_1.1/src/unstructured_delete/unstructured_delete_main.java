package unstructured_delete;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class unstructured_delete_main {

	  private static final String S3_ENDPOINT = "http://172.20.161.11";
	    private static final String REGION = "us-east-1";
	    private static final String ACCESS_KEY = "HPZBCYK10H24VCNRKKM7";
	    private static final String SECRET_KEY = "hNz4QcK6TWQUmCuqfUSr+DBHXC2kHo93Fhq5xqLm";

    private static final String BUCKET_NAME = "testincdata";
    private static final String PREFIX = "dc-bulkarch-bucket1/";

    public static void main(String[] args) {

        AmazonS3 s3 = createS3Client();

        System.out.println("Starting delete process...");
        deleteByPrefix(s3, BUCKET_NAME, PREFIX);
        System.out.println("Delete process completed.");
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

    private static void deleteByPrefix(
            AmazonS3 s3, String bucket, String prefix) {

        ListObjectsV2Request request = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withPrefix(prefix);

        ListObjectsV2Result result;

        do {
            result = s3.listObjectsV2(request);

            for (S3ObjectSummary obj : result.getObjectSummaries()) {
                s3.deleteObject(bucket, obj.getKey());
                System.out.println("DELETED : " + obj.getKey());
            }

            request.setContinuationToken(
                    result.getNextContinuationToken());

        } while (result.isTruncated());
    }
}
