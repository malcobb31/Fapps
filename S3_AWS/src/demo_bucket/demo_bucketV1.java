package demo_bucket;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class demo_bucketV1 {

    
    private static final String ACCESS_KEY = "AKIA3TSA574AXZSJLMXZ";
    private static final String SECRET_KEY = "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";

    public static void main(String[] args) {

        String bucketName = "my-bucket-khushant-demo";
        Region region = Region.AP_SOUTH_1;

        S3Client s3 = S3Client.builder()
                .region(region)
                .credentialsProvider(
                        StaticCredentialsProvider.create(
                                AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)
                        )
                )
                .build();

        try {
            s3.createBucket(CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build());

            System.out.println("Amazon S3 Bucket created: " + bucketName);

        } catch (S3Exception e) {
            System.err.println("AWS Error: " + e.awsErrorDetails().errorMessage());
        } catch (Exception e) {
            System.err.println("General Error: " + e.getMessage());
        } finally {
            s3.close();
        }
    }
}
