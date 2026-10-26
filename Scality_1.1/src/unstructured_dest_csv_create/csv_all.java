package unstructured_dest_csv_create;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import java.io.FileWriter;
import java.util.List;

public class csv_all {

    private static final String S3_ENDPOINT = "http://172.20.161.11";
    private static final String REGION = "us-east-1";
    private static final String ACCESS_KEY = "HPZBCYK10H24VCNRKKM7";
    private static final String SECRET_KEY = "hNz4QcK6TWQUmCuqfUSr+DBHXC2kHo93Fhq5xqLm";

    private static final String DEST_BUCKET = "testincdata";
    private static final String OUTPUT_CSV = "dest_bucket_files.csv";

    public static void main(String[] args) throws Exception {

        AmazonS3 s3 = createS3Client();

        FileWriter writer = new FileWriter(OUTPUT_CSV);
        writer.write("file_name\n");

        ListObjectsV2Request listReq = new ListObjectsV2Request()
                .withBucketName(DEST_BUCKET);

        ListObjectsV2Result result;

        do {
            result = s3.listObjectsV2(listReq);
            List<S3ObjectSummary> objects = result.getObjectSummaries();

            for (S3ObjectSummary obj : objects) {
                writer.write(obj.getKey() + "\n");
            }

            listReq.setContinuationToken(result.getNextContinuationToken());

        } while (result.isTruncated());

        writer.flush();
        writer.close();

        System.out.println("CSV generated successfully: " + OUTPUT_CSV);
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
