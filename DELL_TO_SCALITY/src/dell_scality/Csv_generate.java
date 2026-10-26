package dell_scality;

import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.*;

public class Csv_generate {

    private static final String PROCESS_NAME = "FP";
    private static final String SERVER_IP = "172.20.131.100";
    private static final String PROCESS_VERSION = "1.0.1";

    private static final Logger logger = Logger.getLogger(Csv_generate.class.getName());

    static {
        ConsoleHandler handler = new ConsoleHandler();
        handler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                        .format(new Date(record.getMillis()));

                return "Time:" + time +
                        " [Debug] " +
                        "Process_Name: " + PROCESS_NAME +
                        " Server_IP: " + SERVER_IP +
                        " Process_Version: " + PROCESS_VERSION +
                        " Log_Level: EVT_LOG  " +
                        "EVT_Type: " + record.getMessage() +
                        "\n";
            }
        });

        logger.addHandler(handler);
        logger.setUseParentHandlers(false);
    }

    public static void main(String[] args) throws Exception {

        long startTime = System.currentTimeMillis();

        AmazonS3 source = MigrationV1.createClient(
                "PROTEAN",
                "nLNB6hdI17PDkL3YvsM5FdKcmHv7wVWOuuRdAp1c",
                "http://192.168.90.163"
        );

        String bucket = "dc_ckyc_bucket1";

        logger.info("START Description: CSV Generation Started | Bucket: " + bucket);

        FileWriter writer = new FileWriter("files_80L.csv");

        int count = 0;

        ListObjectsV2Request req = new ListObjectsV2Request()
                .withBucketName(bucket)
                .withMaxKeys(1000);

        ListObjectsV2Result result;

        do {

            result = source.listObjectsV2(req);

            for (S3ObjectSummary obj : result.getObjectSummaries()) {

                writer.write(obj.getKey() + "\n");
                count++;

                // Progress log every 10k
                if (count % 10000 == 0) {

                    long now = System.currentTimeMillis();
                    double seconds = (now - startTime) / 1000.0;
                    double speed = count / seconds;

                    logger.info("PROGRESS Description: Files=" + count +
                            " Time=" + String.format("%.2f", seconds) +
                            " sec Speed=" + String.format("%.2f", speed) + " files/sec");
                }
            }

            req.setContinuationToken(result.getNextContinuationToken());

        } while (result.isTruncated());   //  NO LIMIT NOW

        writer.close();

        long endTime = System.currentTimeMillis();

        double totalSeconds = (endTime - startTime) / 1000.0;
        double speed = count / totalSeconds;

        logger.info("END Description: CSV Created Successfully");
        logger.info("SUMMARY Description: TotalFiles=" + count +
                " TotalTime=" + String.format("%.2f", totalSeconds) +
                " sec AvgSpeed=" + String.format("%.2f", speed) + " files/sec");
    }
}