package com.carsaver.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.waiters.WaiterOverrideConfiguration;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionRequest;
import software.amazon.awssdk.services.sfn.model.DescribeExecutionResponse;
import software.amazon.awssdk.services.sfn.model.ExecutionStatus;
import software.amazon.awssdk.services.sfn.model.ListExecutionsRequest;
import software.amazon.awssdk.services.sfn.paginators.ListExecutionsIterable;

import java.io.File;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

public class Main {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final SfnClient client = SfnClient.create();


    public static void main(String[] args) throws JsonProcessingException, InterruptedException {
        String key = fileUploadToAWSS3();
        Thread.sleep(3_000);
        checkStateMachine(bucket, key);
    }

    private static final String bucket = "upgradeprospectetl-bucket-ingestion-staging";
    private static final String runId = "0bb5c200-6a4e-4f94-b8ad-1df4524eada1";
    private static final String campaignId = "17260442-9dd8-4a40-a6ab-6f40cfc0cb20";
    private static final String tableName = "tableName123";

    private static final String path = runId + "/" + campaignId + "/" + tableName;

    public static String fileUploadToAWSS3() {
        S3Client s3 = S3Client.create();

        UUID uuid = UUID.randomUUID();
        String key = path + "/" + uuid + ".csv";
        PutObjectResponse putObjectResponse = s3.putObject(PutObjectRequest.builder()
                .bucket(bucket).key(key)
                .build(), RequestBody.fromFile(new File("src/main/resources/prospectData.csv"))
        );
        S3Waiter waiter = S3Waiter.create();
        waiter.waitUntilObjectExists(HeadObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build(), WaiterOverrideConfiguration.builder()
                .waitTimeout(Duration.ofSeconds(30))
                .build());

        System.out.println(putObjectResponse);
        return key;
    }

    public static void checkStateMachine(String bucket, String key) throws JsonProcessingException {
        ListExecutionsIterable listExecutionsResponses = client.listExecutionsPaginator(ListExecutionsRequest.builder()
                .stateMachineArn("arn:aws:states:us-east-1:909837171498:stateMachine:UpgradeProspectEtl-SM-Staging")
                .build());

        DescribeExecutionResponse execution = listExecutionsResponses.executions().stream()
                .map(executionListItem -> getDetails(executionListItem.executionArn()))
                .filter(response -> isRightJob(response, bucket, key))
                .limit(5)
                .findFirst().orElseThrow(() -> new IllegalStateException("Unable to find execution for " + bucket + "/" + key));

        AtomicReference<DescribeExecutionResponse> status = new AtomicReference<>();
        System.out.println(execution.executionArn());

        await().atMost(2, TimeUnit.MINUTES)
                .pollDelay(5, TimeUnit.SECONDS)
                .until(() -> {
                    DescribeExecutionResponse details = getDetails(execution.executionArn());
                    status.set(details);
                    return details.status() != ExecutionStatus.RUNNING;
                });

        System.out.println(status.get().status());
        System.out.println(status.get().output()); //Here I am getting null value instead of JSON Response. How can I get output JSON RESPONSE?
    }

    private static DescribeExecutionResponse getDetails(String arn) {
        return client.describeExecution(DescribeExecutionRequest.builder()
                .executionArn(arn)
                .build());
    }

    @SneakyThrows
    private static boolean isRightJob(DescribeExecutionResponse describeExecutionResponse, String bucket, String key) {
        String input = describeExecutionResponse.input();
        String inputBucket = mapper.readTree(input).at("/detail/bucket/name").textValue();
        String inputKey = mapper.readTree(input).at("/detail/object/key").textValue();
        return Objects.equals(Main.bucket, inputBucket) && Objects.equals(key, inputKey);
    }
}

