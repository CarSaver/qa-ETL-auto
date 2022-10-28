package com.carsaver.example;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.File;

public class Main {
    public static void main(String[] args) {
        fileUploadToAWSS3();
    }

    private static final String bucket = "upgradeprospectetl-bucket-ingestion-staging";
    private static final String runId = "0bb5c200-6a4e-4f94-b8ad-1df4524eada1";
    private static final String campaignId = "17260442-9dd8-4a40-a6ab-6f40cfc0cb20";
    private static final String tableName = "tableName";

    private static final String path = runId + "/" + campaignId + "/" + tableName;

    public static void fileUploadToAWSS3() {
        S3Client s3 = S3Client.create();

        PutObjectResponse putObjectResponse = s3.putObject(PutObjectRequest.builder()
                .bucket(bucket).key(path + "/prospectData.csv")
                .build(), RequestBody.fromFile(new File("src/main/resources/prospectData.csv"))
        );

        System.out.println(putObjectResponse);
    }
}

