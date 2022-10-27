package com.carsaver.example;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;

public class Main {
    public static void main(String[] args) {
        fileUploadToAWSS3();
    }

    public static void fileUploadToAWSS3() {
        S3Client s3 = S3Client.create();
        ListBucketsResponse response = s3.listBuckets();
        for (Bucket bucket : response.buckets()) {
            System.out.println(bucket.name());
        }
    }
}

