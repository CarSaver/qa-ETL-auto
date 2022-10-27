package org.example;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.S3ClientOptions;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.PutObjectRequest;


import java.io.*;
import java.net.StandardSocketOptions;
import java.util.List;

public class Main {
        public static void main(String[] args) throws IOException {
//            fileReader();
            fileUploadToAWSS3();
            }
//        public static void fileReader(){
//            String file = "/Users/manav/Downloads/QAUpgradeDemo.csv";
//            String line;
//            try (BufferedReader br =
//                         new BufferedReader(new FileReader(file))) {
//                while((line = br.readLine()) != null){
//                    System.out.println(line);
//                }
//            } catch (Exception e){
//                System.out.println(e);
//            }


        public static void fileUploadToAWSS3(){
             AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
            List<Bucket> buckets = s3.listBuckets();
            for(Bucket bucket : buckets) {
                System.out.println(bucket.getName());
            }




        }

    }
