package org.example;


import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.*;
import software.amazon.awssdk.services.sfn.paginators.ListExecutionsIterable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;

public class Main {
    public static String campaignId = "965e033c-7bd2-4836-bb0c-353d9037dfd5";
    public static final SfnClient client = SfnClient.create();
    public static final ObjectMapper mapper = new ObjectMapper();
    public static String tvfile;
    public static String teamVelocityRunId;
    public static ArrayList<String> tvFileList;
    public static void main(String[] args) throws IOException {
        LambdaAsyncClient awsLambda = LambdaAsyncClient.builder()
                .httpClient(NettyNioAsyncHttpClient.builder()
                        .readTimeout(Duration.ofMinutes(15))
                        .build())
                .build();
        invokeFunction(awsLambda);
        awsLambda.close();
        tvFileLoaderExecution();
        tvFileLoaderResponse();
        downLoadFileFromS3();
    }
    public static void invokeFunction(LambdaAsyncClient awsLambda) {
        try {
            // Need a SdkBytes instance for the payload.
            JSONObject jsonObj = new JSONObject();
            JSONArray array = new JSONArray();
            jsonObj.put("runIds", array.put("20a7f4bf-61ab-4670-a738-b752a4cd56c2"));
            jsonObj.put("campaignId", campaignId);
            jsonObj.put("numberOfCampaignGroups", 3);
            String json = jsonObj.toString();
            System.out.println(json);
            SdkBytes payload = SdkBytes.fromUtf8String(json);
            // Setup an InvokeRequest.
            InvokeRequest request = InvokeRequest.builder()
                    .functionName("arn:aws:lambda:us-east-1:909837171498:function:UpgradeProspectEtl-Function-CampaignGroupCalculator-Staging")
                    .payload(payload)
                    .build();
            CompletableFuture<InvokeResponse> invoke = awsLambda.invoke(request);
            InvokeResponse res = invoke.get();
            String value = res.payload().asUtf8String();
            System.out.println(value);
        } catch (LambdaException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    public static void tvFileLoaderExecution() {
        // (4) Construct the JSON input to the state machine
        JSONObject sfnInput = new JSONObject();
        JSONArray array = new JSONArray();
        sfnInput.put("campaignId", "965e033c-7bd2-4836-bb0c-353d9037dfd5");
        sfnInput.put("runIds", array.put("20a7f4bf-61ab-4670-a738-b752a4cd56c2"));
        sfnInput.put("applyABTesting",true);
        sfnInput.put("disableUpload",true);
        StartExecutionRequest request = StartExecutionRequest.builder()
                .stateMachineArn("arn:aws:states:us-east-1:909837171498:stateMachine:UpgradeProspectEtl-TeamVelocityLoader-SM-Staging")
                .input(sfnInput.toString()).build();
        StartExecutionResponse startExecutionResponse = client.startExecution(request);
        System.out.println("MarketingLoader StartExecution:" + startExecutionResponse.executionArn() + startExecutionResponse.startDate());
    }
    //Check Response from MarketingLoader Response
    public static void tvFileLoaderResponse(){
        ListExecutionsIterable listExecutionsResponses = client.listExecutionsPaginator(ListExecutionsRequest.builder()
                .stateMachineArn("arn:aws:states:us-east-1:909837171498:stateMachine:UpgradeProspectEtl-TeamVelocityLoader-SM-Staging")
                .build());

        DescribeExecutionResponse execution = listExecutionsResponses.executions().stream()
                .map(executionListItem -> getDetailsOfMarket(executionListItem.executionArn()))
                .filter(response -> isRightJobsForTVLoader(response,campaignId))
                .limit(5)
                .findFirst().orElseThrow(() -> new IllegalStateException("Unable to find execution for " + campaignId));
        AtomicReference<DescribeExecutionResponse> status = new AtomicReference<>();
        System.out.println("Execution Arn : "+execution.executionArn());

        await().atMost(15, TimeUnit.MINUTES)
                .pollDelay(5, TimeUnit.SECONDS)
                .until(() -> {
                    DescribeExecutionResponse details = getDetailsOfMarket(execution.executionArn());
                    status.set(details);
                    return details.status() != ExecutionStatus.RUNNING;
                });
        tvfile = status.get().status().toString();
        System.out.println(tvfile);
        JSONObject js = new JSONObject(status.get().output());
        System.out.println("JSON Response: \n"+js);
        JSONArray ja = js.getJSONArray("Payload");
        tvFileList = new ArrayList<String>();
        //Checking whether the JSON array has some value or not
        if (ja != null) {
            //Iterating JSON array
            for (int i=0;i<ja.length();i++){
                //Adding each element of JSON array into ArrayList
                tvFileList.add(ja.getJSONObject(i).getString("location"));
            }
        }
        //Iterating tvFileList to print each element
        System.out.println("TV File List");
        for(int i=0; i<tvFileList.size(); i++) {
            //Printing each element of ArrayList
            System.out.println(tvFileList.get(i));
        }
    }
    public static DescribeExecutionResponse getDetailsOfMarket(String arn){
        return client.describeExecution(DescribeExecutionRequest.builder()
                .executionArn(arn)
                .build());
    }
    @SneakyThrows
    public static boolean isRightJobsForTVLoader(DescribeExecutionResponse describeExecutionResponseForMar,String teamVelocityRunId){
        String input = describeExecutionResponseForMar.input();
        String inputBucket = mapper.readTree(input).at("/campaignId").textValue();
        return Objects.equals(inputBucket,campaignId);
    }
    public static void downLoadFileFromS3() throws IOException {
        String bucket = "upgradeprospectetl-bucket-teamvelocity-staging";
        String key = tvFileList.get(0);
        S3Client client = S3Client.builder().build();
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
      //  client.getObject(request);
        String filename = new File(key).getName();
        ResponseInputStream<GetObjectResponse> response = client.getObject(request);
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(filename));
        byte[] buffer = new byte[4096];
        int bytesRead = -1;
        while ((bytesRead = response.read(buffer)) !=  -1) {
            outputStream.write(buffer, 0, bytesRead);
        }
        response.close();
        outputStream.close();
        File fileToMove = new File(filename);
        fileToMove.renameTo(new File("tvFile/USbank/"+filename));
    }
}

