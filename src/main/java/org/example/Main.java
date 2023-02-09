package org.example;


import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.lambda.LambdaAsyncClient;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.FunctionConfiguration;
import software.amazon.awssdk.services.lambda.model.InvocationType;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.LambdaException;
import software.amazon.awssdk.services.lambda.model.ListFunctionsResponse;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) {
        LambdaAsyncClient awsLambda = LambdaAsyncClient.builder()
                .httpClient(NettyNioAsyncHttpClient.builder()
                        .readTimeout(Duration.ofMinutes(15))
                        .build())
                .build();
        invokeFunction(awsLambda);
        awsLambda.close();
    }

    public static void invokeFunction(LambdaAsyncClient awsLambda) {
        // UpgradeProspectEtl-Function-CampaignGroupCalculator-Staging
        //arn:aws:lambda:us-east-1:909837171498:function:UpgradeProspectEtl-Function-CampaignGroupCalculator-Staging
        try {
            // Need a SdkBytes instance for the payload.
            JSONObject jsonObj = new JSONObject();
            JSONArray array = new JSONArray();
            jsonObj.put("runIds", array.put("e5817710-5491-11ed-bdc3-0242ac120002"));
            jsonObj.put("campaignId", "965e033c-7bd2-4836-bb0c-353d9037dfd5");
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

    public static void listFunctions(LambdaClient awsLambda) {
        try {
            ListFunctionsResponse functionResult = awsLambda.listFunctions();
            List<FunctionConfiguration> list = functionResult.functions();
            for (FunctionConfiguration config : list) {
                System.out.println("The function name is " + config.functionName());
            }

        } catch (LambdaException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}
