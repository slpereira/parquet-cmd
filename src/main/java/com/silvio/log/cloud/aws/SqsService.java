package com.silvio.log.cloud.aws;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

public class SqsService {
    private final SqsClient sqsClient;

    private final String queueUrl;

    private final String receiveUrl;

    public SqsService(SqsClient sqsClient, String queueUrl, String receiveUrl) {
        this.sqsClient = sqsClient;
        this.queueUrl = queueUrl;
        this.receiveUrl = receiveUrl;
    }

    public void sendMessage(String messageBody) {
        SendMessageRequest request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        sqsClient.sendMessage(request);
    }

    public List<Message> receiveMessages() {
        ReceiveMessageRequest request = ReceiveMessageRequest.builder()
                .queueUrl(receiveUrl)
                .build();

        return sqsClient.receiveMessage(request).messages();
    }

    public void deleteMessage(String receiptHandle) {
        sqsClient.deleteMessage(builder -> builder.queueUrl(receiveUrl).receiptHandle(receiptHandle));
    }
}
