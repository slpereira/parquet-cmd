package com.silvio.log.processor;

import com.silvio.log.cloud.aws.SqsService;
import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class SqsMessageListenerApacheLogProcessor {

    @Inject
    ApacheLogProcessor apacheLogProcessor;

    private final SqsService sqsService;

    public SqsMessageListenerApacheLogProcessor(SqsClient sqsClient,
                                              @ConfigProperty(name = "sqs.apache.receiver") String receiveUrl) {
        this.sqsService = new SqsService(sqsClient, null, receiveUrl);
    }

    public void listenToMessages() {

        Multi.createFrom().<Message>emitter(em -> {
            // TODO: deal with exceptions
            while (!em.isCancelled()) {
                List<Message> response = sqsService.receiveMessages();
                response.forEach(em::emit);
            }
            em.complete();
        }).onItem().invoke(message -> {
            apacheLogProcessor.generateParquet(message.body());
            sqsService.deleteMessage(message.receiptHandle());
        }).subscribe().with(
                item -> System.out.println("Received: " + item),
                failure -> System.out.println("Failed with " + failure.getMessage()),
                () -> System.out.println("Completed")
        );
    }
}
