package com.silvio.log.processor;

import com.silvio.log.cloud.aws.SqsService;
import io.smallrye.mutiny.Multi;
import lombok.Getter;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;

@ApplicationScoped
public class SqsMessageListenerIndexerProcessor {
    @Getter
    private final SqsService sqsService;

    public SqsMessageListenerIndexerProcessor(SqsClient sqsClient,
                                              @ConfigProperty(name = "sqs.indexer.receiver") String receiveUrl) {
        this.sqsService = new SqsService(sqsClient, null, receiveUrl);
    }

    public Multi<Message> listenToMessages() {

        return Multi.createFrom().emitter(em -> {
            // TODO: deal with exceptions
            while (!em.isCancelled()) {
                List<Message> response = sqsService.receiveMessages();
                response.forEach(em::emit);
            }
            em.complete();
        });
    }


}
