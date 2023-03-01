package com.silvio.log.processor;

import com.silvio.log.cloud.aws.SqsService;
import io.quarkus.runtime.Startup;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import software.amazon.awssdk.services.sqs.model.Message;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class SqsMessageListenerProcessor {
    @Inject
    SqsService sqsService;

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
