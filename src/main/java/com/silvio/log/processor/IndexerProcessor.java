package com.silvio.log.processor;

import com.silvio.log.cloud.aws.SqsService;
import com.silvio.log.config.S3Config;
import com.silvio.log.index.JdbcIndexer;
import com.silvio.log.index.ParquetIndexer;
import io.smallrye.mutiny.unchecked.Unchecked;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;

@ApplicationScoped
@Slf4j
public class IndexerProcessor {
    @Inject
    SqsMessageListenerIndexerProcessor messageListener;

    @Inject
    S3Config s3Config;

    @Inject
    JdbcIndexer indexer;

    public void start() {
        final var parquetIndexer = new ParquetIndexer(indexer, s3Config.getConfiguration());
        final var listener = messageListener.listenToMessages();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Received CTRL+C");
            try {
                indexer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
        listener.onCompletion().invoke(() -> {
            try {
                indexer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        listener.log().onItem().invoke(
                Unchecked.consumer(m -> parquetIndexer.processFile(m.body()))).subscribe().with(
                m -> {
                    messageListener.getSqsService().deleteMessage(m.receiptHandle());
                    log.info("Message processed: " + m.body());
                },
                (throwable) -> log.error("Error processing message: " + throwable.getMessage(), throwable));
    }

}
