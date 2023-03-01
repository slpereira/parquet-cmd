package com.silvio.log.processor;

import com.silvio.log.cloud.aws.SqsService;
import com.silvio.log.config.S3Config;
import com.silvio.log.index.LuceneIndexer;
import com.silvio.log.index.ParquetIndexer;
import io.smallrye.mutiny.unchecked.Unchecked;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;

@ApplicationScoped
@Slf4j
public class IndexerProcessor {
    @Inject
    SqsMessageListenerProcessor messageListener;

    @Inject
    SqsService sqsService;

    @ConfigProperty(name = "lucene.index.path")
    String luceneIndexPath;

    @Inject
    S3Config s3Config;

    public void onStart() throws IOException {
        final var indexer = new LuceneIndexer(luceneIndexPath);
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
                    log.info("Message processed: " + m.body());
                    sqsService.deleteMessage(m.receiptHandle());
                },
                (throwable) -> log.error("Error processing message: " + throwable.getMessage(), throwable));
    }

}
