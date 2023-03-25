package com.silvio.log.processor;

import com.silvio.log.cloud.HandleEvent;
import com.silvio.log.cloud.SourceFileSystem;
import com.silvio.log.cloud.aws.SqsService;
import com.silvio.log.config.S3Config;
import com.silvio.log.reader.ApacheFastFileReader;
import com.silvio.log.reader.FastFileReaderHadoop;
import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.sqs.SqsClient;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class ApacheLogProcessor {
    @Inject
    S3Config s3Config;

    @Inject
    SourceFileSystem sourceFileSystem;

    private final SqsService sqsService;

    @Inject
    HandleEvent handler;

    public ApacheLogProcessor(SqsClient sqsClient, @ConfigProperty(name = "sqs.indexer.sender") String senderUrl) {
        this.sqsService = new SqsService(sqsClient, senderUrl, null);
    }

    public ApacheLogInfiniteProcessor generateParquet(String jsonEvent) {
        return Multi.createFrom().items(handler.handleEvent(jsonEvent).stream())
                .log()
                .flatMap(
                        objectIdentifier ->
                                new ApacheFastFileReader(new FastFileReaderHadoop(sourceFileSystem.getFileSystem())).readFile(new org.apache.hadoop.fs.Path(objectIdentifier.toURI())))
                .subscribe()
                .withSubscriber(new ApacheLogInfiniteProcessor("s3a://parquet/", s3Config.getConfiguration(), sqsService));
    }
}
