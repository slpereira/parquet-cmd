package com.silvio.log.controller;

import com.silvio.log.cloud.aws.HandleS3Event;
import com.silvio.log.cloud.aws.S3SourceFileSystem;
import com.silvio.log.cloud.aws.SqsService;
import com.silvio.log.config.S3Config;
import com.silvio.log.processor.ApacheLogInfiniteProcessor;
import com.silvio.log.reader.ApacheFastFileReader;
import com.silvio.log.reader.FastFileReaderHadoop;
import io.smallrye.mutiny.Multi;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

@Path("/webhook")
public class WebHook {

    @Inject
    S3Config s3Config;

    @Inject
    S3SourceFileSystem s3SourceFileSystem;

    @Inject
    SqsService sqsService;

    @Path("/notify")
    @POST
    public void generateParquet(String jsonEvent) {
        Multi.createFrom().items(new HandleS3Event().handleEvent(jsonEvent).stream())
                .log()
                .flatMap(
                    objectIdentifier ->
                        new ApacheFastFileReader(new FastFileReaderHadoop(s3SourceFileSystem.getFileSystem())).readFile(new org.apache.hadoop.fs.Path(objectIdentifier.toURI())))
                .subscribe()
                .withSubscriber(new ApacheLogInfiniteProcessor("s3a://parquet/", s3Config.getConfiguration(), sqsService));
    }

}
