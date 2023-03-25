package com.silvio.log.controller;

import com.silvio.log.cloud.HandleEvent;
import com.silvio.log.cloud.SourceFileSystem;
import com.silvio.log.cloud.aws.SqsService;
import com.silvio.log.config.S3Config;
import com.silvio.log.processor.ApacheLogInfiniteProcessor;
import com.silvio.log.processor.ApacheLogProcessor;
import com.silvio.log.reader.ApacheFastFileReader;
import com.silvio.log.reader.FastFileReaderHadoop;
import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.sqs.SqsClient;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

@Path("/webhook")
public class WebHook {

    @Inject
    ApacheLogProcessor apacheLogProcessor;

    @Path("/notify")
    @POST
    public void generateParquet(String jsonEvent) {
        apacheLogProcessor.generateParquet(jsonEvent);
    }

}
