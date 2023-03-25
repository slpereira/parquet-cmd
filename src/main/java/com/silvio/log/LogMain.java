package com.silvio.log;

import com.silvio.log.processor.IndexerProcessor;
import com.silvio.log.processor.SqsMessageListenerApacheLogProcessor;
import io.quarkus.runtime.annotations.QuarkusMain;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;

@QuarkusMain
@Slf4j
public class LogMain implements io.quarkus.runtime.QuarkusApplication {
    @Inject
    IndexerProcessor indexerProcessor;

    @Inject
    SqsMessageListenerApacheLogProcessor logProcessor;
    public static void main(String... args) {
        io.quarkus.runtime.Quarkus.run(LogMain.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        logProcessor.listenToMessages();
        indexerProcessor.start();
        log.info("Started");
        io.quarkus.runtime.Quarkus.waitForExit();
        return 0;
    }
}
