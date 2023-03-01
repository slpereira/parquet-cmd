package com.silvio.log;

import com.silvio.log.processor.IndexerProcessor;
import io.quarkus.runtime.annotations.QuarkusMain;

import javax.inject.Inject;

@QuarkusMain
public class LogMain implements io.quarkus.runtime.QuarkusApplication {
    @Inject
    IndexerProcessor indexerProcessor;
    public static void main(String... args) {
        io.quarkus.runtime.Quarkus.run(LogMain.class, args);
    }

    @Override
    public int run(String... args) throws Exception {
        indexerProcessor.onStart();
        io.quarkus.runtime.Quarkus.waitForExit();
        return 0;
    }
}
