package com.silvio.log.processor;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.reader.ApacheFastFileReader;
import com.silvio.log.reader.FastFileReader;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.vertx.mutiny.core.Vertx;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class ApacheLogInfiniteProcessorTest {

    @Test
    void processLogInfinite() throws InterruptedException {

        ApacheFastFileReader apacheFastFileReader = new ApacheFastFileReader(new FastFileReader(Vertx.vertx()));

        var m = apacheFastFileReader.readFile(new Path("/data/logs/access.log"));
        AssertSubscriber s = AssertSubscriber.create();
        m.subscribe().withSubscriber(s);
        var p = new ApacheLogInfiniteProcessor(m, "/data/logs/test");
        p.processLog();
        s.awaitCompletion(Duration.ofSeconds(300));


    }


}