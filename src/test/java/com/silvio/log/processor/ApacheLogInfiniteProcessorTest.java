package com.silvio.log.processor;

import com.silvio.log.cloud.aws.SqsService;
import com.silvio.log.reader.ApacheFastFileReader;
import com.silvio.log.reader.FastFileReaderHadoop;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.concurrent.Executors;

@QuarkusTest
class ApacheLogInfiniteProcessorTest {

    @Inject
    SqsService sqsService;

    @Test
    void processLogInfinite() throws InterruptedException, URISyntaxException, IOException {

        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key", "WYgfpf1KJfbFFF3M");
        conf.set("fs.s3a.secret.key", "YXLtZX8RMz8sltZq9QxxsoB4sxoFX9ga");
        conf.set("fs.s3a.path.style.access", "true");
        var fs = new S3AFileSystem();
        fs.initialize(new URI("s3://logs"), conf);

        ApacheFastFileReader apacheFastFileReader = new ApacheFastFileReader(new FastFileReaderHadoop(fs));

        AssertSubscriber s = new AssertSubscriber<>();
        var m = apacheFastFileReader.readFile(new Path("/access.log"));
//        var m2 = apacheFastFileReader.readFile(new Path("/data/logs/source/xaa"));
        var lp = new ApacheLogInfiniteProcessor("s3a://parquet/", conf, sqsService);
        m.emitOn(Executors.newSingleThreadExecutor()).subscribe().withSubscriber(lp);
        //m.subscribe().withSubscriber(lp);
        //m2.subscribe().withSubscriber(lp);
//        m2.emitOn(Executors.newSingleThreadExecutor()).subscribe().withSubscriber(lp2);
        m.subscribe().withSubscriber(s);
//        m2.subscribe().withSubscriber(s);
        m.onCompletion().call(() -> {
            System.out.println("m completed");
            return null;
        });
//        m2.onCompletion().call(() -> {
//            System.out.println("m2 completed");
//            return null;
//        });
        s.await(Duration.ofMinutes(2));
    }


}