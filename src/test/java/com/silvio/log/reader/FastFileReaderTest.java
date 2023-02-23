package com.silvio.log.reader;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.spies.MultiOnRequestSpy;
import io.smallrye.mutiny.helpers.spies.Spy;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@QuarkusTest
class FastFileReaderTest {

    private static Logger logger = LoggerFactory.getLogger(FastFileReaderTest.class.getName());

    @Test
    void readFile() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key", "WYgfpf1KJfbFFF3M");
        conf.set("fs.s3a.secret.key", "YXLtZX8RMz8sltZq9QxxsoB4sxoFX9ga");
        conf.set("fs.s3a.path.style.access", "true");

        var fs = new S3AFileSystem();
        fs.initialize(new URI("s3a://logs"), conf);
        var reader = new FastFileReaderHadoop(fs).readFile( new Path("s3a://logs/access.log"));
        final MultiOnRequestSpy<Buffer> spy = Spy.onRequest(reader);
        reader.subscribe().with(
                item -> logger.info(item.toString()),
                failure -> logger.error(failure.getMessage(), failure),
                () -> logger.info("Completed: " + spy.requestedCount())
        );
    }
}