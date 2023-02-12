package com.silvio.log.reader;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.helpers.spies.MultiOnRequestSpy;
import io.smallrye.mutiny.helpers.spies.Spy;
import io.vertx.mutiny.core.buffer.Buffer;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class FastFileReaderTest {

    private static Logger logger = LoggerFactory.getLogger(FastFileReaderTest.class.getName());

    @Inject
    FastFileReader fastFileReader;
    @Test
    void readFile() {
        var reader = fastFileReader.readFile( new Path("/data/logs/access.log"));
        final MultiOnRequestSpy<Buffer> spy = Spy.onRequest(reader);
        reader.subscribe().with(
                item -> logger.info(item.toString()),
                failure -> logger.error(failure.getMessage(), failure),
                () -> logger.info("Completed: " + spy.requestedCount())
        );
    }
}