package com.silvio.log;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.reader.ApacheFastFileReader;
import com.silvio.log.writer.ApacheParquetWriter;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;

@QuarkusMain
public class LogMain implements QuarkusApplication {

    public static void main(String... args) {
        io.quarkus.runtime.Quarkus.run(LogMain.class, args);
    }

    @Inject
    ApacheFastFileReader fastFileReader;

    Logger logger = org.slf4j.LoggerFactory.getLogger(LogMain.class.getName());

    @Override
    public int run(String... args) throws Exception {
        var reader = fastFileReader.readFile(Path.of("/data/logs/access.log"));
        var writer = ApacheParquetWriter
                .builder(Path.of("/data/logs/access.parquet"))
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(ApacheAccessLog.getSchema())
                .build();

        reader.subscribe().with(item -> {
            logger.trace(item.toString());
            try {
                writer.write(item);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, failure -> logger.error(failure.getMessage(), failure),
                () -> {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    logger.info("Completed");
                });

        return 0;
    }
}
