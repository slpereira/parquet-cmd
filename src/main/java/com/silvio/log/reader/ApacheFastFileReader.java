package com.silvio.log.reader;

import com.silvio.log.LogMain;
import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.transformer.ApacheBufferTransform;
import com.silvio.log.writer.ApacheParquetWriter;
import io.smallrye.mutiny.Multi;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.nio.file.Path;

@ApplicationScoped
public class ApacheFastFileReader {

    private final FastFileReader fastFileReader;
    private static Logger logger = org.slf4j.LoggerFactory.getLogger(ApacheFastFileReader.class.getName());

    public ApacheFastFileReader(FastFileReader fastFileReader) {
        this.fastFileReader = fastFileReader;
    }

    public Multi<ApacheAccessLog> readFile(Path path) {
        var reader = fastFileReader.readFile(path);
        return reader.onItem().transformToMultiAndConcatenate(new ApacheBufferTransform());
    }

    public void processApacheLog(Path source, Path dest) throws IOException {
        var reader = readFile(source);
        var writer = ApacheParquetWriter
                .builder(dest)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(ApacheAccessLog.getSchema())
                .build();

        reader.subscribe().with(item -> {
                    logger.info(item.toString());
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
    }
}
