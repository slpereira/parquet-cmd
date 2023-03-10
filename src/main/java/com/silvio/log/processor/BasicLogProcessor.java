package com.silvio.log.processor;

import com.silvio.log.reader.LogFastFileReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.slf4j.Logger;

import java.io.IOException;

public abstract class BasicLogProcessor<T> {
    protected final LogFastFileReader<T> logFastFileReader;

    private static final Logger logger = org.slf4j.LoggerFactory.getLogger(BasicLogProcessor.class);

    public BasicLogProcessor(LogFastFileReader<T> logFastFileReader) {
        this.logFastFileReader = logFastFileReader;
    }

    protected abstract ParquetWriter<T> getWriter(Path dest) throws IOException;

    public void processLog(Path source, Path dest) throws IOException {
        var reader = logFastFileReader.readFile(source);
        var writer = getWriter(dest);

        reader.subscribe().with(
                log -> {
                    logger.info(log.toString());
                    try {
                        writer.write(log);
                    } catch (IOException e) {
                        logger.error("Error writing log to parquet file", e);
                    }
                },
                failure -> logger.error("Error reading log file", failure),
                () -> {
                    try {
                        writer.close();
                    } catch (IOException e) {
                        logger.error("Error closing parquet file", e);
                    }
                });
    }
}
