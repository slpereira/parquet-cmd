package com.silvio.log.reader;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.transformer.ApacheBufferTransform;
import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.buffer.Buffer;
import org.apache.hadoop.fs.Path;

import java.util.function.Function;

public abstract class LogFastFileReader<T> {

    private final FastFileReader fastFileReader;

    public LogFastFileReader(FastFileReader fastFileReader) {
        this.fastFileReader = fastFileReader;
    }

    public Multi<T> readFile(Path path) {
        var reader = fastFileReader.readFile(path);
        return reader.onItem().transformToMultiAndConcatenate(transform());
    }

    protected abstract Function<Buffer, Multi<T>> transform();

}
