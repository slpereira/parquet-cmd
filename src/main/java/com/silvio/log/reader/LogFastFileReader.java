package com.silvio.log.reader;

import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.buffer.Buffer;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.function.Function;

public abstract class LogFastFileReader<T> {

    private final FileReader fileReader;

    public LogFastFileReader(FileReader fileReader) {
        this.fileReader = fileReader;
    }

    public Multi<T> readFile(Path path) {
        var reader = fileReader.readFile(path);
        return reader.onItem().transformToMultiAndConcatenate(transform());
    }

    protected abstract Function<Buffer, Multi<T>> transform();

}
