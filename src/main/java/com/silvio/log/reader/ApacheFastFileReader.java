package com.silvio.log.reader;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.transformer.ApacheBufferTransform;
import io.smallrye.mutiny.Multi;

import javax.enterprise.context.ApplicationScoped;
import java.nio.file.Path;

@ApplicationScoped
public class ApacheFastFileReader {

    private final FastFileReader fastFileReader;

    public ApacheFastFileReader(FastFileReader fastFileReader) {
        this.fastFileReader = fastFileReader;
    }

    public Multi<ApacheAccessLog> readFile(Path path) {
        var reader = fastFileReader.readFile(path);
        return reader.onItem().transformToMultiAndConcatenate(new ApacheBufferTransform());
    }
}
