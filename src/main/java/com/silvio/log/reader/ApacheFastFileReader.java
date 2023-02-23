package com.silvio.log.reader;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.transformer.ApacheBufferTransform;
import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.buffer.Buffer;

import java.util.function.Function;

public class ApacheFastFileReader extends LogFastFileReader<ApacheAccessLog> {

    public ApacheFastFileReader(FileReader fileReader) {
        super(fileReader);
    }

    @Override
    protected Function<Buffer, Multi<ApacheAccessLog>> transform() {
        return new ApacheBufferTransform();
    }


}
