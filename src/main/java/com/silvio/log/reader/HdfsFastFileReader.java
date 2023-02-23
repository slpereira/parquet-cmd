package com.silvio.log.reader;

import com.silvio.log.model.HdfsLog;
import com.silvio.log.transformer.HdfsBufferTransform;
import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.buffer.Buffer;

import java.util.function.Function;

public class HdfsFastFileReader extends LogFastFileReader<HdfsLog> {

    public HdfsFastFileReader(FileReader fileReader) {
        super(fileReader);
    }

    @Override
    protected Function<Buffer, Multi<HdfsLog>> transform() {
        return new HdfsBufferTransform();
    }

}
