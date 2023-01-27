package com.silvio.log.reader;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.model.HdfsLog;
import com.silvio.log.transformer.ApacheBufferTransform;
import com.silvio.log.transformer.HdfsBufferTransform;
import com.silvio.log.writer.ApacheParquetWriter;
import com.silvio.log.writer.HdfsParquetWriter;
import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.buffer.Buffer;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Function;

@ApplicationScoped
public class HdfsFastFileReader extends LogFastFileReader<HdfsLog> {

    public HdfsFastFileReader(FastFileReader fastFileReader) {
        super(fastFileReader);
    }

    @Override
    protected Function<Buffer, Multi<HdfsLog>> transform() {
        return new HdfsBufferTransform();
    }

}
