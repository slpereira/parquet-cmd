package com.silvio.log.processor;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.writer.ApacheParquetWriter;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.groups.MultiSubscribe;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

public class ApacheLogInfiniteProcessor extends InfiniteLogProcessor<ApacheAccessLog> {
    public ApacheLogInfiniteProcessor(Multi<ApacheAccessLog> multi, String destPath) {
        super(multi, destPath);
    }

    @Override
    protected ParquetWriter<ApacheAccessLog> getWriter(Path destPath) throws IOException {
        return ApacheParquetWriter
                .builder(destPath)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(ApacheAccessLog.getSchema())
                .build();
    }
}
