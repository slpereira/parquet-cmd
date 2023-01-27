package com.silvio.log.processor;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.reader.ApacheFastFileReader;
import com.silvio.log.writer.ApacheParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Path;

public class ApacheLogProcessor extends BasicLogProcessor<ApacheAccessLog> {

    public ApacheLogProcessor(ApacheFastFileReader apacheFastFileReader) {
        super(apacheFastFileReader);
    }

    @Override
    protected ParquetWriter<ApacheAccessLog> getWriter(Path dest) throws IOException {
        return ApacheParquetWriter
                .builder(dest)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(ApacheAccessLog.getSchema())
                .build();
    }
}
