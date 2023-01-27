package com.silvio.log.processor;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.model.HdfsLog;
import com.silvio.log.reader.ApacheFastFileReader;
import com.silvio.log.reader.HdfsFastFileReader;
import com.silvio.log.writer.ApacheParquetWriter;
import com.silvio.log.writer.HdfsParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import javax.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.nio.file.Path;

@ApplicationScoped
public class HdfsLogProcessor extends BasicLogProcessor<HdfsLog> {

    public HdfsLogProcessor(HdfsFastFileReader hdfsFastFileReader) {
        super(hdfsFastFileReader);
    }

    @Override
    protected ParquetWriter<HdfsLog> getWriter(Path dest) throws IOException {
        return HdfsParquetWriter
                .builder(dest)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(HdfsLog.getSchema())
                .build();
    }
}
