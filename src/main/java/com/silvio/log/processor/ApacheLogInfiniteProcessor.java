package com.silvio.log.processor;

import com.silvio.log.cloud.aws.SqsService;
import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.writer.ApacheParquetWriter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.util.UUID;

@Slf4j
public class ApacheLogInfiniteProcessor extends InfiniteLogProcessor<ApacheAccessLog> {

    private final String destPath;

    private String currentPath;

    private final Configuration conf;

    private final SqsService sqsService;

    public ApacheLogInfiniteProcessor(String destPath, Configuration conf, SqsService sqsService) {
        this.destPath = destPath;
        this.conf = conf;
        this.sqsService = sqsService;
    }

    @Override
    protected Path getDestPath() {
        this.currentPath = this.destPath + UUID.randomUUID() + ".parquet";
        log.info("Writing to {}", this.currentPath);
        return new Path(this.currentPath);
    }

    @Override
    protected ParquetWriter<ApacheAccessLog> getWriter(Path destPath) throws IOException {
        return ApacheParquetWriter
                .builder(destPath)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(ApacheAccessLog.getSchema())
                .build();
    }

    @Override
    protected void onClose() {
        super.onClose();
        this.sqsService.sendMessage(this.currentPath);
    }

    @Override
    protected String getCurrentPath() {
        return currentPath;
    }
}
