package com.silvio.log.writer;

import com.silvio.log.model.ApacheAccessLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class ApacheParquetWriterTest {


    final String line = """
178.253.33.51 - - [22/Jan/2019:03:56:21 +0330] "GET /image/32574?name=pr465at.jpg&wh=max HTTP/1.1" 200 28825 "https://www.zanbil.ir/m/product/32574/62991/%D9%85%D8%A7%D8%B4%DB%8C%D9%86-%D8%A7%D8%B5%D9%84%D8%A7%D8%AD-%D8%B5%D9%88%D8%B1%D8%AA-%D9%BE%D8%B1%D9%86%D8%B3%D9%84%DB%8C-%D9%85%D8%AF%D9%84-PR465AT" "Mozilla/5.0 (Linux; Android 5.1; HTC Desire 728 dual sim) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.83 Mobile Safari/537.36" "-"
""";

    @Test
    void parquetApache(@TempDir Path tempDir) throws IOException {

        var writer = ApacheParquetWriter
                .builder(new org.apache.hadoop.fs.Path(tempDir.toString(), UUID.randomUUID().toString(), "test.parquet"))
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(ApacheAccessLog.getSchema())
                .build();

        var log = ApacheAccessLog.parse(line);
        writer.write(log);
        writer.close();

    }

    @Test
    void parquetApacheS3a(@TempDir Path tempDir) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.access.key", "AKIAQEE52AOIJETQ7AUV");
        conf.set("fs.s3a.secret.key", "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");
        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key","root"); // MINIO_ROOT_USER
        conf.set("fs.s3a.secret.key","password"); // MINIO_ROOT_PASSWORD
        conf.set("fs.s3a.path.style.access", "true");
        System.setProperty("com.amazonaws.services.s3.enableV4", "true");
        //System.setProperty("com.amazonaws.services.s3.endpoint", "http://localhost:4566");

        var writer = ApacheParquetWriter
                .builder(new org.apache.hadoop.fs.Path("s3a://reader-bucket/test.parquet"))
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(ApacheAccessLog.getSchema())
                .build();

        var log = ApacheAccessLog.parse(line);
        writer.write(log);
        writer.close();

    }


}