package com.silvio.log.writer;

import com.silvio.log.model.ApacheAccessLog;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class ApacheParquetWriterTest {


    final String line = """
178.253.33.51 - - [22/Jan/2019:03:56:21 +0330] "GET /image/32574?name=pr465at.jpg&wh=max HTTP/1.1" 200 28825 "https://www.zanbil.ir/m/product/32574/62991/%D9%85%D8%A7%D8%B4%DB%8C%D9%86-%D8%A7%D8%B5%D9%84%D8%A7%D8%AD-%D8%B5%D9%88%D8%B1%D8%AA-%D9%BE%D8%B1%D9%86%D8%B3%D9%84%DB%8C-%D9%85%D8%AF%D9%84-PR465AT" "Mozilla/5.0 (Linux; Android 5.1; HTC Desire 728 dual sim) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.83 Mobile Safari/537.36" "-"
""";

    @Test
    void parquetApache() throws IOException {
        var writer = ApacheParquetWriter
                .builder(Path.of(File.createTempFile("test", ".parquet").getAbsolutePath()))
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withType(ApacheAccessLog.getSchema())
                .build();

        var log = ApacheAccessLog.parse(line);
        writer.write(log);
        writer.close();

    }

}