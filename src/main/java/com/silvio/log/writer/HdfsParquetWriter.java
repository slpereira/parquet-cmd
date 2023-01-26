package com.silvio.log.writer;

import com.silvio.log.model.ApacheAccessLog;
import com.silvio.log.model.HdfsLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.nio.file.Path;

public class HdfsParquetWriter extends BasicParquetWriter<HdfsLog> {

    /**
     * Creates a Builder for configuring ParquetWriter with the example object
     * model. THIS IS AN EXAMPLE ONLY AND NOT INTENDED FOR USE.
     *
     * @param file the output file to create
     * @return a {@link Builder} to create a {@link ParquetWriter}
     */
    public static Builder builder(Path file) {
        return new Builder(new org.apache.hadoop.fs.Path(file.toAbsolutePath().toString()));
    }

    /**
     * Creates a Builder for configuring ParquetWriter with the example object
     * model. THIS IS AN EXAMPLE ONLY AND NOT INTENDED FOR USE.
     *
     * @param file the output file to create
     * @return a {@link Builder} to create a {@link ParquetWriter}
     */
    public static Builder builder(OutputFile file) {
        return new Builder(file);
    }

    /**
     * Create a new {@link HdfsParquetWriter}.
     *
     * @param file The file name to write to.
     * @param writeSupport The schema to write with.
     * @param compressionCodecName Compression code to use, or CompressionCodecName.UNCOMPRESSED
     * @param blockSize the block size threshold.
     * @param pageSize See parquet write up. Blocks are subdivided into pages for alignment and other purposes.
     * @param enableDictionary Whether to use a dictionary to compress columns.
     * @param conf The Configuration to use.
     * @throws IOException
     */
    HdfsParquetWriter(Path file, WriteSupport<HdfsLog> writeSupport,
                      CompressionCodecName compressionCodecName,
                      int blockSize, int pageSize, boolean enableDictionary,
                      boolean enableValidation,
                      ParquetProperties.WriterVersion writerVersion,
                      Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                enableDictionary, enableValidation, writerVersion, conf);
    }

    public static class Builder extends BasicParquetWriter.Builder<HdfsLog> {
        protected Builder(org.apache.hadoop.fs.Path path) {
            super(path);
        }

        protected Builder(OutputFile path) {
            super(path);
        }

        @Override
        protected WriteSupport<HdfsLog> getWriteSupport(Configuration conf) {
            return new HdfsParquetWriteSupport(type);
        }

    }
}
