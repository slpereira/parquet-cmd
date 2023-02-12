package com.silvio.log.writer;

import com.silvio.log.model.ApacheAccessLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ApacheParquetWriter extends BasicParquetWriter<ApacheAccessLog> {

    /**
     * Creates a Builder for configuring ParquetWriter with the example object
     * model. THIS IS AN EXAMPLE ONLY AND NOT INTENDED FOR USE.
     *
     * @param file the output file to create
     * @return a {@link Builder} to create a {@link ParquetWriter}
     */
    public static Builder builder(Path file) {
        return new Builder(file);
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
     * Create a new {@link ApacheParquetWriter}.
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
    ApacheParquetWriter(Path file, WriteSupport<ApacheAccessLog> writeSupport,
                         CompressionCodecName compressionCodecName,
                         int blockSize, int pageSize, boolean enableDictionary,
                         boolean enableValidation,
                         ParquetProperties.WriterVersion writerVersion,
                         Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                enableDictionary, enableValidation, writerVersion, conf);
    }

    public static class Builder extends BasicParquetWriter.Builder<ApacheAccessLog> {
        protected Builder(org.apache.hadoop.fs.Path path) {
            super(path);
        }

        protected Builder(OutputFile path) {
            super(path);
        }

        @Override
        protected WriteSupport<ApacheAccessLog> getWriteSupport(Configuration conf) {
            return new ApacheParquetWriteSupport(type);
        }

    }
}
