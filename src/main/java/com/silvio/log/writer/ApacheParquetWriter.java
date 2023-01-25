package com.silvio.log.writer;

import com.silvio.log.model.ApacheAccessLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class ApacheParquetWriter extends ParquetWriter<ApacheAccessLog> {

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
        super(new org.apache.hadoop.fs.Path(file.toAbsolutePath().toString()), writeSupport, compressionCodecName, blockSize, pageSize,
                pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    public static class Builder extends ParquetWriter.Builder<ApacheAccessLog, Builder> {
        private MessageType type = null;
        // TODO: Add extra metadata
        private Map<String, String> extraMetaData = new HashMap<>();

        private Builder(Path file) {
            super(new org.apache.hadoop.fs.Path(file.toAbsolutePath().toString()));
        }

        private Builder(OutputFile file) {
            super(file);
        }

        public Builder withType(MessageType type) {
            this.type = type;
            return this;
        }

        public Builder withExtraMetaData(Map<String, String> extraMetaData) {
            this.extraMetaData = extraMetaData;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected WriteSupport<ApacheAccessLog> getWriteSupport(Configuration conf) {
            return new ApacheParquetWriteSupport(type);
        }

    }
}
