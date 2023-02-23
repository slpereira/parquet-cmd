package com.silvio.log.writer;

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

public class BasicParquetWriter<T> extends ParquetWriter<T> {

    /**
     * Create a new {@link BasicParquetWriter}.
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
    BasicParquetWriter(Path file, WriteSupport<T> writeSupport,
                       CompressionCodecName compressionCodecName,
                       int blockSize, int pageSize, boolean enableDictionary,
                       boolean enableValidation,
                       ParquetProperties.WriterVersion writerVersion,
                       Configuration conf)
            throws IOException {
        super(file, writeSupport, compressionCodecName, blockSize, pageSize,
                pageSize, enableDictionary, enableValidation, writerVersion, conf);
    }

    public static abstract class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {
        protected MessageType type = null;
        // TODO: Add extra metadata
        private Map<String, String> extraMetaData = new HashMap<>();

        protected Builder(Path path) {
            super(path);
        }

        protected Builder(OutputFile path) {
            super(path);
        }

        public Builder<T> withType(MessageType type) {
            this.type = type;
            return this;
        }

        public Builder<T> withExtraMetaData(Map<String, String> extraMetaData) {
            this.extraMetaData = extraMetaData;
            return this;
        }

        @Override
        protected Builder<T> self() {
            return this;
        }


    }
}
