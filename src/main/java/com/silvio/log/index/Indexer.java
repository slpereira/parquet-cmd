package com.silvio.log.index;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Indexer extends Closeable {

    void start() throws IOException;

    void index(BlockMetaData metadata, String fileName) throws IOException;

    void commit() throws IOException;
}
