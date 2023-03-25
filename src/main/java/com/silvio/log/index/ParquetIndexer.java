package com.silvio.log.index;

import io.smallrye.mutiny.Uni;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.List;

public class ParquetIndexer {

    private final Indexer indexer;

    private final Configuration configuration;

    public ParquetIndexer(Indexer indexer, Configuration configuration) {
        this.indexer = indexer;
        this.configuration = configuration;
    }

    public Uni<Void> processFile(String source) throws IOException {
        indexer.index(getBlockMetadata(source), source);
        return Uni.createFrom().voidItem();
    }

    public BlockMetaData getBlockMetadata(String source) throws IOException {
        var inputFile = HadoopInputFile.fromPath(new Path(source), configuration);
        var parquetMetadata = ParquetFileReader.readFooter(inputFile, ParquetReadOptions.builder()
                .build(), inputFile.newStream());
        return parquetMetadata.getBlocks().get(0);
    }
}
