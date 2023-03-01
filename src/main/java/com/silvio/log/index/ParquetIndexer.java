package com.silvio.log.index;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple;
import io.smallrye.mutiny.tuples.Tuple2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.document.Document;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetIndexer {

    private final LuceneIndexer indexer;

    private final Configuration configuration;

    public ParquetIndexer(LuceneIndexer indexer, Configuration configuration) {
        this.indexer = indexer;
        this.configuration = configuration;
    }

    public Uni<Void> processFile(String source) throws IOException {
        indexer.index(getColumns(source), source);
        return Uni.createFrom().voidItem();
    }

    public List<ColumnChunkMetaData> getColumns(String source) throws IOException {
        var inputFile = HadoopInputFile.fromPath(new Path(source), configuration);
        var parquetMetadata = ParquetFileReader.readFooter(inputFile, ParquetReadOptions.builder()
                .build(), inputFile.newStream());
        return parquetMetadata.getBlocks().get(0).getColumns();
    }
}
