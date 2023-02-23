package com.silvio.log.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.HashMap;

public class ParquetIndexer {

    public void processFile(String source, Configuration configuration, String dest) throws IOException {

        var inputFile = HadoopInputFile.fromPath(new Path(source), configuration);
        var parquetMetadata = ParquetFileReader.readFooter(inputFile, ParquetReadOptions.builder()
                .build(), inputFile.newStream());
        var mapColumns = new HashMap<String, Statistics>();
        for (ColumnChunkMetaData column : parquetMetadata.getBlocks().get(0).getColumns()) {
            // Get the statistics for the column
            mapColumns.put(column.getPath().toDotString(), column.getStatistics());
        }

        try(var indexer = new LuceneIndexer(dest)) {
            indexer.addStatistics(mapColumns, source);
        }

    }
}
