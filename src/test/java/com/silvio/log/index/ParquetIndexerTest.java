package com.silvio.log.index;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

class ParquetIndexerTest {

    @Test
    void testIndexingParquet() throws IOException {
        Configuration configuration = new Configuration();
        LuceneIndexer luceneIndexer = new LuceneIndexer("/data/logs/lucene");
        var indexer = new ParquetIndexer(luceneIndexer, configuration);
        Files.walk(new java.io.File("/data/logs/test").toPath())
                .filter(Files::isRegularFile)
                .filter(p -> new File(p.toString()).length() > 0)
                .filter(p -> p.toString().endsWith(".parquet"))
                .forEach(p -> {
                    try {
                        indexer.processFile(p.toString());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

}