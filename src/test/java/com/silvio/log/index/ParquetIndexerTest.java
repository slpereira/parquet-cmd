package com.silvio.log.index;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class ParquetIndexerTest {

    @Test
    void testIndexingParquet() throws IOException {
        Configuration configuration = new Configuration();
        new ParquetIndexer().processFile("/data/logs/access.parquet", configuration, "/data/logs/lucene");
    }

}