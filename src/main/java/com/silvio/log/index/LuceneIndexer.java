package com.silvio.log.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.parquet.column.statistics.Statistics;

import java.awt.*;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

public class LuceneIndexer implements Closeable {
    private final Directory directory;
    private final StandardAnalyzer analyzer = new StandardAnalyzer();
    private final IndexWriterConfig config = new IndexWriterConfig(analyzer);

    private final IndexWriter writer;

    public LuceneIndexer(String folder) throws IOException {
        this.directory = FSDirectory.open(Paths.get(folder));
        this.config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        this.writer = new IndexWriter(directory, config);
    }

    public void addStatistics(Map<String,Statistics> statistics, String fileName) throws IOException {
        var document = new Document();
        for (var entry : statistics.entrySet()) {
            var columnName = entry.getKey();
            var columnStatistics = entry.getValue();
            document.add(new TextField("column", columnName, Field.Store.YES));
            if (columnStatistics.hasNonNullValue()) {
                document.add(new TextField(columnName + ".min", columnStatistics.genericGetMin().toString(), Field.Store.YES));
                document.add(new TextField(columnName + ".max", columnStatistics.genericGetMax().toString(), Field.Store.YES));
            }
            document.add(new Field(columnName + ".numNulls", String.valueOf(columnStatistics.getNumNulls()), TextField.TYPE_STORED));
        }
        document.add(new TextField("fileName", fileName, Field.Store.YES));
        this.writer.addDocument(document);
    }

    public void close() throws IOException {
        this.analyzer.close();
        this.writer.close();
        this.directory.close();
    }
}
