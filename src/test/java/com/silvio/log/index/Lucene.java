package com.silvio.log.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;

public class Lucene implements Closeable {
   Directory directory;
   StandardAnalyzer analyzer = new StandardAnalyzer();
   IndexWriterConfig config = new IndexWriterConfig(analyzer);

   IndexSearcher searcher;

   IndexWriter writer;

    public Lucene(String folder) throws IOException {
        this.directory = FSDirectory.open(Paths.get(folder));
        this.config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        this.writer = new IndexWriter(directory, config);
        this.searcher = new IndexSearcher(DirectoryReader.open(writer));
    }


    @Override
    public void close() throws IOException {
        writer.close();
        directory.close();
    }
}
