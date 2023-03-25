package com.silvio.log.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;

public class LuceneTester implements Closeable {
    private IndexReader reader;
    private Directory directory;
   private StandardAnalyzer analyzer = new StandardAnalyzer();
   private IndexWriterConfig config = new IndexWriterConfig(analyzer);

   private IndexSearcher searcher;

   private IndexWriter writer;

    public LuceneTester() throws IOException {
        this.directory = new ByteBuffersDirectory();
        this.config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
    }
    
    public IndexWriter getWriter() throws IOException {
        if (this.writer != null) return writer;
        this.writer = new IndexWriter(directory, config);
        return writer;
    }

    @Override
    public void close() throws IOException {
        writer.close();
        directory.close();
    }
}
