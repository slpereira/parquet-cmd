package com.silvio.log.index;

import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.smallrye.mutiny.unchecked.UncheckedConsumer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class LuceneIndexer implements Indexer {
    private final Directory directory;
    private final StandardAnalyzer analyzer = new StandardAnalyzer();
    private final IndexWriterConfig config = new IndexWriterConfig(analyzer);

    private final IndexWriter writer;

    public LuceneIndexer(String folder) throws IOException {
        this.directory = FSDirectory.open(Paths.get(folder));
        this.config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        this.writer = new IndexWriter(directory, config);
    }

    @Override
    public void start() throws IOException {

    }

    @Override
    public void index(BlockMetaData blockMetaData, String fileName) throws IOException {
        var document = new Document();
        for (var column : blockMetaData.getColumns()) {
            var columnName = column.getPath().toDotString();
            var columnStatistics = column.getStatistics();
            document.add(new StringField(columnName, column.getPrimitiveType().getPrimitiveTypeName().toString(), Field.Store.NO));
            document.add(new StringField(columnName + ".comparator", columnStatistics.comparator().toString(), Field.Store.NO));
            if (columnStatistics.hasNonNullValue()) {
                switch (column.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32 -> {
                        document.add(new IntRange(columnName + ".range", new int[]{(int) columnStatistics.genericGetMin()}, new int[]{(int) columnStatistics.genericGetMax()}));
                    }
                    case INT64 -> {
                        document.add(new LongRange(columnName + ".range", new long[]{(long) columnStatistics.genericGetMin()}, new long[]{(long) columnStatistics.genericGetMax()}));
                    }
                    case BINARY, BOOLEAN, FIXED_LEN_BYTE_ARRAY, INT96 -> {
                        document.add(new StringField(columnName + ".min", new String(columnStatistics.getMinBytes()), Field.Store.YES));
                        document.add(new StringField(columnName + ".max", new String(columnStatistics.getMaxBytes()), Field.Store.YES));
                    }
                    case FLOAT -> {
                        document.add(new FloatRange(columnName + ".range", new float[]{(float) columnStatistics.genericGetMin()}, new float[]{(float) columnStatistics.genericGetMax()}));
                    }
                    case DOUBLE -> {
                        document.add(new DoubleRange(columnName + ".range", new double[]{(double) columnStatistics.genericGetMin()}, new double[]{(double) columnStatistics.genericGetMax()}));
                    }
                    default -> {
                        throw new IllegalStateException("Unexpected value: " + column.getPrimitiveType().getPrimitiveTypeName());
                    }
                }
            }
        }
        document.add(new Field("fileName", fileName, getIdFieldType()));
        writer.addDocument(document);
        // TODO: we can move this to another point
        commit();
    }

    private FieldType getIdFieldType() {
        FieldType fieldType = new FieldType();
        fieldType.setStored(true);
        fieldType.setIndexOptions(IndexOptions.DOCS);
        fieldType.setTokenized(false);
        fieldType.setOmitNorms(true);
        return fieldType;
    }

    @Override
    public void commit() throws IOException {
        writer.commit();
    }

    public void close() throws IOException {
        this.analyzer.close();
        this.writer.close();
        this.directory.close();
    }
}
