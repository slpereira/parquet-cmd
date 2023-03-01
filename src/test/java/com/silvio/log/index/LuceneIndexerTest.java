package com.silvio.log.index;

import org.apache.lucene.document.*;
import org.apache.lucene.geo.Point;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;

public class LuceneIndexerTest {

    @Test
    void rangeTest(@TempDir Path dir) throws IOException {
        try(var indexer = new Lucene("/data/logs/lucene")) {
            var doc = new Document();
            doc.add(new IntRange("range", new int[]{1}, new int[]{20}));
            indexer.writer.addDocument(doc);
            indexer.writer.commit();
            Query query = IntRange.newIntersectsQuery("range", new int[]{10}, new int[]{10});
            TopDocs results = indexer.searcher.search(query, 100);

            System.out.println(results.totalHits);

        }

    }

    @Test
    void testSearch() throws IOException {
        try(var indexer = new Lucene("/data/logs/lucene")) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            //TermQuery termQuery = new TermQuery(new Term("ipAddress",
            Query lowerRangeQuery = TermRangeQuery.newStringRange("ipAddress.min", "31.56.96.51", "31.56.96.51", true, true);
            Query upperRangeQuery = TermRangeQuery.newStringRange("ipAddress.max", "31.56.96.51", "31.56.96.51", true, true);
            builder.add(new BooleanClause(lowerRangeQuery, BooleanClause.Occur.MUST));
            builder.add(new BooleanClause(upperRangeQuery, BooleanClause.Occur.SHOULD));
            BooleanQuery query = builder.build();
            TopDocs results = indexer.searcher.search(query, 100);

            System.out.println(results.totalHits);

        }

    }
}
