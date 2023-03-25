package com.silvio.log.index;

import java.io.IOException;

import com.silvio.log.model.BlockMetadataEntity;
import com.silvio.log.model.ColumnStatistics;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import javax.enterprise.context.ApplicationScoped;
import javax.transaction.Transactional;

@ApplicationScoped
@Slf4j
public class JdbcIndexer implements Indexer {

    @Override
    public void start() throws IOException {
    }

    @Transactional
    public void index(BlockMetaData blockMetaData, String fileName) throws IOException {
        var blockMetadataEntity = new BlockMetadataEntity();
        blockMetadataEntity.parquetFileId = fileName;
        blockMetadataEntity.rowCount = blockMetaData.getRowCount();
        blockMetadataEntity.blockOffset = blockMetaData.getStartingPos();
        blockMetadataEntity.compressedSize = blockMetaData.getCompressedSize();
        blockMetadataEntity.uncompressedSize = blockMetaData.getTotalByteSize();
        blockMetadataEntity.persist();
        for (var col : blockMetaData.getColumns()) {
            var statistics = new ColumnStatistics();
            statistics.filename = fileName;
            statistics.columnName = col.getPath().toDotString();
            statistics.rowGroup = col.getRowGroupOrdinal();
            statistics.nullCount = col.getStatistics().getNumNulls();
            var columnStatistics = col.getStatistics();
            if (columnStatistics.hasNonNullValue()) {
                switch (col.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32 -> {
                        statistics.minLong = (long) (int) columnStatistics.genericGetMin();
                        statistics.maxLong = (long) (int) columnStatistics.genericGetMax();
                    }
                    case INT64, INT96 -> {
                        statistics.minLong = (long) columnStatistics.genericGetMin();
                        statistics.maxLong = (long) columnStatistics.genericGetMax();
                    }
                    case BINARY, FIXED_LEN_BYTE_ARRAY -> {
                        statistics.minVar = new String(columnStatistics.getMinBytes());
                        statistics.maxVar = new String(columnStatistics.getMaxBytes());
                    }
                    default -> {
                        throw new IllegalStateException("Unexpected value: " + col.getPrimitiveType().getPrimitiveTypeName());
                    }
                }
            }
            statistics.persist();
            log.info("Persisted statistics for column {} in file {} row group {}", statistics.columnName, statistics.filename, statistics.rowGroup);
        }

    }

    @Override
    public void commit() throws IOException {
        ColumnStatistics.getEntityManager().getTransaction().commit();
    }

    public void close() throws IOException {
        ColumnStatistics.getEntityManager().close();
    }
}

