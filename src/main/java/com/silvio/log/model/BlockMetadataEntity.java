package com.silvio.log.model;

import javax.persistence.*;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;

@Entity
@Table(name = "BLOCK_METADATA")
public class BlockMetadataEntity extends PanacheEntity {

    @Column(name = "BLOCK_OFFSET")
    public Long blockOffset;

    @Column(name = "ROW_COUNT")
    public Long rowCount;

    @Column(name = "COMPRESSED_SIZE")
    public Long compressedSize;

    @Column(name = "UNCOMPRESSED_SIZE")
    public Long uncompressedSize;

    @Column(name = "PARQUET_FILE_ID")
    public String parquetFileId;

}

