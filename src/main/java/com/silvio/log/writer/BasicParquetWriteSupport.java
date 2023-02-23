package com.silvio.log.writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.List;

public abstract class BasicParquetWriteSupport<T> extends WriteSupport<T> {
    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;

    public BasicParquetWriteSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    protected void writeField(String val, int i) {
        if (val.length() > 0) {
            recordConsumer.startField(cols.get(i).getPath()[0], i);
            switch (cols.get(i).getType()) {
                case BOOLEAN:
                    recordConsumer.addBoolean(Boolean.parseBoolean(val));
                    break;
                case FLOAT:
                    recordConsumer.addFloat(Float.parseFloat(val));
                    break;
                case DOUBLE:
                    recordConsumer.addDouble(Double.parseDouble(val));
                    break;
                case INT32:
                    recordConsumer.addInteger(Integer.parseInt(val));
                    break;
                case INT64:
                    recordConsumer.addLong(Long.parseLong(val));
                    break;
                case BINARY:
                    recordConsumer.addBinary(stringToBinary(val));
                    break;
                default:
                    throw new ParquetEncodingException(
                            "Unsupported column type: " + cols.get(i).getType());
            }
            recordConsumer.endField(cols.get(i).getPath()[0], i);
        }
    }

    private Binary stringToBinary(Object value) {
        return Binary.fromString(value.toString());
    }
}
