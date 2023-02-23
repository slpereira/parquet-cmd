package com.silvio.log.writer;

import com.silvio.log.model.HdfsLog;
import org.apache.parquet.schema.MessageType;

public class HdfsParquetWriteSupport extends BasicParquetWriteSupport<HdfsLog> {

    // TODO: support specifying encodings and compression
    HdfsParquetWriteSupport(MessageType schema) {
        super(schema);
    }

    @Override
    public void write(HdfsLog values) {
        recordConsumer.startMessage();
        writeField(values.getDate(), 0);
        writeField(values.getTime(), 1);
        writeField(values.getProcessId(), 2);
        writeField(values.getLogLevel(), 3);
        writeField(values.getLogSource(), 4);
        writeField(values.getLogMessage(), 5);
        recordConsumer.endMessage();
    }

}
