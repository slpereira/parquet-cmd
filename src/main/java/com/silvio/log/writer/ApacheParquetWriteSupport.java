package com.silvio.log.writer;

import com.silvio.log.model.ApacheAccessLog;
import org.apache.parquet.schema.MessageType;

public class ApacheParquetWriteSupport extends BasicParquetWriteSupport<ApacheAccessLog> {

    public ApacheParquetWriteSupport(MessageType schema) {
        super(schema);
    }

    @Override
    public void write(ApacheAccessLog values) {
        recordConsumer.startMessage();
        writeField(values.getIpAddress(), 0);
        writeField(values.getUserId(), 1);
        writeField(values.getDateTimeString(), 2);
        writeField(values.getRequest(), 3);
        writeField(values.getResponse(), 4);
        writeField(values.getBytesSent(), 5);
        writeField(values.getReferer(), 6);
        writeField(values.getUserAgent(), 7);
        recordConsumer.endMessage();
    }

}
