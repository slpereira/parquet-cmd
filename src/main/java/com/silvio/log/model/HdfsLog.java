package com.silvio.log.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by silvio on 2017-03-21.
 *
 */
@Getter
@Setter
@ToString
public class HdfsLog {

    static final String regex = "([0-9]+)\\s([0-9]+)\\s([0-9]+)\\s(TRACE|DEBUG|INFO|NOTICE|WARN|WARNING|ERROR|SEVERE|FATAL)\\s(([\\p{L}_$][\\p{L}\\p{N}_$]*\\.)*[\\p{L}_$][\\p{L}\\p{N}_$]*):\\s(.*)$";
    static final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);;
    // Match regex against input

    private String date;

    private String time;

    private String processId;

    private String logLevel;

    private String logSource;

    private String logMessage;

    public static HdfsLog parse(String logLine) {
        var matcher = pattern.matcher(logLine);
        if (matcher.find()) {
            var hdfsLog = new HdfsLog();
            hdfsLog.setDate(matcher.group(1));
            hdfsLog.setTime(matcher.group(2));
            hdfsLog.setProcessId(matcher.group(3));
            hdfsLog.setLogLevel(matcher.group(4));
            hdfsLog.setLogSource(matcher.group(5));
            hdfsLog.setLogMessage(matcher.group(7));
            return hdfsLog;
        } else {
            return null;
        }

    }

    // parquet schema
    // message HdfsLog {
    //   required binary date (UTF8);
    //   required binary time (UTF8);
    //   required binary processId (UTF8);
    //   required binary logLevel (UTF8);
    //   required binary logSource (UTF8);
    //   required binary logMessage (UTF8);
    //  }
    public static MessageType getSchema() {
        return new MessageType("HdfsLog",
                new org.apache.parquet.schema.PrimitiveType(org.apache.parquet.schema.PrimitiveType.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "date"),
                new org.apache.parquet.schema.PrimitiveType(org.apache.parquet.schema.PrimitiveType.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,"time"),
                new org.apache.parquet.schema.PrimitiveType(org.apache.parquet.schema.PrimitiveType.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,"processId"),
                new org.apache.parquet.schema.PrimitiveType(org.apache.parquet.schema.PrimitiveType.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,"logLevel"),
                new org.apache.parquet.schema.PrimitiveType(org.apache.parquet.schema.PrimitiveType.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,"logSource"),
                new org.apache.parquet.schema.PrimitiveType(org.apache.parquet.schema.PrimitiveType.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,"logMessage")
        );
    }

}
