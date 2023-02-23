package com.silvio.log.model;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ApacheAccessLog {
    // Apache access log format
    // LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
    final static String regex = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"(.+?)\"";

    final static Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);

    private static Logger logger = org.slf4j.LoggerFactory.getLogger(ApacheAccessLog.class.getName());

    private String ipAddress;
    private String userId;
    private String dateTimeString;
    private String request;
    private String response;
    private String bytesSent;
    private String referer;

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getDateTimeString() {
        return dateTimeString;
    }

    public void setDateTimeString(String dateTimeString) {
        this.dateTimeString = dateTimeString;
    }

    public String getRequest() {
        return request;
    }

    public void setRequest(String request) {
        this.request = request;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    public String getBytesSent() {
        return bytesSent;
    }

    public void setBytesSent(String bytesSent) {
        this.bytesSent = bytesSent;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    private String userAgent;

    public static ApacheAccessLog parse(String logLine) {
        Matcher matcher = pattern.matcher(logLine);
        if (matcher.find()) {
            ApacheAccessLog accessLog = new ApacheAccessLog();
            accessLog.setIpAddress(matcher.group(1));
            accessLog.setUserId(matcher.group(3));
            accessLog.setDateTimeString(matcher.group(4));
            accessLog.setRequest(matcher.group(5));
            accessLog.setResponse(matcher.group(6));
            accessLog.setBytesSent(matcher.group(7));
            accessLog.setReferer(matcher.group(8));
            accessLog.setUserAgent(matcher.group(9));
            return accessLog;
        } else {
            logger.debug("Cannot parse log line: " + logLine);
            return null;
        }
    }

    @Override
    public String toString() {
        return "ApacheAccessLog{" +
                "ipAddress='" + ipAddress + '\'' + ", " +
                "userId='" + userId + '\'' + ", " +
                "dateTimeString='" + dateTimeString + '\'' + ", " +
                "request='" + request + '\'' + ", " +
                "response='" + response + '\'' + ", " +
                "bytesSent='" + bytesSent + '\'' + ", " +
                "referer='" + referer + '\'' + ", " +
                "userAgent='" + userAgent + '\'' +
                '}';
    }

    // parquet schema
    // message ApacheAccessLog {
    //   required binary ipAddress (UTF8);
    //   required binary userId (UTF8);
    //   required binary dateTimeString (UTF8);
    //   required binary request (UTF8);
    //   required binary response (UTF8);
    //   required binary bytesSent (UTF8);
    //   required binary referer (UTF8);
    //   required binary userAgent (UTF8);
    // }
    public static MessageType getSchema() {
        return new MessageType("ApacheAccessLog",
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY,"ipAddress"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "userId"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "dateTimeString"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "request"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, "response"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, "bytesSent"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "referer"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, "userAgent")
        );

    }

}
