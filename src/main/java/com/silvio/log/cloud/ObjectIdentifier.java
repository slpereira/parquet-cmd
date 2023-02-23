package com.silvio.log.cloud;

public interface ObjectIdentifier {

    String getPrefix();

    String getBucketName();

    String getObjectKey();

    String getETag();

    String toURI();
}
