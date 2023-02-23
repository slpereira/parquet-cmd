package com.silvio.log.cloud;

public class CloudObjectIdentifier implements ObjectIdentifier {

    private final String bucketName;

    private final String objectKey;

    private final String prefix;

    private final String eTag;

    public CloudObjectIdentifier(String bucketName, String objectKey, String prefix, String eTag) {
        this.bucketName = bucketName;
        this.objectKey = objectKey;
        this.prefix = prefix;
        this.eTag = eTag;
    }

    @Override
    public String getPrefix() {
        return prefix;
    }

    @Override
    public String getBucketName() {
        return bucketName;
    }

    @Override
    public String getObjectKey() {
        return objectKey;
    }

    @Override
    public String getETag() {
        return eTag;
    }

    @Override
    public String toURI() {
        return new StringBuilder()
                .append(prefix)
                .append("://")
                .append(bucketName)
                .append("/")
                .append(objectKey)
                .toString();
    }
}
