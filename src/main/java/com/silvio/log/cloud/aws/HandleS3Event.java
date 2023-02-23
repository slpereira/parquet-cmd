package com.silvio.log.cloud.aws;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.silvio.log.cloud.CloudObjectIdentifier;
import com.silvio.log.cloud.HandleEvent;
import com.silvio.log.cloud.ObjectIdentifier;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;


@Slf4j
public class HandleS3Event implements HandleEvent {

    @Override
    public List<ObjectIdentifier> handleEvent(String jsonEvent) {
        var result = new ArrayList<ObjectIdentifier>();
        S3EventNotification event = S3EventNotification.parseJson(jsonEvent);
        for (S3EventNotification.S3EventNotificationRecord record : event.getRecords()) {
            S3EventNotification.S3Entity s3Entity = record.getS3();
            String bucketName = s3Entity.getBucket().getName();
            String objectKey = s3Entity.getObject().getKey();
            if (s3Entity.getObject().getSizeAsLong() == 0) {
                log.warn("S3 object is empty in bucket " + bucketName + ": " + objectKey);
                continue;
            }
            log.info("S3 object created in bucket " + bucketName + ": " + objectKey);
            result.add(new CloudObjectIdentifier(bucketName, objectKey, "s3a", s3Entity.getObject().geteTag()));
        }
        return result;
    }

}
