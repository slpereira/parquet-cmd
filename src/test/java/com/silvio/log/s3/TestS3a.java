package com.silvio.log.s3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestS3a {

    @Test
    void tests3a() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", "http://localhost:9000");
        conf.set("fs.s3a.access.key", "WYgfpf1KJfbFFF3M");
        conf.set("fs.s3a.secret.key", "YXLtZX8RMz8sltZq9QxxsoB4sxoFX9ga");
        conf.set("fs.s3a.path.style.access", "true");

        // Create an s3a FileSystem instance using the configuration
        FileSystem fs = FileSystem.get(new URI("s3a://logs"), conf);

        // Use the FileSystem instance to interact with your Minio instance
        // For example, you can list the contents of a bucket like this:
        Path bucketPath = new Path("/access.log");
        for (FileStatus p : fs.listStatus(bucketPath)) {
            System.out.println(p);
        }

        // Make sure to close the FileSystem instance when you're done with it
        fs.close();
    }
}
