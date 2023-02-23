package com.silvio.log.config;

import io.quarkus.runtime.annotations.ConfigDocMapKey;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import io.smallrye.config.WithName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


@ConfigMapping(prefix = "fs.s3a")
public interface S3Config {

    String endpoint();

    @WithName("access.key")
    String accessKey();

    @WithName("secret.key")
    String secretKey();

    @WithDefault("true")
    String pathStyleAccess();

    default Configuration getConfiguration() {
        var conf = new Configuration();
        conf.set("fs.s3a.endpoint", endpoint());
        conf.set("fs.s3a.access.key", accessKey());
        conf.set("fs.s3a.secret.key", secretKey());
        conf.set("fs.s3a.path.style.access", pathStyleAccess());
        return conf;
    }

}
