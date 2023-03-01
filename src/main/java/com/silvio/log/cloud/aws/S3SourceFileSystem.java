package com.silvio.log.cloud.aws;

import com.silvio.log.config.S3Config;
import lombok.Getter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.inject.Singleton;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

@Getter
@Singleton
public class S3SourceFileSystem {
    private final FileSystem fileSystem;

    public S3SourceFileSystem(@ConfigProperty(name = "s3.source.bucket")  String path, S3Config config) throws URISyntaxException, IOException {
        this.fileSystem = new S3AFileSystem();
        this.fileSystem.initialize(new URI(path), config.getConfiguration());
    }

}
