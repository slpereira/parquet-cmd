package com.silvio.log.cloud;

public interface SourceFileSystem {
    org.apache.hadoop.fs.FileSystem getFileSystem();
}
