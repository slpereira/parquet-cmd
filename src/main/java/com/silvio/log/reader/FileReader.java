package com.silvio.log.reader;

import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.buffer.Buffer;
import org.apache.hadoop.fs.Path;

public interface FileReader {
    Multi<Buffer> readFile(Path path);
}
