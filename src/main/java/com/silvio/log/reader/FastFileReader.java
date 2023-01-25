package com.silvio.log.reader;


import com.silvio.log.transformer.ApacheBufferTransform;
import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.core.file.OpenOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.file.AsyncFile;

import javax.enterprise.context.ApplicationScoped;
import java.nio.file.Path;

/**
 * Read a file using Vert.x
 */

@ApplicationScoped
public class FastFileReader {
    public FastFileReader(Vertx vertx) {
        this.vertx = vertx;
    }

    private final Vertx vertx;

    public Multi<Buffer> readFile(Path path) {
        return vertx.fileSystem()
                .open(path.toString(), new OpenOptions().setRead(true).setCreate(false))
                .onItem().transformToMulti(AsyncFile::toMulti);
    }
}
