package com.silvio.log.reader;


import io.smallrye.mutiny.Multi;
import io.vertx.core.file.OpenOptions;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.core.file.AsyncFile;
import org.apache.hadoop.fs.Path;


/**
 * Read a file using Vert.x
 */

public class FastFileReaderVertx implements FileReader {
    public FastFileReaderVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    private final Vertx vertx;

    @Override
    public Multi<Buffer> readFile(Path path) {
        return vertx.fileSystem()
                .open(path.toString(), new OpenOptions().setRead(true).setCreate(false))
                .onItem().transformToMulti(AsyncFile::toMulti).onCompletion().invoke(() -> System.out.println("File read"));
    }

}
