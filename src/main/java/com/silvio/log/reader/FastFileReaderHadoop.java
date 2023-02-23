package com.silvio.log.reader;

import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.buffer.Buffer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;

public class FastFileReaderHadoop implements FileReader {
    private final FileSystem fs;

    public FastFileReaderHadoop(FileSystem fs) {
        this.fs = fs;
    }

    @Override
    public Multi<Buffer> readFile(Path path)  {
        // Use the Buffer.newInstance() method to create new buffers
        // as we read data from the input stream
        return Multi.createFrom().emitter(emitter -> {
            byte[] tmp = new byte[16384];
            try(var in = fs.open(path)) {
                var read = 0;
                while ((read = in.read(tmp)) != -1) {
                    Buffer buffer = read == tmp.length ? Buffer.buffer(tmp): Buffer.buffer(Arrays.copyOf(tmp, read));
                    emitter.emit(buffer);
                }
                emitter.complete();
            } catch (Throwable t) {
                emitter.fail(t);
            }
        });
    }

}
