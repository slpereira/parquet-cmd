package com.silvio.log.transformer;

import com.silvio.log.model.ApacheAccessLog;
import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.core.buffer.Buffer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

public class ApacheBufferTransform implements Function<Buffer, Multi<ApacheAccessLog>> {

    private AtomicReference<Buffer> intermediateBuffer = new AtomicReference<>(Buffer.buffer());

    private Multi<ApacheAccessLog> parseBuffer(Buffer buffer) {
        if (intermediateBuffer.get().length() > 0) {
            // TODO Review this operation because intermediateBuffer is smaller than buffer
            buffer = intermediateBuffer.get().appendBuffer(buffer);
            intermediateBuffer.set(Buffer.buffer());
        }
        Stream<ApacheAccessLog> apacheAccessLogStream = buffer.toString().lines().map(l -> {
            var r = ApacheAccessLog.parse(l);
            if (r == null) {
                intermediateBuffer.set(Buffer.buffer(l));
            }
            return r;
        }).filter(Objects::nonNull);
        return Multi.createFrom().items(apacheAccessLogStream);
    }

    @Override
    public Multi<ApacheAccessLog> apply(Buffer bufferMulti) {
        return parseBuffer(bufferMulti);
    }
}
