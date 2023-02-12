package com.silvio.log.processor;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public abstract class InfiniteLogProcessor<T> implements MultiSubscriber<T> {

    @Getter
    private final Multi<T> multi;
    private final String destPath;

    private AtomicReference<ParquetWriter<T>> writer = new AtomicReference<>();

    private final static int maxRows = 100_000;

    private final static int maxTimeout = 180_000;

    private AtomicInteger rows = new AtomicInteger(0);

    public InfiniteLogProcessor(Multi<T> multi, String destPath) {
        this.multi = multi;
        this.destPath = destPath;
    }

    public MultiSubscriber<T> processLog() {
        return multi.subscribe().withSubscriber(this);
    }

    @Override
    public void onItem(T item) {
        try {
            this.writer.get().write(item);
            this.rows.accumulateAndGet(1, Integer::sum);
            if (this.rows.get() >= maxRows) {
                this.writer.get().close();
                this.writer.set(getWriter(getDestPath()));
                this.rows.set(0);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void onFailure(Throwable failure) {
        log.error("Error processing log", failure);
    }

    @Override
    public void onCompletion() {
        try {
            this.writer.get().close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onSubscribe(Subscription subscription) {
        log.info("Subscribed to log stream");
        try {
            writer.set(getWriter(getDestPath()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.rows.set(0);
        subscription.request(Long.MAX_VALUE);
    }

    protected Path getDestPath() {
        return new Path(Paths.get(destPath, UUID.randomUUID().toString()).toUri().toString() + ".parquet");
    }

    protected abstract ParquetWriter<T> getWriter(Path destPath) throws IOException;
}
