package com.silvio.log.mutiny;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

public class FileTest {

    @Test
    public void testFile() {
        var m = Multi.createFrom().range(1, 1000);
        m.subscribe().with(i -> System.out.println(i));
    }

    @Test
    public void testFileSub() {
        var m = Multi.createFrom().range(1, 12);
        //m.onSubscription().invoke(s -> s.request(10));
        m.subscribe().withSubscriber(new MySubscriber());
        //m.subscribe().withSubscriber(new MySubscriber());
    }

    public static class MySubscriber implements MultiSubscriber<Integer> {

        //private Subscription subscription;

        @Override
        public void onSubscribe(Subscription s) {
            //this.subscription = s;
            s.request(10);
        }

        @Override
        public void onItem(Integer item) {
            System.out.println(item);
        }

        @Override
        public void onFailure(Throwable failure) {
            System.err.println(failure.getMessage());
        }

        @Override
        public void onCompletion() {
            System.out.println("Done");
            //this.subscription.request(10);
        }

    }
}
