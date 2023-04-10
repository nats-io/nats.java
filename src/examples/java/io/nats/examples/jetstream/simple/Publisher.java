package io.nats.examples.jetstream.simple;

import io.nats.client.JetStream;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

class Publisher implements Runnable {
    private final JetStream js;
    private final String subject;
    private final int jitter;
    private final AtomicBoolean keepGoing = new AtomicBoolean(true);
    private int pubNo;

    public Publisher(JetStream js, String subject, int jitter) {
        this.js = js;
        this.subject = subject;
        this.jitter = jitter;
    }

    public void stop() {
        keepGoing.set(false);
    }

    @Override
    public void run() {
        try {
            while (keepGoing.get()) {
                Thread.sleep(ThreadLocalRandom.current().nextLong(jitter));
                js.publish(subject, ("simple-message-" + (++pubNo)).getBytes());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
