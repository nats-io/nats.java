package io.nats.examples.jetstream.simple;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This example will demonstrate simplified fetch
 */
public class SimpleConsume {
    private static final String STREAM = "simple-stream";
    private static final String SUBJECT = "simple-subject";
    private static final int STOP_COUNT = 500;
    private static final int REPORT_EVERY = 50;
    private static final int JITTER = 20;

    // change this is you need to...
    public static String SERVER = "nats://localhost:4222";

    public static void main(String[] args) {
        Options options = Options.builder().server(SERVER).build();
        try (Connection nc = Nats.connect(options)) {

            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            SimpleUtils.setupStream(jsm, STREAM, SUBJECT);

            String name = "simple-consumer-" + NUID.nextGlobal();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(name).build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(STREAM, name);

            // create the consumer then use it
            EndlessConsumer consumer = consumerContext.consume();

            long start = System.nanoTime();
            Thread consumeThread = new Thread(() -> {
                int count = 0;
                try {
                    System.out.println("Starting main loop.");
                    while (count < STOP_COUNT) {
                        Message msg = consumer.nextMessage(1000);
                        if (msg != null) {
                            msg.ack();
                            if (++count % REPORT_EVERY == 0) {
                                report("Main Loop Running", start, count);
                            }
                        }
                    }
                    report("Main Loop Stopped", start, count);

                    System.out.println("Pausing for effect...let some more messages come in.");
                    Thread.sleep(JITTER * 3 / 2); // so at least one more message comes across
                    consumer.drain(Duration.ofSeconds(1));

                    System.out.println("Starting post-drain loop.");
                    Message msg = consumer.nextMessage(1000);
                    while (msg != null) {
                        msg.ack();
                        report("Post Drain Loop Running", start, ++count);
                        msg = consumer.nextMessage(1000);
                    }
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                report("Done", start, count);
            });
            consumeThread.start();

            AtomicInteger pubNo = new AtomicInteger();
            AtomicBoolean pubGo = new AtomicBoolean(true);
            Thread pubThread = new Thread(() -> {
                try {
                    while (pubGo.get()) {
                        Thread.sleep(ThreadLocalRandom.current().nextLong(JITTER));
                        js.publish(SUBJECT, ("simple-message-" + pubNo.incrementAndGet()).getBytes());
                    }
                }
                catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            pubThread.start();

            consumeThread.join();
            pubGo.set(false);
            pubThread.join();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void report(String label, long start, int count) {
        long ms = (System.nanoTime() - start) / 1_000_000;
        System.out.println(label + ": Received " + count + " messages in " + ms + "ms.");
    }
}
