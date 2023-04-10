package io.nats.examples.jetstream.simple;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This example will demonstrate simplified fetch
 */
public class ConsumeWithHandler {
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

            Utils.setupStream(jsm, STREAM, SUBJECT);

            String name = "simple-consumer-" + NUID.nextGlobal();

            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(name).build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(STREAM, name);

            long start = System.nanoTime();
            CountDownLatch latch = new CountDownLatch(1);
            AtomicInteger atomicCount = new AtomicInteger();
            MessageHandler handler = msg -> {
                msg.ack();
                int count = atomicCount.incrementAndGet();
                if (count % REPORT_EVERY == 0) {
                    report("Handler", start, count);
                }
                if (count == STOP_COUNT) {
                    latch.countDown();
                }
            };

            // create the consumer then use it
            SimpleConsumer consumer = consumerContext.consume(handler);

            Publisher publisher = new Publisher(js, SUBJECT, JITTER);
            Thread pubThread = new Thread(publisher);
            pubThread.start();

            latch.await();
            Thread.sleep(JITTER * 2); // allows more messages to come across
            publisher.stop();
            pubThread.join();

            System.out.println("Start draining...");
            consumer.drain(Duration.ofSeconds(1));

            Thread.sleep(250); // let consumer get messages post drain
            report("Final", start, atomicCount.get());
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
