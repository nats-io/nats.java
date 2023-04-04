package io.nats.examples.jetstream.simple;

import io.nats.client.*;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.examples.jetstream.simple.SimpleUtils.*;

/**
 * This example will demonstrate simplified consuming using
 * - consume (manual)
 * - custom ConsumeOptions
 * - pre-existing durable consumer
 */
public class Endless_Passive_Consume_CustomOptions_Durable {

    static final int BATCH_SIZE = 1000;
    static final int REPULL_PCT = 50;

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {

            setupStream(nc.jetStreamManagement(), STREAM, SUBJECT);
            setupConsumer(nc.jetStreamManagement(), STREAM, CONSUMER);

            // JetStream context
            JetStream js = nc.jetStream();

            // Start publishing
            Thread pubThread = startPublish(js);

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(STREAM, CONSUMER);

            // We want custom consume options
            ConsumeOptions co = ConsumeOptions.builder()
                .expiresIn(3000)
                .batchSize(BATCH_SIZE)
                .thresholdPercent(REPULL_PCT)
                .build();

            final AtomicInteger count = new AtomicInteger();
            final AtomicLong mark = new AtomicLong();

            MessageHandler handler = msg -> {
                // Handler gets messages does work
                long elapsed = System.currentTimeMillis() - mark.get();
                if (count.get() == 0) {
                    System.out.println("--- First Message");
                    count.set(1);
                }
                else if (elapsed > 100) {
                    System.out.println("+++ " + count + " messages over " + elapsed + "ms");
                    count.set(1);
                }
                else {
                    count.incrementAndGet();
                }

                // handler acks when work is done
                msg.ack();

                // reset the mark
                mark.set(System.currentTimeMillis());
            };

            // Create the consumer. It just needs to be alive in a thread somewhere.
            MessageConsumer consumer = consumerContext.consume(handler, co);

            pubThread.join();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Thread startPublish(JetStream js) {
        Thread t = new Thread(() -> {
            while (true) {
                int count = ThreadLocalRandom.current().nextInt(BATCH_SIZE) + (BATCH_SIZE/2);
                String text = new NUID().next();
                for (int x = 1; x <= count; x++) {
                    try {
                        js.publish(SUBJECT, ("simple-message-" + text + "-" + x).getBytes());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(1000));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.start();
        return t;
    }
}
