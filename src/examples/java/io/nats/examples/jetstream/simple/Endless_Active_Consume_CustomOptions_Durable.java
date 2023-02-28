package io.nats.examples.jetstream.simple;

import io.nats.client.*;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static io.nats.examples.jetstream.simple.SimpleUtils.*;

/**
 * This example will demonstrate simplified consuming using
 * - consume (active calling of nextMessage(...))
 * - custom ConsumeOptions
 * - pre-existing durable consumer
 */
public class Endless_Active_Consume_CustomOptions_Durable {

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
                .expiresIn(Duration.ofSeconds(3))
                .batchSize(BATCH_SIZE)
                .repullPercent(REPULL_PCT)
                .build();

            // create and use the iterator
            EndlessConsumer consumer = consumerContext.consume(co);

            boolean state = true;
            int count = 0;
            long mark = 0;
            while (true) {
                Message msg = consumer.nextMessage(500);
                if (msg == null) {
                    if (!state) {
                        long elapsed = System.currentTimeMillis() - mark;
                        System.out.print("+++ " + count + " messages in " + elapsed + "ms");
                        state = true;
                        mark = System.currentTimeMillis();
                        count = 0;
                    }
                }
                else {
                    if (state) {
                        long elapsed = System.currentTimeMillis() - mark;
                        if (mark == 0) {
                            System.out.println("--- Receiving");
                        }
                        else {
                            System.out.println(" ... " + elapsed + "ms without messages");
                        }
                        state = false;
                        mark = System.currentTimeMillis();
                    }
                    ++count;
                    msg.ack();
                }
            }
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
