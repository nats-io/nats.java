package io.nats.examples.jetstream.simple;

import io.nats.client.*;

import java.io.IOException;

import static io.nats.examples.jetstream.simple.SimpleUtils.*;

/**
 * This example will demonstrate simplified consuming using
 * - fetch (non blocking)
 * - custom ConsumeOptions
 * - pre-existing durable consumer
 */
public class Fetch_CustomOptions_Durable {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {

            setupStreamAndData(nc);

            // different batch sizes demonstrate expiration behavior
//            demo(nc, SIMPLE_COUNT / 2);
//            demo(nc, SIMPLE_COUNT);
            demo(nc, SIMPLE_COUNT * 2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void demo(Connection nc, int batch) throws IOException, JetStreamApiException, InterruptedException {
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();

        String name = CONSUMER + batch;

        // Pre define a consumer
        setupConsumer(jsm, STREAM, name);

        // Consumer[Context]
        ConsumerContext consumerContext = js.getConsumerContext(STREAM, name);

        // We want custom consume options
        ConsumeOptions co = ConsumeOptions.builder().expiresIn(3000).build();

        // create and use the iterator
        if (batch > SIMPLE_COUNT) {
            System.out.println("\n=== Batch (" + batch + ") is larger than available messages (" + SIMPLE_COUNT + ")");
            System.out.println("=== Custom ConsumeOption \"expires in\" is 3 seconds.");
            System.out.println("=== nextMessage() blocks until expiration when there are no messages.");
            System.out.println("=== null indicates consume is done");
        }
        else {
            System.out.println("\n=== Batch (" + batch + ") is less than or equal to available messages (" + SIMPLE_COUNT + ")");
            System.out.println("=== nextMessage() will return null when consume is done");
        }

        long start = System.currentTimeMillis();

        // create the consumer then use it
        FetchConsumer consumer = consumerContext.fetch(co);
        int rec = 0;
        Message msg = consumer.nextMessage();
        while (msg != null) {
            ++rec;
            msg.ack();
            msg = consumer.nextMessage();
        }
        long elapsed = System.currentTimeMillis() - start;

        System.out.println("### " + rec + " message(s) were received in " + elapsed + " ms.");
    }

    public static void mainx(String[] args) {
        try (Connection nc = Nats.connect()) {

            setupStreamAndDataAndConsumer(nc);

            // JetStream context
            JetStream js = nc.jetStream();

            // Consumer[Context]
            ConsumerContext consumerContext = js.getConsumerContext(STREAM, CONSUMER);

            // We want custom consume options
            ConsumeOptions co = ConsumeOptions.builder()
                .expiresIn(3000)
                .build();

            // different batch size demonstrates expiration behavior
            // int batch = COUNT;
            // int batch = COUNT - 1;
            int batch = SIMPLE_COUNT + 1;

            // create and use the iterator
            FetchConsumer consumer = consumerContext.fetch(co);

            int rec = 0;
            Message msg = consumer.nextMessage();
            while (msg != null) {
                msg.ack();
                if (++rec == SIMPLE_COUNT) {
                    if (batch > SIMPLE_COUNT) {
                        System.out.println("\n*** Batch is larger than available messages.");
                        System.out.println("*** Custom ConsumeOption \"expires in\" is 3 seconds.");
                        System.out.println("*** Wait for it to expire...");
                    }
                }
                msg = consumer.nextMessage();
            }
            System.out.println("\n" + rec + " message(s) were received.\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
