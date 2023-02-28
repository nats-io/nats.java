package io.nats.examples.jetstream.simple;

import io.nats.client.*;

import java.io.IOException;
import java.util.List;

import static io.nats.examples.jetstream.simple.SimpleUtils.*;

/**
 * This example will demonstrate simplified consuming using
 * - fetch all (blocking until timeout or entire batch received)
 * - custom ConsumeOptions
 * - pre-existing durable consumer
 */
public class Simple_FetchAll_CustomOptions_Durable {

    public static void main(String[] args) {
        try (Connection nc = Nats.connect()) {

            setupStreamAndData(nc);

            // different batch sizes demonstrate expiration behavior
            fetchAll(nc, COUNT / 2);
            fetchAll(nc, COUNT);
            fetchAll(nc, COUNT * 2);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void fetchAll(Connection nc, int batch) throws IOException, JetStreamApiException {
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
        if (batch > COUNT) {
            System.out.println("\n=== Batch (" + batch + ") is larger than available messages (" + COUNT + ")");
            System.out.println("=== Custom ConsumeOption \"expires in\" is 3 seconds.");
            System.out.println("=== fetchAll will block until expiration...");
        }
        else {
            System.out.println("\n=== Batch (" + batch + ") is less than or equal to available messages (" + COUNT + ")");
            System.out.println("=== fetchAll will return quickly...");
        }

        long start = System.currentTimeMillis();
        List<Message> messages = consumerContext.fetchAll(batch, co);
        long elapsed = System.currentTimeMillis() - start;
        for (Message msg : messages) {
            msg.ack();
        }
        System.out.println("### " + messages.size() + " message(s) were received in " + elapsed + " ms.");
    }
}
