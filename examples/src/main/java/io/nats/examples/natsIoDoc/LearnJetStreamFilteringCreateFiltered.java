package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamFilteringCreateFiltered {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");

            // NATS-DOC-START
            // Create a durable pull consumer that only sees orders.shipped.
            // The filter subject narrows the stream's subjects down to one.
            ConsumerContext cc = sc.createOrUpdateConsumer(
                ConsumerConfiguration.builder()
                    .durable("analytics")
                    .filterSubject("orders.shipped")
                    .ackPolicy(AckPolicy.Explicit)
                    .build());

            System.out.println("Created filtered consumer: " + cc.getConsumerName());

            // Fetch a small batch. Only orders.shipped messages come back;
            // orders.created is filtered out before it reaches this consumer.
            try (FetchConsumer fc = cc.fetch(
                    FetchConsumeOptions.builder().maxMessages(5).expiresIn(2000).build())) {
                Message m;
                while ((m = fc.nextMessage()) != null) {
                    System.out.println("subject=" + m.getSubject());
                    m.ack();
                }
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
