package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamFilteringFilterMatchesNothing {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");

            // NATS-DOC-START
            // The filter subject has a typo: "orders.shiped" matches no stored
            // subject. The consumer is still created without error.
            ConsumerContext cc = sc.createOrUpdateConsumer(
                ConsumerConfiguration.builder()
                    .durable("analytics-typo")
                    .filterSubject("orders.shiped")
                    .ackPolicy(AckPolicy.Explicit)
                    .build());

            // The fetch blocks until the expiry, then returns nothing. A wrong
            // filter fails silently: no messages, no error.
            try (FetchConsumer fc = cc.fetch(
                    FetchConsumeOptions.builder().maxMessages(5).expiresIn(2000).build())) {
                Message m = fc.nextMessage();
                if (m == null) {
                    System.out.println("Pull returned nothing: filter matched no stored subject.");
                }
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
