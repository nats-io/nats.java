package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamWorkerPoolMaxPending {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");

            // NATS-DOC-START
            // Raise MaxAckPending so a larger pool can hold more orders in
            // progress at once. The cap is shared across the whole "shipping"
            // consumer, not per worker, so size it to at least your worker count.
            // createOrUpdateConsumer updates the existing consumer in place.
            sc.createOrUpdateConsumer(
                ConsumerConfiguration.builder()
                    .durable("shipping")
                    .deliverPolicy(DeliverPolicy.All)
                    .ackPolicy(AckPolicy.Explicit)
                    .maxAckPending(5000)
                    .build());

            System.out.println("shipping MaxAckPending set to 5000");
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
