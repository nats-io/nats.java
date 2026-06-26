package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamYourFirstConsumerCreate {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");

            // NATS-DOC-START
            // Create a durable pull consumer that delivers every stored message.
            // AckPolicy.Explicit means each message must be acknowledged.
            ConsumerContext cc = sc.createOrUpdateConsumer(
                ConsumerConfiguration.builder()
                    .durable("shipping")
                    .deliverPolicy(DeliverPolicy.All)
                    .ackPolicy(AckPolicy.Explicit)
                    .build());

            System.out.println("Created durable consumer: " + cc.getConsumerName());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
