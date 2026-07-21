package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamWorkerPoolRedeliveryCount {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("shipping");

            // NATS-DOC-START
            // Each message reports how many times it has been delivered. A count
            // above one means a redelivery: the server handed this order out
            // before, but a worker crashed or ran past AckWait before acking.
            // Key your side effects by order_id so handling the same order twice
            // is harmless.
            try (FetchConsumer fc = cc.fetch(
                    FetchConsumeOptions.builder().maxMessages(10).build())) {
                Message m;
                while ((m = fc.nextMessage()) != null) {
                    long delivered = m.metaData().deliveredCount();
                    String data = new String(m.getData(), StandardCharsets.UTF_8);
                    if (delivered > 1) {
                        System.out.printf("redelivery #%d of %s%n", delivered, data);
                    } else {
                        System.out.printf("first delivery of %s%n", data);
                    }
                    // Acknowledge so the server advances the consumer.
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
