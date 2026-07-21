package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamPublishingDedup {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStream js = nc.jetStream();

            byte[] body = "{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\",\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}".getBytes(StandardCharsets.UTF_8);

            // NATS-DOC-START
            // Tag the message with a stable id so a retry is deduplicated
            PublishOptions options = PublishOptions.builder()
                .messageId("ord_8w2k-created")
                .build();

            // First publish stores the message
            PublishAck first = js.publish("orders.created", body, options);
            System.out.printf("First:  seq=%d duplicate=%b%n", first.getSeqno(), first.isDuplicate());

            // Re-publishing the same id returns the original sequence, marked duplicate
            PublishAck second = js.publish("orders.created", body, options);
            System.out.printf("Second: seq=%d duplicate=%b%n", second.getSeqno(), second.isDuplicate());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
