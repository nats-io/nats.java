package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamPublishingPubAck {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStream js = nc.jetStream();

            // NATS-DOC-START
            // Publish one order and print every field of the ack
            PublishAck ack = js.publish("orders.created",
                "{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\",\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}".getBytes(StandardCharsets.UTF_8));

            System.out.println("Stream:    " + ack.getStream());
            System.out.println("Sequence:  " + ack.getSeqno());
            System.out.println("Duplicate: " + ack.isDuplicate());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
