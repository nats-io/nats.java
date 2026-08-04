package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamPublishingConfirmStored {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStream js = nc.jetStream();

            // NATS-DOC-START
            // Publish one order; the returned ack confirms it was stored
            PublishAck ack = js.publish("orders.created",
                "{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\",\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}".getBytes(StandardCharsets.UTF_8));

            // A non-null ack means the server persisted the message
            System.out.printf("Confirmed stored in %s at sequence %d%n", ack.getStream(), ack.getSeqno());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
