package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamPublishingSync {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStream js = nc.jetStream();

            // NATS-DOC-START
            // Publish three orders to the "ORDERS" stream and read each ack
            PublishAck ack1 = js.publish("orders.created",
                "{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\",\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}".getBytes(StandardCharsets.UTF_8));
            System.out.printf("Stored in %s at sequence %d%n", ack1.getStream(), ack1.getSeqno());

            PublishAck ack2 = js.publish("orders.created",
                "{\"order_id\":\"ord_2zr9\",\"customer\":\"globex\",\"total_cents\":7800,\"ts\":\"2026-05-22T10:14:25Z\"}".getBytes(StandardCharsets.UTF_8));
            System.out.printf("Stored in %s at sequence %d%n", ack2.getStream(), ack2.getSeqno());

            PublishAck ack3 = js.publish("orders.shipped",
                "{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\",\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:31Z\"}".getBytes(StandardCharsets.UTF_8));
            System.out.printf("Stored in %s at sequence %d%n", ack3.getStream(), ack3.getSeqno());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
