package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class LearnJetStreamAdvancedPublishingAsync {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStream js = nc.jetStream();

            // NATS-DOC-START
            // Async publish: publishAsync returns a CompletableFuture immediately,
            // before the server replies, so the round trips overlap. Collect the
            // futures, then read each ack -- a future that completes exceptionally
            // is a failed publish you must re-send.
            String[] orders = {
                "{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\",\"total_cents\":4200}",
                "{\"order_id\":\"ord_2zr9\",\"customer\":\"globex\",\"total_cents\":7800}",
                "{\"order_id\":\"ord_5t1m\",\"customer\":\"initech\",\"total_cents\":1500}",
                "{\"order_id\":\"ord_9p3x\",\"customer\":\"hooli\",\"total_cents\":9900}"
            };

            List<CompletableFuture<PublishAck>> futures = new ArrayList<>();
            for (String order : orders) {
                futures.add(js.publishAsync("orders.created",
                    order.getBytes(StandardCharsets.UTF_8)));
            }

            for (int i = 0; i < futures.size(); i++) {
                try {
                    PublishAck ack = futures.get(i).get();
                    System.out.printf("order %d stored at sequence %d%n", i + 1, ack.getSeqno());
                } catch (Exception e) {
                    System.out.printf("order %d failed, re-publish it: %s%n", i + 1, e.getMessage());
                }
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
