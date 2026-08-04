package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class LearnCoreNatsRequestReplyReplies {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // Two inventory instances that both answer.
            for (int i = 0; i < 2; i++) {
                nc.createDispatcher(msg -> {
                    if (msg.getReplyTo() != null) {
                        nc.publish(msg.getReplyTo(),
                            "{\"in_stock\":true,\"warehouse\":\"us-east\"}".getBytes(StandardCharsets.UTF_8));
                    }
                }).subscribe("orders.inventory.check");
            }
            Thread.sleep(200);

            // NATS-DOC-START
            // Gather more than one reply to a single request. A plain request returns
            // only the first reply, so when several services may answer, subscribe to
            // your own inbox, publish the request with that inbox as the reply subject,
            // and collect replies until they stop arriving.
            byte[] order = ("{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\","
                + "\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}").getBytes(StandardCharsets.UTF_8);
            String inbox = nc.createInbox();
            Subscription sub = nc.subscribe(inbox);
            nc.publish("orders.inventory.check", inbox, order);

            List<String> replies = new ArrayList<>();
            Message m = sub.nextMessage(Duration.ofMillis(300));
            while (m != null) {
                replies.add(new String(m.getData(), StandardCharsets.UTF_8));
                m = sub.nextMessage(Duration.ofMillis(300));
            }
            System.out.println("gathered " + replies.size() + " replies");
            // NATS-DOC-END
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
}
