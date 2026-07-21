package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class LearnCoreNatsQueueGroupsQueueSubscribe {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // NATS-DOC-START
            // Join the "packers" queue group on orders.created. Every subscriber that
            // names the same group shares the load: each order is delivered to exactly
            // one member. Run this in several processes to watch the load balance.
            nc.createDispatcher(msg ->
                System.out.println("packer handling: " + new String(msg.getData(), StandardCharsets.UTF_8))
            ).subscribe("orders.created", "packers");
            // NATS-DOC-END

            byte[] order = ("{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\","
                + "\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}").getBytes(StandardCharsets.UTF_8);
            Thread.sleep(200);
            nc.publish("orders.created", order);
            Thread.sleep(300);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
}
