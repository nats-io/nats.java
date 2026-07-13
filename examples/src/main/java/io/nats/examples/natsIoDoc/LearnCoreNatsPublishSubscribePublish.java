package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class LearnCoreNatsPublishSubscribePublish {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            nc.createDispatcher(msg ->
                System.out.println("warehouse received: " + new String(msg.getData(), StandardCharsets.UTF_8))
            ).subscribe("orders.created");
            Thread.sleep(200);

            // NATS-DOC-START
            // Publish one order to the orders.created subject. Publishing is
            // fire-and-forget: the call hands the message to the server and returns.
            byte[] order = ("{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\","
                + "\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}").getBytes(StandardCharsets.UTF_8);
            nc.publish("orders.created", order);
            // NATS-DOC-END

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
