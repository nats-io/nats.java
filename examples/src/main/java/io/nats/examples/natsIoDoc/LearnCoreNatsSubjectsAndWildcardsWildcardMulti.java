package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class LearnCoreNatsSubjectsAndWildcardsWildcardMulti {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // NATS-DOC-START
            // Audit service: catch every order message at any depth. The multi-token
            // wildcard > matches one or more tokens and must be the last token, so
            // orders.> matches orders.created, orders.us.created, and
            // orders.us.west.created alike.
            nc.createDispatcher(msg ->
                System.out.println("audit: " + msg.getSubject())
            ).subscribe("orders.>");
            // NATS-DOC-END

            byte[] order = ("{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\","
                + "\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}").getBytes(StandardCharsets.UTF_8);
            Thread.sleep(200);
            nc.publish("orders.created", order);
            nc.publish("orders.shipped", order);
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
