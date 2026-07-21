package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class LearnCoreNatsSubjectsAndWildcardsWildcardSingle {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // NATS-DOC-START
            // Regional analytics: one subscription catches created orders from every
            // region. The single-token wildcard * matches exactly one token, so both
            // orders.us.created and orders.eu.created match, while orders.created and
            // orders.us.west.created do not.
            nc.createDispatcher(msg ->
                System.out.println("analytics: new order on " + msg.getSubject())
            ).subscribe("orders.*.created");
            // NATS-DOC-END

            byte[] order = ("{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\","
                + "\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}").getBytes(StandardCharsets.UTF_8);
            Thread.sleep(200);
            nc.publish("orders.us.created", order);
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
