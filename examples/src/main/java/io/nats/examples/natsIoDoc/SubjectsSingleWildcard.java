package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.io.IOException;

public class SubjectsSingleWildcard {
    // NATS-DOC-START
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Subscribe to the shipped orders
            Dispatcher dShipped = nc.createDispatcher(msg -> {
                String[] subjectSegments = msg.getSubject().split("\\.");
                System.out.println("[orders.*.shipped] " + new String(msg.getData()) + ": " + subjectSegments[1] + "," + subjectSegments[2]);
            });
            dShipped.subscribe("orders.*.shipped");

            Dispatcher dPlaced = nc.createDispatcher(msg -> {
                String[] subjectSegments = msg.getSubject().split("\\.");
                System.out.println("[orders.*.placed]  " + new String(msg.getData()) + ": " + subjectSegments[1] + "," + subjectSegments[2]);
            });
            dPlaced.subscribe("orders.*.placed");

            // Subscribe to the retail orders
            Dispatcher dRetail = nc.createDispatcher(msg -> {
                String[] subjectSegments = msg.getSubject().split("\\.");
                System.out.println("[orders.retail.*]  " + new String(msg.getData()) + ": " + subjectSegments[1] + "," + subjectSegments[2]);
            });
            dRetail.subscribe("orders.retail.*");

            // Publish messages to the various subjects
            nc.publish("orders.wholesale.placed", "Order W73737".getBytes());
            nc.publish("orders.retail.placed", "Order R65432".getBytes());
            nc.publish("orders.wholesale.shipped", "Order W73001".getBytes());
            nc.publish("orders.retail.shipped", "Order R65321".getBytes());
            // NATS-DOC-END
            Thread.sleep(500);
        }
        catch (InterruptedException e) {
            // can be thrown by connect
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
    // NATS-DOC-END
}