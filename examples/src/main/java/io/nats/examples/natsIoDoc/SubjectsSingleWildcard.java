package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.io.IOException;

public class SubjectsSingleWildcard {
    // NATS-DOC-START
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("demo.nats.io:4222")) {

            // NATS-DOC-START
            // Subscribe to the shipped orders
            Dispatcher dShipped = nc.createDispatcher(msg -> {
                System.out.printf("[orders.*.shipped]  %s  (%s)\n", new String(msg.getData()),  msg.getSubject());
            });
            dShipped.subscribe("orders.*.shipped");

            Dispatcher dPlaced = nc.createDispatcher(msg -> {
                System.out.printf("[orders.*.placed]   %s  (%s)\n", new String(msg.getData()),  msg.getSubject());
            });
            dPlaced.subscribe("orders.*.placed");

            // Subscribe to the retail orders
            Dispatcher dRetail = nc.createDispatcher(msg -> {
                System.out.printf("[orders.retail.*]   %s  (%s)\n", new String(msg.getData()),  msg.getSubject());
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