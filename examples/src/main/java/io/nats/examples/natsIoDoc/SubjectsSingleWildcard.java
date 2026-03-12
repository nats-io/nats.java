package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class SubjectsSingleWildcard {
    // NATS-DOC-START
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Asynchronous subscribers require a dispatcher

            // Subscribe to the placed orders
            CountDownLatch latchPlaced = new CountDownLatch(2);
            Dispatcher dPlaced = nc.createDispatcher(msg -> {
                latchPlaced.countDown();
                String[] split = msg.getSubject().split("\\.");
                System.out.println("Placed | " + split[1] + " | " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });
            dPlaced.subscribe("orders.*.placed");

            // Subscribe to the shipped orders
            CountDownLatch latchShipped = new CountDownLatch(2);
            Dispatcher dShipped = nc.createDispatcher(msg -> {
                latchShipped.countDown();
                String[] split = msg.getSubject().split("\\.");
                System.out.println("Shipped | " + split[1] + " | " +
                        new String(msg.getData(), StandardCharsets.UTF_8));
            });
            dShipped.subscribe("orders.*.shipped");

            // Publish a message to the various subjects
            byte[] data = "Order W7373".getBytes(StandardCharsets.UTF_8);
            nc.publish("orders.Wholesale.placed", data);

            data = "Order R65432".getBytes(StandardCharsets.UTF_8);
            nc.publish("orders.Retail.placed", data);

            data = "Order W7300".getBytes(StandardCharsets.UTF_8);
            nc.publish("orders.Wholesale.shipped", data);

            data = "Order R65321".getBytes(StandardCharsets.UTF_8);
            nc.publish("orders.Retail.shipped", data);

            System.out.println("Waiting for messages...");
            latchPlaced.await();
            latchShipped.await();
            // NATS-DOC-END
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