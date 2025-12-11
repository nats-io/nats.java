package io.nats.examples.doc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class QueueGroupsBasic {
    // NATS-DOC-START
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            AtomicInteger count1 = new AtomicInteger();
            AtomicInteger count2 = new AtomicInteger();
            AtomicInteger count3 = new AtomicInteger();

            // Asynchronous Subscriber 1
            Dispatcher worker1 = nc.createDispatcher(msg -> {
                count1.incrementAndGet();
                System.out.println("Asynchronous Subscriber 1 Received: " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });
            worker1.subscribe("orders.new", "new-orders-queue");

            // Asynchronous Subscriber 2
            Dispatcher worker2 = nc.createDispatcher(msg -> {
                count2.incrementAndGet();
                System.out.println("Asynchronous Subscriber 2 Received: " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });
            worker2.subscribe("orders.new", "new-orders-queue");

            // Asynchronous Subscriber 3
            Dispatcher worker3 = nc.createDispatcher(msg -> {
                count3.incrementAndGet();
                System.out.println("Asynchronous Subscriber 3 Received: " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });
            worker3.subscribe("orders.new", "new-orders-queue");

            try {
                // flush() queues a ping message and waits for a pong
                // response. This ensures that all the subscriptions
                // are at the server before publish starts
                nc.flush(Duration.ofSeconds(1));
            }
            catch (TimeoutException e) {
                throw new RuntimeException(e);
            }

            for (int i = 1; i <= 10; i++) {
                byte[] data = ("Order Number: " + i).getBytes(StandardCharsets.UTF_8);
                nc.publish("orders.new", data);
            }
            System.out.println("Messages published to orders.new");

            Thread.sleep(1000); // give time for all the messages to be received

            System.out.println("Asynchronous Subscriber 1 received " + count1.get() + " messages.");
            System.out.println("Asynchronous Subscriber 2 received " + count2.get() + " messages.");
            System.out.println("Asynchronous Subscriber 3 received " + count3.get() + " messages.");

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