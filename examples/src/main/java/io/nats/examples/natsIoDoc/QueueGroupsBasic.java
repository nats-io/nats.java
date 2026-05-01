package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

public class QueueGroupsBasic {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("demo.nats.io:4222")) {
            // NATS-DOC-START
            // Worker A
            Dispatcher workerA = nc.createDispatcher(msg -> {
                System.out.println("Worker A Received: " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });
            workerA.subscribe("orders.new", "new-orders-queue");

            // Worker B
            Dispatcher workerB = nc.createDispatcher(msg -> {
                System.out.println("Worker B Received: " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });
            workerB.subscribe("orders.new", "new-orders-queue");

            // Worker C
            Dispatcher workerC = nc.createDispatcher(msg -> {
                System.out.println("Worker C Received: " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });
            workerC.subscribe("orders.new", "new-orders-queue");

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
                byte[] data = ("Order " + i).getBytes(StandardCharsets.UTF_8);
                nc.publish("orders.new", data);
            }
            // NATS-DOC-END

            Thread.sleep(1000); // give time for all the messages to be received
        }
        catch (InterruptedException e) {
            // can be thrown by connect
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
}