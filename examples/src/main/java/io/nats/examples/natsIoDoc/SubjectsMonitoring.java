package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class SubjectsMonitoring {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Asynchronous subscribers require a dispatcher
            // Subscribe to everything
            CountDownLatch latch = new CountDownLatch(3);
            Dispatcher dEverything = nc.createDispatcher(msg -> {
                latch.countDown();
                System.out.println("[MONITOR] " +
                    msg.getSubject() + " --> " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });
            dEverything.subscribe(">");
            // NATS-DOC-END

            // Publish a message to the various subjects
            nc.publish("hello", "Hello NATS!".getBytes(StandardCharsets.UTF_8));
            nc.publish("event.new", "click".getBytes(StandardCharsets.UTF_8));
            nc.publish("weather.north.fr", "Temperature: 11°C".getBytes(StandardCharsets.UTF_8));

            System.out.println("Waiting for messages...");
            latch.await();
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