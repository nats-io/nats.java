package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class SubjectsMultiWildcard {
    // NATS-DOC-START
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Asynchronous subscribers require a dispatcher

            // Subscribe to the weather
            CountDownLatch latch = new CountDownLatch(4);
            Dispatcher dMulti = nc.createDispatcher(msg -> {
                latch.countDown();
                System.out.println("Received weather for " +
                    msg.getSubject() + " --> " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });
            dMulti.subscribe("weather.>");

            // Publish a message to the various subjects
            byte[] data = "Temperature: 11°C".getBytes(StandardCharsets.UTF_8);
            nc.publish("weather.north.fr", data);

            data = "Temperature: 15°C".getBytes(StandardCharsets.UTF_8);
            nc.publish("weather.south.fr", data);

            data = "Temperature: 74°F".getBytes(StandardCharsets.UTF_8);
            nc.publish("weather.southwest.us", data);

            data = "Temperature: 30°F".getBytes(StandardCharsets.UTF_8);
            nc.publish("weather.midwest.us", data);

            System.out.println("Waiting for messages...");
            latch.await();
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