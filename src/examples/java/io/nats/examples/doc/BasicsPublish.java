package io.nats.examples.doc;

import io.nats.client.Connection;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BasicsPublish {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // NATS-DOC-START
            // Publish a message to the subject "weather.updates"
            byte[] data = "Temperature: 72°F".getBytes(StandardCharsets.UTF_8);
            nc.publish("weather.updates", data);
            System.out.println("Message published to weather.updates");
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
}
