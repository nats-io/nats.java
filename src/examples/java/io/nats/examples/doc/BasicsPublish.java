package io.nats.examples.doc;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BasicsPublish {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect(Options.DEFAULT_URL)) {
            // NATS-DOC-START
            // Publish a message to the "weather.updates" subject
            nc.publish("weather.updates", "Temperature: 72°F".getBytes(StandardCharsets.UTF_8));
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
