package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class GettingStartedPublish {
    // NATS-DOC-START
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // Publish a message to the subject "hello"
            byte[] data = "Hello NATS!".getBytes(StandardCharsets.UTF_8);
            nc.publish("hello", data);
            System.out.println("Message published to hello");
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
