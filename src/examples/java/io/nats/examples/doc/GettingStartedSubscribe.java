package io.nats.examples.doc;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class GettingStartedSubscribe {
    // NATS-DOC-START
    public static void main(String[] args) {
        // NATS-DOC-START
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // Subscribe to 'hello'
            Subscription sub = nc.subscribe("hello");
            Message m = sub.nextMessage(60_000); // wait 60 seconds to get a message
            System.out.println("Received: " + new String(m.getData(), StandardCharsets.UTF_8));
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