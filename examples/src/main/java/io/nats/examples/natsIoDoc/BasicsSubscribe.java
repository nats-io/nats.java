package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Subscription;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BasicsSubscribe {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("demo.nats.io:4222")) {
            // NATS-DOC-START
            // Subscribe to 'weather.updates' synchronously
            Subscription sub = nc.subscribe("weather.updates");

            // Process messages
            Message m = sub.nextMessage(1000);
            while (m != null) {
                System.out.println("Received: " + new String(m.getData(), StandardCharsets.UTF_8));
                m = sub.nextMessage(1000);
            }
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
