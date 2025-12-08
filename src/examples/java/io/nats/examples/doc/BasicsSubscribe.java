package io.nats.examples.doc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BasicsSubscribe {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // NATS-DOC-START
            // Subscribe to the "weather.updates" subject on a dispatcher
            Dispatcher d = nc.createDispatcher(msg ->
                System.out.println("Received: " + new String(msg.getData(), StandardCharsets.UTF_8)));
            d.subscribe("weather.updates");
            // NATS-DOC-END

            System.out.println("Waiting 10 seconds to receive a message from the BasicsPublish example...");
            Thread.sleep(10000);
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
