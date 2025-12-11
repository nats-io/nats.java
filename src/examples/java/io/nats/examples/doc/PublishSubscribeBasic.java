package io.nats.examples.doc;

import io.nats.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class PublishSubscribeBasic {
    // NATS-DOC-START
    public static void main(String[] args) {
        // NATS-DOC-START
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // Asynchronous Subscriber
            Dispatcher d = nc.createDispatcher(msg ->
                System.out.println("Asynchronous Subscriber Received: " + new String(msg.getData(), StandardCharsets.UTF_8)));
            d.subscribe("events.data");

            // Synchronous Subscriber
            Subscription syncSub = nc.subscribe("events.data");

            nc.publish("events.data", "Hello from NATS!".getBytes(StandardCharsets.UTF_8));
            System.out.println("Message published to events.data");

            Message m = syncSub.nextMessage(500); // wait 500 ms to get a message
            System.out.println("Synchronous Subscriber Read: " + new String(m.getData(), StandardCharsets.UTF_8));

            Thread.sleep(100); // just in case the async sub needs some more time
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