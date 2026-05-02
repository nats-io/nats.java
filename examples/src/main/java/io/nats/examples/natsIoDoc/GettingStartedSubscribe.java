package io.nats.examples.natsIoDoc;

import io.nats.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class GettingStartedSubscribe {
    // NATS-DOC-START
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("demo.nats.io")) {
            // Asynchronous Subscriber requires a dispatcher
            // Dispatchers can be shared
            Dispatcher d = nc.createDispatcher(msg -> {
                System.out.println("Asynchronous Subscriber Received: " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
            });

            // Subscribe to 'hello' subject
            d.subscribe("hello");

            // Subscribe to 'hello' synchronously
            Subscription syncSub = nc.subscribe("hello");

            System.out.println("Waiting for message on 'hello'");

            // Process messages
            Message m = syncSub.nextMessage(1000);
            while (m != null) {
                System.out.println("Synchronous Subscriber Received: " +
                    new String(m.getData(), StandardCharsets.UTF_8));
                m = syncSub.nextMessage(1000);
            }
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