package io.nats.examples.doc;

import io.nats.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;

public class GettingStartedSubscribe {
    // NATS-DOC-START
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            // Asynchronous Subscriber requires a dispatcher
            // Dispatchers can be shared
            CountDownLatch latch = new CountDownLatch(1);
            Dispatcher d = nc.createDispatcher(msg -> {
                System.out.println("Asynchronous Subscriber Received: " +
                    new String(msg.getData(), StandardCharsets.UTF_8));
                latch.countDown();
            });
            // Subscribe to 'hello' subject
            d.subscribe("hello");

            // Subscribe to 'hello' synchronously
            Subscription syncSub = nc.subscribe("hello");

            System.out.println("Waiting for message on 'hello'");
            latch.await(); // will release when async gets the message

            // wait 500 ms to get a message, should already be at the client though
            Message m = syncSub.nextMessage(500);
            System.out.println("Synchronous Subscriber Read: " +
                new String(m.getData(), StandardCharsets.UTF_8));
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