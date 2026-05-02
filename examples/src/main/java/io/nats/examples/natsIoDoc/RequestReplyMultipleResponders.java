package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.*;

public class RequestReplyMultipleResponders {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("demo.nats.io:4222")) {

            // NATS-DOC-START
            // Set up 2 instances of the service
            Dispatcher dServiceA = nc.createDispatcher(msg -> {
                byte[] response = ("calculated result from A").getBytes(StandardCharsets.ISO_8859_1);
                nc.publish(msg.getReplyTo(), response);
            });
            dServiceA.subscribe("calc.add");

            Dispatcher dServiceB = nc.createDispatcher(msg -> {
                byte[] response = ("calculated result from B").getBytes(StandardCharsets.ISO_8859_1);
                nc.publish(msg.getReplyTo(), response);
            });
            dServiceB.subscribe("calc.add");

            // Make a request expecting a future
            CompletableFuture<Message> responseFuture = nc.request("calc.add", null);
            try {
                Message m = responseFuture.get(500, TimeUnit.MILLISECONDS);
                System.out.println("1) Got response: " + new String(m.getData()));
            }
            catch (CancellationException | ExecutionException | TimeoutException e) {
                System.out.println("1) No Response");
            }

            // Make a request with a timeout and direct response
            Message m = nc.request("calc.add", null, Duration.ofMillis(500));
            if (m == null) {
                System.out.println("2) No Response");
            }
            else {
                System.out.println("2) Got response: " + new String(m.getData()));
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