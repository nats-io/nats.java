package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.*;

public class RequestReplyBasic {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Set up the time service
            Dispatcher dTime = nc.createDispatcher(msg -> {
                nc.publish(msg.getReplyTo(), ("" + System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
            });
            dTime.subscribe("time");

            // Make a request expecting a future
            CompletableFuture<Message> responseFuture = nc.request("time", null);
            try {
                Message m = responseFuture.get(500, TimeUnit.MILLISECONDS);
                System.out.println("1) Time is " + new Date(Long.parseLong(new String(m.getData()))));
            }
            catch (CancellationException | ExecutionException | TimeoutException e) {
                System.out.println("1) No Response");
            }

            // Make a request with a timeout and direct response
            Message m = nc.request("time", null, Duration.ofMillis(500));
            if (m == null) {
                System.out.println("2) No Response");
            }
            else {
                System.out.println("2) Time is " + new Date(Long.parseLong(new String(m.getData()))));
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