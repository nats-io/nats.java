package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;

public class RequestReplyTimeout {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Make a request expecting a future
            CompletableFuture<Message> responseFuture = nc.request("service", null);
            try {
                Message m = responseFuture.get(1, TimeUnit.SECONDS);
                System.out.println("1) Response: " + new String(m.getData()));
            }
            catch (CancellationException | ExecutionException | TimeoutException e) {
                System.out.println("1) No Response");
            }

            // Make a request with a timeout and direct response
            Message m = nc.request("service", null, Duration.ofSeconds(1));
            if (m == null) {
                System.out.println("2) No Response");
            }
            else {
                System.out.println("2) Response: " + new String(m.getData()));
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