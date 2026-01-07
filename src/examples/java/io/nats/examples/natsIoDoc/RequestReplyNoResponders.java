package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.util.concurrent.*;

public class RequestReplyNoResponders {
    public static void main(String[] args) {
        // NATS-DOC-START
        // You must specify the reportNoResponders() connect option
        Options options = Options.builder()
            .server("nats://localhost:4222")
            .reportNoResponders()
            .build();
        try (Connection nc = Nats.connect(options)) {
            // Make a request expecting a future
            CompletableFuture<Message> responseFuture = nc.request("no.such.service", null);
            try {
                Message m = responseFuture.get(500, TimeUnit.MILLISECONDS);
                System.out.println("Response: " + new String(m.getData()));
            }
            catch (CancellationException | ExecutionException | TimeoutException e) {
                System.out.println("No Response: " + e);
            }
        }
        // NATS-DOC-END
        catch (InterruptedException e) {
            // can be thrown by connect
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
}