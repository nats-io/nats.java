package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.impl.Headers;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.*;

public class RequestReplyHeaders {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Set up the header echo service
            Dispatcher dService = nc.createDispatcher(msg -> {
                Headers hIncoming = msg.getHeaders();
                Headers hResponse = new Headers();
                for (String keys : hIncoming.keySet()) {
                    hResponse.put(keys, hIncoming.get(keys));
                }
                nc.publish(msg.getReplyTo(), hResponse, null);
            });
            dService.subscribe("header.echo");

            // Make a request expecting a future
            Headers headers1 = new Headers();
            headers1.put("X-Request-ID", "1");
            headers1.put("X-Priority", "high");
            CompletableFuture<Message> responseFuture = nc.request("header.echo", headers1, null);
            try {
                Message m = responseFuture.get(500, TimeUnit.MILLISECONDS);
                Headers hIncoming = m.getHeaders();
                System.out.println("Response Headers: " + hIncoming);
            }
            catch (CancellationException | ExecutionException | TimeoutException e) {
                System.out.println("1) No Response");
            }

            // Make a request with a timeout and direct response
            Headers headers2 = new Headers();
            headers2.put("X-Request-ID", "2");
            headers2.put("X-Priority", "med");
            Message m = nc.request("header.echo", headers2, null, Duration.ofMillis(500));
            if (m == null) {
                System.out.println("2) No Response");
            }
            else {
                Headers hIncoming = m.getHeaders();
                System.out.println("Response Headers: " + hIncoming);
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