package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.impl.Headers;

import java.io.IOException;
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
                    hResponse.put("X-Response-ID", hIncoming.getFirst("X-Request-ID"));
                    hResponse.put(keys, hIncoming.get(keys));
                }
                nc.publish(msg.getReplyTo(), hResponse, msg.getData());
            });
            dService.subscribe("service");

            // Make a request expecting a future
            Headers headers = new Headers();
            headers.put("X-Request-ID", "123");
            headers.put("X-Priority", "high");
            CompletableFuture<Message> responseFuture = nc.request("service", headers, "data".getBytes());
            try {
                Message m = responseFuture.get(500, TimeUnit.MILLISECONDS);
                Headers hIncoming = m.getHeaders();
                byte[] data = m.getData();
                String s = data == null ? "" : new String(data);
                System.out.println("Response: " + s);
                System.out.println("Response ID: " + hIncoming.getFirst("X-Response-ID"));
            }
            catch (CancellationException | ExecutionException | TimeoutException e) {
                System.out.println("No Response");
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