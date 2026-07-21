package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.JetStreamStatusException;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class LearnCoreNatsRequestReplyRequest {
    public static void main(String[] args) {
        // reportNoResponders() makes a request to an unserved subject fail fast
        // instead of waiting out the timeout.
        Options options = Options.builder()
            .server("nats://localhost:4222")
            .reportNoResponders()
            .build();
        try (Connection nc = Nats.connect(options)) {
            // A running inventory service so the request gets an answer.
            nc.createDispatcher(msg -> {
                if (msg.getReplyTo() != null) {
                    nc.publish(msg.getReplyTo(),
                        "{\"in_stock\":true,\"warehouse\":\"us-east\"}".getBytes(StandardCharsets.UTF_8));
                }
            }).subscribe("orders.inventory.check");
            Thread.sleep(200);

            // NATS-DOC-START
            // Ask the inventory service whether an order's item is in stock. A missing
            // service surfaces immediately as a canceled request; a slow one as a
            // timeout.
            byte[] order = ("{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\","
                + "\"total_cents\":4200,\"ts\":\"2026-05-22T10:14:22Z\"}").getBytes(StandardCharsets.UTF_8);
            CompletableFuture<Message> future = nc.request("orders.inventory.check", order);
            try {
                Message reply = future.get(2, TimeUnit.SECONDS);
                System.out.println("inventory replied: " + new String(reply.getData(), StandardCharsets.UTF_8));
            }
            catch (TimeoutException e) {
                System.out.println("inventory service did not answer in time");
            }
            catch (ExecutionException e) {
                // reportNoResponders() surfaces a missing service as a 503 status
                if (e.getCause() instanceof JetStreamStatusException) {
                    System.out.println("no inventory service is running");
                }
                else {
                    System.out.println("request failed: " + e.getMessage());
                }
            }
            // NATS-DOC-END
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
}
