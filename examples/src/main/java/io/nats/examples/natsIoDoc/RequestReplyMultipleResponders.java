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
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Set up 2 instances of the service
            Dispatcher dService1 = nc.createDispatcher(msg -> {
                byte[] response = calculateResponse(1, msg);
                nc.publish(msg.getReplyTo(), response);
            });
            dService1.subscribe("service");

            Dispatcher dService2 = nc.createDispatcher(msg -> {
                byte[] response = calculateResponse(2, msg);
                nc.publish(msg.getReplyTo(), response);
            });
            dService2.subscribe("service");

            // Make a request expecting a future
            CompletableFuture<Message> responseFuture = nc.request("service", null);
            try {
                Message m = responseFuture.get(500, TimeUnit.MILLISECONDS);
                System.out.println("1) " + new String(m.getData()));
            }
            catch (CancellationException | ExecutionException | TimeoutException e) {
                System.out.println("1) No Response");
            }

            // Make a request with a timeout and direct response
            Message m = nc.request("service", null, Duration.ofMillis(500));
            if (m == null) {
                System.out.println("2) No Response");
            }
            else {
                System.out.println("2) " + new String(m.getData()));
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

    private static byte[] calculateResponse(int i, Message msg) {
        return ("Result from service instance " + i).getBytes(StandardCharsets.UTF_8);
    }
}