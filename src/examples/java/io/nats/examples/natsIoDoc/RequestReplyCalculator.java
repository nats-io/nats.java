package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RequestReplyCalculator {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Set up the calculator add service
            Dispatcher dCalcAdd = nc.createDispatcher(msg -> {
                // data is in the for "x y"
                try {
                    String[] parts = new String(msg.getData()).split(" ");
                    if (parts.length == 2) {
                        int x = Integer.parseInt(parts[0]);
                        int y = Integer.parseInt(parts[1]);
                        nc.publish(msg.getReplyTo(), ("" + (x + y)).getBytes(StandardCharsets.UTF_8));
                    }
                }
                catch (Exception e) {
                    // you could make some other reply here
                }
            });
            dCalcAdd.subscribe("calc.add");

            CompletableFuture<Message> responseFuture = nc.request("calc.add", "5 3".getBytes(StandardCharsets.UTF_8));
            try {
                Message m = responseFuture.get(1, TimeUnit.SECONDS);
                System.out.printf("5 + 3 = %s\n", new String(m.getData()));
            }
            catch (ExecutionException | TimeoutException e) {
                System.out.println("1) No Response");
                throw new RuntimeException(e);
            }

            Message m = nc.request("calc.add", "10 7".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(1));
            if (m == null) {
                System.out.println("2) No Response");
            }
            else {
                System.out.printf("10 + 7 = %s\n", new String(m.getData()));
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