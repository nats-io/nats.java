package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class QueueGroupsMixedSubscribers {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Set up 3 instances of the service
            Dispatcher dService1 = nc.createDispatcher(msg -> {
                byte[] response = calculateResponse(1, msg);
                nc.publish(msg.getReplyTo(), response);
            });
            dService1.subscribe("api.calculate", "api-workers-queue");

            Dispatcher dService2 = nc.createDispatcher(msg -> {
                byte[] response = calculateResponse(2, msg);
                nc.publish(msg.getReplyTo(), response);
            });
            dService2.subscribe("api.calculate", "api-workers-queue");

            Dispatcher dService3 = nc.createDispatcher(msg -> {
                byte[] response = calculateResponse(3, msg);
                nc.publish(msg.getReplyTo(), response);
            });
            dService3.subscribe("api.calculate", "api-workers-queue");

            // Make requests - messages are balanced among the subscribers in the queue
            for (int x = 0; x < 10; x++) {
                Message m = nc.request("api.calculate", null, Duration.ofMillis(500));
                if (m == null) {
                    System.out.println(x + ") No Response");
                }
                else {
                    System.out.println(x + ") " + new String(m.getData()));
                }
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