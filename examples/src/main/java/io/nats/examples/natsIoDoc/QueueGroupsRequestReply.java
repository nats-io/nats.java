package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class QueueGroupsRequestReply {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("demo.nats.io:4222")) {

            // NATS-DOC-START
            List<Dispatcher> dispatchers = new ArrayList<>();
            // Set up 3 instances of the service
            for (int i = 1; i <= 3; i++) {
                final int instanceID = i;
                Dispatcher d = nc.createDispatcher(msg -> {
                    String[] data = new String(msg.getData()).split(",");
                    int result = Integer.parseInt(data[0]) + Integer.parseInt(data[1]);
                    String response = String.format("Result: %d, processed by: instance-%d", result, instanceID);
                    nc.publish(msg.getReplyTo(), response.getBytes(StandardCharsets.ISO_8859_1));
                    System.out.printf("Instance instance-%d processed request\n", instanceID);
                });
                d.subscribe("api.calculate", "api-workers-queue");
                dispatchers.add(d);
            }

            // Make requests - messages are balanced among the subscribers in the queue
            for (int i = 0; i < 10; i++) {
                String data = String.format("%d,%d", i, i * 2);
                Message m = nc.request("api.calculate", data.getBytes(StandardCharsets.ISO_8859_1), Duration.ofMillis(500));
                if (m == null) {
                    System.out.println(i + ") No Response");
                }
                else {
                    System.out.println(new String(m.getData()));
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