package io.nats.examples.natsIoDoc;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class QueueGroupsRequestReply {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Audit logger - receives all messages
            Dispatcher dAudit = nc.createDispatcher(msg -> {
                System.out.printf("[AUDIT] %s: %s\n", msg.getSubject(), new String(msg.getData(), StandardCharsets.UTF_8));
            });
            dAudit.subscribe("orders.>");

            Dispatcher dMetrics = nc.createDispatcher(msg -> {
                System.out.printf("[METRICS] %s: %s\n", msg.getSubject(), new String(msg.getData(), StandardCharsets.UTF_8));
            });
            dMetrics.subscribe("orders.>");

            Dispatcher dNewOrderWorker1 = nc.createDispatcher(msg -> {
                System.out.printf("[WORKER 1] %s: %s\n", msg.getSubject(), new String(msg.getData(), StandardCharsets.UTF_8));
            });
            dNewOrderWorker1.subscribe("orders.new", "new-orders-queue");

            Dispatcher dNewOrderWorker2 = nc.createDispatcher(msg -> {
                System.out.printf("[WORKER 2] %s: %s\n", msg.getSubject(), new String(msg.getData(), StandardCharsets.UTF_8));
            });
            dNewOrderWorker2.subscribe("orders.new", "new-orders-queue");

            // Publish order
            nc.publish("orders.new", "Order 123".getBytes(StandardCharsets.UTF_8));
            nc.publish("orders.new", "Order 124".getBytes(StandardCharsets.UTF_8));
            // Audit and metrics see it, one worker processes it
            // NATS-DOC-END

            Thread.sleep(100);
        }
        catch (InterruptedException e) {
            // can be thrown by connect
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String processNewOrder(int i, Message msg) {
        return "Order processed by instance " + i;
    }
}