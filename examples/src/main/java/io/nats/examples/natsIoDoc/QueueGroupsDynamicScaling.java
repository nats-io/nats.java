package io.nats.examples.natsIoDoc;

import io.nats.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class QueueGroupsDynamicScaling {
    // NATS-DOC-START

    static class Worker implements MessageHandler {
        final int id;
        Dispatcher dispatcher;

        public Worker(int id, Connection nc, String subject, String queueName) {
            this.id = id;
            dispatcher = nc.createDispatcher(this); // this is a MessageHandler
            dispatcher.subscribe(subject, queueName);
        }

        @Override
        public void onMessage(Message msg) throws InterruptedException {
            System.out.println("Worker " + id + " processing: " +
                new String(msg.getData(), StandardCharsets.UTF_8));
        }
    }

    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {
            List<Worker> workers = new ArrayList<Worker>();

            String subject = "tasks";
            String queueName = "workers";

            // Scale up
            for (int i = 1; i <= 5; i++) {
                workers.add(new Worker(i, nc, subject, queueName));
            }

            // Scale down
            for (Worker w : workers) {
                w.dispatcher.unsubscribe(subject);
            }
        }
        catch (InterruptedException e) {
            // can be thrown by connect
            Thread.currentThread().interrupt();
        }
        catch (IOException e) {
            // can be thrown by connect
        }
    }
    // NATS-DOC-END
}