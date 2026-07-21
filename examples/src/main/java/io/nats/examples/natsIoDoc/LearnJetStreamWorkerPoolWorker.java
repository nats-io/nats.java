package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamWorkerPoolWorker {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("shipping");

            // NATS-DOC-START
            // consume() runs the handler for every order the server hands this
            // worker. Run this same program in several processes: they all share
            // the one "shipping" consumer, and the server splits the stored
            // orders across them, one order to one worker.
            try (MessageConsumer mc = cc.consume(msg -> {
                System.out.println("shipping " + new String(msg.getData(), StandardCharsets.UTF_8));
                msg.ack();
            })) {
                // Keep this worker running until the process is stopped.
                Thread.sleep(Long.MAX_VALUE);
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
