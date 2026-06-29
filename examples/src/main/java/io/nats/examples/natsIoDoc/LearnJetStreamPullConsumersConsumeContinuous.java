package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamPullConsumersConsumeContinuous {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("shipping");

            // NATS-DOC-START
            // consume() sets up a continuous flow: the library keeps pull
            // requests open and runs the handler for each order as soon as it
            // lands in the stream. It runs until you stop it.
            try (MessageConsumer mc = cc.consume(msg -> {
                System.out.println("shipping " + new String(msg.getData(), StandardCharsets.UTF_8));
                msg.ack();
            })) {
                // Keep consuming until the process is stopped.
                Thread.sleep(Long.MAX_VALUE);
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
