package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamPullConsumersFetchBatch {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("shipping");

            // NATS-DOC-START
            // Fetch a batch of up to 10 orders, waiting up to 2 seconds for
            // them. The fetch ends when the batch is full or the wait elapses,
            // whichever comes first. Process and ack each, then fetch again.
            try (FetchConsumer fc = cc.fetch(
                    FetchConsumeOptions.builder().maxMessages(10).expiresIn(2000).build())) {
                Message m;
                while ((m = fc.nextMessage()) != null) {
                    System.out.println("shipping " + new String(m.getData(), StandardCharsets.UTF_8));
                    m.ack();
                }
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
