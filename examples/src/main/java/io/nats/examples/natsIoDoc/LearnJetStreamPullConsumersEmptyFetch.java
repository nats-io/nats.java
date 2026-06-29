package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamPullConsumersEmptyFetch {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("shipping");

            // NATS-DOC-START
            // On a drained consumer the fetch ends with no messages once the
            // expiry passes, and nextMessage() returns null right away. Treat
            // that as "nothing right now," not a failure: note it and fetch
            // again instead of erroring.
            try (FetchConsumer fc = cc.fetch(
                    FetchConsumeOptions.builder().maxMessages(10).expiresIn(2000).build())) {
                Message m = fc.nextMessage();
                if (m == null) {
                    System.out.println("no orders waiting, will retry");
                }
                while (m != null) {
                    System.out.println("shipping " + new String(m.getData(), StandardCharsets.UTF_8));
                    m.ack();
                    m = fc.nextMessage();
                }
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
