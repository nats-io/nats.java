package io.nats.examples.natsIoDoc;

import io.nats.client.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class LearnJetStreamYourFirstConsumerNext {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("shipping");

            // NATS-DOC-START
            // Pull one message but do not acknowledge it. Because there is no
            // ack, the message stays in flight and the server redelivers it
            // after the consumer's Ack Wait window expires.
            Message m = cc.next(Duration.ofSeconds(5));
            if (m == null) {
                System.out.println("Nothing to read.");
                return;
            }

            System.out.printf("subject=%s data=%s%n",
                m.getSubject(),
                new String(m.getData(), StandardCharsets.UTF_8));
            // No m.ack() — the message is redelivered after Ack Wait.
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
