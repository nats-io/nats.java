package io.nats.examples.natsIoDoc;

import io.nats.client.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class LearnJetStreamYourFirstConsumerPullAndAck {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("shipping");

            // NATS-DOC-START
            // Pull one message, process it, then acknowledge so the server
            // advances the consumer and never redelivers this message.
            Message m = cc.next(Duration.ofSeconds(5));
            if (m == null) {
                System.out.println("Nothing to read.");
                return;
            }

            System.out.printf("subject=%s data=%s%n",
                m.getSubject(),
                new String(m.getData(), StandardCharsets.UTF_8));

            m.ack();
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
