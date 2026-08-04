package io.nats.examples.natsIoDoc;

import io.nats.client.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class LearnJetStreamYourFirstConsumerDoubleAck {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("shipping");

            // NATS-DOC-START
            // Pull one message and acknowledge it with a confirmed ack.
            // ackSync waits for the server to confirm the ack was stored, so
            // you know the consumer advanced before moving on.
            Message m = cc.next(Duration.ofSeconds(5));
            if (m == null) {
                System.out.println("Nothing to read.");
                return;
            }

            System.out.printf("subject=%s data=%s%n",
                m.getSubject(),
                new String(m.getData(), StandardCharsets.UTF_8));

            m.ackSync(Duration.ofSeconds(1));
            System.out.println("Ack confirmed by server.");
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
