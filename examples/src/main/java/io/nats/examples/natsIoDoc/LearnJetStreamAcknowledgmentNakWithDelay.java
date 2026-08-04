package io.nats.examples.natsIoDoc;

import io.nats.client.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class LearnJetStreamAcknowledgmentNakWithDelay {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("shipping");

            // NATS-DOC-START
            // Pull one message and negatively acknowledge it with a delay.
            // The server holds the message and redelivers it after 10 seconds,
            // which backs off retries instead of looping immediately.
            Message m = cc.next(Duration.ofSeconds(5));
            if (m == null) {
                System.out.println("Nothing to read.");
                return;
            }

            System.out.printf("subject=%s data=%s%n",
                m.getSubject(),
                new String(m.getData(), StandardCharsets.UTF_8));

            m.nakWithDelay(Duration.ofSeconds(10));
            System.out.println("Nak sent; redelivery delayed 10s.");
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
