package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamReadingBackRead {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");
            ConsumerContext cc = sc.getConsumerContext("orders-reader");

            // NATS-DOC-START
            // Ask the consumer how many messages are still waiting, then fetch
            // exactly that many so the loop ends without guessing a count.
            long pending = cc.getConsumerInfo().getNumPending();
            if (pending == 0) {
                System.out.println("Nothing to read.");
                return;
            }

            try (FetchConsumer fc = cc.fetch(
                    FetchConsumeOptions.builder().maxMessages((int) pending).build())) {
                Message m;
                while ((m = fc.nextMessage()) != null) {
                    System.out.printf("stream_seq=%d consumer_seq=%d data=%s%n",
                        m.metaData().streamSequence(),
                        m.metaData().consumerSequence(),
                        new String(m.getData(), StandardCharsets.UTF_8));
                    // Acknowledge each message so the server advances the consumer.
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
