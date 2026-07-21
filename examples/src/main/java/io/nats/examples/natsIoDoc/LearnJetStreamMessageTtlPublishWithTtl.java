package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;
import io.nats.client.impl.Headers;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamMessageTtlPublishWithTtl {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // The stream must allow per-message TTLs before any Nats-TTL header is
            // honored. AllowMsgTTL is set once, when the stream is created.
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name("ORDERS")
                    .subjects("orders.>")
                    .allowMessageTtl()
                    .build();
            jsm.addStream(sc);

            JetStream js = nc.jetStream();

            // NATS-DOC-START
            // Attach a per-message TTL with the Nats-TTL header. The server keeps
            // this message for 60 seconds after it is stored, then deletes it.
            Headers headers = new Headers();
            headers.add("Nats-TTL", "60s");

            PublishAck ack = js.publish("orders.canceled", headers,
                    "order 4242 canceled".getBytes(StandardCharsets.UTF_8));

            System.out.println("Stored in stream: " + ack.getStream());
            System.out.println("At sequence: " + ack.getSeqno());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
