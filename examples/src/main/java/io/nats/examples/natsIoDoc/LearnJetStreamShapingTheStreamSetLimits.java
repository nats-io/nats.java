package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.time.Duration;

public class LearnJetStreamShapingTheStreamSetLimits {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // NATS-DOC-START
            // Cap ORDERS with a seven-day age limit and a 1 GiB byte ceiling.
            // Build from the current config so other fields, and the stored
            // messages, are left in place, then update the stream.
            StreamInfo si = jsm.getStreamInfo("ORDERS");
            StreamConfiguration cfg = StreamConfiguration.builder(si.getConfiguration())
                .maxAge(Duration.ofDays(7))
                .maxBytes(1L << 30) // 1 GiB
                .build();
            jsm.updateStream(cfg);
            System.out.println("ORDERS capped at 7d age and 1 GiB");
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
