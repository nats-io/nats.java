package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamShapingTheStreamPerSubjectLimit {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // NATS-DOC-START
            // Add a per-subject ceiling so one noisy subject can't evict
            // another's messages. maxMessagesPerSubject keeps the most recent N
            // messages for every subject independently, alongside the
            // whole-stream limits.
            StreamInfo si = jsm.getStreamInfo("ORDERS");
            StreamConfiguration cfg = StreamConfiguration.builder(si.getConfiguration())
                .maxMessagesPerSubject(100000)
                .build();
            jsm.updateStream(cfg);
            System.out.println("ORDERS now keeps 100000 messages per subject");
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
