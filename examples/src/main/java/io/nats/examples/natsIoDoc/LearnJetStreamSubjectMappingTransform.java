package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamSubjectMappingTransform {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // NATS-DOC-START
            // Rewrite each ingested subject as it lands in the stream. The template
            // shards each customer into one of three buckets: partition(3,1) hashes
            // the first wildcard token into 0, 1 or 2, and wildcard(1) keeps that
            // same token as the trailing part of the stored subject.
            SubjectTransform st = new SubjectTransform(
                    "ingest.*",
                    "orders.{{partition(3,1)}}.{{wildcard(1)}}");

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name("ORDERS-SHARDED")
                    .subjects("ingest.*")
                    .subjectTransform(st)
                    .build();
            StreamInfo info = jsm.addStream(sc);

            System.out.println("Created stream: " + info.getConfiguration().getName());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
