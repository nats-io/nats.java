package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamRetentionPoliciesWorkqueueOverlap {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // Make sure the FULFILLMENT WorkQueue stream exists.
            try {
                jsm.getStreamInfo("FULFILLMENT");
            }
            catch (JetStreamApiException notFound) {
                jsm.addStream(StreamConfiguration.builder()
                    .name("FULFILLMENT")
                    .subjects("fulfill.>")
                    .retentionPolicy(RetentionPolicy.WorkQueue)
                    .build());
            }

            // NATS-DOC-START
            // One unfiltered consumer on a WorkQueue stream is fine: it owns every
            // subject, so each task still has exactly one owner.
            jsm.addOrUpdateConsumer("FULFILLMENT", ConsumerConfiguration.builder()
                .durable("shippers")
                .ackPolicy(AckPolicy.Explicit)
                .build());
            System.out.println("Added consumer: shippers");

            // A second unfiltered consumer would let two workers claim the same
            // task, so the WorkQueue stream rejects it.
            try {
                jsm.addOrUpdateConsumer("FULFILLMENT", ConsumerConfiguration.builder()
                    .durable("eu-shippers")
                    .ackPolicy(AckPolicy.Explicit)
                    .build());
            }
            catch (JetStreamApiException e) {
                System.out.println("Rejected: " + e.getMessage());
                // multiple non-filtered consumers not allowed on workqueue stream [10099]
            }

            // To split the work, drop the catch-all consumer...
            jsm.deleteConsumer("FULFILLMENT", "shippers");

            // ...and give each region its own subject filter. The filters don't
            // overlap, so every task still maps to exactly one consumer.
            jsm.addOrUpdateConsumer("FULFILLMENT", ConsumerConfiguration.builder()
                .durable("us-shippers")
                .filterSubject("fulfill.us")
                .ackPolicy(AckPolicy.Explicit)
                .build());
            jsm.addOrUpdateConsumer("FULFILLMENT", ConsumerConfiguration.builder()
                .durable("eu-shippers")
                .filterSubject("fulfill.eu")
                .ackPolicy(AckPolicy.Explicit)
                .build());
            System.out.println("Added filtered consumers: us-shippers (fulfill.us), eu-shippers (fulfill.eu)");
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
