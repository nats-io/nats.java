package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamRetentionPoliciesRetentionSwitchRejected {
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
            // You can't change a stream's retention policy after it's created.
            // Switching FULFILLMENT from WorkQueue to Limits is rejected; to change
            // retention you would delete the stream and recreate it.
            try {
                jsm.updateStream(StreamConfiguration.builder()
                    .name("FULFILLMENT")
                    .subjects("fulfill.>")
                    .retentionPolicy(RetentionPolicy.Limits)
                    .build());
            }
            catch (JetStreamApiException e) {
                System.out.println("Rejected: " + e.getMessage());
                // stream configuration update can not change retention policy to/from workqueue [10052]
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
