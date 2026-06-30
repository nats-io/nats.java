package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamGetDirectEnable {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // Read the current configuration so the update keeps every other setting.
            StreamConfiguration current = jsm.getStreamInfo("ORDERS").getConfiguration();

            // NATS-DOC-START
            // Turn on Direct Get for ORDERS. Reads can then be served by any
            // replica or mirror instead of only the stream leader.
            StreamConfiguration updated = StreamConfiguration.builder(current)
                    .allowDirect(true)
                    .build();
            StreamInfo info = jsm.updateStream(updated);

            System.out.println("AllowDirect: " + info.getConfiguration().getAllowDirect());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
