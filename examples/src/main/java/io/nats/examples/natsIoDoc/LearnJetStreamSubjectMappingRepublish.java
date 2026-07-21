package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamSubjectMappingRepublish {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // Read the current configuration so the update keeps every other setting.
            StreamConfiguration current = jsm.getStreamInfo("ORDERS").getConfiguration();

            // NATS-DOC-START
            // Republish every message ORDERS stores onto a parallel subject that
            // ordinary core subscribers can watch, without them touching the stream.
            Republish rp = Republish.builder()
                    .source("orders.>")
                    .destination("dash.orders.>")
                    // .headersOnly(true) // republish only the headers, not the body
                    .build();

            StreamConfiguration updated = StreamConfiguration.builder(current)
                    .republish(rp)
                    .build();
            StreamInfo info = jsm.updateStream(updated);

            System.out.println("Republish destination: " + info.getConfiguration().getRepublish().getDestination());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
