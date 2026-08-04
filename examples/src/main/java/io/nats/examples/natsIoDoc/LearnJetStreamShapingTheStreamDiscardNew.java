package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamShapingTheStreamDiscardNew {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // NATS-DOC-START
            // Switch ORDERS to Discard New. Discard New never drops stored
            // messages, so capping it at one message leaves the existing orders
            // in place and puts the stream instantly over its limit; the next
            // publish is rejected.
            StreamInfo si = jsm.getStreamInfo("ORDERS");
            jsm.updateStream(StreamConfiguration.builder(si.getConfiguration())
                .discardPolicy(DiscardPolicy.New)
                .maxMessages(1)
                .build());

            // This publish hits the full stream and is rejected instead of
            // succeeding silently. Handle it in the publisher.
            try {
                js.publish("orders.created", "{\"order_id\":\"ord_8w2k\"}".getBytes());
            }
            catch (JetStreamApiException e) {
                System.out.println("publish rejected: " + e.getMessage());
            }

            // Put ORDERS back: Discard Old, no message cap (age and byte limits
            // stay).
            jsm.updateStream(StreamConfiguration.builder(si.getConfiguration())
                .discardPolicy(DiscardPolicy.Old)
                .maxMessages(-1)
                .build());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
