package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamMirrorsAndSourcesMirrorLag {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // NATS-DOC-START
            // A mirror is eventually consistent. Read its lag before trusting
            // it to hold what the upstream just received: 0 means caught up.
            StreamInfo streamInfo = jsm.getStreamInfo("ORDERS-ARCHIVE");
            MirrorInfo mirror = streamInfo.getMirrorInfo();

            System.out.println("Upstream:  " + mirror.getName());
            System.out.println("Lag:       " + mirror.getLag());
            System.out.println("Last seen: " + mirror.getActive());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
