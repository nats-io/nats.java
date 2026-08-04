package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamMirrorsAndSourcesCreateMirror {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Create ORDERS-ARCHIVE as a read-only mirror of ORDERS. A mirror
            // takes no subjects of its own; it follows the upstream stream.
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamInfo streamInfo = jsm.addStream(StreamConfiguration.builder()
                    .name("ORDERS-ARCHIVE")
                    .mirror(Mirror.builder().sourceName("ORDERS").build())
                    .storageType(StorageType.File)
                .build());

            // Confirm: the new stream mirrors ORDERS
            StreamConfiguration config = streamInfo.getConfiguration();
            System.out.println("Created mirror " + config.getName()
                    + " of " + config.getMirror().getSourceName());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
