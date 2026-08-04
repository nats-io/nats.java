package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamMirrorsAndSourcesCreateSource {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // Setup: the three regional streams ALL-ORDERS aggregates.
            jsm.addStream(StreamConfiguration.builder().name("ORDERS-US").subjects("us.orders.>").storageType(StorageType.File).build());
            jsm.addStream(StreamConfiguration.builder().name("ORDERS-EU").subjects("eu.orders.>").storageType(StorageType.File).build());
            jsm.addStream(StreamConfiguration.builder().name("ORDERS-APAC").subjects("apac.orders.>").storageType(StorageType.File).build());

            // NATS-DOC-START
            // Create ALL-ORDERS as an aggregate that sources the three regional
            // streams into one. Unlike a mirror, a stream can list several sources.
            StreamInfo streamInfo = jsm.addStream(StreamConfiguration.builder()
                    .name("ALL-ORDERS")
                    .sources(
                        Source.builder().sourceName("ORDERS-US").build(),
                        Source.builder().sourceName("ORDERS-EU").build(),
                        Source.builder().sourceName("ORDERS-APAC").build())
                    .storageType(StorageType.File)
                .build());

            // Confirm: the aggregate lists three sources
            StreamConfiguration config = streamInfo.getConfiguration();
            System.out.println("Created " + config.getName()
                    + " sourcing " + config.getSources().size() + " streams");
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
