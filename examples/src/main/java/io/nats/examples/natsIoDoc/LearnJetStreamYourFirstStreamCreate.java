package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamYourFirstStreamCreate {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Create a stream named "ORDERS" that captures any subject under `orders.`
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamInfo streamInfo = jsm.addStream(StreamConfiguration.builder()
                    .name("ORDERS")
                    .subjects("orders.>")
                    .storageType(StorageType.File)
                .build());

            // Confirm the stream was created
            System.out.println("Created stream: " + streamInfo.getConfiguration().getName());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
