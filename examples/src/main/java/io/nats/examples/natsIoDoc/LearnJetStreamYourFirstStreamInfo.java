package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

public class LearnJetStreamYourFirstStreamInfo {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStreamManagement jsm = nc.jetStreamManagement();

            // NATS-DOC-START
            // Fetch info for the "ORDERS" stream and print key fields
            StreamInfo streamInfo = jsm.getStreamInfo("ORDERS");

            StreamConfiguration config = streamInfo.getConfiguration();
            StreamState state = streamInfo.getStreamState();

            System.out.println("Name:     " + config.getName());
            System.out.println("Subjects: " + config.getSubjects());
            System.out.println("Messages: " + state.getMsgCount());
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
