package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamGetDirectBySequence {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");

            // NATS-DOC-START
            // Read the message stored at stream sequence 2. Without Direct Get
            // enabled this is a regular get, served by the stream leader.
            MessageInfo mi = sc.getMessage(2);

            System.out.println("Subject: " + mi.getSubject());
            System.out.println("Payload: " + new String(mi.getData(), StandardCharsets.UTF_8));
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
