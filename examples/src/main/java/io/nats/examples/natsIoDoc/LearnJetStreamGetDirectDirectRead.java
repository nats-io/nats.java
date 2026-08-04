package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamGetDirectDirectRead {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");

            // NATS-DOC-START
            // Read the message at stream sequence 1. Because ORDERS has
            // AllowDirect enabled, the client sends this over the Direct Get
            // API, so any replica or mirror can answer instead of the leader.
            MessageInfo mi = sc.getMessage(1);

            System.out.println("Subject: " + mi.getSubject());
            System.out.println("Payload: " + new String(mi.getData(), StandardCharsets.UTF_8));
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
