package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamGetDirectLastForSubject {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");

            // NATS-DOC-START
            // Read the most recent message on a subject. Without Direct Get
            // enabled this is a regular get, served by the stream leader.
            MessageInfo mi = sc.getLastMessage("orders.shipped");

            System.out.println("Subject: " + mi.getSubject());
            System.out.println("Payload: " + new String(mi.getData(), StandardCharsets.UTF_8));
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
