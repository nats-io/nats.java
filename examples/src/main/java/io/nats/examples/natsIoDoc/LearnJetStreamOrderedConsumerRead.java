package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class LearnJetStreamOrderedConsumerRead {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            StreamContext sc = nc.getStreamContext("ORDERS");

            // NATS-DOC-START
            // Ask for an ordered consumer over the stream. There's no ack to
            // send: the library runs the consumer for you and recreates it if it
            // ever misses a message, so you read every order in stream order.
            // DeliverPolicy.All starts from the first order.
            OrderedConsumerContext occ = sc.createOrderedConsumer(
                new OrderedConsumerConfiguration().deliverPolicy(DeliverPolicy.All));

            // Read the whole log once, in order, stopping when caught up.
            Message m;
            while ((m = occ.next(Duration.ofSeconds(5))) != null) {
                System.out.println("order " + new String(m.getData(), StandardCharsets.UTF_8));
                if (m.metaData().pendingCount() == 0) {
                    break;
                }
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
