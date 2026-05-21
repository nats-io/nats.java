package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class JetStreamBasic {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            // NATS-DOC-START
            // Create a stream that captures any subject under `orders.`
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamInfo streamInfo = jsm.addStream(StreamConfiguration.builder()
                    .name("ORDERS")
                    .subjects("orders.>")
                    .storageType(StorageType.File)
                .build());

            // Publish a few orders
            JetStream js = nc.jetStream();
            js.publish("orders.new", "Order #1001".getBytes(StandardCharsets.UTF_8));
            js.publish("orders.new", "Order #1002".getBytes(StandardCharsets.UTF_8));
            js.publish("orders.shipped", "Order #1001 shipped".getBytes(StandardCharsets.UTF_8));

            // Create a durable pull consumer that delivers from the beginning
            StreamContext stream = js.getStreamContext("ORDERS");
            ConsumerContext consumer = stream.createOrUpdateConsumer(ConsumerConfiguration.builder()
                .durable("order-processor")
                .ackPolicy(AckPolicy.Explicit)
                .build());

            // Fetch a batch and acknowledge each message
            try (FetchConsumer fetchConsumer = consumer.fetchMessages(3)) {
                Message msg = fetchConsumer.nextMessage();
                while (msg != null) {
                    System.out.printf("Received on %s: %s\n", msg.getSubject(), new String(msg.getData(), StandardCharsets.UTF_8));
                    msg.ack();
                    msg = fetchConsumer.nextMessage();
                }
            }
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}