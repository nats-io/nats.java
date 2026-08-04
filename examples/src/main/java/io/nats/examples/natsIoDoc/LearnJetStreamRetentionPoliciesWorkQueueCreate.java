package io.nats.examples.natsIoDoc;

import io.nats.client.*;
import io.nats.client.api.*;

import java.nio.charset.StandardCharsets;

public class LearnJetStreamRetentionPoliciesWorkQueueCreate {
    public static void main(String[] args) {
        try (Connection nc = Nats.connect("nats://localhost:4222")) {

            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            // NATS-DOC-START
            // FULFILLMENT is the queue of paid orders awaiting shipment, separate
            // from the ORDERS record stream. WorkQueue retention delivers each task
            // to one consumer; the first ack removes it, so the stream drains to empty.
            StreamInfo info = jsm.addStream(StreamConfiguration.builder()
                .name("FULFILLMENT")
                .subjects("fulfill.>")
                .retentionPolicy(RetentionPolicy.WorkQueue)
                .build());

            System.out.println("Retention policy: " + info.getConfiguration().getRetentionPolicy());

            // Queue one paid order for a US shipper to pick up.
            js.publish("fulfill.us",
                "{\"order_id\":\"ord_8w2k\",\"customer\":\"acme-co\"}".getBytes(StandardCharsets.UTF_8));

            // A durable pull consumer with explicit ack: a shipping worker must
            // acknowledge each task once it has handled the order.
            jsm.addOrUpdateConsumer("FULFILLMENT", ConsumerConfiguration.builder()
                .durable("shippers")
                .ackPolicy(AckPolicy.Explicit)
                .build());

            // Fetch one task and ack it, as a shipping worker would.
            ConsumerContext shippers = js.getConsumerContext("FULFILLMENT", "shippers");
            try (FetchConsumer fc = shippers.fetchMessages(1)) {
                Message task = fc.nextMessage();
                System.out.println("Shipping: " + new String(task.getData(), StandardCharsets.UTF_8));
                task.ack();
            }

            // The ack removed the task, so the WorkQueue stream is now empty.
            // A Limits stream like ORDERS would still hold the message.
            long count = jsm.getStreamInfo("FULFILLMENT").getStreamState().getMsgCount();
            System.out.println("Messages remaining in FULFILLMENT: " + count);
            // NATS-DOC-END
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
