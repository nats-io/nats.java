package io.nats.examples.jetstream.simple;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;

import java.io.IOException;

import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
 * This example will demonstrate simplified consuming using iterate
 */
public class SimpleUtils {
    public static String STREAM = "simple-stream";
    public static String SUBJECT = "simple-subject";
    public static String CONSUMER = "simple-durable";
    public static int COUNT = 20;

    public static void setupStreamAndDataAndConsumer(Connection nc) throws IOException, JetStreamApiException {
        setupStreamAndDataAndConsumer(nc, STREAM, SUBJECT, COUNT, CONSUMER);
    }

    public static void setupStreamAndDataAndConsumer(Connection nc, int count) throws IOException, JetStreamApiException {
        setupStreamAndDataAndConsumer(nc, STREAM, SUBJECT, count, CONSUMER);
    }

    public static void setupStreamAndDataAndConsumer(Connection nc, String stream, String subject, int count, String durable) throws IOException, JetStreamApiException {
        setupStream(nc.jetStreamManagement(), stream, subject);
        setupPublish(nc.jetStream(), subject, count);
        setupConsumer(nc.jetStreamManagement(), stream, durable);
    }

    public static void setupStreamAndData(Connection nc) throws IOException, JetStreamApiException {
        setupStreamAndData(nc, STREAM, SUBJECT, COUNT);
    }

    public static void setupStreamAndData(Connection nc, String stream, String subject, int count) throws IOException, JetStreamApiException {
        setupStream(nc.jetStreamManagement(), stream, subject);
        setupPublish(nc.jetStream(), subject, count);
    }

    public static void setupStream(Connection nc) throws IOException, JetStreamApiException {
        setupStream(nc.jetStreamManagement(), STREAM, SUBJECT);
    }

    public static void setupStream(JetStreamManagement jsm, String stream, String subject) throws IOException, JetStreamApiException {
        createOrReplaceStream(jsm, stream, StorageType.Memory, subject);
    }

    public static void setupPublish(JetStream js, String subject, int count) throws IOException, JetStreamApiException {
        for (int x = 1; x <= count; x++) {
            js.publish(subject, ("simple-message-" + x).getBytes());
        }
    }

    public static void setupConsumer(JetStreamManagement jsm, String stream, String durable) throws IOException, JetStreamApiException {
        // Create durable consumer
        ConsumerConfiguration cc =
            ConsumerConfiguration.builder()
                .durable(durable)
                .build();
        jsm.addOrUpdateConsumer(stream, cc);
    }
}
