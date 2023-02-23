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
    public static String DURABLE = "simple-durable";
    public static int COUNT = 20;

    public static void setupStreamAndDataAndConsumer(Connection nc) throws IOException, JetStreamApiException {
        JetStreamManagement jsm = nc.jetStreamManagement();
        createOrReplaceStream(jsm, STREAM, StorageType.Memory, SUBJECT);

        JetStream js = nc.jetStream();
        for (int x = 1; x <= COUNT; x++) {
            js.publish(SUBJECT, ("simple-message-" + x).getBytes());
        }

        // Create durable consumer
        ConsumerConfiguration cc =
            ConsumerConfiguration.builder()
                .durable(DURABLE)
                .build();
        nc.jetStreamManagement().addOrUpdateConsumer(STREAM, cc);
    }
}
