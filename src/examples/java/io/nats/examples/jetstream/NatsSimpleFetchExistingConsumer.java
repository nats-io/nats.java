package io.nats.examples.jetstream;

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StorageType;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;

import static io.nats.examples.jetstream.NatsJsUtils.createOrReplaceStream;

/**
 * This example will demonstrate simplified consuming with an existing durable consumer
 */
public class NatsSimpleFetchExistingConsumer {
    static String stream = "simple-stream";
    static String subject = "simple-subject";
    static String durable = "simple-durable";
    static int count = 20;

    public static void main(String[] args) {

        try (Connection nc = Nats.connect("nats://localhost")) {

            setupStreamAndData(nc);

            // Create durable consumer
            ConsumerConfiguration cc =
                ConsumerConfiguration.builder()
                    .durable(durable)
                    .build();
            nc.jetStreamManagement().addOrUpdateConsumer(stream, cc);

            // JetStream2 context
            JetStream js = nc.jetStream();

            // ********************************************************************************
            // Set up Reader. Reader is a SimpleConsumer
            // ********************************************************************************
            ConsumeOptions sco = ConsumeOptions.builder()
                .batchSize(10)
                .repullPercent(50)
                .expiresIn(Duration.ofMillis(10000))
                .build();

            ConsumerContext context = js.getConsumerContext(stream, durable);
            Iterator<Message> iterator = context.iterate(count);

            // read loop
            int red = 0;
            while (iterator.hasNext()) {
                Message msg = iterator.next();
                ++red;
                System.out.printf("Subject: %s | Data: %s | Meta: %s\n",
                    msg.getSubject(), new String(msg.getData()), msg.getReplyTo());
                msg.ack();
            }

            System.out.println("\n" + red + " message(s) were received.\n");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void setupStreamAndData(Connection nc) throws IOException, JetStreamApiException {
        JetStreamManagement jsm = nc.jetStreamManagement();
        createOrReplaceStream(jsm, stream, StorageType.Memory, subject);

        JetStream js = nc.jetStream();
        for (int x = 1; x <= count; x++) {
            js.publish(subject, ("simple-message-" + x).getBytes());
        }
    }
}
