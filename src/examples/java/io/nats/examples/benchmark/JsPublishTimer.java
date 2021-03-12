package io.nats.examples.benchmark;

import io.nats.client.*;
import io.nats.client.impl.NatsMessage;
import io.nats.client.impl.StreamConfiguration;

import java.nio.charset.StandardCharsets;

public class JsPublishTimer {
    static final String STREAM = "jspt-stream";
    static final String SUBJECT = "jspt-subject";
    static final byte[] DATA = "jspt-data".getBytes(StandardCharsets.US_ASCII);
    static final int COUNT = 10000;

    public static void main(String[] args) throws Exception {

        try (Connection nc = Nats.connect(Options.DEFAULT_URL)) {

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(STREAM)
                    .subjects(SUBJECT)
                    .storageType(StreamConfiguration.StorageType.Memory)
                    .build();

            JetStreamManagement jsm = nc.jetStreamManagement();
            jsm.addStream(sc);
            JetStream js = nc.jetStream();

            long timer = 0;
            for (int x = 0; x < COUNT; x++) {
                Message msg = NatsMessage.builder().subject(SUBJECT).data(DATA).build();

                long now = System.currentTimeMillis();
                js.publish(msg);
                timer += (System.currentTimeMillis() - now);
                int y = x + 1;
                if (y == 100 || (y % 500 == 0)) {
                    System.out.println("Time for " + y + " messages: " + timer);
                }
            }
        }
    }
}
