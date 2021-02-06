// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.StreamConfiguration.StorageType;
import io.nats.client.utils.TestBase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JetStreamTestBase extends TestBase {
    public static final String JS_REPLY_TO = "$JS.ACK.test-stream.test-consumer.1.2.3.1605139610113260000";

    public NatsMessage getJsMessage(String replyTo) {
        return new NatsMessage.IncomingMessageFactory("sid", "subj", replyTo, 0, false).getMessage();
    }

    public static StreamInfo createMemoryStream(JetStreamManagement jsm, String streamName, String... subjects)
            throws IOException, JetStreamApiException {

        StreamConfiguration sc = StreamConfiguration.builder().name(streamName).storageType(StorageType.Memory)
                .subjects(subjects).build();

        return jsm.addStream(sc);
    }

    public static StreamInfo createMemoryStream(Connection nc, String streamName, String... subjects)
            throws IOException, JetStreamApiException {
        return createMemoryStream(nc.jetStreamManagement(), streamName, subjects);
    }

    public StreamInfo createTestStream(Connection nc) throws IOException, JetStreamApiException {
        return createMemoryStream(nc, STREAM, SUBJECT);
    }

    public static void publish(JetStream js, String subject, int startId, int count) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data((data(startId++)).getBytes(StandardCharsets.US_ASCII))
                    .build();
            js.publish(msg);
        }
    }

    public static void publish(JetStream js, String subject, int count) throws IOException, JetStreamApiException {
        publish(js, subject, 1, count);
    }

    public static void publish(Connection nc, String subject, int count) throws IOException, JetStreamApiException {
        publish(nc.jetStream(), subject, 1, count);
    }

    public static void publish(Connection nc, String subject, int startId, int count) throws IOException, JetStreamApiException {
        publish(nc.jetStream(), subject, startId, count);
    }

    public static PublishAck publish(JetStream js) throws IOException, JetStreamApiException {
        Message msg = NatsMessage.builder()
                .subject(SUBJECT)
                .data(DATA.getBytes(StandardCharsets.US_ASCII))
                .build();
        return js.publish(msg);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        List<Message> messages = new ArrayList<>();
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        while (msg != null) {
            messages.add(msg);
            if (msg.isJetStream()) {
//                System.out.println("ACK " + new String(msg.getData()));
                msg.ack();
                msg = sub.nextMessage(Duration.ofSeconds(1));
            }
            else {
//                System.out.println("NOT");
                msg = null; // so we break the loop
            }
        }
        return messages;
    }

    public static void validateRedAndTotal(int expectedRed, int actualRed, int expectedTotal, int actualTotal) {
        validateRead(expectedRed, actualRed);
        validateTotal(expectedTotal, actualTotal);
    }

    public static void validateTotal(int expectedTotal, int actualTotal) {
        assertEquals(expectedTotal, actualTotal, "Total does not match");
    }

    public static void validateRead(int expectedRed, int actualRed) {
        assertEquals(expectedRed, actualRed, "Read does not match");
    }
}
