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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamTestBase extends TestBase {
    public static final String JS_REPLY_TO = "$JS.ACK.test-stream.test-consumer.1.2.3.1605139610113260000";
    public static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(500);

    public NatsMessage getJsMessage(String replyTo) {
        return new NatsMessage.IncomingMessageFactory("sid", "subj", replyTo, 0, false).getMessage();
    }

    // ----------------------------------------------------------------------------------------------------
    // Management
    // ----------------------------------------------------------------------------------------------------
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

    public StreamInfo createTestStream(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        return createMemoryStream(jsm, STREAM, SUBJECT);
    }

    public StreamInfo getStreamInfo(JetStreamManagement jsm, String streamName) throws IOException, JetStreamApiException {
        try {
            return jsm.getStreamInfo(streamName);
        }
        catch (JetStreamApiException jsae) {
            if (jsae.getErrorCode() == 404) {
                return null;
            }
            throw jsae;
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Publish / Read
    // ----------------------------------------------------------------------------------------------------
    public static void publish(JetStream js, String subject, int startId, int count) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            js.publish(NatsMessage.builder().subject(subject).data((dataBytes(startId++))).build());
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

    // ----------------------------------------------------------------------------------------------------
    // Validate / Assert
    // ----------------------------------------------------------------------------------------------------
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

    public static void assertSameMessages(List<Message> l1, List<Message> l2) {
        assertEquals(l1.size(), l2.size());
        List<String> data1 = l1.stream()
                .map(m -> new String(m.getData()))
                .collect(Collectors.toList());
        List<String> data2 = l2.stream()
                .map(m -> new String(m.getData()))
                .collect(Collectors.toList());
        assertEquals(data1, data2);
    }

    public static void assertAllJetStream(List<Message> messages) {
        for (Message m : messages) {
            assertTrue(m.isJetStream());
        }
    }

    public static void assertLastIsStatus(List<Message> messages, int code) {
        int lastIndex = messages.size() - 1;
        for (int x = 0; x < lastIndex; x++) {
            Message m = messages.get(x);
            assertTrue(m.isJetStream());
        }
        Message m = messages.get(lastIndex);
        assertFalse(m.isJetStream());
        assertIsStatus(messages, code, lastIndex);
    }

    public static void assertIsStatus(List<Message> messages, int code, int index) {
        assertEquals(index + 1, messages.size());
        Message statusMsg = messages.get(index);
        assertFalse(statusMsg.isJetStream());
        assertNotNull(statusMsg.getStatus());
        assertEquals(code, statusMsg.getStatus().getCode());
    }
}
