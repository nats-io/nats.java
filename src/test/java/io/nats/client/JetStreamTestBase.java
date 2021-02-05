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

package io.nats.client;

import io.nats.client.StreamConfiguration.StorageType;
import io.nats.client.impl.JetStreamApiException;
import io.nats.client.impl.NatsMessage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static io.nats.client.support.DebugUtil.printable;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class JetStreamTestBase {

    interface JsTest {
        void test(Connection nc) throws Exception;
    }

    protected static void runInJsServer(JsTest jsTest) throws Exception {
        try (NatsTestServer ts = new NatsTestServer(false, true); Connection nc = Nats.connect(ts.getURI())) {
            jsTest.test(nc);
        }
    }

    protected static StreamInfo createMemoryStream(JetStreamManagement jsm, String streamName, String... subjects)
            throws IOException, JetStreamApiException {

        StreamConfiguration sc = StreamConfiguration.builder().name(streamName).storageType(StorageType.Memory)
                .subjects(subjects).build();

        return jsm.addStream(sc);
    }

    protected static StreamInfo createMemoryStream(Connection nc, String streamName, String... subjects)
            throws IOException, JetStreamApiException {
        return createMemoryStream(nc.jetStreamManagement(), streamName, subjects);
    }

    protected StreamInfo createTestStream(Connection nc) throws IOException, JetStreamApiException {
        return createMemoryStream(nc, STREAM, SUBJECT);
    }

    protected static void publish(JetStream js, String subject, int startId, int count) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            Message msg = NatsMessage.builder()
                    .subject(subject)
                    .data((data(startId++)).getBytes(StandardCharsets.US_ASCII))
                    .build();
            js.publish(msg);
        }
    }

    protected static void publish(JetStream js, String subject, int count) throws IOException, JetStreamApiException {
        publish(js, subject, 1, count);
    }

    protected static void publish(Connection nc, String subject, int count) throws IOException, JetStreamApiException {
        publish(nc.jetStream(), subject, 1, count);
    }

    protected static void publish(Connection nc, String subject, int startId, int count) throws IOException, JetStreamApiException {
        publish(nc.jetStream(), subject, startId, count);
    }

    protected static PublishAck publish(JetStream js) throws IOException, JetStreamApiException {
        Message msg = NatsMessage.builder()
                .subject(SUBJECT)
                .data(DATA.getBytes(StandardCharsets.US_ASCII))
                .build();
        return js.publish(msg);
    }

    protected static int readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        int red = 0;
        Message msg = sub.nextMessage(Duration.ofSeconds(1));
        while (msg != null) {
            ++red;
            msg.ack();
            msg = sub.nextMessage(Duration.ofSeconds(1));
        }
        return red;
    }

    protected static void validateRedAndTotal(int expectedRed, int actualRed, int expectedTotal, int actualTotal) {
        validateRead(expectedRed, actualRed);
        validateTotal(expectedTotal, actualTotal);
    }

    private static void validateTotal(int expectedTotal, int actualTotal) {
        assertEquals(expectedTotal, actualTotal, "Total does not match");
    }

    protected static void validateRead(int expectedRed, int actualRed) {
        assertEquals(expectedRed, actualRed, "Read does not match");
    }

    protected static void printObject(Object o) {
        System.out.println(printable(o.toString()) + "\n");
    }

    protected static final String STREAM = "jstest-stream-";
    protected static final String SUBJECT = "jstest-subject-";
    protected static final String DURABLE = "jstest-durable-";
    protected static final String DATA = "jstest-data-";
    protected static final String DELIVER = "jstest-deliver-";

    protected static String stream(int seq) {
        return STREAM + seq;
    }

    protected static String subject(int seq) {
        return SUBJECT + seq;
    }

    protected static String durable(int seq) {
        return DURABLE + seq;
    }

    protected static String data(int seq) {
        return DATA + seq;
    }
}
