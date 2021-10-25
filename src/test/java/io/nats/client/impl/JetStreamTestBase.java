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
import io.nats.client.api.*;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static io.nats.examples.jetstream.NatsJsUtils.printConsumerInfo;
import static io.nats.examples.jetstream.NatsJsUtils.printStreamInfo;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamTestBase extends TestBase {
    public static final String TestMetaV0 = "$JS.ACK.test-stream.test-consumer.1.2.3.1605139610113260000";
    public static final String TestMetaV1 = "$JS.ACK.test-stream.test-consumer.1.2.3.1605139610113260000.4";
    public static final String TestMetaV2 = "$JS.ACK.v2Domain.v2Hash.test-stream.test-consumer.1.2.3.1605139610113260000.4";
    public static final String TestMetaVFuture = "$JS.ACK.v2Domain.v2Hash.test-stream.test-consumer.1.2.3.1605139610113260000.4.dont.care.how.many.more";
    public static final String InvalidMetaNoAck = "$JS.nope.test-stream.test-consumer.1.2.3.1605139610113260000";
    public static final String InvalidMetaLt8Tokens = "$JS.ACK.less-than.8-tokens.1.2.3";
    public static final String InvalidMeta10Tokens = "$JS.ACK.v2Domain.v2Hash.test-stream.test-consumer.1.2.3.1605139610113260000";
    public static final String InvalidMetaData = "$JS.ACK.v2Domain.v2Hash.test-stream.test-consumer.1.2.3.1605139610113260000.not-a-number";

    public static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(500);

    public NatsMessage getTestNatsMessage() {
        return getTestMessage("replyTo");
    }

    public NatsMessage getTestJsMessage() {
        return getTestMessage(TestMetaV2);
    }

    public NatsMessage getTestJsMessage(long seq) {
        return getTestMessage("$JS.ACK.v2Domain.v2Hash.test-stream.test-consumer.1." + seq + "." + seq + ".1605139610113260000.4");
    }

    public NatsMessage getTestMessage(String replyTo) {
        return new NatsMessage.InternalMessageFactory("sid", "subj", replyTo, 0, false).getMessage();
    }

    // ----------------------------------------------------------------------------------------------------
    // Management
    // ----------------------------------------------------------------------------------------------------
    public static void createMemoryStream(JetStreamManagement jsm, String streamName, String... subjects)
            throws IOException, JetStreamApiException {

        StreamConfiguration sc = StreamConfiguration.builder()
                .name(streamName)
                .storageType(StorageType.Memory)
                .subjects(subjects).build();

        jsm.addStream(sc);
    }

    public static void createMemoryStream(Connection nc, String streamName, String... subjects)
            throws IOException, JetStreamApiException {
        createMemoryStream(nc.jetStreamManagement(), streamName, subjects);
    }

    public static void createDefaultTestStream(Connection nc) throws IOException, JetStreamApiException {
        createMemoryStream(nc, STREAM, SUBJECT);
    }

    public static void createDefaultTestStream(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        createMemoryStream(jsm, STREAM, SUBJECT);
    }

    public static void debug(JetStreamManagement jsm, int n) throws IOException, JetStreamApiException {
        System.out.println("\n" + n + ". -------------------------------");
        printStreamInfo(jsm.getStreamInfo(STREAM));
        printConsumerInfo(jsm.getConsumerInfo(STREAM, DURABLE));
    }

    public static <T extends Throwable> T assertThrowsPrint(Class<T> expectedType, Executable executable) {
        T t = org.junit.jupiter.api.Assertions.assertThrows(expectedType, executable);
        t.printStackTrace();
        return t;
    }

    // ----------------------------------------------------------------------------------------------------
    // Publish / Read
    // ----------------------------------------------------------------------------------------------------
    public static void jsPublish(JetStream js, String subject, String prefix, int count) throws IOException, JetStreamApiException {
        for (int x = 1; x <= count; x++) {
            String data = prefix + x;
            js.publish(NatsMessage.builder()
                    .subject(subject)
                    .data(data.getBytes(StandardCharsets.US_ASCII))
                    .build()
            );
        }
    }

    public static void jsPublish(JetStream js, String subject, int startId, int count) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            js.publish(NatsMessage.builder().subject(subject).data((dataBytes(startId++))).build());
        }
    }

    public static void jsPublish(JetStream js, String subject, int count) throws IOException, JetStreamApiException {
        jsPublish(js, subject, 1, count);
    }

    public static void jsPublish(Connection nc, String subject, int count) throws IOException, JetStreamApiException {
        jsPublish(nc.jetStream(), subject, 1, count);
    }

    public static void jsPublish(Connection nc, String subject, int startId, int count) throws IOException, JetStreamApiException {
        jsPublish(nc.jetStream(), subject, startId, count);
    }

    public static PublishAck jsPublish(JetStream js) throws IOException, JetStreamApiException {
        Message msg = NatsMessage.builder()
                .subject(SUBJECT)
                .data(DATA.getBytes(StandardCharsets.US_ASCII))
                .build();
        return js.publish(msg);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub) throws InterruptedException {
        return readMessagesAck(sub, false, Duration.ofSeconds(1), -1);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, boolean noisy) throws InterruptedException {
        return readMessagesAck(sub, noisy, Duration.ofSeconds(1), -1);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, Duration timeout) throws InterruptedException {
        return readMessagesAck(sub, false, timeout, -1);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, Duration timeout, int max) throws InterruptedException {
        return readMessagesAck(sub, false, timeout, max);
    }

    public static List<Message> readMessagesAck(JetStreamSubscription sub, boolean noisy, Duration timeout, int max) throws InterruptedException {
        List<Message> messages = new ArrayList<>();
        Message msg = sub.nextMessage(timeout);
        while (msg != null) {
            messages.add(msg);
            if (msg.isJetStream()) {
                if (noisy) {
                    System.out.println("ACK " + new String(msg.getData()));
                }
                msg.ack();
            }
            else if (msg.isStatusMessage()) {
                if (noisy) {
                    System.out.println("STATUS " + msg.getStatus());
                }
            }
            else if (noisy) {
                System.out.println("? " + new String(msg.getData()) + "?");
            }
            if (messages.size() == max) {
                return messages;
            }
            msg = sub.nextMessage(timeout);
        }
        return messages;
    }

    public static List<Message> readMessages(Iterator<Message> iter) {
        List<Message> messages = new ArrayList<>();
        while (iter.hasNext()) {
            messages.add(iter.next());
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

    public static void assertSubscription(JetStreamSubscription sub, String stream, String consumer, String deliver, boolean isPullMode) {
        NatsJetStreamSubscription njssub = (NatsJetStreamSubscription)sub;
        assertEquals(stream, njssub.getStream());
        if (consumer == null) {
            assertNotNull(njssub.getConsumerName());
        }
        else {
            assertEquals(consumer, njssub.getConsumerName());
        }
        if (deliver != null) {
            assertEquals(deliver, njssub.getDeliverSubject());
        }

        boolean pm = njssub.isPullMode();
        assertEquals(isPullMode, pm);

        // coverage
        assertTrue(sub.toString().contains("isPullMode=" + pm));
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
            assertIsJetStream(m);
        }
    }

    public static void assertIsJetStream(Message m) {
        assertTrue(m.isJetStream());
        assertFalse(m.isStatusMessage());
        assertNull(m.getStatus());
    }

    public static void assertLastIsStatus(List<Message> messages, int code) {
        int lastIndex = messages.size() - 1;
        for (int x = 0; x < lastIndex; x++) {
            Message m = messages.get(x);
            assertTrue(m.isJetStream());
        }
        assertIsStatus(messages.get(lastIndex), code);
    }

    public static void assertStarts408(List<Message> messages, int count408, int expectedJs) {
        for (int x = 0; x < count408; x++) {
            assertIsStatus(messages.get(x), 408);
        }
        int countedJs = 0;
        int lastIndex = messages.size() - 1;
        for (int x = count408; x <= lastIndex; x++) {
            Message m = messages.get(x);
            assertTrue(m.isJetStream());
            countedJs++;
        }
        assertEquals(expectedJs, countedJs);
    }

    private static void assertIsStatus(Message statusMsg, int code) {
        assertFalse(statusMsg.isJetStream());
        assertTrue(statusMsg.isStatusMessage());
        assertNotNull(statusMsg.getStatus());
        assertEquals(code, statusMsg.getStatus().getCode());
    }

    public static void assertSource(JetStreamManagement jsm, String stream, Long msgCount, Long firstSeq)
            throws IOException, JetStreamApiException {
        sleep(1000);
        StreamInfo si = jsm.getStreamInfo(stream);

        assertConfig(stream, msgCount, firstSeq, si);
    }

    public static void assertMirror(JetStreamManagement jsm, String stream, String mirroring, Long msgCount, Long firstSeq)
            throws IOException, JetStreamApiException {
        sleep(1000);
        StreamInfo si = jsm.getStreamInfo(stream);

        MirrorInfo msi = si.getMirrorInfo();
        assertNotNull(msi);
        assertEquals(mirroring, msi.getName());

        assertConfig(stream, msgCount, firstSeq, si);
    }

    public static void assertConfig(String stream, Long msgCount, Long firstSeq, StreamInfo si) {
        StreamConfiguration sc = si.getConfiguration();
        assertNotNull(sc);
        assertEquals(stream, sc.getName());

        StreamState ss = si.getStreamState();
        if (msgCount != null) {
            assertEquals(msgCount, ss.getMsgCount());
        }
        if (firstSeq != null) {
            assertEquals(firstSeq, ss.getFirstSequence());
        }
    }

    public static void assertStreamSource(MessageInfo info, String stream, int i) {
        String hval = info.getHeaders().get("Nats-Stream-Source").get(0);
        String[] parts = hval.split(" ");
        assertEquals(stream, parts[0]);
        assertEquals("" + i, parts[1]);

    }

    public static Options.Builder optsWithEl() {
        return new Options.Builder().errorListener(new ErrorListener() {
            @Override
            public void errorOccurred(Connection conn, String error) {}

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {}

            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {}
        });
    }

    public static Options.Builder optsWithEl(ErrorListener el) {
        return new Options.Builder().errorListener(el);
    }
}
