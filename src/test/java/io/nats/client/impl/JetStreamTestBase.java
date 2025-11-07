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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

    public static LongRunningNatsTestServer jsServer;

    @BeforeAll
    public static void beforeAll() throws IOException, InterruptedException {
        jsServer = new LongRunningNatsTestServer(false, true, null);
    }

    @AfterAll
    public static void afterAll() throws Exception {
        if (jsServer != null) {
            jsServer.close();
        }
    }

    public static final Duration DEFAULT_TIMEOUT = Duration.ofMillis(1000);

    private static final AtomicInteger MOCK_SID_HOLDER = new AtomicInteger(4273);
    public static String mockSid() {
        return "" + MOCK_SID_HOLDER.incrementAndGet();
    }

    public NatsMessage getTestNatsMessage() {
        return getTestMessage("replyTo", mockSid());
    }

    public NatsMessage getTestJsMessage() {
        return getTestMessage(TestMetaV2, mockSid());
    }

    public NatsMessage getTestJsMessage(long seq) {
        return getTestJsMessage(seq, mockSid());
    }

    public NatsMessage getTestJsMessage(long seq, String sid) {
        return getTestMessage("$JS.ACK.v2Domain.v2Hash.test-stream.test-consumer.1." + seq + "." + seq + ".1605139610113260000.4", sid);
    }

    public NatsMessage getTestMessage(String replyTo) {
        return new IncomingMessageFactory(mockSid(), "subj", replyTo, 0, false).getMessage();
    }

    public NatsMessage getTestMessage(String replyTo, String sid) {
        return new IncomingMessageFactory(sid, "subj", replyTo, 0, false).getMessage();
    }

    // ----------------------------------------------------------------------------------------------------
    // Management
    // ----------------------------------------------------------------------------------------------------
    public static class TestingStreamContainer {
        private String defaultSubjectVariant;
        private final String defaultNameVariant = TestBase.name();
        public final StreamInfo si;
        public final String stream = stream();
        private final Map<Object, String> subjects = new HashMap<>();
        private final Map<Object, String> names = new HashMap<>();

        public TestingStreamContainer(Connection nc) throws JetStreamApiException, IOException {
            this(nc.jetStreamManagement(), (String[])null);
        }

        public TestingStreamContainer(Connection nc, int subjectCount) throws JetStreamApiException, IOException {
            this(nc.jetStreamManagement(), subjectCount);
        }

        public TestingStreamContainer(Connection nc, String... subjects) throws JetStreamApiException, IOException {
            this(nc.jetStreamManagement(), subjects);
        }

        public TestingStreamContainer(JetStreamManagement jsm) throws JetStreamApiException, IOException {
            this(jsm, (String[])null);
        }

        public TestingStreamContainer(JetStreamManagement jsm, String... subjects) throws JetStreamApiException, IOException {
            if (subjects == null) {
                this.si = createMemoryStream(jsm, stream, subject());
            }
            else {
                this.si = createMemoryStream(jsm, stream, subjects);
            }
        }

        public TestingStreamContainer(JetStreamManagement jsm, int subjectCount) throws JetStreamApiException, IOException {
            String[] subjects = new String[subjectCount];
            for (int x = 0; x < subjectCount; x++) {
                subjects[x] = subject(x);
            }
            this.si = createMemoryStream(jsm, stream, subjects);
        }

        public String subject() {
            if (defaultSubjectVariant == null) {
                defaultSubjectVariant = TestBase.variant(null);
            }
            return subject(defaultSubjectVariant);
        }

        public String subject(Object variant) {
            return subjects.computeIfAbsent(variant, TestBase::subject);
        }

        public String consumerName() {
            return consumerName(defaultNameVariant);
        }

        public String consumerName(Object variant) {
            return names.computeIfAbsent(variant, TestBase::name);
        }
    }

    public static StreamInfo createMemoryStream(JetStreamManagement jsm, String streamName, String... subjects) throws IOException, JetStreamApiException {
        if (streamName == null) {
            streamName = stream();
        }

        if (subjects == null || subjects.length == 0) {
            subjects = new String[]{subject()};
        }

        StreamConfiguration sc = StreamConfiguration.builder()
                .name(streamName)
                .storageType(StorageType.Memory)
                .subjects(subjects).build();

        return jsm.addStream(sc);
    }

    public static StreamInfo createMemoryStream(Connection nc, String streamName, String... subjects)
        throws IOException, JetStreamApiException {
        return createMemoryStream(nc.jetStreamManagement(), streamName, subjects);
    }

    public static StreamInfo createDefaultTestStream(Connection nc) throws IOException, JetStreamApiException {
        return createMemoryStream(nc, STREAM, SUBJECT);
    }

    public static StreamInfo createDefaultTestStream(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        return createMemoryStream(jsm, STREAM, SUBJECT);
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
        jsPublish(js, subject, prefix, 1, count);
    }

    public static void jsPublish(JetStream js, String subject, String prefix, int startId, int count) throws IOException, JetStreamApiException {
        int end = startId + count - 1;
        for (int x = startId; x <= end; x++) {
            String data = prefix + x;
            js.publish(NatsMessage.builder()
                    .subject(subject)
                    .data(data.getBytes(StandardCharsets.US_ASCII))
                    .build()
            );
        }
    }

    public static void jsPublish(JetStream js, String subject, int startId, int count, long sleep) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            js.publish(NatsMessage.builder().subject(subject).data((dataBytes(startId++))).build());
            sleep(sleep);
        }
    }

    public static void jsPublish(JetStream js, String subject, int startId, int count) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            js.publish(NatsMessage.builder().subject(subject).data((dataBytes(startId++))).build());
        }
    }

    public static void jsPublishBytes(JetStream js, String subject, int count, byte[] bytes) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            js.publish(subject, bytes);
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

    public static PublishAck jsPublish(JetStream js, String subject, String data) throws IOException, JetStreamApiException {
        return js.publish(NatsMessage.builder().subject(subject).data(data.getBytes(StandardCharsets.US_ASCII)).build());
    }

    public static PublishAck jsPublish(JetStream js) throws IOException, JetStreamApiException {
        return jsPublish(js, SUBJECT, DATA);
    }

    public static PublishAck jsPublish(JetStream js, String subject) throws IOException, JetStreamApiException {
        return jsPublish(js, subject, DATA);
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

    public static class Publisher implements Runnable {
        private final JetStream js;
        private final String subject;
        private final int jitter;
        private final AtomicBoolean keepGoing = new AtomicBoolean(true);
        private int dataId;

        public Publisher(JetStream js, String subject, int jitter) {
            this.js = js;
            this.subject = subject;
            this.jitter = jitter;
        }

        public void stop() {
            keepGoing.set(false);
        }

        @Override
        public void run() {
            try {
                while (keepGoing.get()) {
                    if (jitter > 0) {
                        Thread.sleep(ThreadLocalRandom.current().nextLong(jitter));
                    }
                    js.publish(subject, dataBytes(++dataId));
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
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
        assertEquals(stream, njssub.getStreamName());
        if (consumer == null) {
            assertNotNull(njssub.getConsumerName());
        }
        else {
            assertEquals(consumer, njssub.getConsumerName());
        }
        if (deliver != null) {
            assertEquals(deliver, njssub.getSubject());
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
        assertTrue(hval.contains(stream));
        assertTrue(hval.contains("" + i));
    }

    // ----------------------------------------------------------------------------------------------------
    // Subscription or test macros
    // ----------------------------------------------------------------------------------------------------
    public static void unsubscribeEnsureNotBound(JetStreamSubscription sub) throws IOException, JetStreamApiException {
        sub.unsubscribe();
        ensureNotBound(sub);
    }

    public static void unsubscribeEnsureNotBound(Dispatcher dispatcher, JetStreamSubscription sub) throws JetStreamApiException, IOException {
        dispatcher.unsubscribe(sub);
        ensureNotBound(sub);
    }

    public static void ensureNotBound(JetStreamSubscription sub) throws IOException, JetStreamApiException {
        ConsumerInfo ci = sub.getConsumerInfo();
        long start = System.currentTimeMillis();
        while (ci.isPushBound()) {
            if (System.currentTimeMillis() - start > 5000) {
                return; // don't wait forever
            }
            sleep(5);
            ci = sub.getConsumerInfo();
        }
    }

    // Flapper fix: For whatever reason 10 seconds isn't enough on slow machines
    // I've put this in a function so all latch awaits give plenty of time
    public static void awaitAndAssert(CountDownLatch latch) throws InterruptedException {
        long start = System.currentTimeMillis();
        while (latch.getCount() > 0 && System.currentTimeMillis() - start < 30000) {
            latch.await(1, TimeUnit.SECONDS);
        }
        assertEquals(0, latch.getCount());
    }
}
