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
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.NatsConstants.EMPTY;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamGeneralTests extends JetStreamTestBase {

    @Test
    public void testJetStreamContextCreate() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc); // tries management functions
            nc.jetStreamManagement().getAccountStatistics(); // another management
            nc.jetStream().publish(SUBJECT, dataBytes(1));
        });
    }

    @Test
    public void testJetNotEnabled() throws Exception {
        runInServer(nc -> {
            // get normal context, try to do an operation
            JetStream js = nc.jetStream();
            assertThrows(IOException.class, () -> js.subscribe(SUBJECT));

            // get management context, try to do an operation
            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(IOException.class, jsm::getAccountStatistics);
        });
    }

    @Test
    public void testJetEnabledGoodAccount() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/js_authorization.conf", false, true)) {
            Options options = new Options.Builder().server(ts.getURI())
                    .userInfo("serviceup".toCharArray(), "uppass".toCharArray()).build();
            Connection nc = standardConnection(options);
            nc.jetStreamManagement();
            nc.jetStream();
        }
    }

    @Test
    public void testJetStreamPublishDefaultOptions() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();
            PublishAck ack = jsPublish(js);
            assertEquals(1, ack.getSeqno());
        });
    }

    @Test
    public void testConnectionClosing() throws Exception {
        runInJsServer(nc -> {
            nc.close();
            assertThrows(IOException.class, nc::jetStream);
            assertThrows(IOException.class, nc::jetStreamManagement);
        });
    }

    @Test
    public void testCreateWithOptionsForCoverage() throws Exception {
        runInJsServer(nc -> {
            JetStreamOptions jso = JetStreamOptions.builder().build();
            nc.jetStream(jso);
        });
    }

    @Test
    public void notJetStream() {
        NatsMessage m = NatsMessage.builder().subject("test").build();
        assertThrows(IllegalStateException.class, m::ack);
        assertThrows(IllegalStateException.class, m::nak);
        assertThrows(IllegalStateException.class, () -> m.ackSync(Duration.ZERO));
        assertThrows(IllegalStateException.class, m::inProgress);
        assertThrows(IllegalStateException.class, m::term);
        assertThrows(IllegalStateException.class, m::metaData);
    }

    @Test
    public void testJetStreamSubscribe() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            createTestStream(jsm);
            jsPublish(js);

            // default ephemeral subscription.
            Subscription s = js.subscribe(SUBJECT);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            List<String> names = jsm.getConsumerNames(STREAM);
            assertEquals(1, names.size());

            // default subscribe options // ephemeral subscription.
            s = js.subscribe(SUBJECT, PushSubscribeOptions.builder().build());
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jsm.getConsumerNames(STREAM);
            assertEquals(2, names.size());

            // set the stream
            PushSubscribeOptions pso = PushSubscribeOptions.builder().stream(STREAM).durable(DURABLE).build();
            s = js.subscribe(SUBJECT, pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jsm.getConsumerNames(STREAM);
            assertEquals(3, names.size());
        });
    }

    @Test
    public void testJetStreamSubscribeErrors() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();

            // stream not found
            PushSubscribeOptions psoInvalidStream = PushSubscribeOptions.builder().stream(STREAM).build();
            assertThrows(JetStreamApiException.class, () -> js.subscribe(SUBJECT, psoInvalidStream));

            // subject
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(null));
            assertTrue(iae.getMessage().startsWith("Subject"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(HAS_SPACE));
            assertTrue(iae.getMessage().startsWith("Subject"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, (PushSubscribeOptions)null));
            assertTrue(iae.getMessage().startsWith("Subject"));

            // queue
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, HAS_SPACE, null));
            assertTrue(iae.getMessage().startsWith("Queue"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, HAS_SPACE, null, null, false, null));
            assertTrue(iae.getMessage().startsWith("Queue"));

            // dispatcher
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, null, null, false));
            assertTrue(iae.getMessage().startsWith("Dispatcher"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, null, null, false, null));
            assertTrue(iae.getMessage().startsWith("Dispatcher"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, QUEUE, null, null, false, null));
            assertTrue(iae.getMessage().startsWith("Dispatcher"));

            // handler
            Dispatcher dispatcher = nc.createDispatcher();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, dispatcher, null, false));
            assertTrue(iae.getMessage().startsWith("Handler"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, dispatcher, null, false, null));
            assertTrue(iae.getMessage().startsWith("Handler"));
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, QUEUE, dispatcher, null, false, null));
            assertTrue(iae.getMessage().startsWith("Handler"));

            // valid
            createMemoryStream(nc, STREAM, SUBJECT);
            js.subscribe(SUBJECT);
            js.subscribe(SUBJECT, (PushSubscribeOptions)null);
            js.subscribe(SUBJECT, QUEUE, null);
            js.subscribe(SUBJECT, dispatcher, m -> {}, false);
            js.subscribe(SUBJECT, dispatcher, m -> {}, false, null);
            js.subscribe(SUBJECT, QUEUE, dispatcher, m -> {}, false, null);
        });
    }

    @Test
    public void testNoMatchingStreams() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            assertThrows(IllegalStateException.class, () -> js.subscribe(SUBJECT));
        });
    }

    @Test
    public void testFilterSubjectEphemeral() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            String subject = SUBJECT + ".*";
            String subjectA = SUBJECT + ".A";
            String subjectB = SUBJECT + ".B";

            // create the stream.
            createMemoryStream(nc, STREAM, subject);

            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);
            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);

            // subscribe to the wildcard
            ConsumerConfiguration cc = ConsumerConfiguration.builder().ackPolicy(AckPolicy.None).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = js.subscribe(subject, pso);
            nc.flush(Duration.ofSeconds(1));

            Message m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(1, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectB, m.getSubject());
            assertEquals(2, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(3, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectB, m.getSubject());
            assertEquals(4, m.metaData().streamSequence());

            // subscribe to A
            cc = ConsumerConfiguration.builder().filterSubject(subjectA).ackPolicy(AckPolicy.None).build();
            pso = PushSubscribeOptions.builder().configuration(cc).build();
            sub = js.subscribe(subject, pso);
            nc.flush(Duration.ofSeconds(1));

            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(1, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(3, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertNull(m);

            // subscribe to B
            cc = ConsumerConfiguration.builder().filterSubject(subjectB).ackPolicy(AckPolicy.None).build();
            pso = PushSubscribeOptions.builder().configuration(cc).build();
            sub = js.subscribe(subject, pso);
            nc.flush(Duration.ofSeconds(1));

            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectB, m.getSubject());
            assertEquals(2, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectB, m.getSubject());
            assertEquals(4, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertNull(m);
        });
    }

    @Test
    public void testFilterSubjectDurable() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            String subjectWild = SUBJECT + ".*";
            String subjectA = SUBJECT + ".A";
            String subjectB = SUBJECT + ".B";

            // create the stream.
            createMemoryStream(nc, STREAM, subjectWild);

            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);
            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);

            ConsumerConfiguration cc = ConsumerConfiguration.builder().filterSubject(subjectA).ackPolicy(AckPolicy.None).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().durable(DURABLE).configuration(cc).build();

            JetStreamSubscription sub = js.subscribe(subjectWild, pso);
            nc.flush(Duration.ofSeconds(1));

            Message m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(1, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(3, m.metaData().streamSequence());
            sub.unsubscribe();

            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);
            jsPublish(js, subjectA, 1);
            jsPublish(js, subjectB, 1);

            sub = js.subscribe(subjectWild, pso);
            nc.flush(Duration.ofSeconds(1));

            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(subjectA, m.getSubject());
            assertEquals(5, m.metaData().streamSequence());
            m = sub.nextMessage(Duration.ofSeconds(1));
            assertEquals(7, m.metaData().streamSequence());
            sub.unsubscribe();

            ConsumerConfiguration cc1 = ConsumerConfiguration.builder().filterSubject(subjectWild).build();
            PushSubscribeOptions pso1 = PushSubscribeOptions.builder().durable(DURABLE).configuration(cc1).build();
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subjectWild, pso1));

            ConsumerConfiguration cc2 = ConsumerConfiguration.builder().filterSubject(subjectB).build();
            PushSubscribeOptions pso2 = PushSubscribeOptions.builder().durable(DURABLE).configuration(cc2).build();
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(subjectWild, pso2));
        });
    }

    @Test
    public void testPrefix() throws Exception {
        String prefix = "tar.api";
        String streamMadeBySrc = "stream-made-by-src";
        String streamMadeByTar = "stream-made-by-tar";
        String subjectMadeBySrc = "sub-made-by.src";
        String subjectMadeByTar = "sub-made-by.tar";

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/js_prefix.conf", false)) {
            Options optionsSrc = new Options.Builder().server(ts.getURI())
                    .userInfo("src".toCharArray(), "spass".toCharArray()).build();

            Options optionsTar = new Options.Builder().server(ts.getURI())
                    .userInfo("tar".toCharArray(), "tpass".toCharArray()).build();

            try (Connection ncSrc = Nats.connect(optionsSrc);
                 Connection ncTar = Nats.connect(optionsTar)
            ) {
                // Setup JetStreamOptions. SOURCE does not need prefix
                JetStreamOptions jsoSrc = JetStreamOptions.builder().build();
                JetStreamOptions jsoTar = JetStreamOptions.builder().prefix(prefix).build();

                // Management api allows us to create streams
                JetStreamManagement jsmSrc = ncSrc.jetStreamManagement(jsoSrc);
                JetStreamManagement jsmTar = ncTar.jetStreamManagement(jsoTar);

                // add streams with both account
                StreamConfiguration scSrc = StreamConfiguration.builder()
                        .name(streamMadeBySrc)
                        .storageType(StorageType.Memory)
                        .subjects(subjectMadeBySrc)
                        .build();
                jsmSrc.addStream(scSrc);

                StreamConfiguration scTar = StreamConfiguration.builder()
                        .name(streamMadeByTar)
                        .storageType(StorageType.Memory)
                        .subjects(subjectMadeByTar)
                        .build();
                jsmTar.addStream(scTar);

                JetStream jsSrc = ncSrc.jetStream(jsoSrc);
                JetStream jsTar = ncTar.jetStream(jsoTar);

                jsSrc.publish(subjectMadeBySrc, "src-src".getBytes());
                jsSrc.publish(subjectMadeByTar, "src-tar".getBytes());
                jsTar.publish(subjectMadeBySrc, "tar-src".getBytes());
                jsTar.publish(subjectMadeByTar, "tar-tar".getBytes());

                // subscribe and read messages
                readPrefixMessages(ncSrc, jsSrc, subjectMadeBySrc, "src");
                readPrefixMessages(ncSrc, jsSrc, subjectMadeByTar, "tar");
                readPrefixMessages(ncTar, jsTar, subjectMadeBySrc, "src");
                readPrefixMessages(ncTar, jsTar, subjectMadeByTar, "tar");
            }
        }
    }

    private void readPrefixMessages(Connection nc, JetStream js, String subject, String dest) throws InterruptedException, IOException, JetStreamApiException, TimeoutException {
        JetStreamSubscription sub = js.subscribe(subject);
        nc.flush(Duration.ofSeconds(1));
        List<Message> msgs = readMessagesAck(sub);
        assertEquals(2, msgs.size());
        assertEquals(subject, msgs.get(0).getSubject());
        assertEquals(subject, msgs.get(1).getSubject());

        assertEquals("src-" + dest, new String(msgs.get(0).getData()));
        assertEquals("tar-" + dest, new String(msgs.get(1).getData()));
    }

    @Test
    public void testBindPush() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();

            jsPublish(js, SUBJECT, 1, 1);
            PushSubscribeOptions pso = PushSubscribeOptions.builder()
                    .durable(DURABLE)
                    .build();
            JetStreamSubscription s = js.subscribe(SUBJECT, pso);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(1), new String(m.getData()));
            m.ack();
            s.unsubscribe();

            jsPublish(js, SUBJECT, 2, 1);
            pso = PushSubscribeOptions.builder()
                    .stream(STREAM)
                    .durable(DURABLE)
                    .bind(true)
                    .build();
            s = js.subscribe(SUBJECT, pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(2), new String(m.getData()));
            m.ack();
            s.unsubscribe();

            jsPublish(js, SUBJECT, 3, 1);
            pso = PushSubscribeOptions.bind(STREAM, DURABLE);
            s = js.subscribe(SUBJECT, pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(3), new String(m.getData()));

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().stream(STREAM).bind(true).build());

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().durable(DURABLE).bind(true).build());

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().stream(EMPTY).bind(true).build());

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().stream(STREAM).durable(EMPTY).bind(true).build());
        });
    }

    @Test
    public void testBindPull() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();

            jsPublish(js, SUBJECT, 1, 1);

            PullSubscribeOptions pso = PullSubscribeOptions.builder()
                    .durable(DURABLE)
                    .build();
            JetStreamSubscription s = js.subscribe(SUBJECT, pso);
            s.pull(1);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(1), new String(m.getData()));
            m.ack();
            s.unsubscribe();

            jsPublish(js, SUBJECT, 2, 1);
            pso = PullSubscribeOptions.builder()
                    .stream(STREAM)
                    .durable(DURABLE)
                    .bind(true)
                    .build();
            s = js.subscribe(SUBJECT, pso);
            s.pull(1);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(2), new String(m.getData()));
            m.ack();
            s.unsubscribe();

            jsPublish(js, SUBJECT, 3, 1);
            pso = PullSubscribeOptions.bind(STREAM, DURABLE);
            s = js.subscribe(SUBJECT, pso);
            s.pull(1);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(3), new String(m.getData()));
        });
    }

    @Test
    public void testBindErrors() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            createTestStream(nc);

            PushSubscribeOptions pushso = PushSubscribeOptions.bind(STREAM, DURABLE);
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pushso));
            assertTrue(iae.getMessage().contains("[SUB-BND01]"));

            PullSubscribeOptions pullso = PullSubscribeOptions.bind(STREAM, DURABLE);
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pullso));
            assertTrue(iae.getMessage().contains("[SUB-BND01]"));
        });
    }

    @Test
    public void testBindDurableDeliverSubject() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(jsm, STREAM, SUBJECT);

            // create a durable push subscriber - has deliver subject
            ConsumerConfiguration ccDurPush = ConsumerConfiguration.builder()
                    .durable(durable(1))
                    .deliverSubject(deliver(1))
                    .build();
            jsm.addOrUpdateConsumer(STREAM, ccDurPush);

            // create a durable pull subscriber - notice no deliver subject
            ConsumerConfiguration ccDurPull = ConsumerConfiguration.builder()
                    .durable(durable(2))
                    .build();
            jsm.addOrUpdateConsumer(STREAM, ccDurPull);

            // try to pull subscribe against a push durable
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(SUBJECT, PullSubscribeOptions.builder().durable(durable(1)).build())
            );
            assertTrue(iae.getMessage().contains("[SUB-DS01]"));

            // try to pull bind against a push durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(SUBJECT, PullSubscribeOptions.bind(STREAM, durable(1)))
            );
            assertTrue(iae.getMessage().contains("[SUB-DS01]"));

            // this one is okay
            JetStreamSubscription sub = js.subscribe(SUBJECT, PullSubscribeOptions.builder().durable(durable(2)).build());
            sub.unsubscribe(); // so I can re-use the durable

            // try to push subscribe against a pull durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(SUBJECT, PushSubscribeOptions.builder().durable(durable(2)).build())
            );
            assertTrue(iae.getMessage().contains("[SUB-DS02]"));

            // try to push bind against a pull durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(SUBJECT, PushSubscribeOptions.bind(STREAM, durable(2)))
            );
            assertTrue(iae.getMessage().contains("[SUB-DS02]"));

            // try to push subscribe but mismatch the deliver subject
            ConsumerConfiguration ccMis = ConsumerConfiguration.builder().deliverSubject("not-match").build();
            PushSubscribeOptions psoMis = PushSubscribeOptions.builder().durable(durable(1)).configuration(ccMis).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, psoMis));
            assertTrue(iae.getMessage().contains("[SUB-DS03]"));

            // this one is okay
            js.subscribe(SUBJECT, PushSubscribeOptions.builder().durable(durable(1)).build());
        });
    }

    @Test
    public void testConsumerCannotBeModified() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();

            createMemoryStream(nc, STREAM, SUBJECT);

            ConsumerConfiguration.Builder builder = durBuilder();
            nc.jetStreamManagement().addOrUpdateConsumer(STREAM, builder.build());

            ccbmEx(js, durBuilder().flowControl(true), "Flow Control");
            ccbmEx(js, durBuilder().deliverPolicy(DeliverPolicy.Last), "Deliver Policy");
            ccbmEx(js, durBuilder().deliverPolicy(DeliverPolicy.New), "Deliver Policy");
            ccbmEx(js, durBuilder().ackPolicy(AckPolicy.None), "Ack Policy");
            ccbmEx(js, durBuilder().ackPolicy(AckPolicy.All), "Ack Policy");
            ccbmEx(js, durBuilder().replayPolicy(ReplayPolicy.Original), "Replay Policy");

            ccbmEx(js, durBuilder().description("x"), "Description");
            ccbmEx(js, durBuilder().startTime(ZonedDateTime.now()), "Start Time");
            ccbmEx(js, durBuilder().ackWait(Duration.ofMillis(1)), "Ack Wait");
            ccbmEx(js, durBuilder().sampleFrequency("x"), "Sample Frequency");
            ccbmEx(js, durBuilder().idleHeartbeat(Duration.ofMillis(1)), "Idle Heartbeat");

            ccbmEx(js, durBuilder().startSequence(5), "Start Sequence");
            ccbmEx(js, durBuilder().maxDeliver(5), "Max Deliver");
            ccbmEx(js, durBuilder().rateLimit(5), "Rate Limit");
            ccbmEx(js, durBuilder().maxAckPending(5), "Max Ack Pending");
            ccbmEx(js, durBuilder().maxPullWaiting(5), "Max Pull Waiting");

            ccbmOk(js, durBuilder().startSequence(0));
            ccbmOk(js, durBuilder().startSequence(-1));
            ccbmOk(js, durBuilder().maxDeliver(0));
            ccbmOk(js, durBuilder().maxDeliver(-1));
            ccbmOk(js, durBuilder().rateLimit(0));
            ccbmOk(js, durBuilder().rateLimit(-1));
            ccbmOk(js, durBuilder().maxAckPending(20000));
            ccbmOk(js, durBuilder().maxPullWaiting(0));
            ccbmOk(js, durBuilder().maxPullWaiting(512));
        });
    }

    private void ccbmOk(JetStream js, ConsumerConfiguration.Builder builder) throws IOException, JetStreamApiException {
        js.subscribe(SUBJECT, PushSubscribeOptions.builder().configuration(builder.build()).build()).unsubscribe();
    }

    private void ccbmEx(JetStream js, ConsumerConfiguration.Builder builder, String err) {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> js.subscribe(SUBJECT, PushSubscribeOptions.builder().configuration(builder.build()).build()));
//        ccbmEx(js, durBuilder().filterSubject("x"), "Filter Subject");
//        ccbmEx(js, durBuilder().deliverSubject("x"), "Deliver Subject");
//        ccbmEx(js, durBuilder().deliverGroup("x"), "Deliver Group");
        assertTrue(iae.getMessage().contains("[SUB-CC01]"));
        assertTrue(iae.getMessage().contains(err));
    }

    private ConsumerConfiguration.Builder durBuilder() {
        return ConsumerConfiguration.builder().durable(DURABLE).deliverSubject(DELIVER);
    }

    @Test
    public void testGetConsumerInfoFromSubscription() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context to receive JetStream messages.
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            JetStreamSubscription sub = js.subscribe(SUBJECT);
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            ConsumerInfo ci = sub.getConsumerInfo();
            assertEquals(STREAM, ci.getStreamName());
        });
    }

    @Test
    public void testInternalLookupConsumerInfoCoverage() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();

            // create the stream.
            createMemoryStream(nc, STREAM, SUBJECT);

            // - consumer not found
            // - stream does not exist
            assertNull(((NatsJetStream)js).lookupConsumerInfo(STREAM, DURABLE));
            assertThrows(JetStreamApiException.class,
                    () -> ((NatsJetStream)js).lookupConsumerInfo(stream(999), DURABLE));
        });
    }

    @Test
    public void testGetJetStreamValidatedConnectionCoverage() {
        NatsJetStreamMessage njsm = new NatsJetStreamMessage();

        IllegalStateException ise = assertThrows(IllegalStateException.class, njsm::getJetStreamValidatedConnection);
        assertTrue(ise.getMessage().contains("subscription"));

        njsm.subscription = new NatsSubscription("sid", "sub", "q", null, null);
        ise = assertThrows(IllegalStateException.class, njsm::getJetStreamValidatedConnection);
        assertTrue(ise.getMessage().contains("connection"));
    }

    // SFF TODO
//    @Test
    public void testAutoAckMessageHandlerCoverage() throws Exception {
        AtomicReference<Exception> errorListenerRef = new AtomicReference<>();

        ErrorListener el = new ErrorListener() {

            @Override
            public void errorOccurred(Connection conn, String error) {
            }

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                errorListenerRef.set(exp);
            }

            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {
            }
        };

        Options.Builder builder = new Options.Builder().errorListener(el);

        runInJsServer(builder, nc -> {

//            MessageHandler aamh = NatsJetStream.getAutoAckMessageHandler((NatsConnection) nc, (msg) -> {}) ;
//
//            AtomicBoolean messageHandlerFlag = new AtomicBoolean();
//            MessageHandler aamhEx =
//                    NatsJetStream.getAutoAckMessageHandler((NatsConnection) nc, (msg) -> {
//                        messageHandlerFlag.set(true);
//                        msg.ack();
//                    });
//
//            Message m = new NatsMessage(SUBJECT, null, null);
//            aamh.onMessage(m); // not a JetStream is handled by not acking
//            aamhEx.onMessage(m);
//            assertTrue(messageHandlerFlag.get());
//            assertTrue(errorListenerRef.get() instanceof IllegalStateException);
        });
    }
}