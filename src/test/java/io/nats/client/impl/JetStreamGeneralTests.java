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
import io.nats.client.support.RandomUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static io.nats.client.api.ConsumerConfiguration.*;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamGeneralTests extends JetStreamTestBase {

    @Test
    public void testJetStreamContextCreate() throws Exception {
        runInJsServer(nc -> {
            createDefaultTestStream(nc); // tries management functions
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
            createDefaultTestStream(nc);
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
            nc.jetStreamManagement(jso);
        });
    }

    @Test
    public void testMiscMetaDataCoverage() {
        Message jsMsg = getTestJsMessage();
        assertTrue(jsMsg.isJetStream());

        // two calls to msg.metaData are for coverage to test lazy initializer
        assertNotNull(jsMsg.metaData()); // this call takes a different path
        assertNotNull(jsMsg.metaData()); // this call shows that the lazy will work
    }

    @Test
    public void testJetStreamSubscribe() throws Exception {
        runInJsServer(nc -> {
            boolean atLeast290 = ((NatsConnection)nc).getInfo().isSameOrNewerThanVersion("2.9.0");

            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);
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

            // coverage
            Dispatcher dispatcher = nc.createDispatcher();
            js.subscribe(SUBJECT);
            js.subscribe(SUBJECT, (PushSubscribeOptions)null);
            js.subscribe(SUBJECT, QUEUE, null);
            js.subscribe(SUBJECT, dispatcher, mh -> {}, false);
            js.subscribe(SUBJECT, dispatcher, mh -> {}, false, null);
            js.subscribe(SUBJECT, QUEUE, dispatcher, mh -> {}, false, null);

            // bind with w/o subject
            jsm.addOrUpdateConsumer(STREAM,
                builder()
                    .durable(durable(101))
                    .deliverSubject(deliver(101))
                    .build());

            PushSubscribeOptions psoBind = PushSubscribeOptions.bind(STREAM, durable(101));
            unsubscribeEnsureNotBound(js.subscribe(null, psoBind));
            unsubscribeEnsureNotBound(js.subscribe("", psoBind));
            JetStreamSubscription sub = js.subscribe(null, dispatcher, mh -> {}, false, psoBind);
            unsubscribeEnsureNotBound(dispatcher, sub);
            js.subscribe("", dispatcher, mh -> {}, false, psoBind);

            jsm.addOrUpdateConsumer(STREAM,
                builder()
                    .durable(durable(102))
                    .deliverSubject(deliver(102))
                    .deliverGroup(queue(102))
                    .build());

            psoBind = PushSubscribeOptions.bind(STREAM, durable(102));
            unsubscribeEnsureNotBound(js.subscribe(null, queue(102), psoBind));
            unsubscribeEnsureNotBound(js.subscribe("", queue(102), psoBind));
            sub = js.subscribe(null, queue(102), dispatcher, mh -> {}, false, psoBind);
            unsubscribeEnsureNotBound(dispatcher, sub);
            js.subscribe("", queue(102), dispatcher, mh -> {}, false, psoBind);

            // test 2.9.0
            if (atLeast290) {
                ConsumerConfiguration cc = builder().name(name(1)).build();
                pso = PushSubscribeOptions.builder().configuration(cc).build();
                sub = js.subscribe(SUBJECT, pso);
                m = sub.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                assertEquals(DATA, new String(m.getData()));
                ConsumerInfo ci = sub.getConsumerInfo();
                assertEquals(name(1), ci.getName());
                assertEquals(name(1), ci.getConsumerConfiguration().getName());
                assertNull(ci.getConsumerConfiguration().getDurable());

                cc = builder().durable(durable(1)).build();
                pso = PushSubscribeOptions.builder().configuration(cc).build();
                sub = js.subscribe(SUBJECT, pso);
                m = sub.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                assertEquals(DATA, new String(m.getData()));
                ci = sub.getConsumerInfo();
                assertEquals(durable(1), ci.getName());
                assertEquals(durable(1), ci.getConsumerConfiguration().getName());
                assertEquals(durable(1), ci.getConsumerConfiguration().getDurable());

                cc = builder().durable(name(2)).name(name(2)).build();
                pso = PushSubscribeOptions.builder().configuration(cc).build();
                sub = js.subscribe(SUBJECT, pso);
                m = sub.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                assertEquals(DATA, new String(m.getData()));
                ci = sub.getConsumerInfo();
                assertEquals(name(2), ci.getName());
                assertEquals(name(2), ci.getConsumerConfiguration().getName());
                assertEquals(name(2), ci.getConsumerConfiguration().getDurable());
            }
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
        });
    }

    @Test
    public void testFilterSubjectEphemeral() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context.
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

            // subscribe to the wildcard
            ConsumerConfiguration cc = builder().ackPolicy(AckPolicy.None).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = js.subscribe(subjectWild, pso);
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
            cc = builder().filterSubject(subjectA).ackPolicy(AckPolicy.None).build();
            pso = PushSubscribeOptions.builder().configuration(cc).build();
            sub = js.subscribe(subjectWild, pso);
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
            cc = builder().filterSubject(subjectB).ackPolicy(AckPolicy.None).build();
            pso = PushSubscribeOptions.builder().configuration(cc).build();
            sub = js.subscribe(subjectWild, pso);
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
            createDefaultTestStream(nc);
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
            unsubscribeEnsureNotBound(s);

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
            unsubscribeEnsureNotBound(s);

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
            createDefaultTestStream(nc);
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
            unsubscribeEnsureNotBound(s);

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
            unsubscribeEnsureNotBound(s);

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
            createDefaultTestStream(nc);

            // bind errors
            PushSubscribeOptions pushbinderr = PushSubscribeOptions.bind(STREAM, "binddur");
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pushbinderr));
            assertTrue(iae.getMessage().contains(JsSubConsumerNotFoundRequiredInBind.id()));

            PullSubscribeOptions pullbinderr = PullSubscribeOptions.bind(STREAM, "binddur");
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pullbinderr));
            assertTrue(iae.getMessage().contains(JsSubConsumerNotFoundRequiredInBind.id()));
        });
    }

    @Test
    public void testFilterMismatchErrors() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // single subject
            createMemoryStream(jsm, STREAM, SUBJECT);

            // will work as SubscribeSubject equals Filter Subject
            subscribeOk(js, jsm, SUBJECT, SUBJECT);
            subscribeOk(js, jsm, ">", ">");
            subscribeOk(js, jsm, "*", "*");

            // will work as SubscribeSubject != empty Filter Subject,
            // b/c Stream has exactly 1 subject and is a match.
            subscribeOk(js, jsm, "", SUBJECT);

            // will work as SubscribeSubject != Filter Subject of '>'
            // b/c Stream has exactly 1 subject and is a match.
            subscribeOk(js, jsm, ">", SUBJECT);

            // will not work
            subscribeEx(js, jsm, "*", SUBJECT);

            // multiple subjects no wildcards
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, SUBJECT, subject(2));

            // will work as SubscribeSubject equals Filter Subject
            subscribeOk(js, jsm, SUBJECT, SUBJECT);
            subscribeOk(js, jsm, ">", ">");
            subscribeOk(js, jsm, "*", "*");

            // will not work because stream has more than 1 subject
            subscribeEx(js, jsm, "", SUBJECT);
            subscribeEx(js, jsm, ">", SUBJECT);
            subscribeEx(js, jsm, "*", SUBJECT);

            // multiple subjects via '>'
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, SUBJECT_GT);

            // will work, exact matches
            subscribeOk(js, jsm, subjectDot("1"), subjectDot("1"));
            subscribeOk(js, jsm, ">", ">");

            // will not work because mismatch / stream has more than 1 subject
            subscribeEx(js, jsm, "", subjectDot("1"));
            subscribeEx(js, jsm, ">", subjectDot("1"));
            subscribeEx(js, jsm, SUBJECT_GT, subjectDot("1"));

            // multiple subjects via '*'
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, SUBJECT_STAR);

            // will work, exact matches
            subscribeOk(js, jsm, subjectDot("1"), subjectDot("1"));
            subscribeOk(js, jsm, ">", ">");

            // will not work because mismatch / stream has more than 1 subject
            subscribeEx(js, jsm, "", subjectDot("1"));
            subscribeEx(js, jsm, ">", subjectDot("1"));
            subscribeEx(js, jsm, SUBJECT_STAR, subjectDot("1"));
        });
    }

    private void subscribeOk(JetStream js, JetStreamManagement jsm, String fs, String ss) throws IOException, JetStreamApiException {
        int i = RandomUtils.PRAND.nextInt(); // just want a unique number
        setupConsumer(jsm, i, fs);
        unsubscribeEnsureNotBound(js.subscribe(ss, builder().durable(durable(i)).buildPushSubscribeOptions()));
    }

    private void subscribeEx(JetStream js, JetStreamManagement jsm, String fs, String ss) throws IOException, JetStreamApiException {
        int i = RandomUtils.PRAND.nextInt(); // just want a unique number
        setupConsumer(jsm, i, fs);
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> js.subscribe(ss, builder().durable(durable(i)).buildPushSubscribeOptions()));
        assertTrue(iae.getMessage().contains(JsSubSubjectDoesNotMatchFilter.id()));
    }

    private void setupConsumer(JetStreamManagement jsm, int i, String fs) throws IOException, JetStreamApiException {
        jsm.addOrUpdateConsumer(STREAM,
            builder().deliverSubject(deliver(i)).durable(durable(i)).filterSubject(fs).build());
    }

    @Test
    public void testBindDurableDeliverSubject() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // create the stream.
            createDefaultTestStream(jsm);

            // create a durable push subscriber - has deliver subject
            ConsumerConfiguration ccDurPush = builder()
                    .durable(durable(1))
                    .deliverSubject(deliver(1))
                    .build();
            jsm.addOrUpdateConsumer(STREAM, ccDurPush);

            // create a durable pull subscriber - notice no deliver subject
            ConsumerConfiguration ccDurPull = builder()
                    .durable(durable(2))
                    .build();
            jsm.addOrUpdateConsumer(STREAM, ccDurPull);

            // try to pull subscribe against a push durable
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(SUBJECT, PullSubscribeOptions.builder().durable(durable(1)).build())
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPush.id()));

            // try to pull bind against a push durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(SUBJECT, PullSubscribeOptions.bind(STREAM, durable(1)))
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPush.id()));

            // this one is okay
            JetStreamSubscription sub = js.subscribe(SUBJECT, PullSubscribeOptions.builder().durable(durable(2)).build());
            unsubscribeEnsureNotBound(sub); // so I can re-use the durable

            // try to push subscribe against a pull durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(SUBJECT, PushSubscribeOptions.builder().durable(durable(2)).build())
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPull.id()));

            // try to push bind against a pull durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(SUBJECT, PushSubscribeOptions.bind(STREAM, durable(2)))
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPull.id()));

            // this one is okay
            js.subscribe(SUBJECT, PushSubscribeOptions.builder().durable(durable(1)).build());
        });
    }

    @Test
    public void testConsumerIsNotModified() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);

            // test with config in issue 105
            ConsumerConfiguration cc = builder()
                .description("desc")
                .ackPolicy(AckPolicy.Explicit)
                .deliverPolicy(DeliverPolicy.All)
                .deliverSubject(deliver(1))
                .deliverGroup(queue(1))
                .durable(durable(1))
                .maxAckPending(65000)
                .maxDeliver(5)
                .maxBatch(10)
                .maxBytes(11)
                .replayPolicy(ReplayPolicy.Instant)
                .build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            PushSubscribeOptions pushOpts = PushSubscribeOptions.bind(STREAM, durable(1));
            js.subscribe(SUBJECT, queue(1), pushOpts); // should not throw an error

            // testing numerics
            cc = builder()
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .deliverSubject(deliver(21))
                .durable(durable(21))
                .startSequence(42)
                .maxDeliver(43)
                .maxBatch(47)
                .maxBytes(48)
                .rateLimit(44)
                .maxAckPending(45)
                .build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            pushOpts = PushSubscribeOptions.bind(STREAM, durable(21));
            js.subscribe(SUBJECT, pushOpts); // should not throw an error

            cc = builder()
                .durable(durable(22))
                .maxPullWaiting(46)
                .build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            PullSubscribeOptions pullOpts = PullSubscribeOptions.bind(STREAM, durable(22));
            js.subscribe(SUBJECT, pullOpts); // should not throw an error

            // testing DateTime
            cc = builder()
                .deliverPolicy(DeliverPolicy.ByStartTime)
                .deliverSubject(deliver(3))
                .durable(durable(3))
                .startTime(ZonedDateTime.now().plusHours(1))
                .build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            pushOpts = PushSubscribeOptions.bind(STREAM, durable(3));
            js.subscribe(SUBJECT, pushOpts); // should not throw an error

            // testing boolean and duration
            cc = builder()
                .deliverSubject(deliver(4))
                .durable(durable(4))
                .flowControl(1000)
                .headersOnly(true)
                .maxExpires(30000)
                .inactiveThreshold(40000)
                .ackWait(2000)
                .build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            pushOpts = PushSubscribeOptions.bind(STREAM, durable(4));
            js.subscribe(SUBJECT, pushOpts); // should not throw an error

            // testing enums
            cc = builder()
                .deliverSubject(deliver(5))
                .durable(durable(5))
                .deliverPolicy(DeliverPolicy.Last)
                .ackPolicy(AckPolicy.None)
                .replayPolicy(ReplayPolicy.Original)
                .build();
            jsm.addOrUpdateConsumer(STREAM, cc);

            pushOpts = PushSubscribeOptions.bind(STREAM, durable(5));
            js.subscribe(SUBJECT, pushOpts); // should not throw an error
        });
    }

    @Test
    public void testSubscribeDurableConsumerMustMatch() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);

            // push
            nc.jetStreamManagement().addOrUpdateConsumer(STREAM, pushDurableBuilder().build());

            changeExPush(js, pushDurableBuilder().deliverPolicy(DeliverPolicy.Last), "deliverPolicy");
            changeExPush(js, pushDurableBuilder().deliverPolicy(DeliverPolicy.New), "deliverPolicy");
            changeExPush(js, pushDurableBuilder().ackPolicy(AckPolicy.None), "ackPolicy");
            changeExPush(js, pushDurableBuilder().ackPolicy(AckPolicy.All), "ackPolicy");
            changeExPush(js, pushDurableBuilder().replayPolicy(ReplayPolicy.Original), "replayPolicy");

            changeExPush(js, pushDurableBuilder().flowControl(10000), "flowControl");
            changeExPush(js, pushDurableBuilder().headersOnly(true), "headersOnly");

            changeExPush(js, pushDurableBuilder().startTime(ZonedDateTime.now()), "startTime");
            changeExPush(js, pushDurableBuilder().ackWait(Duration.ofMillis(1)), "ackWait");
            changeExPush(js, pushDurableBuilder().description("x"), "description");
            changeExPush(js, pushDurableBuilder().sampleFrequency("x"), "sampleFrequency");
            changeExPush(js, pushDurableBuilder().idleHeartbeat(Duration.ofMillis(1000)), "idleHeartbeat");
            changeExPush(js, pushDurableBuilder().maxExpires(Duration.ofMillis(1000)), "maxExpires");
            changeExPush(js, pushDurableBuilder().inactiveThreshold(Duration.ofMillis(1000)), "inactiveThreshold");

            // value
            changeExPush(js, pushDurableBuilder().maxDeliver(MAX_DELIVER_MIN), "maxDeliver");
            changeExPush(js, pushDurableBuilder().maxAckPending(0), "maxAckPending");
            changeExPush(js, pushDurableBuilder().ackWait(0), "ackWait");

            // value unsigned
            changeExPush(js, pushDurableBuilder().startSequence(1), "startSequence");
            changeExPush(js, pushDurableBuilder().rateLimit(1), "rateLimit");

            // unset doesn't fail because the server provides a value equal to the unset
            changeOkPush(js, pushDurableBuilder().maxDeliver(INTEGER_UNSET));

            // unset doesn't fail because the server does not provide a value
            // negatives are considered the unset
            changeOkPush(js, pushDurableBuilder().startSequence(ULONG_UNSET));
            changeOkPush(js, pushDurableBuilder().startSequence(-1));
            changeOkPush(js, pushDurableBuilder().rateLimit(ULONG_UNSET));
            changeOkPush(js, pushDurableBuilder().rateLimit(-1));

            // unset fail b/c the server does set a value that is not equal to the unset or the minimum
            changeExPush(js, pushDurableBuilder().maxAckPending(LONG_UNSET), "maxAckPending");
            changeExPush(js, pushDurableBuilder().maxAckPending(0), "maxAckPending");
            changeExPush(js, pushDurableBuilder().ackWait(LONG_UNSET), "ackWait");
            changeExPush(js, pushDurableBuilder().ackWait(0), "ackWait");

            // pull
            nc.jetStreamManagement().addOrUpdateConsumer(STREAM, pullDurableBuilder().build());

            // value
            changeExPull(js, pullDurableBuilder().maxPullWaiting(0), "maxPullWaiting");
            changeExPull(js, pullDurableBuilder().maxBatch(0), "maxBatch");
            changeExPull(js, pullDurableBuilder().maxBytes(0), "maxBytes");

            // unsets fail b/c the server does set a value
            changeExPull(js, pullDurableBuilder().maxPullWaiting(-1), "maxPullWaiting");

            // unset
            changeOkPull(js, pullDurableBuilder().maxBatch(-1));
            changeOkPull(js, pullDurableBuilder().maxBytes(-1));
        });
    }

    private void changeOkPush(JetStream js, Builder builder) throws IOException, JetStreamApiException {
        unsubscribeEnsureNotBound(js.subscribe(SUBJECT, builder.buildPushSubscribeOptions()));
    }

    private void changeOkPull(JetStream js, Builder builder) throws IOException, JetStreamApiException {
        unsubscribeEnsureNotBound(js.subscribe(SUBJECT, builder.buildPullSubscribeOptions()));
    }

    private void changeExPush(JetStream js, Builder builder, String changedField) {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> js.subscribe(SUBJECT, PushSubscribeOptions.builder().configuration(builder.build()).build()));
        _changeEx(iae, changedField);
    }

    private void changeExPull(JetStream js, Builder builder, String changedField) {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> js.subscribe(SUBJECT, PullSubscribeOptions.builder().configuration(builder.build()).build()));
        _changeEx(iae, changedField);
    }

    private void _changeEx(IllegalArgumentException iae, String changedField) {
        String iaeMessage = iae.getMessage();
        assertTrue(iaeMessage.contains(JsSubExistingConsumerCannotBeModified.id()));
        assertTrue(iaeMessage.contains(changedField));
    }

    private Builder pushDurableBuilder() {
        return builder().durable(PUSH_DURABLE).deliverSubject(DELIVER);
    }

    private Builder pullDurableBuilder() {
        return builder().durable(PULL_DURABLE);
    }

    @Test
    public void testGetConsumerInfoFromSubscription() throws Exception {
        runInJsServer(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            createDefaultTestStream(nc);

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
            createDefaultTestStream(nc);

            // - consumer not found
            // - stream does not exist
            JetStreamSubscription sub = js.subscribe(SUBJECT);
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

    @Test
    public void testMoreCreateSubscriptionErrors() throws Exception {
        runInJsServer(nc -> {
            JetStream js = nc.jetStream();

            IllegalStateException ise = assertThrows(IllegalStateException.class, () -> js.subscribe(SUBJECT));
            assertTrue(ise.getMessage().contains(JsSubNoMatchingStreamForSubject.id()));

            // create the stream.
            createDefaultTestStream(nc);

            // general pull push validation
            ConsumerConfiguration ccCantHave = builder().durable("pulldur").deliverGroup("cantHave").build();
            PullSubscribeOptions pullCantHaveDlvrGrp = PullSubscribeOptions.builder().configuration(ccCantHave).build();
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pullCantHaveDlvrGrp));
            assertTrue(iae.getMessage().contains(JsSubPullCantHaveDeliverGroup.id()));

            ccCantHave = builder().durable("pulldur").deliverSubject("cantHave").build();
            PullSubscribeOptions pullCantHaveDlvrSub = PullSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pullCantHaveDlvrSub));
            assertTrue(iae.getMessage().contains(JsSubPullCantHaveDeliverSubject.id()));

            ccCantHave = builder().maxPullWaiting(1L).build();
            PushSubscribeOptions pushCantHaveMpw = PushSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pushCantHaveMpw));
            assertTrue(iae.getMessage().contains(JsSubPushCantHaveMaxPullWaiting.id()));

            ccCantHave = builder().maxBatch(1L).build();
            PushSubscribeOptions pushCantHaveMb = PushSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pushCantHaveMb));
            assertTrue(iae.getMessage().contains(JsSubPushCantHaveMaxBatch.id()));

            ccCantHave = builder().maxBytes(1L).build();
            PushSubscribeOptions pushCantHaveMby = PushSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, pushCantHaveMby));
            assertTrue(iae.getMessage().contains(JsSubPushCantHaveMaxBytes.id()));

            // create some consumers
            PushSubscribeOptions psoDurNoQ = PushSubscribeOptions.builder().durable("durNoQ").build();
            js.subscribe(SUBJECT, psoDurNoQ);

            PushSubscribeOptions psoDurYesQ = PushSubscribeOptions.builder().durable("durYesQ").build();
            js.subscribe(SUBJECT, "yesQ", psoDurYesQ);

            // already bound
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, psoDurNoQ));
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyBound.id()));

            // queue match
            PushSubscribeOptions qmatch = PushSubscribeOptions.builder()
                .durable("qmatchdur").deliverGroup("qmatchq").build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, "qnotmatch", qmatch));
            assertTrue(iae.getMessage().contains(JsSubQueueDeliverGroupMismatch.id()));

            // queue vs config
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, "notConfigured", psoDurNoQ));
            assertTrue(iae.getMessage().contains(JsSubExistingConsumerNotQueue.id()));

            PushSubscribeOptions psoNoVsYes = PushSubscribeOptions.builder().durable("durYesQ").build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, psoNoVsYes));
            assertTrue(iae.getMessage().contains(JsSubExistingConsumerIsQueue.id()));

            PushSubscribeOptions psoYesVsNo = PushSubscribeOptions.builder().durable("durYesQ").build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, "qnotmatch", psoYesVsNo));
            assertTrue(iae.getMessage().contains(JsSubExistingQueueDoesNotMatchRequestedQueue.id()));

            // flow control heartbeat push / pull
            ConsumerConfiguration ccFc = builder().durable("ccFcDur").flowControl(1000).build();
            ConsumerConfiguration ccHb = builder().durable("ccHbDur").idleHeartbeat(1000).build();

            PullSubscribeOptions psoPullCcFc = PullSubscribeOptions.builder().configuration(ccFc).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, psoPullCcFc));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidPull.id()));

            PullSubscribeOptions psoPullCcHb = PullSubscribeOptions.builder().configuration(ccHb).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, psoPullCcHb));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidPull.id()));

            PushSubscribeOptions psoPushCcFc = PushSubscribeOptions.builder().configuration(ccFc).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, "cantHaveQ", psoPushCcFc));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidQueue.id()));

            PushSubscribeOptions psoPushCcHb = PushSubscribeOptions.builder().configuration(ccHb).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, "cantHaveQ", psoPushCcHb));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidQueue.id()));
        });
    }
}