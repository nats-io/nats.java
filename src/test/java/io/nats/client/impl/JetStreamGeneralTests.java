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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static io.nats.client.api.ConsumerConfiguration.*;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamGeneralTests extends JetStreamTestBase {

    @Test
    public void testJetStreamContextCreate() throws Exception {
        jsServer.run(nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc); // tries management functions
            nc.jetStreamManagement().getAccountStatistics(); // another management
            nc.jetStream().publish(tsc.subject(), dataBytes(1));
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
        jsServer.run(nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStream js = nc.jetStream();
            PublishAck ack = jsPublish(js, tsc.subject());
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
        jsServer.run(nc -> {
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
        jsServer.run(nc -> {

            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            jsPublish(js, tsc.subject());

            // default ephemeral subscription.
            Subscription s = js.subscribe(tsc.subject());
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            List<String> names = jsm.getConsumerNames(tsc.stream);
            assertEquals(1, names.size());

            // default subscribe options // ephemeral subscription.
            s = js.subscribe(tsc.subject(), PushSubscribeOptions.builder().build());
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jsm.getConsumerNames(tsc.stream);
            assertEquals(2, names.size());

            // set the stream
            PushSubscribeOptions pso = PushSubscribeOptions.builder().stream(tsc.stream).durable(DURABLE).build();
            s = js.subscribe(tsc.subject(), pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jsm.getConsumerNames(tsc.stream);
            assertEquals(3, names.size());

            // coverage
            Dispatcher dispatcher = nc.createDispatcher();
            js.subscribe(tsc.subject());
            js.subscribe(tsc.subject(), (PushSubscribeOptions)null);
            js.subscribe(tsc.subject(), QUEUE, null);
            js.subscribe(tsc.subject(), dispatcher, mh -> {}, false);
            js.subscribe(tsc.subject(), dispatcher, mh -> {}, false, null);
            js.subscribe(tsc.subject(), QUEUE, dispatcher, mh -> {}, false, null);

            // bind with w/o subject
            jsm.addOrUpdateConsumer(tsc.stream,
                builder()
                    .durable(durable(101))
                    .deliverSubject(deliver(101))
                    .build());

            PushSubscribeOptions psoBind = PushSubscribeOptions.bind(tsc.stream, durable(101));
            unsubscribeEnsureNotBound(js.subscribe(null, psoBind));
            unsubscribeEnsureNotBound(js.subscribe("", psoBind));
            JetStreamSubscription sub = js.subscribe(null, dispatcher, mh -> {}, false, psoBind);
            unsubscribeEnsureNotBound(dispatcher, sub);
            js.subscribe("", dispatcher, mh -> {}, false, psoBind);

            jsm.addOrUpdateConsumer(tsc.stream,
                builder()
                    .durable(durable(102))
                    .deliverSubject(deliver(102))
                    .deliverGroup(queue(102))
                    .build());

            psoBind = PushSubscribeOptions.bind(tsc.stream, durable(102));
            unsubscribeEnsureNotBound(js.subscribe(null, queue(102), psoBind));
            unsubscribeEnsureNotBound(js.subscribe("", queue(102), psoBind));
            sub = js.subscribe(null, queue(102), dispatcher, mh -> {}, false, psoBind);
            unsubscribeEnsureNotBound(dispatcher, sub);
            js.subscribe("", queue(102), dispatcher, mh -> {}, false, psoBind);

            if (atLeast2_9_0(nc)) {
                ConsumerConfiguration cc = builder().name(name(1)).build();
                pso = PushSubscribeOptions.builder().configuration(cc).build();
                sub = js.subscribe(tsc.subject(), pso);
                m = sub.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                assertEquals(DATA, new String(m.getData()));
                ConsumerInfo ci = sub.getConsumerInfo();
                assertEquals(name(1), ci.getName());
                assertEquals(name(1), ci.getConsumerConfiguration().getName());
                assertNull(ci.getConsumerConfiguration().getDurable());

                cc = builder().durable(durable(1)).build();
                pso = PushSubscribeOptions.builder().configuration(cc).build();
                sub = js.subscribe(tsc.subject(), pso);
                m = sub.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                assertEquals(DATA, new String(m.getData()));
                ci = sub.getConsumerInfo();
                assertEquals(durable(1), ci.getName());
                assertEquals(durable(1), ci.getConsumerConfiguration().getName());
                assertEquals(durable(1), ci.getConsumerConfiguration().getDurable());

                cc = builder().durable(name(2)).name(name(2)).build();
                pso = PushSubscribeOptions.builder().configuration(cc).build();
                sub = js.subscribe(tsc.subject(), pso);
                m = sub.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                assertEquals(DATA, new String(m.getData()));
                ci = sub.getConsumerInfo();
                assertEquals(name(2), ci.getName());
                assertEquals(name(2), ci.getConsumerConfiguration().getName());
                assertEquals(name(2), ci.getConsumerConfiguration().getDurable());

                // test opt out
                JetStreamOptions jso = JetStreamOptions.builder().optOut290ConsumerCreate(true).build();
                JetStream jsOptOut = nc.jetStream(jso);
                ConsumerConfiguration ccOptOut = builder().name(name(99)).build();
                PushSubscribeOptions psoOptOut = PushSubscribeOptions.builder().configuration(ccOptOut).build();
                assertClientError(JsConsumerCreate290NotAvailable, () -> jsOptOut.subscribe(tsc.subject(), psoOptOut));
            }
        });
    }

    @Test
    public void testJetStreamSubscribeLenientSubject() throws Exception {
        jsServer.run(nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStream js = nc.jetStream();
            Dispatcher d = nc.createDispatcher();

            js.subscribe(tsc.subject(), (PushSubscribeOptions)null);
            js.subscribe(tsc.subject(), null, (PushSubscribeOptions)null); // queue name is not required, just a weird way to call this api
            js.subscribe(tsc.subject(), d, m -> {}, false, (PushSubscribeOptions)null);
            js.subscribe(tsc.subject(), null, d, m -> {}, false, (PushSubscribeOptions)null); // queue name is not required, just a weird way to call this api

            PushSubscribeOptions pso = ConsumerConfiguration.builder().filterSubject(tsc.subject()).buildPushSubscribeOptions();
            js.subscribe(null, pso);
            js.subscribe(null, null, pso);
            js.subscribe(null, d, m -> {}, false, pso);
            js.subscribe(null, null, d, m -> {}, false, pso);

            PushSubscribeOptions psoF = ConsumerConfiguration.builder().buildPushSubscribeOptions();

            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, psoF));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, psoF));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, null, psoF));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, d, m -> {}, false, psoF));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, null, d, m -> {}, false, psoF));

            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, (PushSubscribeOptions)null));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, (PushSubscribeOptions)null));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, null, (PushSubscribeOptions)null));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, d, m -> {}, false, (PushSubscribeOptions)null));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, null, d, m -> {}, false, (PushSubscribeOptions)null));

            PullSubscribeOptions lso = ConsumerConfiguration.builder().filterSubject(tsc.subject()).buildPullSubscribeOptions();
            js.subscribe(null, lso);
            js.subscribe(null, d, m -> {}, lso);

            PullSubscribeOptions lsoF = ConsumerConfiguration.builder().buildPullSubscribeOptions();
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, lsoF));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, d, m -> {}, lsoF));

            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, (PullSubscribeOptions)null));
            assertThrows(IllegalArgumentException.class, () -> js.subscribe(null, d, m -> {}, (PullSubscribeOptions)null));
        });
    }

    @Test
    public void testJetStreamSubscribeErrors() throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();

            // stream not found
            PushSubscribeOptions psoInvalidStream = PushSubscribeOptions.builder().stream(STREAM).build();
            assertThrows(JetStreamApiException.class, () -> js.subscribe(SUBJECT, psoInvalidStream));

            Dispatcher d = nc.createDispatcher();

            for (String bad : BAD_SUBJECTS_OR_QUEUES) {
                if (bad == null || bad.isEmpty()) {
                    // subject
                    IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(bad));
                    assertTrue(iae.getMessage().startsWith("Subject"));
                    assertClientError(JsSubSubjectNeededToLookupStream, () -> js.subscribe(bad, (PushSubscribeOptions)null));
                }
                else {
                    // subject
                    IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(bad));
                    assertTrue(iae.getMessage().startsWith("Subject"));
                    iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(bad, (PushSubscribeOptions)null));
                    assertTrue(iae.getMessage().startsWith("Subject"));

                    // queue
                    iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, bad, null));
                    assertTrue(iae.getMessage().startsWith("Queue"));
                    iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, bad, d, m -> {}, false, null));
                    assertTrue(iae.getMessage().startsWith("Queue"));
                }
            }

            // dispatcher
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(SUBJECT, null, null, false));
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
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            String subjectWild = SUBJECT + ".*";
            String subjectA = SUBJECT + ".A";
            String subjectB = SUBJECT + ".B";
            TestingStreamContainer tsc = new TestingStreamContainer(nc, subjectWild);

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
            sub = js.subscribe(subjectA, pso);
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
            sub = js.subscribe(subjectB, pso);
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
        jsServer.run(nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStream js = nc.jetStream();

            jsPublish(js, tsc.subject(), 1, 1);
            PushSubscribeOptions pso = PushSubscribeOptions.builder()
                    .durable(tsc.consumerName())
                    .build();
            JetStreamSubscription s = js.subscribe(tsc.subject(), pso);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(1), new String(m.getData()));
            m.ack();
            unsubscribeEnsureNotBound(s);

            jsPublish(js, tsc.subject(), 2, 1);
            pso = PushSubscribeOptions.builder()
                    .stream(tsc.stream)
                    .durable(tsc.consumerName())
                    .bind(true)
                    .build();
            s = js.subscribe(tsc.subject(), pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(2), new String(m.getData()));
            m.ack();
            unsubscribeEnsureNotBound(s);

            jsPublish(js, tsc.subject(), 3, 1);
            pso = PushSubscribeOptions.bind(tsc.stream, tsc.consumerName());
            s = js.subscribe(tsc.subject(), pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(3), new String(m.getData()));

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().stream(tsc.stream).bind(true).build());

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().durable(tsc.consumerName()).bind(true).build());

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().stream(EMPTY).bind(true).build());

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().stream(tsc.stream).durable(EMPTY).bind(true).build());
        });
    }

    @Test
    public void testBindPull() throws Exception {
        jsServer.run(nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStream js = nc.jetStream();

            jsPublish(js, tsc.subject(), 1, 1);

            PullSubscribeOptions pso = PullSubscribeOptions.builder()
                    .durable(tsc.consumerName())
                    .build();
            JetStreamSubscription s = js.subscribe(tsc.subject(), pso);
            s.pull(1);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(1), new String(m.getData()));
            m.ack();
            unsubscribeEnsureNotBound(s);

            jsPublish(js, tsc.subject(), 2, 1);
            pso = PullSubscribeOptions.builder()
                    .stream(tsc.stream)
                    .durable(tsc.consumerName())
                    .bind(true)
                    .build();
            s = js.subscribe(tsc.subject(), pso);
            s.pull(1);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(2), new String(m.getData()));
            m.ack();
            unsubscribeEnsureNotBound(s);

            jsPublish(js, tsc.subject(), 3, 1);
            pso = PullSubscribeOptions.bind(tsc.stream, tsc.consumerName());
            s = js.subscribe(tsc.subject(), pso);
            s.pull(1);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(3), new String(m.getData()));
        });
    }

    @Test
    public void testBindErrors() throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // bind errors
            PushSubscribeOptions pushbinderr = PushSubscribeOptions.bind(tsc.stream, "binddur");
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), pushbinderr));
            assertTrue(iae.getMessage().contains(JsSubConsumerNotFoundRequiredInBind.id()));

            PullSubscribeOptions pullbinderr = PullSubscribeOptions.bind(tsc.stream, "binddur");
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), pullbinderr));
            assertTrue(iae.getMessage().contains(JsSubConsumerNotFoundRequiredInBind.id()));
        });
    }

    @Test
    public void testFilterMismatchErrors() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // single subject
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // will work as SubscribeSubject equals Filter Subject
            filterMatchSubscribeOk(js, jsm, tsc.stream, tsc.subject(), tsc.subject());
            filterMatchSubscribeOk(js, jsm, tsc.stream, ">", ">");
            filterMatchSubscribeOk(js, jsm, tsc.stream, "*", "*");

            // will not work
            filterMatchSubscribeEx(js, jsm, tsc.stream, tsc.subject(), "");
            filterMatchSubscribeEx(js, jsm, tsc.stream, tsc.subject(), ">");
            filterMatchSubscribeEx(js, jsm, tsc.stream, tsc.subject(), "*");

            // multiple subjects no wildcards
            jsm.deleteStream(tsc.stream);
            createMemoryStream(jsm, tsc.stream, tsc.subject(), subject(2));

            // will work as SubscribeSubject equals Filter Subject
            filterMatchSubscribeOk(js, jsm, tsc.stream, tsc.subject(), tsc.subject());
            filterMatchSubscribeOk(js, jsm, tsc.stream, ">", ">");
            filterMatchSubscribeOk(js, jsm, tsc.stream, "*", "*");

            // will not work because stream has more than 1 subject
            filterMatchSubscribeEx(js, jsm, tsc.stream, tsc.subject(), "");
            filterMatchSubscribeEx(js, jsm, tsc.stream, tsc.subject(), ">");
            filterMatchSubscribeEx(js, jsm, tsc.stream, tsc.subject(), "*");

            String subjectGt = tsc.subject() + ".>";
            String subjectStar = tsc.subject() + ".*";
            String subjectDot = tsc.subject() + "." + name();

            // multiple subjects via '>'
            jsm.deleteStream(tsc.stream);
            createMemoryStream(jsm, tsc.stream, subjectGt);

            // will work, exact matches
            filterMatchSubscribeOk(js, jsm, tsc.stream, subjectDot, subjectDot);
            filterMatchSubscribeOk(js, jsm, tsc.stream, ">", ">");

            // will not work because mismatch / stream has more than 1 subject
            filterMatchSubscribeEx(js, jsm, tsc.stream, subjectDot, "");
            filterMatchSubscribeEx(js, jsm, tsc.stream, subjectDot, ">");
            filterMatchSubscribeEx(js, jsm, tsc.stream, subjectDot, subjectGt);

            // multiple subjects via '*'
            jsm.deleteStream(tsc.stream);
            createMemoryStream(jsm, tsc.stream, subjectStar);

            // will work, exact matches
            filterMatchSubscribeOk(js, jsm, tsc.stream, subjectDot, subjectDot);
            filterMatchSubscribeOk(js, jsm, tsc.stream, ">", ">");

            // will not work because mismatch / stream has more than 1 subject
            filterMatchSubscribeEx(js, jsm, tsc.stream, subjectDot, "");
            filterMatchSubscribeEx(js, jsm, tsc.stream, subjectDot, ">");
            filterMatchSubscribeEx(js, jsm, tsc.stream, subjectDot, subjectStar);
        });
    }

    private void filterMatchSubscribeOk(JetStream js, JetStreamManagement jsm, String stream, String subscribeSubject, String... filterSubjects) throws IOException, JetStreamApiException {
        int i = RandomUtils.PRAND.nextInt(); // just want a unique number
        filterMatchSetupConsumer(jsm, i, stream, filterSubjects);
        unsubscribeEnsureNotBound(js.subscribe(subscribeSubject, builder().durable(durable(i)).buildPushSubscribeOptions()));
    }

    private void filterMatchSubscribeEx(JetStream js, JetStreamManagement jsm, String stream, String subscribeSubject, String... filterSubjects) throws IOException, JetStreamApiException {
        int i = RandomUtils.PRAND.nextInt(); // just want a unique number
        filterMatchSetupConsumer(jsm, i, stream, filterSubjects);
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> js.subscribe(subscribeSubject, builder().durable(durable(i)).buildPushSubscribeOptions()));
        assertTrue(iae.getMessage().contains(JsSubSubjectDoesNotMatchFilter.id()));
    }

    private void filterMatchSetupConsumer(JetStreamManagement jsm, int i, String stream, String... fs) throws IOException, JetStreamApiException {
        jsm.addOrUpdateConsumer(stream,
            builder().deliverSubject(deliver(i)).durable(durable(i)).filterSubjects(fs).build());
    }

    @Test
    public void testBindDurableDeliverSubject() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            // create a durable push subscriber - has a deliver subject
            ConsumerConfiguration ccDurPush = builder()
                .durable(durable(1))
                .deliverSubject(deliver(1))
                .filterSubject(tsc.subject())
                .build();
            jsm.addOrUpdateConsumer(tsc.stream, ccDurPush);

            // create a durable pull subscriber - notice no deliver subject
            ConsumerConfiguration ccDurPull = builder()
                .durable(durable(2))
                .filterSubject(tsc.subject())
                .build();
            jsm.addOrUpdateConsumer(tsc.stream, ccDurPull);

            // try to pull subscribe against a push durable
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(tsc.subject(), PullSubscribeOptions.builder().durable(durable(1)).build())
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPush.id()));

            // try to pull bind against a push durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(tsc.subject(), PullSubscribeOptions.bind(tsc.stream, durable(1)))
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPush.id()));

            // try to push subscribe against a pull durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(tsc.subject(), PushSubscribeOptions.builder().durable(durable(2)).build())
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPull.id()));

            // try to push bind against a pull durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> js.subscribe(tsc.subject(), PushSubscribeOptions.bind(tsc.stream, durable(2)))
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPull.id()));

            // this one is okay
            js.subscribe(tsc.subject(), PushSubscribeOptions.builder().durable(durable(1)).build());
        });
    }

    @Test
    public void testConsumerIsNotModified() throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

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
                .filterSubject(tsc.subject())
                .build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            PushSubscribeOptions pushOpts = PushSubscribeOptions.bind(tsc.stream, durable(1));
            js.subscribe(tsc.subject(), queue(1), pushOpts); // should not throw an error

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
                .filterSubject(tsc.subject())
                .build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            pushOpts = PushSubscribeOptions.bind(tsc.stream, durable(21));
            js.subscribe(tsc.subject(), pushOpts); // should not throw an error

            cc = builder()
                .durable(durable(22))
                .maxPullWaiting(46)
                .filterSubject(tsc.subject())
                .build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            PullSubscribeOptions pullOpts = PullSubscribeOptions.bind(tsc.stream, durable(22));
            js.subscribe(tsc.subject(), pullOpts); // should not throw an error

            // testing DateTime
            cc = builder()
                .deliverPolicy(DeliverPolicy.ByStartTime)
                .deliverSubject(deliver(3))
                .durable(durable(3))
                .startTime(ZonedDateTime.now().plusHours(1))
                .filterSubject(tsc.subject())
                .build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            pushOpts = PushSubscribeOptions.bind(tsc.stream, durable(3));
            js.subscribe(tsc.subject(), pushOpts); // should not throw an error

            // testing boolean and duration
            cc = builder()
                .deliverSubject(deliver(4))
                .durable(durable(4))
                .flowControl(1000)
                .headersOnly(true)
                .maxExpires(30000)
                .ackWait(2000)
                .filterSubject(tsc.subject())
                .build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            pushOpts = PushSubscribeOptions.bind(tsc.stream, durable(4));
            js.subscribe(tsc.subject(), pushOpts); // should not throw an error

            // testing enums
            cc = builder()
                .deliverSubject(deliver(5))
                .durable(durable(5))
                .deliverPolicy(DeliverPolicy.Last)
                .ackPolicy(AckPolicy.None)
                .replayPolicy(ReplayPolicy.Original)
                .filterSubject(tsc.subject())
                .build();
            jsm.addOrUpdateConsumer(tsc.stream, cc);

            pushOpts = PushSubscribeOptions.bind(tsc.stream, durable(5));
            js.subscribe(tsc.subject(), pushOpts); // should not throw an error
        });
    }

    @Test
    public void testSubscribeDurableConsumerMustMatch() throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();

            String stream = stream();
            String subject = subject();
            createMemoryStream(nc, stream, subject);

            // push
            String uname = durable();
            String deliver = deliver();
            nc.jetStreamManagement().addOrUpdateConsumer(stream, pushDurableBuilder(subject, uname, deliver).build());

            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).deliverPolicy(DeliverPolicy.Last), "deliverPolicy");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).deliverPolicy(DeliverPolicy.New), "deliverPolicy");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).ackPolicy(AckPolicy.None), "ackPolicy");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).ackPolicy(AckPolicy.All), "ackPolicy");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).replayPolicy(ReplayPolicy.Original), "replayPolicy");

            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).flowControl(10000), "flowControl");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).headersOnly(true), "headersOnly");

            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).startTime(ZonedDateTime.now()), "startTime");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).ackWait(Duration.ofMillis(1)), "ackWait");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).description("x"), "description");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).sampleFrequency("x"), "sampleFrequency");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).idleHeartbeat(Duration.ofMillis(1000)), "idleHeartbeat");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).maxExpires(Duration.ofMillis(1000)), "maxExpires");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).inactiveThreshold(Duration.ofMillis(1000)), "inactiveThreshold");

            // value
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).maxDeliver(MAX_DELIVER_MIN), "maxDeliver");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).maxAckPending(0), "maxAckPending");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).ackWait(0), "ackWait");

            // value unsigned
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).startSequence(1), "startSequence");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).rateLimit(1), "rateLimit");

            // unset doesn't fail because the server provides a value equal to the unset
            changeOkPush(js, subject, pushDurableBuilder(subject, uname, deliver).maxDeliver(INTEGER_UNSET));

            // unset doesn't fail because the server does not provide a value
            // negatives are considered the unset
            changeOkPush(js, subject, pushDurableBuilder(subject, uname, deliver).startSequence(ULONG_UNSET));
            changeOkPush(js, subject, pushDurableBuilder(subject, uname, deliver).startSequence(-1));
            changeOkPush(js, subject, pushDurableBuilder(subject, uname, deliver).rateLimit(ULONG_UNSET));
            changeOkPush(js, subject, pushDurableBuilder(subject, uname, deliver).rateLimit(-1));

            // unset fail b/c the server does set a value that is not equal to the unset or the minimum
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).maxAckPending(LONG_UNSET), "maxAckPending");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).maxAckPending(0), "maxAckPending");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).ackWait(LONG_UNSET), "ackWait");
            changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).ackWait(0), "ackWait");

            // pull
            String lname = durable();
            nc.jetStreamManagement().addOrUpdateConsumer(stream, pullDurableBuilder(subject, lname).build());

            // value
            changeExPull(js, subject, pullDurableBuilder(subject, lname).maxPullWaiting(0), "maxPullWaiting");
            changeExPull(js, subject, pullDurableBuilder(subject, lname).maxBatch(0), "maxBatch");
            changeExPull(js, subject, pullDurableBuilder(subject, lname).maxBytes(0), "maxBytes");

            // unsets fail b/c the server does set a value
            changeExPull(js, subject, pullDurableBuilder(subject, lname).maxPullWaiting(-1), "maxPullWaiting");

            // unset
            changeOkPull(js, subject, pullDurableBuilder(subject, lname).maxBatch(-1));
            changeOkPull(js, subject, pullDurableBuilder(subject, lname).maxBytes(-1));

            // metadata
            Map<String, String> metadataA = new HashMap<>(); metadataA.put("a", "A");
            Map<String, String> metadataB = new HashMap<>(); metadataB.put("b", "B");

            if (atLeast2_10()) {
                // metadata server null versus new not null
                nc.jetStreamManagement().addOrUpdateConsumer(stream, pushDurableBuilder(subject, uname, deliver).build());
                changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).metadata(metadataA), "metadata");

                // metadata server not null versus new null
                nc.jetStreamManagement().addOrUpdateConsumer(stream, pushDurableBuilder(subject, uname, deliver).metadata(metadataA).build());
                changeOkPush(js, subject, pushDurableBuilder(subject, uname, deliver));

                // metadata server not null versus new not null but different
                changeExPush(js, subject, pushDurableBuilder(subject, uname, deliver).metadata(metadataB), "metadata");

                if (before2_11()) {
                    // metadata server not null versus new not null and same
                    changeOkPush(js, subject, pushDurableBuilder(subject, uname, deliver).metadata(metadataA));
                }
            }
        });
    }

    private void changeOkPush(JetStream js, String subject, Builder builder) throws IOException, JetStreamApiException {
        unsubscribeEnsureNotBound(js.subscribe(subject, builder.buildPushSubscribeOptions()));
    }

    private void changeOkPull(JetStream js, String subject, Builder builder) throws IOException, JetStreamApiException {
        unsubscribeEnsureNotBound(js.subscribe(subject, builder.buildPullSubscribeOptions()));
    }

    private void changeExPush(JetStream js, String subject, Builder builder, String changedField) {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> js.subscribe(subject, PushSubscribeOptions.builder().configuration(builder.build()).build()));
        _changeEx(iae, changedField);
    }

    private void changeExPull(JetStream js, String subject, Builder builder, String changedField) {
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> js.subscribe(subject, PullSubscribeOptions.builder().configuration(builder.build()).build()));
        _changeEx(iae, changedField);
    }

    private void _changeEx(IllegalArgumentException iae, String changedField) {
        String iaeMessage = iae.getMessage();
        assertTrue(iaeMessage.contains(JsSubExistingConsumerCannotBeModified.id()));
        assertTrue(iaeMessage.contains(changedField));
    }

    private Builder pushDurableBuilder(String subject, String durable, String deliver) {
        return builder().durable(durable).deliverSubject(deliver).filterSubject(subject);
    }

    private Builder pullDurableBuilder(String subject, String durable) {
        return builder().durable(durable).filterSubject(subject);
    }

    @Test
    public void testGetConsumerInfoFromSubscription() throws Exception {
        jsServer.run(nc -> {
            // Create our JetStream context.
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            JetStreamSubscription sub = js.subscribe(tsc.subject());
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            ConsumerInfo ci = sub.getConsumerInfo();
            assertEquals(tsc.stream, ci.getStreamName());
        });
    }

    @Test
    public void testInternalLookupConsumerInfoCoverage() throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // - consumer not found
            // - stream does not exist
            JetStreamSubscription sub = js.subscribe(tsc.subject());
            assertNull(((NatsJetStream)js).lookupConsumerInfo(tsc.stream, tsc.consumerName()));
            assertThrows(JetStreamApiException.class,
                    () -> ((NatsJetStream)js).lookupConsumerInfo(stream(999), tsc.consumerName()));
        });
    }

    @Test
    public void testGetJetStreamValidatedConnectionCoverage() {
        NatsJetStreamMessage njsm = new NatsJetStreamMessage(null);

        IllegalStateException ise = assertThrows(IllegalStateException.class, njsm::getJetStreamValidatedConnection);
        assertTrue(ise.getMessage().contains("subscription"));

        // make a dummy connection so we can make a subscription
        Options options = Options.builder().build();
        NatsConnection nc = new NatsConnection(options);
        njsm.subscription = new NatsSubscription("sid", "sub", "q", nc, null);
        // remove the connection so we can test the coverage
        njsm.subscription.connection = null;
        ise = assertThrows(IllegalStateException.class, njsm::getJetStreamValidatedConnection);
        assertTrue(ise.getMessage().contains("connection"));
    }

    static class TimeCheckLogicLogger implements TimeTraceLogger {
        public String lastTrace;
        @Override
        public void trace(String format, Object... args) {
            lastTrace = String.format(format, args);
        }
    }

    @Test
    public void testNatsConnectionTimeCheckLogic() {
        TimeCheckLogicLogger l = new TimeCheckLogicLogger();
        // make a dummy connection so we can make a subscription
        Options options = Options.builder()
            .timeTraceLogger(l)
            .build();
        NatsConnection nc = new NatsConnection(options);

        nc.traceTimeCheck("zero", 0);
        assertEquals("zero, 0 (ns) remaining", l.lastTrace);

        nc.traceTimeCheck("gt 0, lt 1_000_000", 1);
        assertEquals("gt 0, lt 1_000_000, 1 (ns) remaining", l.lastTrace);

        nc.traceTimeCheck("gt 0, lt 1_000_000_000", 1_000_000);
        assertEquals("gt 0, lt 1_000_000_000, 1 (ms) remaining", l.lastTrace);

        nc.traceTimeCheck("gt 0, gt 1_000_000_000", 1_100_000_000);
        assertEquals("gt 0, gt 1_000_000_000, 1.100 (s) remaining", l.lastTrace);

        nc.traceTimeCheck("lt 0, gt -1_000_000", -1);
        assertEquals("lt 0, gt -1_000_000, 1 (ns) beyond timeout", l.lastTrace);

        nc.traceTimeCheck("lt 0, gt -1_000_000_000", -1_000_000);
        assertEquals("lt 0, gt -1_000_000_000, 1 (ms) beyond timeout", l.lastTrace);

        nc.traceTimeCheck("lt 0, lt -1_000_000_000", -1_100_000_000);
        assertEquals("lt 0, lt -1_000_000_000, 1.100 (s) beyond timeout", l.lastTrace);
    }

    @Test
    public void testMoreCreateSubscriptionErrors() throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();

            IllegalStateException ise = assertThrows(IllegalStateException.class, () -> js.subscribe(SUBJECT));
            assertTrue(ise.getMessage().contains(JsSubNoMatchingStreamForSubject.id()));

            // create the stream.
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            // general pull push validation
            ConsumerConfiguration ccCantHave = builder().durable("pulldur").deliverGroup("cantHave").build();
            PullSubscribeOptions pullCantHaveDlvrGrp = PullSubscribeOptions.builder().configuration(ccCantHave).build();
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), pullCantHaveDlvrGrp));
            assertTrue(iae.getMessage().contains(JsSubPullCantHaveDeliverGroup.id()));

            ccCantHave = builder().durable("pulldur").deliverSubject("cantHave").build();
            PullSubscribeOptions pullCantHaveDlvrSub = PullSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), pullCantHaveDlvrSub));
            assertTrue(iae.getMessage().contains(JsSubPullCantHaveDeliverSubject.id()));

            ccCantHave = builder().maxPullWaiting(1L).build();
            PushSubscribeOptions pushCantHaveMpw = PushSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), pushCantHaveMpw));
            assertTrue(iae.getMessage().contains(JsSubPushCantHaveMaxPullWaiting.id()));

            ccCantHave = builder().maxBatch(1L).build();
            PushSubscribeOptions pushCantHaveMb = PushSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), pushCantHaveMb));
            assertTrue(iae.getMessage().contains(JsSubPushCantHaveMaxBatch.id()));

            ccCantHave = builder().maxBytes(1L).build();
            PushSubscribeOptions pushCantHaveMby = PushSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), pushCantHaveMby));
            assertTrue(iae.getMessage().contains(JsSubPushCantHaveMaxBytes.id()));

            // create some consumers
            PushSubscribeOptions psoDurNoQ = PushSubscribeOptions.builder().durable("durNoQ").build();
            js.subscribe(tsc.subject(), psoDurNoQ);

            PushSubscribeOptions psoDurYesQ = PushSubscribeOptions.builder().durable("durYesQ").build();
            js.subscribe(tsc.subject(), "yesQ", psoDurYesQ);

            // already bound
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), psoDurNoQ));
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyBound.id()));

            // queue match
            PushSubscribeOptions qmatch = PushSubscribeOptions.builder()
                .durable("qmatchdur").deliverGroup("qmatchq").build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), "qnotmatch", qmatch));
            assertTrue(iae.getMessage().contains(JsSubQueueDeliverGroupMismatch.id()));

            // queue vs config
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), "notConfigured", psoDurNoQ));
            assertTrue(iae.getMessage().contains(JsSubExistingConsumerNotQueue.id()));

            PushSubscribeOptions psoNoVsYes = PushSubscribeOptions.builder().durable("durYesQ").build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), psoNoVsYes));
            assertTrue(iae.getMessage().contains(JsSubExistingConsumerIsQueue.id()));

            PushSubscribeOptions psoYesVsNo = PushSubscribeOptions.builder().durable("durYesQ").build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), "qnotmatch", psoYesVsNo));
            assertTrue(iae.getMessage().contains(JsSubExistingQueueDoesNotMatchRequestedQueue.id()));

            // flow control heartbeat push / pull
            ConsumerConfiguration ccFc = builder().durable("ccFcDur").flowControl(1000).build();
            ConsumerConfiguration ccHb = builder().durable("ccHbDur").idleHeartbeat(1000).build();

            PullSubscribeOptions psoPullCcFc = PullSubscribeOptions.builder().configuration(ccFc).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), psoPullCcFc));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidPull.id()));

            PullSubscribeOptions psoPullCcHb = PullSubscribeOptions.builder().configuration(ccHb).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), psoPullCcHb));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidPull.id()));

            PushSubscribeOptions psoPushCcFc = PushSubscribeOptions.builder().configuration(ccFc).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), "cantHaveQ", psoPushCcFc));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidQueue.id()));

            PushSubscribeOptions psoPushCcHb = PushSubscribeOptions.builder().configuration(ccHb).build();
            iae = assertThrows(IllegalArgumentException.class, () -> js.subscribe(tsc.subject(), "cantHaveQ", psoPushCcHb));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidQueue.id()));
        });
    }
}