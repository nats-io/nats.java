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
import io.nats.client.support.NatsJetStreamUtil;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.nats.client.NatsTestServer.configuredJsServer;
import static io.nats.client.api.ConsumerConfiguration.*;
import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static io.nats.client.utils.ConnectionUtils.longConnectionWait;
import static io.nats.client.utils.ConnectionUtils.standardConnectionWait;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.VersionUtils.*;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamGeneralTests extends JetStreamTestBase {

    @Test
    public void testJetStreamContextCreate() throws Exception {
        runInShared((nc, jstc) -> {
            jstc.jsm.getAccountStatistics(); // another management
            jstc.js.publish(jstc.subject(), dataBytes(1));
        });
    }

    @Test
    public void testJetNotEnabled() throws Exception {
        runInOwnServer(nc -> {
            // get normal context, try to do an operation
            JetStream js = nc.jetStream();
            assertThrows(IOException.class, () -> js.subscribe(random()));

            // get management context, try to do an operation
            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(IOException.class, jsm::getAccountStatistics);
        });
    }

    @Test
    public void testJetEnabledGoodAccount() throws Exception {
        try (NatsTestServer ts = configuredJsServer("js_authorization.conf")) {
            Options options = optionsBuilder(ts)
                .userInfo("serviceup".toCharArray(), "uppass".toCharArray()).build();
            try (Connection nc = longConnectionWait(options)) {
                nc.jetStreamManagement();
                nc.jetStream();
            }
        }
    }

    @Test
    public void testJetStreamPublishDefaultOptions() throws Exception {
        runInShared((nc, jstc) -> {
            PublishAck ack = jsPublish(jstc.js, jstc.subject());
            assertEquals(1, ack.getSeqno());
        });
    }

    @Test
    public void testExceptionsAndCoverage() throws Exception {
        runInSharedOwnNc(nc -> {
            JetStreamOptions jso = JetStreamOptions.builder().build();
            nc.jetStream(jso);
            nc.jetStreamManagement(jso);

            nc.close();
            assertThrows(IOException.class, nc::jetStream);
            assertThrows(IOException.class, nc::jetStreamManagement);
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
        runInShared((nc, jstc) -> {
            jsPublish(jstc.js, jstc.subject());

            // default ephemeral subscription.
            Subscription s = jstc.js.subscribe(jstc.subject());
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            List<String> names = jstc.jsm.getConsumerNames(jstc.stream);
            assertEquals(1, names.size());

            // default subscribe options // ephemeral subscription.
            s = jstc.js.subscribe(jstc.subject(), PushSubscribeOptions.builder().build());
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jstc.jsm.getConsumerNames(jstc.stream);
            assertEquals(2, names.size());

            // set the stream
            String durable = random();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().stream(jstc.stream).durable(durable).build();
            s = jstc.js.subscribe(jstc.subject(), pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(DATA, new String(m.getData()));
            names = jstc.jsm.getConsumerNames(jstc.stream);
            assertEquals(3, names.size());

            // coverage
            Dispatcher dispatcher = nc.createDispatcher();
            jstc.js.subscribe(jstc.subject());
            jstc.js.subscribe(jstc.subject(), (PushSubscribeOptions)null);
            jstc.js.subscribe(jstc.subject(), random(), null);
            jstc.js.subscribe(jstc.subject(), dispatcher, mh -> {}, false);
            jstc.js.subscribe(jstc.subject(), dispatcher, mh -> {}, false, null);
            jstc.js.subscribe(jstc.subject(), random(), dispatcher, mh -> {}, false, null);

            // bind with w/o subject
            durable = random();
            String deliver = random();
            jstc.jsm.addOrUpdateConsumer(jstc.stream,
                builder()
                    .durable(durable)
                    .deliverSubject(deliver)
                    .build());

            PushSubscribeOptions psoBind = PushSubscribeOptions.bind(jstc.stream, durable);
            unsubscribeEnsureNotBound(jstc.js.subscribe(null, psoBind));
            unsubscribeEnsureNotBound(jstc.js.subscribe("", psoBind));
            JetStreamSubscription sub = jstc.js.subscribe(null, dispatcher, mh -> {}, false, psoBind);
            unsubscribeEnsureNotBound(dispatcher, sub);
            jstc.js.subscribe("", dispatcher, mh -> {}, false, psoBind);

            durable = random();
            deliver = random();
            String queue = random();
            jstc.jsm.addOrUpdateConsumer(jstc.stream,
                builder()
                    .durable(durable)
                    .deliverSubject(deliver)
                    .deliverGroup(queue)
                    .build());

            psoBind = PushSubscribeOptions.bind(jstc.stream, durable);
            unsubscribeEnsureNotBound(jstc.js.subscribe(null, queue, psoBind));
            unsubscribeEnsureNotBound(jstc.js.subscribe("", queue, psoBind));
            sub = jstc.js.subscribe(null, queue, dispatcher, mh -> {}, false, psoBind);
            unsubscribeEnsureNotBound(dispatcher, sub);
            jstc.js.subscribe("", queue, dispatcher, mh -> {}, false, psoBind);

            if (atLeast2_9_0(nc)) {
                String name = random();
                ConsumerConfiguration cc = builder().name(name).build();
                pso = PushSubscribeOptions.builder().configuration(cc).build();
                sub = jstc.js.subscribe(jstc.subject(), pso);
                m = sub.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                assertEquals(DATA, new String(m.getData()));
                ConsumerInfo ci = sub.getConsumerInfo();
                assertEquals(name, ci.getName());
                assertEquals(name, ci.getConsumerConfiguration().getName());
                assertNull(ci.getConsumerConfiguration().getDurable());

                durable = random();
                cc = builder().durable(durable).build();
                pso = PushSubscribeOptions.builder().configuration(cc).build();
                sub = jstc.js.subscribe(jstc.subject(), pso);
                m = sub.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                assertEquals(DATA, new String(m.getData()));
                ci = sub.getConsumerInfo();
                assertEquals(durable, ci.getName());
                assertEquals(durable, ci.getConsumerConfiguration().getName());
                assertEquals(durable, ci.getConsumerConfiguration().getDurable());

                String durName = random();
                cc = builder().durable(durName).name(durName).build();
                pso = PushSubscribeOptions.builder().configuration(cc).build();
                sub = jstc.js.subscribe(jstc.subject(), pso);
                m = sub.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                assertEquals(DATA, new String(m.getData()));
                ci = sub.getConsumerInfo();
                assertEquals(durName, ci.getName());
                assertEquals(durName, ci.getConsumerConfiguration().getName());
                assertEquals(durName, ci.getConsumerConfiguration().getDurable());

                // test opt out
                JetStreamOptions jso = JetStreamOptions.builder().optOut290ConsumerCreate(true).build();
                JetStream jsOptOut = nc.jetStream(jso);
                ConsumerConfiguration ccOptOut = builder().name(random()).build();
                PushSubscribeOptions psoOptOut = PushSubscribeOptions.builder().configuration(ccOptOut).build();
                assertClientError(JsConsumerCreate290NotAvailable, () -> jsOptOut.subscribe(jstc.subject(), psoOptOut));
            }
        });
    }

    @Test
    public void testJetStreamSubscribeLenientSubject() throws Exception {
        runInShared((nc, jstc) -> {
            Dispatcher d = nc.createDispatcher();

            jstc.js.subscribe(jstc.subject(), (PushSubscribeOptions)null);
            jstc.js.subscribe(jstc.subject(), null, (PushSubscribeOptions)null); // queue name is not required, just a weird way to call this api
            jstc.js.subscribe(jstc.subject(), d, m -> {}, false, (PushSubscribeOptions)null);
            jstc.js.subscribe(jstc.subject(), null, d, m -> {}, false, (PushSubscribeOptions)null); // queue name is not required, just a weird way to call this api

            PushSubscribeOptions pso = ConsumerConfiguration.builder().filterSubject(jstc.subject()).buildPushSubscribeOptions();
            jstc.js.subscribe(null, pso);
            jstc.js.subscribe(null, null, pso);
            jstc.js.subscribe(null, d, m -> {}, false, pso);
            jstc.js.subscribe(null, null, d, m -> {}, false, pso);

            PushSubscribeOptions psoF = ConsumerConfiguration.builder().buildPushSubscribeOptions();

            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, psoF));
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, psoF));
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, null, psoF));
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, d, m -> {}, false, psoF));
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, null, d, m -> {}, false, psoF));

            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, (PushSubscribeOptions)null));
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, (PushSubscribeOptions)null));
            //noinspection RedundantCast
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, null, (PushSubscribeOptions)null));
            //noinspection RedundantCast
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, d, m -> {}, false, (PushSubscribeOptions)null));
            //noinspection RedundantCast
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, null, d, m -> {}, false, (PushSubscribeOptions)null));

            PullSubscribeOptions lso = ConsumerConfiguration.builder().filterSubject(jstc.subject()).buildPullSubscribeOptions();
            jstc.js.subscribe(null, lso);
            jstc.js.subscribe(null, d, m -> {}, lso);

            PullSubscribeOptions lsoF = ConsumerConfiguration.builder().buildPullSubscribeOptions();
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, lsoF));
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, d, m -> {}, lsoF));

            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, (PullSubscribeOptions)null));
            //noinspection RedundantCast
            assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(null, d, m -> {}, (PullSubscribeOptions)null));
        });
    }

    @Test
    public void testJetStreamSubscribeErrors() throws Exception {
        runInShared((nc, jstc) -> {
            String stream = random();
            // stream not found
            PushSubscribeOptions psoInvalidStream = PushSubscribeOptions.builder().stream(stream).build();
            assertThrows(JetStreamApiException.class, () -> jstc.js.subscribe(random(), psoInvalidStream));

            Dispatcher d = nc.createDispatcher();

            for (String bad : BAD_SUBJECTS_OR_QUEUES) {
                if (bad == null || bad.isEmpty()) {
                    // subject
                    IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(bad));
                    assertTrue(iae.getMessage().startsWith("Subject"));
                    assertClientError(JsSubSubjectNeededToLookupStream, () -> jstc.js.subscribe(bad, (PushSubscribeOptions)null));
                }
                else {
                    // subject
                    IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(bad));
                    assertTrue(iae.getMessage().startsWith("Subject"));
                    iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(bad, (PushSubscribeOptions)null));
                    assertTrue(iae.getMessage().startsWith("Subject"));

                    // queue
                    iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(random(), bad, null));
                    assertTrue(iae.getMessage().startsWith("Queue"));
                    iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(random(), bad, d, m -> {}, false, null));
                    assertTrue(iae.getMessage().startsWith("Queue"));
                }
            }

            // dispatcher
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(random(), null, null, false));
            assertTrue(iae.getMessage().startsWith("Dispatcher"));
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(random(), null, null, false, null));
            assertTrue(iae.getMessage().startsWith("Dispatcher"));
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(random(), random(), null, null, false, null));
            assertTrue(iae.getMessage().startsWith("Dispatcher"));

            // handler
            Dispatcher dispatcher = nc.createDispatcher();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(random(), dispatcher, null, false));
            assertTrue(iae.getMessage().startsWith("Handler"));
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(random(), dispatcher, null, false, null));
            assertTrue(iae.getMessage().startsWith("Handler"));
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(random(), random(), dispatcher, null, false, null));
            assertTrue(iae.getMessage().startsWith("Handler"));
        });
    }

    @Test
    public void testFilterSubjectEphemeral() throws Exception {
        runInSharedCustomStream((nc, jstc) -> {
            String subject = random();
            String subjectWild = subject + ".*";
            String subjectA = subject + ".A";
            String subjectB = subject + ".B";
            jstc.createStream(subjectWild);

            jsPublish(jstc.js, subjectA, 1);
            jsPublish(jstc.js, subjectB, 1);
            jsPublish(jstc.js, subjectA, 1);
            jsPublish(jstc.js, subjectB, 1);

            // subscribe to the wildcard
            ConsumerConfiguration cc = builder().ackPolicy(AckPolicy.None).build();
            PushSubscribeOptions pso = PushSubscribeOptions.builder().configuration(cc).build();
            JetStreamSubscription sub = jstc.js.subscribe(subjectWild, pso);
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
            sub = jstc.js.subscribe(subjectA, pso);
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
            sub = jstc.js.subscribe(subjectB, pso);
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

        try (NatsTestServer ts = configuredJsServer("js_prefix.conf")) {
            Options optionsSrc = optionsBuilder(ts)
                    .userInfo("src".toCharArray(), "spass".toCharArray()).build();

            Options optionsTar = optionsBuilder(ts)
                    .userInfo("tar".toCharArray(), "tpass".toCharArray()).build();

            try (Connection ncSrc = standardConnectionWait(optionsSrc);
                 Connection ncTar = standardConnectionWait(optionsTar)
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
        runInShared((nc, jstc) -> {
            jsPublish(jstc.js, jstc.subject(), 1, 1);
            PushSubscribeOptions pso = PushSubscribeOptions.builder()
                    .durable(jstc.consumerName())
                    .build();
            JetStreamSubscription s = jstc.js.subscribe(jstc.subject(), pso);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(1), new String(m.getData()));
            m.ack();
            unsubscribeEnsureNotBound(s);

            jsPublish(jstc.js, jstc.subject(), 2, 1);
            pso = PushSubscribeOptions.builder()
                    .stream(jstc.stream)
                    .durable(jstc.consumerName())
                    .bind(true)
                    .build();
            s = jstc.js.subscribe(jstc.subject(), pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(2), new String(m.getData()));
            m.ack();
            unsubscribeEnsureNotBound(s);

            jsPublish(jstc.js, jstc.subject(), 3, 1);
            pso = PushSubscribeOptions.bind(jstc.stream, jstc.consumerName());
            s = jstc.js.subscribe(jstc.subject(), pso);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(3), new String(m.getData()));

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().stream(jstc.stream).bind(true).build());

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().durable(jstc.consumerName()).bind(true).build());

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().stream(EMPTY).bind(true).build());

            assertThrows(IllegalArgumentException.class,
                    () -> PushSubscribeOptions.builder().stream(jstc.stream).durable(EMPTY).bind(true).build());
        });
    }

    @Test
    public void testBindPull() throws Exception {
        runInShared((nc, jstc) -> {
            jsPublish(jstc.js, jstc.subject(), 1, 1);

            PullSubscribeOptions pso = PullSubscribeOptions.builder()
                    .durable(jstc.consumerName())
                    .build();
            JetStreamSubscription s = jstc.js.subscribe(jstc.subject(), pso);
            s.pull(1);
            Message m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(1), new String(m.getData()));
            m.ack();
            unsubscribeEnsureNotBound(s);

            jsPublish(jstc.js, jstc.subject(), 2, 1);
            pso = PullSubscribeOptions.builder()
                    .stream(jstc.stream)
                    .durable(jstc.consumerName())
                    .bind(true)
                    .build();
            s = jstc.js.subscribe(jstc.subject(), pso);
            s.pull(1);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(2), new String(m.getData()));
            m.ack();
            unsubscribeEnsureNotBound(s);

            jsPublish(jstc.js, jstc.subject(), 3, 1);
            pso = PullSubscribeOptions.bind(jstc.stream, jstc.consumerName());
            s = jstc.js.subscribe(jstc.subject(), pso);
            s.pull(1);
            m = s.nextMessage(DEFAULT_TIMEOUT);
            assertNotNull(m);
            assertEquals(data(3), new String(m.getData()));
        });
    }

    @Test
    public void testBindErrors() throws Exception {
        runInShared((nc, jstc) -> {
            // bind errors
            PushSubscribeOptions pushbinderr = PushSubscribeOptions.bind(jstc.stream, "binddur");
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), pushbinderr));
            assertTrue(iae.getMessage().contains(JsSubConsumerNotFoundRequiredInBind.id()));

            PullSubscribeOptions pullbinderr = PullSubscribeOptions.bind(jstc.stream, "binddur");
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), pullbinderr));
            assertTrue(iae.getMessage().contains(JsSubConsumerNotFoundRequiredInBind.id()));
        });
    }

    @Test
    public void testFilterMismatchErrors() throws Exception {
        runInOwnJsServer((nc, jsm, js) -> {
            String stream = random();
            String subject = random();

            createMemoryStream(nc, stream, subject);

            // will work as SubscribeSubject equals Filter Subject
            filterMatchSubscribeOk(js, jsm, stream, subject, subject);
            filterMatchSubscribeOk(js, jsm, stream, ">", ">");
            filterMatchSubscribeOk(js, jsm, stream, "*", "*");

            // will not work
            filterMatchSubscribeEx(js, jsm, stream, subject, "");
            filterMatchSubscribeEx(js, jsm, stream, subject, ">");
            filterMatchSubscribeEx(js, jsm, stream, subject, "*");

            // multiple subjects no wildcards
            jsm.deleteStream(stream);
            createMemoryStream(jsm, stream, subject, subject(2));

            // will work as SubscribeSubject equals Filter Subject
            filterMatchSubscribeOk(js, jsm, stream, subject, subject);
            filterMatchSubscribeOk(js, jsm, stream, ">", ">");
            filterMatchSubscribeOk(js, jsm, stream, "*", "*");

            // will not work because stream has more than 1 subject
            filterMatchSubscribeEx(js, jsm, stream, subject, "");
            filterMatchSubscribeEx(js, jsm, stream, subject, ">");
            filterMatchSubscribeEx(js, jsm, stream, subject, "*");

            String subjectGt = subject + ".>";
            String subjectStar = subject + ".*";
            String subjectDot = subject + "." + random();

            // multiple subjects via '>'
            jsm.deleteStream(stream);
            createMemoryStream(jsm, stream, subjectGt);

            // will work, exact matches
            filterMatchSubscribeOk(js, jsm, stream, subjectDot, subjectDot);
            filterMatchSubscribeOk(js, jsm, stream, ">", ">");

            // will not work because mismatch / stream has more than 1 subject
            filterMatchSubscribeEx(js, jsm, stream, subjectDot, "");
            filterMatchSubscribeEx(js, jsm, stream, subjectDot, ">");
            filterMatchSubscribeEx(js, jsm, stream, subjectDot, subjectGt);

            // multiple subjects via '*'
            jsm.deleteStream(stream);
            createMemoryStream(jsm, stream, subjectStar);

            // will work, exact matches
            filterMatchSubscribeOk(js, jsm, stream, subjectDot, subjectDot);
            filterMatchSubscribeOk(js, jsm, stream, ">", ">");

            // will not work because mismatch / stream has more than 1 subject
            filterMatchSubscribeEx(js, jsm, stream, subjectDot, "");
            filterMatchSubscribeEx(js, jsm, stream, subjectDot, ">");
            filterMatchSubscribeEx(js, jsm, stream, subjectDot, subjectStar);
        });
    }

    private void filterMatchSubscribeOk(JetStream js, JetStreamManagement jsm, String stream, String subscribeSubject, String... filterSubjects) throws IOException, JetStreamApiException {
        String deliver = random();
        String dur = random();
        filterMatchSetupConsumer(jsm, deliver, dur, stream, filterSubjects);
        unsubscribeEnsureNotBound(js.subscribe(subscribeSubject, builder().durable(dur).buildPushSubscribeOptions()));
    }

    private void filterMatchSubscribeEx(JetStream js, JetStreamManagement jsm, String stream, String subscribeSubject, String... filterSubjects) throws IOException, JetStreamApiException {
        String deliver = random();
        String dur = random();
        filterMatchSetupConsumer(jsm, deliver, dur, stream, filterSubjects);
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
            () -> js.subscribe(subscribeSubject, builder().durable(dur).buildPushSubscribeOptions()));
        assertTrue(iae.getMessage().contains(JsSubSubjectDoesNotMatchFilter.id()));
    }

    private void filterMatchSetupConsumer(JetStreamManagement jsm, String deliver, String dur, String stream, String... fs) throws IOException, JetStreamApiException {
        jsm.addOrUpdateConsumer(stream,
            builder().deliverSubject(deliver).durable(dur).filterSubjects(fs).build());
    }

    @Test
    public void testBindDurableDeliverSubject() throws Exception {
        runInShared((nc, jstc) -> {
            // create a durable push subscriber - has a deliver subject
            String dur1 = random();
            String dur2 = random();
            String deliver = random();
            ConsumerConfiguration ccDurPush = builder()
                .durable(dur1)
                .deliverSubject(deliver)
                .filterSubject(jstc.subject())
                .build();
            jstc.jsm.addOrUpdateConsumer(jstc.stream, ccDurPush);

            // create a durable pull subscriber - notice no deliver subject
            ConsumerConfiguration ccDurPull = builder()
                .durable(dur2)
                .filterSubject(jstc.subject())
                .build();
            jstc.jsm.addOrUpdateConsumer(jstc.stream, ccDurPull);

            // try to pull subscribe against a push durable
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> jstc.js.subscribe(jstc.subject(), PullSubscribeOptions.builder().durable(dur1).build())
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPush.id()));

            // try to pull bind against a push durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> jstc.js.subscribe(jstc.subject(), PullSubscribeOptions.bind(jstc.stream, dur1))
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPush.id()));

            // try to push subscribe against a pull durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> jstc.js.subscribe(jstc.subject(), PushSubscribeOptions.builder().durable(dur2).build())
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPull.id()));

            // try to push bind against a pull durable
            iae = assertThrows(IllegalArgumentException.class,
                    () -> jstc.js.subscribe(jstc.subject(), PushSubscribeOptions.bind(jstc.stream, dur2))
            );
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyConfiguredAsPull.id()));

            // this one is okay
            jstc.js.subscribe(jstc.subject(), PushSubscribeOptions.builder().durable(dur1).build());
        });
    }

    @Test
    public void testConsumerIsNotModified() throws Exception {
        runInShared((nc, jstc) -> {
            // test with config in issue 105
            String dur = random();
            String q = random();
            ConsumerConfiguration cc = builder()
                .description("desc")
                .ackPolicy(AckPolicy.Explicit)
                .deliverPolicy(DeliverPolicy.All)
                .deliverSubject(random())
                .deliverGroup(q)
                .durable(dur)
                .maxAckPending(65000)
                .maxDeliver(5)
                .maxBatch(10)
                .maxBytes(11)
                .replayPolicy(ReplayPolicy.Instant)
                .filterSubject(jstc.subject())
                .build();
            jstc.jsm.addOrUpdateConsumer(jstc.stream, cc);

            PushSubscribeOptions pushOpts = PushSubscribeOptions.bind(jstc.stream, dur);
            jstc.js.subscribe(jstc.subject(), q, pushOpts); // should not throw an error

            // testing numerics
            dur = random();
            cc = builder()
                .deliverPolicy(DeliverPolicy.ByStartSequence)
                .deliverSubject(random())
                .durable(dur)
                .startSequence(42)
                .maxDeliver(43)
                .maxBatch(47)
                .maxBytes(48)
                .rateLimit(44)
                .maxAckPending(45)
                .filterSubject(jstc.subject())
                .build();
            jstc.jsm.addOrUpdateConsumer(jstc.stream, cc);

            pushOpts = PushSubscribeOptions.bind(jstc.stream, dur);
            jstc.js.subscribe(jstc.subject(), pushOpts); // should not throw an error

            dur = random();
            cc = builder()
                .durable(dur)
                .maxPullWaiting(46)
                .filterSubject(jstc.subject())
                .build();
            jstc.jsm.addOrUpdateConsumer(jstc.stream, cc);

            PullSubscribeOptions pullOpts = PullSubscribeOptions.bind(jstc.stream, dur);
            jstc.js.subscribe(jstc.subject(), pullOpts); // should not throw an error

            // testing DateTime
            dur = random();
            cc = builder()
                .deliverPolicy(DeliverPolicy.ByStartTime)
                .deliverSubject(random())
                .durable(dur)
                .startTime(ZonedDateTime.now().plusHours(1))
                .filterSubject(jstc.subject())
                .build();
            jstc.jsm.addOrUpdateConsumer(jstc.stream, cc);

            pushOpts = PushSubscribeOptions.bind(jstc.stream, dur);
            jstc.js.subscribe(jstc.subject(), pushOpts); // should not throw an error

            // testing boolean and duration
            dur = random();
            cc = builder()
                .deliverSubject(random())
                .durable(dur)
                .flowControl(1000)
                .headersOnly(true)
                .maxExpires(30000)
                .ackWait(2000)
                .filterSubject(jstc.subject())
                .build();
            jstc.jsm.addOrUpdateConsumer(jstc.stream, cc);

            pushOpts = PushSubscribeOptions.bind(jstc.stream, dur);
            jstc.js.subscribe(jstc.subject(), pushOpts); // should not throw an error

            // testing enums
            dur = random();
            cc = builder()
                .deliverSubject(random())
                .durable(dur)
                .deliverPolicy(DeliverPolicy.Last)
                .ackPolicy(AckPolicy.None)
                .replayPolicy(ReplayPolicy.Original)
                .filterSubject(jstc.subject())
                .build();
            jstc.jsm.addOrUpdateConsumer(jstc.stream, cc);

            pushOpts = PushSubscribeOptions.bind(jstc.stream, dur);
            jstc.js.subscribe(jstc.subject(), pushOpts); // should not throw an error
        });
    }

    @Test
    public void testSubscribeDurableConsumerMustMatch() throws Exception {
        runInShared((nc, jstc) -> {
            String stream = jstc.stream;
            String subject = jstc.subject();

            // push
            String uname = random();
            String deliver = random();
            nc.jetStreamManagement().addOrUpdateConsumer(stream, pushDurableBuilder(subject, uname, deliver).build());

            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).deliverPolicy(DeliverPolicy.Last), "deliverPolicy");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).deliverPolicy(DeliverPolicy.New), "deliverPolicy");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).ackPolicy(AckPolicy.None), "ackPolicy");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).ackPolicy(AckPolicy.All), "ackPolicy");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).replayPolicy(ReplayPolicy.Original), "replayPolicy");

            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).flowControl(10000), "flowControl");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).headersOnly(true), "headersOnly");

            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).startTime(ZonedDateTime.now()), "startTime");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).ackWait(Duration.ofMillis(1)), "ackWait");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).description("x"), "description");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).sampleFrequency("x"), "sampleFrequency");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).idleHeartbeat(Duration.ofMillis(1000)), "idleHeartbeat");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).maxExpires(Duration.ofMillis(1000)), "maxExpires");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).inactiveThreshold(Duration.ofMillis(1000)), "inactiveThreshold");

            // value
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).maxDeliver(MAX_DELIVER_MIN), "maxDeliver");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).maxAckPending(0), "maxAckPending");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).ackWait(0), "ackWait");

            // value unsigned
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).startSequence(1), "startSequence");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).rateLimit(1), "rateLimit");

            // unset doesn't fail because the server provides a value equal to the unset
            changeOkPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).maxDeliver(INTEGER_UNSET));

            // unset doesn't fail because the server does not provide a value
            // negatives are considered the unset
            changeOkPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).startSequence(ULONG_UNSET));
            changeOkPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).startSequence(-1));
            changeOkPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).rateLimit(ULONG_UNSET));
            changeOkPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).rateLimit(-1));

            // unset fail b/c the server does set a value that is not equal to the unset or the minimum
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).maxAckPending(LONG_UNSET), "maxAckPending");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).maxAckPending(0), "maxAckPending");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).ackWait(LONG_UNSET), "ackWait");
            changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).ackWait(0), "ackWait");

            // pull
            String lname = random();
            nc.jetStreamManagement().addOrUpdateConsumer(stream, pullDurableBuilder(subject, lname).build());

            // value
            changeExPull(jstc.js, subject, pullDurableBuilder(subject, lname).maxPullWaiting(0), "maxPullWaiting");
            changeExPull(jstc.js, subject, pullDurableBuilder(subject, lname).maxBatch(0), "maxBatch");
            changeExPull(jstc.js, subject, pullDurableBuilder(subject, lname).maxBytes(0), "maxBytes");

            // unsets fail b/c the server does set a value
            changeExPull(jstc.js, subject, pullDurableBuilder(subject, lname).maxPullWaiting(-1), "maxPullWaiting");

            // unset
            changeOkPull(jstc.js, subject, pullDurableBuilder(subject, lname).maxBatch(-1));
            changeOkPull(jstc.js, subject, pullDurableBuilder(subject, lname).maxBytes(-1));

            // metadata
            Map<String, String> metadataA = new HashMap<>(); metadataA.put("a", "A");
            Map<String, String> metadataB = new HashMap<>(); metadataB.put("b", "B");

            if (atLeast2_10()) {
                // metadata server null versus new not null
                nc.jetStreamManagement().addOrUpdateConsumer(stream, pushDurableBuilder(subject, uname, deliver).build());
                changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).metadata(metadataA), "metadata");

                // metadata server not null versus new null
                nc.jetStreamManagement().addOrUpdateConsumer(stream, pushDurableBuilder(subject, uname, deliver).metadata(metadataA).build());
                changeOkPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver));

                // metadata server not null versus new not null but different
                changeExPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).metadata(metadataB), "metadata");

                if (before2_11()) {
                    // metadata server not null versus new not null and same
                    changeOkPush(jstc.js, subject, pushDurableBuilder(subject, uname, deliver).metadata(metadataA));
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
        runInShared((nc, jstc) -> {
            JetStreamSubscription sub = jstc.js.subscribe(jstc.subject());
            nc.flush(Duration.ofSeconds(1)); // flush outgoing communication with/to the server

            ConsumerInfo ci = sub.getConsumerInfo();
            assertEquals(jstc.stream, ci.getStreamName());
        });
    }

    @Test
    public void testInternalLookupConsumerInfoCoverage() throws Exception {
        runInShared((nc, jstc) -> {
            // - consumer not found
            // - stream does not exist
            jstc.js.subscribe(jstc.subject());
            assertNull(jstc.js.lookupConsumerInfo(jstc.stream, random()));
            assertThrows(JetStreamApiException.class,
                    () -> jstc.js.lookupConsumerInfo(random(), random()));
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
        //noinspection resource
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
        runInShared((nc, jstc) -> {
            IllegalStateException ise = assertThrows(IllegalStateException.class, () -> jstc.js.subscribe(random()));
            assertTrue(ise.getMessage().contains(JsSubNoMatchingStreamForSubject.id()));

            // general pull push validation

            String pulldur = random();
            String dgCantHave = random();
            ConsumerConfiguration ccCantHave = builder().durable(pulldur).deliverGroup(dgCantHave).build();
            PullSubscribeOptions pullCantHaveDlvrGrp = PullSubscribeOptions.builder().configuration(ccCantHave).build();
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), pullCantHaveDlvrGrp));
            assertTrue(iae.getMessage().contains(JsSubPullCantHaveDeliverGroup.id()));

            ccCantHave = builder().durable(pulldur).deliverSubject(dgCantHave).build();
            PullSubscribeOptions pullCantHaveDlvrSub = PullSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), pullCantHaveDlvrSub));
            assertTrue(iae.getMessage().contains(JsSubPullCantHaveDeliverSubject.id()));

            ccCantHave = builder().maxPullWaiting(1L).build();
            PushSubscribeOptions pushCantHaveMpw = PushSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), pushCantHaveMpw));
            assertTrue(iae.getMessage().contains(JsSubPushCantHaveMaxPullWaiting.id()));

            ccCantHave = builder().maxBatch(1L).build();
            PushSubscribeOptions pushCantHaveMb = PushSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), pushCantHaveMb));
            assertTrue(iae.getMessage().contains(JsSubPushCantHaveMaxBatch.id()));

            ccCantHave = builder().maxBytes(1L).build();
            PushSubscribeOptions pushCantHaveMby = PushSubscribeOptions.builder().configuration(ccCantHave).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), pushCantHaveMby));
            assertTrue(iae.getMessage().contains(JsSubPushCantHaveMaxBytes.id()));

            // create some consumers
            String durNoQ = random();
            String durYesQ = random();

            PushSubscribeOptions psoDurNoQ = PushSubscribeOptions.builder().durable(durNoQ).build();
            jstc.js.subscribe(jstc.subject(), psoDurNoQ);

            PushSubscribeOptions psoDurYesQ = PushSubscribeOptions.builder().durable(durYesQ).build();
            jstc.js.subscribe(jstc.subject(), "yesQ", psoDurYesQ);

            // already bound
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), psoDurNoQ));
            assertTrue(iae.getMessage().contains(JsSubConsumerAlreadyBound.id()));

            // queue match
            String qmatchdur = random();
            String qmatchq = random();
            PushSubscribeOptions qmatch = PushSubscribeOptions.builder().durable(qmatchdur).deliverGroup(qmatchq).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), "qnotmatch", qmatch));
            assertTrue(iae.getMessage().contains(JsSubQueueDeliverGroupMismatch.id()));

            // queue vs config
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), "notConfigured", psoDurNoQ));
            assertTrue(iae.getMessage().contains(JsSubExistingConsumerNotQueue.id()));

            PushSubscribeOptions psoNoVsYes = PushSubscribeOptions.builder().durable(durYesQ).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), psoNoVsYes));
            assertTrue(iae.getMessage().contains(JsSubExistingConsumerIsQueue.id()));

            PushSubscribeOptions psoYesVsNo = PushSubscribeOptions.builder().durable(durYesQ).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), "qnotmatch", psoYesVsNo));
            assertTrue(iae.getMessage().contains(JsSubExistingQueueDoesNotMatchRequestedQueue.id()));

            // flow control heartbeat push / pull
            String ccFcDur = random();
            String ccHbDur = random();
            ConsumerConfiguration ccFc = builder().durable(ccFcDur).flowControl(1000).build();
            ConsumerConfiguration ccHb = builder().durable(ccHbDur).idleHeartbeat(1000).build();

            PullSubscribeOptions psoPullCcFc = PullSubscribeOptions.builder().configuration(ccFc).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), psoPullCcFc));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidPull.id()));

            PullSubscribeOptions psoPullCcHb = PullSubscribeOptions.builder().configuration(ccHb).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), psoPullCcHb));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidPull.id()));

            PushSubscribeOptions psoPushCcFc = PushSubscribeOptions.builder().configuration(ccFc).build();
            String cantHaveQ = random();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), cantHaveQ, psoPushCcFc));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidQueue.id()));

            PushSubscribeOptions psoPushCcHb = PushSubscribeOptions.builder().configuration(ccHb).build();
            iae = assertThrows(IllegalArgumentException.class, () -> jstc.js.subscribe(jstc.subject(), cantHaveQ, psoPushCcHb));
            assertTrue(iae.getMessage().contains(JsSubFcHbNotValidQueue.id()));
        });
    }

    @Test
    public void testNatsJetStreamUtil() {
        assertNotNull(NatsJetStreamUtil.generateConsumerName());
        String gen = NatsJetStreamUtil.generateConsumerName("prefix");
        assertNotNull(gen);
        assertTrue(gen.startsWith("prefix-"));
    }

    @Test
    public void testRequestNoResponder() throws Exception {
        runInSharedCustomStream((ncCancel, jstc) -> {
            Options optReport = optionsBuilder(ncCancel.getConnectedUrl()).reportNoResponders().build();
            try (Connection ncReport = standardConnectionWait(optReport)) {
                assertThrows(CancellationException.class, () -> ncCancel.request(random(), null).get());
                ExecutionException ee = assertThrows(ExecutionException.class, () -> ncReport.request(random(), null).get());
                assertInstanceOf(JetStreamStatusException.class, ee.getCause());
                assertTrue(ee.getMessage().contains("503 No Responders Available For Request"));

                String subject = random();
                jstc.jsm.addStream(
                    StreamConfiguration.builder()
                        .name(jstc.stream).subjects(subject).storageType(StorageType.Memory)
                        .build());

                JetStream jsCancel = ncCancel.jetStream();
                JetStream jsReport = ncReport.jetStream();

                IOException ioe = assertThrows(IOException.class, () -> jsCancel.publish("not-exist", null));
                assertTrue(ioe.getMessage().contains("503"));
                ioe = assertThrows(IOException.class, () -> jsReport.publish("trnrNotExist", null));
                assertTrue(ioe.getMessage().contains("503"));
            }
        });
    }
}