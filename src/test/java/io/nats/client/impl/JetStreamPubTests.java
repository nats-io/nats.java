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
import io.nats.client.api.MessageInfo;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static io.nats.client.support.NatsJetStreamConstants.MSG_TTL_HDR;
import static io.nats.client.support.NatsJetStreamConstants.NATS_MARKER_REASON_HDR;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ThreadUtils.sleep;
import static io.nats.client.utils.VersionUtils.atLeast2_12;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPubTests extends JetStreamTestBase {

    @Test
    public void testPublishVarieties() throws Exception {
        runInShared((nc, ctx) -> {
            PublishAck pa = ctx.js.publish(ctx.subject(), dataBytes(1));
            assertPublishAck(pa, ctx.stream, 1);

            Message msg = NatsMessage.builder().subject(ctx.subject()).data(dataBytes(2)).build();
            pa = ctx.js.publish(msg);
            assertPublishAck(pa, ctx.stream, 2);

            PublishOptions po = PublishOptions.builder().build();
            pa = ctx.js.publish(ctx.subject(), dataBytes(3), po);
            assertPublishAck(pa, ctx.stream, 3);

            msg = NatsMessage.builder().subject(ctx.subject()).data(dataBytes(4)).build();
            pa = ctx.js.publish(msg, po);
            assertPublishAck(pa, ctx.stream, 4);

            pa = ctx.js.publish(ctx.subject(), null);
            assertPublishAck(pa, ctx.stream, 5);

            msg = NatsMessage.builder().subject(ctx.subject()).build();
            pa = ctx.js.publish(msg);
            assertPublishAck(pa, ctx.stream, 6);

            pa = ctx.js.publish(ctx.subject(), null, po);
            assertPublishAck(pa, ctx.stream, 7);

            msg = NatsMessage.builder().subject(ctx.subject()).build();
            pa = ctx.js.publish(msg, po);
            assertPublishAck(pa, ctx.stream, 8);

            Headers h = new Headers().put("foo", "bar9");
            pa = ctx.js.publish(ctx.subject(), h, dataBytes(9));
            assertPublishAck(pa, ctx.stream, 9);

            h = new Headers().put("foo", "bar10");
            pa = ctx.js.publish(ctx.subject(), h, dataBytes(10), po);
            assertPublishAck(pa, ctx.stream, 10);

            Subscription s = ctx.js.subscribe(ctx.subject());
            assertNextMessage(s, data(1), null);
            assertNextMessage(s, data(2), null);
            assertNextMessage(s, data(3), null);
            assertNextMessage(s, data(4), null);
            assertNextMessage(s, null, null); // 5
            assertNextMessage(s, null, null); // 6
            assertNextMessage(s, null, null); // 7
            assertNextMessage(s, null, null); // 8
            assertNextMessage(s, data(9), "bar9");
            assertNextMessage(s, data(10), "bar10");

            // 503
            assertThrows(IOException.class, () -> ctx.js.publish(random(), null));
        });
    }

    private void assertNextMessage(Subscription s, String data, String header) throws InterruptedException {
        Message m = s.nextMessage(DEFAULT_TIMEOUT);
        assertNotNull(m);
        if (data == null) {
            assertNotNull(m.getData());
            assertEquals(0, m.getData().length);
        }
        else {
            assertEquals(data, new String(m.getData()));
        }
        if (header != null) {
            assertTrue(m.hasHeaders());
            assertEquals(header, m.getHeaders().getFirst("foo"));
        }
    }

    private void assertPublishAck(PublishAck pa, String stream, long seqno) {
        assertEquals(stream, pa.getStream());
        if (seqno != -1) {
            assertEquals(seqno, pa.getSeqno());
        }
        assertFalse(pa.isDuplicate());
    }

    @Test
    public void testPublishAsyncVarieties() throws Exception {
        runInShared((nc, ctx) -> {
            List<CompletableFuture<PublishAck>> futures = new ArrayList<>();

            futures.add(ctx.js.publishAsync(ctx.subject(), dataBytes(1)));

            Message msg = NatsMessage.builder().subject(ctx.subject()).data(dataBytes(2)).build();
            futures.add(ctx.js.publishAsync(msg));

            PublishOptions po = PublishOptions.builder().build();
            futures.add(ctx.js.publishAsync(ctx.subject(), dataBytes(3), po));

            msg = NatsMessage.builder().subject(ctx.subject()).data(dataBytes(4)).build();
            futures.add(ctx.js.publishAsync(msg, po));

            Headers h = new Headers().put("foo", "bar5");
            futures.add(ctx.js.publishAsync(ctx.subject(), h, dataBytes(5)));

            h = new Headers().put("foo", "bar6");
            futures.add(ctx.js.publishAsync(ctx.subject(), h, dataBytes(6), po));

            sleep(100); // just make sure all the publish complete

            for (int i = 1; i <= 6; i++) {
                CompletableFuture<PublishAck> future = futures.get(i-1);
                PublishAck pa = future.get();
                assertEquals(ctx.stream, pa.getStream());
                assertFalse(pa.isDuplicate());
                assertEquals(i, pa.getSeqno());
            }

            Subscription s = ctx.js.subscribe(ctx.subject());
            for (int x = 1; x <= 6; x++) {
                Message m = s.nextMessage(DEFAULT_TIMEOUT);
                assertNotNull(m);
                String data = new String(m.getData());
                assertEquals(data(x), data);
                if (x > 4) {
                    assertTrue(m.hasHeaders());
                    assertEquals("bar" + x, m.getHeaders().getFirst("foo"));
                }
            }

            assertFutureIOException(ctx.js.publishAsync(random(), null));

            msg = NatsMessage.builder().subject(random()).build();
            assertFutureIOException(ctx.js.publishAsync(msg));

            PublishOptions pox1 = PublishOptions.builder().build();

            assertFutureIOException(ctx.js.publishAsync(random(), null, pox1));

            msg = NatsMessage.builder().subject(random()).build();
            assertFutureIOException(ctx.js.publishAsync(msg, pox1));

            PublishOptions pox2 = PublishOptions.builder().expectedLastMsgId(random()).build();

            assertFutureJetStreamApiException(ctx.js.publishAsync(ctx.subject(), null, pox2));

            msg = NatsMessage.builder().subject(ctx.subject()).build();
            assertFutureJetStreamApiException(ctx.js.publishAsync(msg, pox2));
        });
    }

    @Test
    public void testMultithreadedPublishAsync() throws Exception {
        //noinspection resource
        final ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            runInShared((nc, ctx) -> {
                final int messagesToPublish = 6;
                // create a new connection that does not have the inbox dispatcher set
                try (NatsConnection nc2 = new NatsConnection(nc.getOptions())){
                    nc2.connect(true);
                    JetStream js2 = nc2.jetStream();

                    List<Future<CompletableFuture<PublishAck>>> futures = new ArrayList<>();
                    for (int i = 0; i < messagesToPublish; i++) {
                        final Future<CompletableFuture<PublishAck>> submitFuture = executorService.submit(() ->
                            js2.publishAsync(ctx.subject(), dataBytes(1)));
                        futures.add(submitFuture);
                    }
                    // verify all messages were published
                    for (int i = 0; i < messagesToPublish; i++) {
                        CompletableFuture<PublishAck> future = futures.get(i).get(200, TimeUnit.MILLISECONDS);
                        PublishAck pa = future.get(200, TimeUnit.MILLISECONDS);
                        assertEquals(ctx.stream, pa.getStream());
                        assertFalse(pa.isDuplicate());
                    }
                }
            });
        } finally {
            executorService.shutdownNow();
        }
    }

    private void assertFutureIOException(CompletableFuture<PublishAck> future) {
        ExecutionException ee = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(RuntimeException.class, ee.getCause());
        assertInstanceOf(IOException.class, ee.getCause().getCause());
    }

    private void assertFutureJetStreamApiException(CompletableFuture<PublishAck> future) {
        ExecutionException ee = assertThrows(ExecutionException.class, future::get);
        assertInstanceOf(RuntimeException.class, ee.getCause());
        assertInstanceOf(JetStreamApiException.class, ee.getCause().getCause());
    }

    @Test
    public void testPublishExpectations() throws Exception {
        runInSharedCustom((nc, jstc1) -> {
            try (JetStreamTestingContext ctx2 = new JetStreamTestingContext(nc, 1);
                 JetStreamTestingContext ctx3 = new JetStreamTestingContext(nc, 1);
                 JetStreamTestingContext ctx4 = new JetStreamTestingContext(nc, 1)
            ) {
                String stream1 = jstc1.stream;
                String subjectPrefix = random();
                String streamSubject = subjectPrefix + ".>";
                String sub1 = subjectPrefix + ".foo.1";
                String sub2 = subjectPrefix + ".foo.2";
                String sub3 = subjectPrefix + ".bar.3";
                jstc1.createOrReplaceStream(streamSubject);

                String mid = random();
                PublishOptions po = PublishOptions.builder()
                    .expectedStream(stream1)
                    .messageId(mid)
                    .build();
                PublishAck pa = jstc1.js.publish(sub1, dataBytes(1), po);
                assertPublishAck(pa, stream1, 1);

                String lastId = mid;
                mid = random();
                po = PublishOptions.builder()
                    .expectedLastMsgId(lastId)
                    .messageId(mid)
                    .build();
                pa = jstc1.js.publish(sub1, dataBytes(2), po);
                assertPublishAck(pa, stream1, 2);

                mid = random();
                po = PublishOptions.builder()
                    .expectedLastSequence(2)
                    .messageId(mid)
                    .build();
                pa = jstc1.js.publish(sub1, dataBytes(3), po);
                assertPublishAck(pa, stream1, 3);

                mid = random();
                po = PublishOptions.builder()
                    .expectedLastSequence(3)
                    .messageId(mid)
                    .build();
                pa = jstc1.js.publish(sub2, dataBytes(4), po);
                assertPublishAck(pa, stream1, 4);

                mid = random();
                po = PublishOptions.builder()
                    .expectedLastSubjectSequence(3)
                    .messageId(mid)
                    .build();
                pa = jstc1.js.publish(sub1, dataBytes(5), po);
                assertPublishAck(pa, stream1, 5);

                mid = random();
                po = PublishOptions.builder()
                    .expectedLastSubjectSequence(4)
                    .messageId(mid)
                    .build();
                pa = jstc1.js.publish(sub2, dataBytes(6), po);
                assertPublishAck(pa, stream1, 6);

                PublishOptions po1 = PublishOptions.builder().expectedStream(random()).build();
                JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> jstc1.js.publish(sub1, dataBytes(), po1));
                assertEquals(10060, e.getApiErrorCode());

                PublishOptions po2 = PublishOptions.builder().expectedLastMsgId(random()).build();
                e = assertThrows(JetStreamApiException.class, () -> jstc1.js.publish(sub1, dataBytes(), po2));
                assertEquals(10070, e.getApiErrorCode());

                PublishOptions po3 = PublishOptions.builder().expectedLastSequence(999).build();
                e = assertThrows(JetStreamApiException.class, () -> jstc1.js.publish(sub1, dataBytes(), po3));
                assertEquals(10071, e.getApiErrorCode());

                PublishOptions po4 = PublishOptions.builder().expectedLastSubjectSequence(999).build();
                e = assertThrows(JetStreamApiException.class, () -> jstc1.js.publish(sub1, dataBytes(), po4));
                assertEquals(10071, e.getApiErrorCode());

                // 0 has meaning to expectedLastSubjectSequence
                PublishOptions poLss = PublishOptions.builder().expectedLastSubjectSequence(0).build();
                pa = ctx2.js.publish(ctx2.subject(), dataBytes(22), poLss);
                assertPublishAck(pa, ctx2.stream, 1);

                final String fSubject = ctx2.subject();
                e = assertThrows(JetStreamApiException.class, () -> ctx2.js.publish(fSubject, dataBytes(), poLss));
                assertEquals(10071, e.getApiErrorCode());

                // 0 has meaning
                PublishOptions poLs = PublishOptions.builder().expectedLastSequence(0).build();
                pa = ctx3.js.publish(ctx3.subject(), dataBytes(331), poLs);
                assertPublishAck(pa, ctx3.stream, 1);

                poLs = PublishOptions.builder().expectedLastSubjectSequence(0).build();
                pa = ctx4.js.publish(ctx4.subject(), dataBytes(441), poLs);
                assertPublishAck(pa, ctx4.stream, 1);

                // expectedLastSubjectSequenceSubject
                pa = ctx4.js.publish(sub3, dataBytes(500));
                assertPublishAck(pa, stream1, 7);

                PublishOptions poLsss = PublishOptions.builder()
                    .expectedLastSubjectSequence(5)
                    .build();
                pa = ctx4.js.publish(sub1, dataBytes(501), poLsss);
                assertPublishAck(pa, stream1, 8);

                poLsss = PublishOptions.builder()
                    .expectedLastSubjectSequence(6)
                    .build();
                pa = ctx4.js.publish(sub2, dataBytes(502), poLsss);
                assertPublishAck(pa, stream1, 9);

                poLsss = PublishOptions.builder()
                    .expectedLastSubjectSequence(9)
                    .expectedLastSubjectSequenceSubject(streamSubject)
                    .build();
                pa = ctx4.js.publish(sub2, dataBytes(503), poLsss);
                assertPublishAck(pa, stream1, 10);

                poLsss = PublishOptions.builder()
                    .expectedLastSubjectSequence(10)
                    .expectedLastSubjectSequenceSubject(subjectPrefix + ".foo.*")
                    .build();
                pa = ctx4.js.publish(sub2, dataBytes(504), poLsss);
                assertPublishAck(pa, stream1, 11);

                PublishOptions final1 = poLsss;
                assertThrows(JetStreamApiException.class, () -> ctx4.js.publish(sub2, dataBytes(505), final1));

                poLsss = PublishOptions.builder()
                    .expectedLastSubjectSequence(7)
                    .expectedLastSubjectSequenceSubject(subjectPrefix + ".bar.*")
                    .build();
                pa = ctx4.js.publish(sub3, dataBytes(506), poLsss);
                assertPublishAck(pa, stream1, 12);

                poLsss = PublishOptions.builder()
                    .expectedLastSubjectSequence(12)
                    .expectedLastSubjectSequenceSubject(streamSubject)
                    .build();
                pa = ctx4.js.publish(sub3, dataBytes(507), poLsss);
                assertPublishAck(pa, stream1, 13);

                poLsss = PublishOptions.builder()
                    .expectedLastSubjectSequenceSubject("not-even-a-subject")
                    .build();
                if (atLeast2_12()) {
                    PublishOptions fpoLsss = poLsss;
                    assertThrows(JetStreamApiException.class, () -> ctx4.js.publish(sub3, dataBytes(508), fpoLsss));
                }
                else {
                    pa = ctx4.js.publish(sub3, dataBytes(508), poLsss);
                    assertPublishAck(pa, stream1, 14);
                }

                poLsss = PublishOptions.builder()
                    .expectedLastSequence(14)
                    .expectedLastSubjectSequenceSubject("not-even-a-subject")
                    .build();
                if (atLeast2_12()) {
                    PublishOptions fpoLsss = poLsss;
                    assertThrows(JetStreamApiException.class, () -> ctx4.js.publish(sub3, dataBytes(509), fpoLsss));
                }
                else {
                    pa = ctx4.js.publish(sub3, dataBytes(509), poLsss);
                    assertPublishAck(pa, stream1, 15);
                }

                poLsss = PublishOptions.builder()
                    .expectedLastSubjectSequence(15)
                    .expectedLastSubjectSequenceSubject("not-even-a-subject")
                    .build();
                PublishOptions final2 = poLsss;
                // JetStreamApiException: wrong last sequence: 0 [10071]
                assertThrows(JetStreamApiException.class, () -> ctx4.js.publish(sub3, dataBytes(510), final2));
            }
        });
    }

    @Test
    public void testPublishMiscExceptions() throws Exception {
        runInShared((nc, ctx) -> {
            // stream supplied and matches
            //noinspection deprecation
            PublishOptions po = PublishOptions.builder().stream(ctx.stream).build();
            ctx.js.publish(ctx.subject(), dataBytes(9), po);

            // mismatch stream to PO stream
            //noinspection deprecation
            PublishOptions pox = PublishOptions.builder().stream(random()).build();
            assertThrows(IOException.class, () -> ctx.js.publish(ctx.subject(), dataBytes(), pox));

            // invalid subject
            assertThrows(IOException.class, () -> ctx.js.publish(random(), dataBytes()));
        });
    }

    @Test
    public void testPublishAckJson() throws IOException, JetStreamApiException {
        String json = "{\"stream\":\"sname\", \"seq\":42, \"duplicate\":false}";
        PublishAck pa = new PublishAck(getDataMessage(json));
        assertEquals("sname", pa.getStream());
        assertEquals(42, pa.getSeqno());
        assertFalse(pa.isDuplicate());
        assertNotNull(pa.toString());
    }

    @Test
    public void testPublishNoAck() throws Exception {
        runInShared((nc, ctx) -> {
            JetStreamOptions jso = JetStreamOptions.builder().publishNoAck(true).build();
            JetStream customJs = nc.jetStream(jso);

            String data1 = "noackdata1";
            String data2 = "noackdata2";

            PublishAck pa = customJs.publish(ctx.subject(), data1.getBytes());
            assertNull(pa);

            CompletableFuture<PublishAck> f = customJs.publishAsync(ctx.subject(), data2.getBytes());
            assertNull(f);

            JetStreamSubscription sub = customJs.subscribe(ctx.subject());
            Message m = sub.nextMessage(Duration.ofSeconds(2));
            assertNotNull(m);
            assertEquals(data1, new String(m.getData()));
            m = sub.nextMessage(Duration.ofSeconds(2));
            assertNotNull(m);
            assertEquals(data2, new String(m.getData()));
        });
    }

    @Test
    public void testMaxPayloadJs() throws Exception {
        runInSharedCustom(optionsBuilder().noReconnect(), (nc, ctx) -> {
            long expectedSeq = 0;
            StreamConfiguration.Builder builder = ctx.scBuilder().maximumMessageSize(1000);
            ctx.createOrReplaceStream(builder);
            String subject0 = ctx.subject(0);

            for (int x = 1; x <= 3; x++) {
                int size = 1000 + x - 2;
                if (size > 1000) {
                    JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> ctx.js.publish(subject0, new byte[size]));
                    assertEquals(10054, e.getApiErrorCode());
                }
                else
                {
                    PublishAck pa = ctx.js.publish(subject0, new byte[size]);
                    assertEquals(++expectedSeq, pa.getSeqno());
                }
            }

            for (int x = 1; x <= 3; x++) {
                int size = 1000 + x - 2;
                CompletableFuture<PublishAck> paFuture = ctx.js.publishAsync(subject0, new byte[size]);
                if (size > 1000)
                {
                    ExecutionException e = assertThrows(ExecutionException.class, () -> paFuture.get(1000, TimeUnit.MILLISECONDS));
                    JetStreamApiException j = (JetStreamApiException)e.getCause().getCause();
                    assertEquals(10054, j.getApiErrorCode());
                }
                else
                {
                    PublishAck pa = paFuture.get(1000, TimeUnit.MILLISECONDS);
                    assertEquals(++expectedSeq, pa.getSeqno());
                }
            }
        });
    }

    @Test
    public void testPublishWithTTL() throws Exception {
        runInShared((nc, ctx) -> {
            StreamConfiguration.Builder builder = ctx.scBuilder().allowMessageTtl();
            ctx.createOrReplaceStream(builder);

            String stream = ctx.stream;
            String subject = ctx.subject();

            PublishOptions opts = PublishOptions.builder().messageTtlSeconds(1).build();
            PublishAck pa1 = ctx.js.publish(subject, null, opts);
            assertNotNull(pa1);

            opts = PublishOptions.builder().messageTtlNever().build();
            PublishAck paNever = ctx.js.publish(subject, null, opts);
            assertNotNull(paNever);

            MessageInfo mi1 = ctx.jsm.getMessage(stream, pa1.getSeqno());
            Headers h = mi1.getHeaders();
            assertNotNull(h);
            assertEquals("1s",h.getFirst(MSG_TTL_HDR));

            MessageInfo miNever = ctx.jsm.getMessage(stream, paNever.getSeqno());
            h = miNever.getHeaders();
            assertNotNull(h);
            assertEquals("never",h.getFirst(MSG_TTL_HDR));

            sleep(1200);

            JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> ctx.jsm.getMessage(stream, pa1.getSeqno()));
            assertEquals(10037, e.getApiErrorCode());

            assertNotNull((ctx.jsm.getMessage(stream, paNever.getSeqno())));
        });
    }

    @Test
    public void testMsgDeleteMarkerMaxAge() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            StreamConfiguration sc = ctx.scBuilder(1)
                .allowMessageTtl()
                .subjectDeleteMarkerTtl(Duration.ofSeconds(50))
                .maxAge(1000)
                .build();
            ctx.createOrReplaceStream(sc);
            String subject = ctx.subject();

            PublishOptions opts = PublishOptions.builder().messageTtlSeconds(1).build();
            PublishAck pa = ctx.js.publish(subject, null, opts);
            assertNotNull(pa);

            sleep(1200);

            MessageInfo mi = ctx.jsm.getLastMessage(ctx.stream, subject);
            Headers h = mi.getHeaders();
            assertNotNull(h);
            assertEquals("MaxAge", h.getFirst(NATS_MARKER_REASON_HDR));
            assertEquals("50s", h.getFirst(MSG_TTL_HDR));

            assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder()
                .name(ctx.stream)
                .storageType(StorageType.Memory)
                .allowMessageTtl()
                .subjectDeleteMarkerTtl(Duration.ofMillis(999))
                .subjects(subject).build());

            assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder()
                .name(ctx.stream)
                .storageType(StorageType.Memory)
                .allowMessageTtl()
                .subjectDeleteMarkerTtl(999)
                .subjects(subject).build());
        });
    }
}
