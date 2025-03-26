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
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPubTests extends JetStreamTestBase {

    @Test
    public void testPublishVarieties() throws Exception {
        jsServer.run(nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            JetStream js = nc.jetStream();

            PublishAck pa = js.publish(tsc.subject(), dataBytes(1));
            assertPublishAck(pa, tsc.stream, 1);

            Message msg = NatsMessage.builder().subject(tsc.subject()).data(dataBytes(2)).build();
            pa = js.publish(msg);
            assertPublishAck(pa, tsc.stream, 2);

            PublishOptions po = PublishOptions.builder().build();
            pa = js.publish(tsc.subject(), dataBytes(3), po);
            assertPublishAck(pa, tsc.stream, 3);

            msg = NatsMessage.builder().subject(tsc.subject()).data(dataBytes(4)).build();
            pa = js.publish(msg, po);
            assertPublishAck(pa, tsc.stream, 4);

            pa = js.publish(tsc.subject(), null);
            assertPublishAck(pa, tsc.stream, 5);

            msg = NatsMessage.builder().subject(tsc.subject()).build();
            pa = js.publish(msg);
            assertPublishAck(pa, tsc.stream, 6);

            pa = js.publish(tsc.subject(), null, po);
            assertPublishAck(pa, tsc.stream, 7);

            msg = NatsMessage.builder().subject(tsc.subject()).build();
            pa = js.publish(msg, po);
            assertPublishAck(pa, tsc.stream, 8);

            Headers h = new Headers().put("foo", "bar9");
            pa = js.publish(tsc.subject(), h, dataBytes(9));
            assertPublishAck(pa, tsc.stream, 9);

            h = new Headers().put("foo", "bar10");
            pa = js.publish(tsc.subject(), h, dataBytes(10), po);
            assertPublishAck(pa, tsc.stream, 10);

            Subscription s = js.subscribe(tsc.subject());
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
            assertThrows(IOException.class, () -> js.publish(subject(999), null));
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
        jsServer.run(nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStream js = nc.jetStream();

            List<CompletableFuture<PublishAck>> futures = new ArrayList<>();

            futures.add(js.publishAsync(tsc.subject(), dataBytes(1)));

            Message msg = NatsMessage.builder().subject(tsc.subject()).data(dataBytes(2)).build();
            futures.add(js.publishAsync(msg));

            PublishOptions po = PublishOptions.builder().build();
            futures.add(js.publishAsync(tsc.subject(), dataBytes(3), po));

            msg = NatsMessage.builder().subject(tsc.subject()).data(dataBytes(4)).build();
            futures.add(js.publishAsync(msg, po));

            Headers h = new Headers().put("foo", "bar5");
            futures.add(js.publishAsync(tsc.subject(), h, dataBytes(5)));

            h = new Headers().put("foo", "bar6");
            futures.add(js.publishAsync(tsc.subject(), h, dataBytes(6), po));

            sleep(100); // just make sure all the publish complete

            for (int i = 1; i <= 6; i++) {
                CompletableFuture<PublishAck> future = futures.get(i-1);
                PublishAck pa = future.get();
                assertEquals(tsc.stream, pa.getStream());
                assertFalse(pa.isDuplicate());
                assertEquals(i, pa.getSeqno());
            }

            Subscription s = js.subscribe(tsc.subject());
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

            assertFutureIOException(js.publishAsync(subject(999), null));

            msg = NatsMessage.builder().subject(subject(999)).build();
            assertFutureIOException(js.publishAsync(msg));

            PublishOptions pox1 = PublishOptions.builder().build();

            assertFutureIOException(js.publishAsync(subject(999), null, pox1));

            msg = NatsMessage.builder().subject(subject(999)).build();
            assertFutureIOException(js.publishAsync(msg, pox1));

            PublishOptions pox2 = PublishOptions.builder().expectedLastMsgId(messageId(999)).build();

            assertFutureJetStreamApiException(js.publishAsync(tsc.subject(), null, pox2));

            msg = NatsMessage.builder().subject(tsc.subject()).build();
            assertFutureJetStreamApiException(js.publishAsync(msg, pox2));
        });
    }

    @Test
    public void testMultithreadedPublishAsync() throws Exception {
        final ExecutorService executorService = Executors.newFixedThreadPool(3);
        try {
            jsServer.run(nc -> {
                TestingStreamContainer tsc = new TestingStreamContainer(nc);
                final int messagesToPublish = 6;
                // create a new connection that does not have the inbox dispatcher set
                try (NatsConnection nc2 = new NatsConnection(nc.getOptions())){
                    nc2.connect(true);
                    JetStream js = nc2.jetStream();

                    List<Future<CompletableFuture<PublishAck>>> futures = new ArrayList<>();
                    for (int i = 0; i < messagesToPublish; i++) {
                        final Future<CompletableFuture<PublishAck>> submitFuture = executorService.submit(() ->
                                js.publishAsync(tsc.subject(), dataBytes(1)));
                        futures.add(submitFuture);
                    }
                    // verify all messages were published
                    for (int i = 0; i < messagesToPublish; i++) {
                        CompletableFuture<PublishAck> future = futures.get(i).get(200, TimeUnit.MILLISECONDS);
                        PublishAck pa = future.get(200, TimeUnit.MILLISECONDS);
                        assertEquals(tsc.stream, pa.getStream());
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
        assertTrue(ee.getCause() instanceof RuntimeException);
        assertTrue(ee.getCause().getCause() instanceof IOException);
    }

    private void assertFutureJetStreamApiException(CompletableFuture<PublishAck> future) {
        ExecutionException ee = assertThrows(ExecutionException.class, future::get);
        assertTrue(ee.getCause() instanceof RuntimeException);
        assertTrue(ee.getCause().getCause() instanceof JetStreamApiException);
    }

    @Test
    public void testPublishExpectations() throws Exception {
        jsServer.run(nc -> {
            JetStream js = nc.jetStream();
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(nc, 2);
            createMemoryStream(jsm, tsc.stream, tsc.subject(0), tsc.subject(1));

            PublishOptions po = PublishOptions.builder()
                .expectedStream(tsc.stream)
                .messageId(messageId(1))
                .build();
            PublishAck pa = js.publish(tsc.subject(0), dataBytes(1), po);
            assertPublishAck(pa, tsc.stream, 1);

            po = PublishOptions.builder()
                .expectedLastMsgId(messageId(1))
                .messageId(messageId(2))
                .build();
            pa = js.publish(tsc.subject(0), dataBytes(2), po);
            assertPublishAck(pa, tsc.stream, 2);

            po = PublishOptions.builder()
                .expectedLastSequence(2)
                .messageId(messageId(3))
                .build();
            pa = js.publish(tsc.subject(0), dataBytes(3), po);
            assertPublishAck(pa, tsc.stream, 3);

            po = PublishOptions.builder()
                .expectedLastSequence(3)
                .messageId(messageId(4))
                .build();
            pa = js.publish(tsc.subject(1), dataBytes(4), po);
            assertPublishAck(pa, tsc.stream, 4);

            po = PublishOptions.builder()
                .expectedLastSubjectSequence(3)
                .messageId(messageId(5))
                .build();
            pa = js.publish(tsc.subject(0), dataBytes(5), po);
            assertPublishAck(pa, tsc.stream, 5);

            po = PublishOptions.builder()
                .expectedLastSubjectSequence(4)
                .messageId(messageId(6))
                .build();
            pa = js.publish(tsc.subject(1), dataBytes(6), po);
            assertPublishAck(pa, tsc.stream, 6);

            String subject0 = tsc.subject(0);
            PublishOptions po1 = PublishOptions.builder().expectedStream(stream(999)).build();
            JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> js.publish(subject0, dataBytes(999), po1));
            assertEquals(10060, e.getApiErrorCode());

            PublishOptions po2 = PublishOptions.builder().expectedLastMsgId(messageId(999)).build();
            e = assertThrows(JetStreamApiException.class, () -> js.publish(subject0, dataBytes(999), po2));
            assertEquals(10070, e.getApiErrorCode());

            PublishOptions po3 = PublishOptions.builder().expectedLastSequence(999).build();
            e = assertThrows(JetStreamApiException.class, () -> js.publish(subject0, dataBytes(999), po3));
            assertEquals(10071, e.getApiErrorCode());

            PublishOptions po4 = PublishOptions.builder().expectedLastSubjectSequence(999).build();
            e = assertThrows(JetStreamApiException.class, () -> js.publish(subject0, dataBytes(999), po4));
            assertEquals(10071, e.getApiErrorCode());

            // 0 has meaning to expectedLastSubjectSequence
            tsc = new TestingStreamContainer(nc);
            createMemoryStream(jsm, tsc.stream, tsc.subject());
            PublishOptions poLss = PublishOptions.builder().expectedLastSubjectSequence(0).build();
            pa = js.publish(tsc.subject(), dataBytes(22), poLss);
            assertPublishAck(pa, tsc.stream, 1);

            final String fSubject = tsc.subject();
            e = assertThrows(JetStreamApiException.class, () -> js.publish(fSubject, dataBytes(999), poLss));
            assertEquals(10071, e.getApiErrorCode());

            // 0 has meaning
            tsc = new TestingStreamContainer(nc);
            PublishOptions poLs = PublishOptions.builder().expectedLastSequence(0).build();
            pa = js.publish(tsc.subject(), dataBytes(331), poLs);
            assertPublishAck(pa, tsc.stream, 1);

            tsc = new TestingStreamContainer(nc);
            poLs = PublishOptions.builder().expectedLastSubjectSequence(0).build();
            pa = js.publish(tsc.subject(), dataBytes(441), poLs);
            assertPublishAck(pa, tsc.stream, 1);
        });
    }

    @Test
    public void testPublishMiscExceptions() throws Exception {
        jsServer.run(nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            JetStream js = nc.jetStream();

            // stream supplied and matches
            PublishOptions po = PublishOptions.builder().stream(tsc.stream).build();
            js.publish(tsc.subject(), dataBytes(999), po);

            // mismatch stream to PO stream
            PublishOptions po1 = PublishOptions.builder().stream(stream(999)).build();
            assertThrows(IOException.class, () -> js.publish(tsc.subject(), dataBytes(999), po1));

            // invalid subject
            assertThrows(IOException.class, () -> js.publish(subject(999), dataBytes(999)));
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
        jsServer.run(nc -> {
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            JetStreamOptions jso = JetStreamOptions.builder().publishNoAck(true).build();
            JetStream js = nc.jetStream(jso);

            String data1 = "noackdata1";
            String data2 = "noackdata2";

            PublishAck pa = js.publish(tsc.subject(), data1.getBytes());
            assertNull(pa);

            CompletableFuture<PublishAck> f = js.publishAsync(tsc.subject(), data2.getBytes());
            assertNull(f);

            JetStreamSubscription sub = js.subscribe(tsc.subject());
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
        String streamName = "stream-max-payload-test";
        String subject1 = "mptest1";
        String subject2 = "mptest2";

        try (NatsTestServer ts = new NatsTestServer(false, true))
        {
            Options options = standardOptionsBuilder().noReconnect().server(ts.getURI()).build();
            long expectedSeq = 0;
            try (Connection nc = standardConnection(options)){
                JetStreamManagement jsm = nc.jetStreamManagement();
                try { jsm.deleteStream(streamName); } catch (JetStreamApiException ignore) {}
                jsm.addStream(StreamConfiguration.builder()
                    .name(streamName)
                    .storageType(StorageType.Memory)
                    .subjects(subject1, subject2)
                    .maximumMessageSize(1000)
                    .build()
                );

                JetStream js = nc.jetStream();
                for (int x = 1; x <= 3; x++)
                {
                    int size = 1000 + x - 2;
                    if (size > 1000)
                    {
                        JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> js.publish(subject1, new byte[size]));
                        assertEquals(10054, e.getApiErrorCode());
                    }
                    else
                    {
                        PublishAck pa = js.publish(subject1, new byte[size]);
                        assertEquals(++expectedSeq, pa.getSeqno());
                    }
                }
            }

            try (Connection nc = standardConnection(options)){
                JetStream js = nc.jetStream();
                for (int x = 1; x <= 3; x++)
                {
                    int size = 1000 + x - 2;
                    CompletableFuture<PublishAck> paFuture = js.publishAsync(subject1, new byte[size]);
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
            }
        }
    }

    @Test
    public void testPublishWithTTL() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            String stream = stream();
            String subject = subject();
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .allowMessageTtl()
                .subjects(subject).build();

            jsm.addStream(sc);

            PublishOptions opts = PublishOptions.builder().msgTtlSeconds(1).build();
            PublishAck pa = js.publish(subject, null, opts);
            assertNotNull(pa);

            MessageInfo mi = jsm.getMessage(stream, pa.getSeqno());
            assertEquals("1", mi.getHeaders().getFirst(MSG_TTL_HDR));

            sleep(1200);
            JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> jsm.getMessage(stream, pa.getSeqno()));
            assertEquals(10037, e.getApiErrorCode());
        });
    }

    @Test
    public void testMsgDeleteMarkerMaxAge() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            String stream = stream();
            String subject = subject();
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .allowMessageTtl()
                .subjectDeleteMarkerTtl(Duration.ofSeconds(50))
                .maxAge(1000)
                .subjects(subject).build();

            jsm.addStream(sc);

            PublishOptions opts = PublishOptions.builder().msgTtlSeconds(1).build();
            PublishAck pa = js.publish(subject, null, opts);
            assertNotNull(pa);

            sleep(1200);

            MessageInfo mi = jsm.getLastMessage(stream, subject);
            assertEquals("MaxAge", mi.getHeaders().getFirst(NATS_MARKER_REASON_HDR));
            assertEquals("50s", mi.getHeaders().getFirst(MSG_TTL_HDR));
        });
    }
}
