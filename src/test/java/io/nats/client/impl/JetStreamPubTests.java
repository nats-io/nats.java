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
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.support.Ulong;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamPubTests extends JetStreamTestBase {

    @Test
    public void testPublishVarieties() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();

            PublishAck pa = js.publish(SUBJECT, dataBytes(1));
            assertPublishAck(pa, new Ulong(1));

            Message msg = NatsMessage.builder().subject(SUBJECT).data(dataBytes(2)).build();
            pa = js.publish(msg);
            assertPublishAck(pa, new Ulong(2));

            PublishOptions po = PublishOptions.builder().build();
            pa = js.publish(SUBJECT, dataBytes(3), po);
            assertPublishAck(pa, new Ulong(3));

            msg = NatsMessage.builder().subject(SUBJECT).data(dataBytes(4)).build();
            pa = js.publish(msg, po);
            assertPublishAck(pa, new Ulong(4));

            pa = js.publish(SUBJECT, null);
            assertPublishAck(pa, new Ulong(5));

            msg = NatsMessage.builder().subject(SUBJECT).build();
            pa = js.publish(msg);
            assertPublishAck(pa, new Ulong(6));

            pa = js.publish(SUBJECT, null, po);
            assertPublishAck(pa, new Ulong(7));

            msg = NatsMessage.builder().subject(SUBJECT).build();
            pa = js.publish(msg, po);
            assertPublishAck(pa, new Ulong(8));

            Subscription s = js.subscribe(SUBJECT);
            assertNextMessage(s, data(1));
            assertNextMessage(s, data(2));
            assertNextMessage(s, data(3));
            assertNextMessage(s, data(4));
            assertNextMessage(s, null); // 5
            assertNextMessage(s, null); // 6
            assertNextMessage(s, null); // 7
            assertNextMessage(s, null); // 8
        });
    }

    private void assertNextMessage(Subscription s, String data) throws InterruptedException {
        Message m = s.nextMessage(DEFAULT_TIMEOUT);
        System.out.println(m.getClass() + " " + m);
        assertNotNull(m);
        if (data == null) {
            assertNotNull(m.getData());
            assertEquals(0, m.getData().length);
        }
        else {
            assertEquals(data, new String(m.getData()));
        }
    }

    private void assertPublishAck(PublishAck pa, Ulong seqno) {
        assertEquals(STREAM, pa.getStream());
        if (seqno.greaterThan(Ulong.ZERO)) {
            assertEquals(seqno, pa.getSequenceNum());
        }
        assertFalse(pa.isDuplicate());
    }

    @Test
    public void testPublishAsyncVarieties() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();

            List<CompletableFuture<PublishAck>> futures = new ArrayList<>();

            futures.add(js.publishAsync(SUBJECT, dataBytes(1)));

            Message msg = NatsMessage.builder().subject(SUBJECT).data(dataBytes(2)).build();
            futures.add(js.publishAsync(msg));

            PublishOptions po = PublishOptions.builder().build();
            futures.add(js.publishAsync(SUBJECT, dataBytes(3), po));

            msg = NatsMessage.builder().subject(SUBJECT).data(dataBytes(4)).build();
            futures.add(js.publishAsync(msg, po));

            Subscription s = js.subscribe(SUBJECT);
            List<String> datas = new ArrayList<>(Arrays.asList(data(1), data(2), data(3), data(4)));
            assertContainsMessage(s, datas);
            assertContainsMessage(s, datas);
            assertContainsMessage(s, datas);
            assertContainsMessage(s, datas);
            assertEquals(0, datas.size());

            List<Ulong> seqnos = new ArrayList<>(Arrays.asList(new Ulong(1), new Ulong(2), new Ulong(3), new Ulong(4)));
            for (CompletableFuture<PublishAck> future : futures) {
                assertContainsPublishAck(future.get(), seqnos);
            }
            assertEquals(0, seqnos.size());

            assertFutureIOException(js.publishAsync(subject(999), null));

            msg = NatsMessage.builder().subject(subject(999)).build();
            assertFutureIOException(js.publishAsync(msg));

            PublishOptions pox1 = PublishOptions.builder().build();

            assertFutureIOException(js.publishAsync(subject(999), null, pox1));

            msg = NatsMessage.builder().subject(subject(999)).build();
            assertFutureIOException(js.publishAsync(msg, pox1));

            PublishOptions pox2 = PublishOptions.builder().expectedLastMsgId(messageId(999)).build();

            assertFutureJetStreamApiException(js.publishAsync(SUBJECT, null, pox2));

            msg = NatsMessage.builder().subject(SUBJECT).build();
            assertFutureJetStreamApiException(js.publishAsync(msg, pox2));

        });
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

    private void assertContainsMessage(Subscription s, List<String> datas) throws InterruptedException {
        Message m = s.nextMessage(DEFAULT_TIMEOUT);
        assertNotNull(m);
        String data = new String(m.getData());
        assertTrue(datas.contains(data));
        datas.remove(data);
    }

    private void assertContainsPublishAck(PublishAck pa, List<Ulong> seqnos) {
        assertEquals(STREAM, pa.getStream());
        assertFalse(pa.isDuplicate());
        assertTrue(seqnos.contains(pa.getSequenceNum()));
        seqnos.remove(pa.getSequenceNum());
    }

    @Test
    public void testPublishExpectations() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();

            PublishOptions po = PublishOptions.builder()
                    .expectedStream(STREAM)
                    .messageId(messageId(1))
                    .build();
            PublishAck pa = js.publish(SUBJECT, dataBytes(1), po);
            assertPublishAck(pa, new Ulong(1));

            po = PublishOptions.builder()
                    .expectedLastMsgId(messageId(1))
                    .messageId(messageId(2))
                    .build();
            pa = js.publish(SUBJECT, dataBytes(2), po);
            assertPublishAck(pa, new Ulong(2));

            po = PublishOptions.builder()
                    .expectedLastSequence(new Ulong(2))
                    .messageId(messageId(3))
                    .build();
            pa = js.publish(SUBJECT, dataBytes(3), po);
            assertPublishAck(pa, new Ulong(3));

            PublishOptions po1 = PublishOptions.builder().expectedStream(stream(999)).build();
            assertThrows(JetStreamApiException.class, () -> js.publish(SUBJECT, dataBytes(999), po1));

            PublishOptions po2 = PublishOptions.builder().expectedLastMsgId(messageId(999)).build();
            assertThrows(JetStreamApiException.class, () -> js.publish(SUBJECT, dataBytes(999), po2));

            PublishOptions po3 = PublishOptions.builder().expectedLastSequence(new Ulong(999)).build();
            assertThrows(JetStreamApiException.class, () -> js.publish(SUBJECT, dataBytes(999), po3));
        });
    }

    @Test
    public void testPublishMiscExceptions() throws Exception {
        runInJsServer(nc -> {
            createTestStream(nc);
            JetStream js = nc.jetStream();

            // stream supplied and matches
            PublishOptions po = PublishOptions.builder().stream(STREAM).build();
            js.publish(SUBJECT, dataBytes(999), po);

            // mismatch stream to PO stream
            PublishOptions po1 = PublishOptions.builder().stream(stream(999)).build();
            assertThrows(IOException.class, () -> js.publish(SUBJECT, dataBytes(999), po1));

            // invalid subject
            assertThrows(IOException.class, () -> js.publish(subject(999), dataBytes(999)));
        });
    }

    @Test
    public void testPublishAckJson() throws IOException, JetStreamApiException {
        String json = "{\"stream\":\"sname\", \"seq\":42, \"duplicate\":false}";
        PublishAck pa = new PublishAck(getDataMessage(json));
        assertEquals("sname", pa.getStream());
        assertEquals(new Ulong(42), pa.getSequenceNum());
        assertFalse(pa.isDuplicate());
        assertNotNull(pa.toString());
    }

    @Test
    public void testPublishNoAck() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(STREAM)
                    .storageType(StorageType.Memory)
                    .subjects(SUBJECT)
                    .noAck(true)
                    .build();

            jsm.addStream(sc);

            JetStreamOptions jso = JetStreamOptions.builder().publishNoAck(true).build();
            JetStream js = nc.jetStream(jso);

            String data1 = "noackdata1";
            String data2 = "noackdata2";

            PublishAck pa = js.publish(SUBJECT, data1.getBytes());
            assertNull(pa);

            CompletableFuture<PublishAck> f = js.publishAsync(SUBJECT, data2.getBytes());
            assertNull(f);

            JetStreamSubscription sub = js.subscribe(SUBJECT);
            Message m = sub.nextMessage(Duration.ofSeconds(2));
            assertNotNull(m);
            assertEquals(data1, new String(m.getData()));
            m = sub.nextMessage(Duration.ofSeconds(2));
            assertNotNull(m);
            assertEquals(data2, new String(m.getData()));
        });
    }
}
