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
import io.nats.client.StreamConfiguration.DiscardPolicy;
import io.nats.client.StreamConfiguration.RetentionPolicy;
import io.nats.client.StreamConfiguration.StorageType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JetStreamManagementTests extends JetStreamTestBase {

    @Test
    public void testStreamCreate() throws Exception {
        runInJsServer(nc -> {
            StreamInfo si = createMemoryStream(nc, STREAM, subject(0), subject(1));
            StreamConfiguration sc = si.getConfiguration();
            assertEquals(STREAM, sc.getName());
            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));

            assertEquals(StreamConfiguration.RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(StreamConfiguration.DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(StreamConfiguration.StorageType.Memory, sc.getStorageType());

            assertNotNull(si.getConfiguration());
            assertNotNull(si.getStreamState());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxMsgs());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize());
            assertEquals(1, sc.getReplicas());

            assertEquals(Duration.ofSeconds(0), sc.getMaxAge());
            assertEquals(Duration.ofSeconds(120), sc.getDuplicateWindow());

            StreamState ss = si.getStreamState();
            assertEquals(0, ss.getMsgCount());
            assertEquals(0, ss.getByteCount());
            assertEquals(0, ss.getFirstSequence());
            assertEquals(0, ss.getLastSequence());
            assertEquals(0, ss.getConsumerCount());
        });
    }

    @Test
    public void addStream() throws Exception {
        runInJsServer(nc -> {
            long now = ZonedDateTime.now().toEpochSecond();

            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamInfo si = addTestStream(jsm);
            assertTrue(now <= si.getCreateTime().toEpochSecond());

            StreamConfiguration sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(STREAM, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));
            assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize());
            assertEquals(Duration.ZERO, sc.getMaxAge());
            assertEquals(StorageType.Memory, sc.getStorageType());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(1, sc.getReplicas());
            assertFalse(sc.getNoAck());
            assertEquals(Duration.ofMinutes(2), sc.getDuplicateWindow());
            assertNull(sc.getTemplateOwner());

            StreamState state = si.getStreamState();
            assertNotNull(state);
            assertEquals(0, state.getMsgCount());
            assertEquals(0, state.getByteCount());
            assertEquals(0, state.getFirstSequence());
            assertEquals(0, state.getMsgCount());
            assertEquals(0, state.getLastSequence());
            assertEquals(0, state.getConsumerCount());
        });
    }

    @Test
    public void updateStream() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            addTestStream(jsm);

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(STREAM)
                    .storageType(StorageType.Memory)
                    .subjects(subject(0), subject(1))
                    .maxBytes(43)
                    .maxMsgSize(44)
                    .maxAge(Duration.ofDays(100))
                    .discardPolicy(DiscardPolicy.New)
                    .noAck(true)
                    .duplicateWindow(Duration.ofMinutes(3))
                    .build();
            StreamInfo si = jsm.updateStream(sc);
            assertNotNull(si);

            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(STREAM, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));
            assertEquals(43, sc.getMaxBytes());
            assertEquals(44, sc.getMaxMsgSize());
            assertEquals(Duration.ofDays(100), sc.getMaxAge());
            assertEquals(StorageType.Memory, sc.getStorageType());
            assertEquals(DiscardPolicy.New, sc.getDiscardPolicy());
            assertEquals(1, sc.getReplicas());
            assertTrue(sc.getNoAck());
            assertEquals(Duration.ofMinutes(3), sc.getDuplicateWindow());
            assertNull(sc.getTemplateOwner());

            StreamConfiguration scFail = StreamConfiguration.builder().build();
            assertThrows(IllegalArgumentException.class, () -> jsm.updateStream(scFail));
        });
    }

    @Test
    public void addOrUpdateStream_nullConfiguration_isNotValid() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(IllegalArgumentException.class, () -> jsm.updateStream(null));
        });
    }

    @Test
    public void updateStream_cannotUpdate_nonExistentStream() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamConfiguration sc = getTestStreamConfiguration();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(sc));
        });
    }

    @Test
    public void updateStream_cannotChangeMaxConsumers() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            addTestStream(jsm);
            StreamConfiguration sc = getTestStreamConfigurationBuilder()
                    .maxConsumers(2)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(sc));
        });
    }

    @Test
    public void testUpdateStream_cannotChangeRetentionPolicy() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            addTestStream(jsm);
            StreamConfiguration sc = getTestStreamConfigurationBuilder()
                    .retentionPolicy(RetentionPolicy.Interest)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(sc));
        });
    }

    private static StreamInfo addTestStream(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        StreamInfo si = jsm.addStream(getTestStreamConfiguration());
        assertNotNull(si);
        return si;
    }

    private static StreamConfiguration getTestStreamConfiguration() {
        return getTestStreamConfigurationBuilder().build();
    }

    private static StreamConfiguration.Builder getTestStreamConfigurationBuilder() {
        return StreamConfiguration.builder()
                .name(STREAM)
                .storageType(StorageType.Memory)
                .subjects(subject(0), subject(1));
    }

    @Test
    public void testGetStreamInfo() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(JetStreamApiException.class, () -> jsm.getStreamInfo(STREAM));
            createMemoryStream(nc, STREAM, SUBJECT);
            StreamInfo si = jsm.getStreamInfo(STREAM);
            assertEquals(STREAM, si.getConfiguration().getName());
        });
    }

    @Test
    public void testDeleteStream() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(JetStreamApiException.class, () -> jsm.deleteStream(STREAM));
            createMemoryStream(nc, STREAM, SUBJECT);
            assertNotNull(getStreamInfo(jsm, STREAM));
            assertTrue(jsm.deleteStream(STREAM));
            assertNull(getStreamInfo(jsm, STREAM));
            assertThrows(JetStreamApiException.class, () -> jsm.deleteStream(STREAM));
        });
    }

    @Test
    public void testPurgeStream() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(JetStreamApiException.class, () -> jsm.purgeStream(STREAM));
            createMemoryStream(nc, STREAM, SUBJECT);

            StreamInfo si = jsm.getStreamInfo(STREAM);
            assertEquals(0, si.getStreamState().getMsgCount());

            publish(nc, SUBJECT, 1);
            si = jsm.getStreamInfo(STREAM);
            assertEquals(1, si.getStreamState().getMsgCount());

            PurgeResponse pr = jsm.purgeStream(STREAM);
            assertTrue(pr.isSuccess());
            assertEquals(1, pr.getPurgedCount());
        });
    }

    @Test
    public void testAddDeleteConsumer() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(nc, STREAM, subject(0), subject(1));

            List<ConsumerInfo> list = jsm.getConsumers(STREAM);
            assertEquals(0, list.size());

            final ConsumerConfiguration cc = ConsumerConfiguration.builder().build();
            IllegalArgumentException iae =
                    assertThrows(IllegalArgumentException.class, () -> jsm.addOrUpdateConsumer(null, cc));
            assertTrue(iae.getMessage().contains("Stream cannot be null or empty"));
            iae = assertThrows(IllegalArgumentException.class, () -> jsm.addOrUpdateConsumer(STREAM, null));
            assertTrue(iae.getMessage().contains("Config cannot be null"));
            iae = assertThrows(IllegalArgumentException.class, () -> jsm.addOrUpdateConsumer(STREAM, cc));
            assertTrue(iae.getMessage().contains("Durable cannot be null"));

            final ConsumerConfiguration cc0 = ConsumerConfiguration.builder()
                    .durable(durable(0))
                    .build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(STREAM, cc0);
            assertEquals(durable(0), ci.getName());
            assertEquals(durable(0), ci.getConsumerConfiguration().getDurable());
            assertNull(ci.getConsumerConfiguration().getDeliverSubject());

            final ConsumerConfiguration cc1 = ConsumerConfiguration.builder()
                    .durable(durable(1))
                    .deliverSubject(deliver(1))
                    .build();
            ci = jsm.addOrUpdateConsumer(STREAM, cc1);
            assertEquals(durable(1), ci.getName());
            assertEquals(durable(1), ci.getConsumerConfiguration().getDurable());
            assertEquals(deliver(1), ci.getConsumerConfiguration().getDeliverSubject());

            List<String> consumers = jsm.getConsumerNames(STREAM);
            assertEquals(2, consumers.size());
            assertTrue(jsm.deleteConsumer(STREAM, cc1.getDurable()));
            consumers = jsm.getConsumerNames(STREAM);
            assertEquals(1, consumers.size());
            assertThrows(JetStreamApiException.class, () -> jsm.deleteConsumer(STREAM, cc1.getDurable()));
        });
    }

    @Test
    public void testCreateConsumersWithFilters() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder().durable(DURABLE);

            // plain subject
            createMemoryStream(nc, STREAM, SUBJECT);

            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(SUBJECT).build());

            assertThrows(JetStreamApiException.class,
                    () -> jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subject("not-match")).build()));

            // wildcard subject
            jsm.deleteStream(STREAM);
            createMemoryStream(nc, STREAM, SUBJECT_STAR);

            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subject("A")).build());

            assertThrows(JetStreamApiException.class,
                    () -> jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subject("not-match")).build()));

            // gt subject
            jsm.deleteStream(STREAM);
            createMemoryStream(nc, STREAM, SUBJECT_GT);

            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subject("A")).build());

            assertThrows(JetStreamApiException.class,
                    () -> jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subject("not-match")).build()));
        });
    }
    @Test
    public void testGetConsumerInfo() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(nc, STREAM, SUBJECT);
            assertThrows(JetStreamApiException.class, () -> jsm.getConsumerInfo(STREAM, DURABLE));
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(DURABLE).build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(STREAM, cc);
            assertEquals(STREAM, ci.getStreamName());
            assertEquals(DURABLE, ci.getName());
            ci = jsm.getConsumerInfo(STREAM, DURABLE);
            assertEquals(STREAM, ci.getStreamName());
            assertEquals(DURABLE, ci.getName());
            assertThrows(JetStreamApiException.class, () -> jsm.getConsumerInfo(STREAM, durable(999)));
        });
    }

    @Test
    public void testGetConsumers() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(nc, STREAM, subject(0), subject(1));

            addConsumers(jsm, STREAM, 600, "A", null); // getConsumers pages at 256

            List<ConsumerInfo> list = jsm.getConsumers(STREAM);
            assertEquals(600, list.size());

            addConsumers(jsm, STREAM, 500, "B", null); // getConsumerNames pages at 1024
            List<String> names = jsm.getConsumerNames(STREAM);
            assertEquals(1100, names.size());
        });
    }

    private List<ConsumerInfo> addConsumers(JetStreamManagement jsm, String stream, int count, String durableVary, String filterSubject) throws IOException, JetStreamApiException {
        List<ConsumerInfo> consumers = new ArrayList<>();
        for (int x = 1; x <= count; x++) {
            String dur = durable(durableVary, x);
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(dur)
                    .filterSubject(filterSubject)
                    .build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(stream, cc);
            consumers.add(ci);
            assertEquals(dur, ci.getName());
            assertEquals(dur, ci.getConsumerConfiguration().getDurable());
            assertNull(ci.getConsumerConfiguration().getDeliverSubject());
        }
        return consumers;
    }

    @Test
    public void testGetStreams() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            addStreams(nc, 600, 0); // getStreams pages at 256

            List<StreamInfo> list = jsm.getStreams();
            assertEquals(600, list.size());

            addStreams(nc, 500, 600); // getStreamNames pages at 1024

            List<String> names = jsm.getStreamNames();
            assertEquals(1100, names.size());
        });
    }

    private void addStreams(Connection nc, int count, int adj) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            createMemoryStream(nc, stream(x + adj), subject(x + adj));
        }
    }

    @Test
    public void testGetAndDeleteMessage() throws Exception {
        runInJsServer(nc -> {
            createMemoryStream(nc, STREAM, SUBJECT);
            JetStream js = nc.jetStream();

            Headers h = new Headers();
            h.add("foo", "bar");

            js.publish(NatsMessage.builder().subject(SUBJECT).headers(h).data(dataBytes(1)).build());
            js.publish(NatsMessage.builder().subject(SUBJECT).headers(h).data(dataBytes(2)).build());

            JetStreamManagement jsm = nc.jetStreamManagement();

            MessageInfo mi = jsm.getMessage(STREAM, 1);
            assertNotNull(mi.toString());
            assertEquals(data(1), new String(mi.getData()));
            assertNotNull(mi.getHeaders());
            assertEquals("bar", mi.getHeaders().get("foo").get(0));
            assertTrue(jsm.deleteMessage(STREAM, 1));
            assertThrows(JetStreamApiException.class, () -> jsm.deleteMessage(STREAM, 1));
            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(STREAM, 1));
            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(STREAM, 3));
            assertThrows(JetStreamApiException.class, () -> jsm.deleteMessage(stream(999), 1));
            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(stream(999), 1));
        });
    }
}
