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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.nats.client.support.NatsJetStreamConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamManagementTests extends JetStreamTestBase {

    @Test
    public void testStreamCreate() throws Exception {
        runInJsServer(nc -> {
            long now = ZonedDateTime.now().toEpochSecond();

            JetStreamManagement jsm = nc.jetStreamManagement();

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(STREAM)
                    .storageType(StorageType.Memory)
                    .subjects(subject(0), subject(1))
                    .build();

            StreamInfo si = jsm.addStream(sc);
            assertTrue(now <= si.getCreateTime().toEpochSecond());

            assertNotNull(si.getConfiguration());
            sc = si.getConfiguration();
            assertEquals(STREAM, sc.getName());

            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));

            assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(StorageType.Memory, sc.getStorageType());

            assertNotNull(si.getStreamState());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxMsgs());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize());
            assertEquals(1, sc.getReplicas());

            assertEquals(Duration.ZERO, sc.getMaxAge());
            assertEquals(Duration.ofSeconds(120), sc.getDuplicateWindow());
            assertFalse(sc.getNoAck());
            assertNull(sc.getTemplateOwner());

            StreamState ss = si.getStreamState();
            assertEquals(0, ss.getMsgCount());
            assertEquals(0, ss.getByteCount());
            assertEquals(0, ss.getFirstSequence());
            assertEquals(0, ss.getLastSequence());
            assertEquals(0, ss.getConsumerCount());
        });
    }

    @Test
    public void testStreamCreateWithNoSubject() throws Exception {
        runInJsServer(nc -> {
            long now = ZonedDateTime.now().toEpochSecond();

            JetStreamManagement jsm = nc.jetStreamManagement();

            StreamConfiguration sc = StreamConfiguration.builder()
                    .name(STREAM)
                    .storageType(StorageType.Memory)
                    .build();

            StreamInfo si = jsm.addStream(sc);
            assertTrue(now <= si.getCreateTime().toEpochSecond());

            sc = si.getConfiguration();
            assertEquals(STREAM, sc.getName());

            assertEquals(1, sc.getSubjects().size());
            assertEquals(STREAM, sc.getSubjects().get(0));

            assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(StorageType.Memory, sc.getStorageType());

            assertNotNull(si.getConfiguration());
            assertNotNull(si.getStreamState());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxMsgs());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize());
            assertEquals(1, sc.getReplicas());

            assertEquals(Duration.ZERO, sc.getMaxAge());
            assertEquals(Duration.ofSeconds(120), sc.getDuplicateWindow());
            assertFalse(sc.getNoAck());
            assertNull(sc.getTemplateOwner());

            StreamState ss = si.getStreamState();
            assertEquals(0, ss.getMsgCount());
            assertEquals(0, ss.getByteCount());
            assertEquals(0, ss.getFirstSequence());
            assertEquals(0, ss.getLastSequence());
            assertEquals(0, ss.getConsumerCount());
        });
    }

    @Test
    public void testUpdateStream() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamInfo si = addTestStream(jsm);
            StreamConfiguration sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(STREAM, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));
            assertEquals(-1, sc.getMaxMsgs());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize());
            assertEquals(Duration.ZERO, sc.getMaxAge());
            assertEquals(StorageType.Memory, sc.getStorageType());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(1, sc.getReplicas());
            assertFalse(sc.getNoAck());
            assertEquals(Duration.ofMinutes(2), sc.getDuplicateWindow());
            assertNull(sc.getTemplateOwner());

            sc = StreamConfiguration.builder()
                .name(STREAM)
                .storageType(StorageType.Memory) // File is default, this ensures it's not a change
                .subjects(subject(0), subject(1), subject(2))
                .maxMessages(42)
                .maxBytes(43)
                .maxMsgSize(44)
                .maxAge(Duration.ofDays(100))
                .discardPolicy(DiscardPolicy.New)
                .noAck(true)
                .duplicateWindow(Duration.ofMinutes(3))
                .maxMessagesPerSubject(45)
                .build();
            si = jsm.updateStream(sc);
            assertNotNull(si);

            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(STREAM, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(3, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));
            assertEquals(subject(2), sc.getSubjects().get(2));
            assertEquals(42, sc.getMaxMsgs());
            assertEquals(43, sc.getMaxBytes());
            assertEquals(44, sc.getMaxMsgSize());
            assertEquals(45, sc.getMaxMsgsPerSubject());
            assertEquals(Duration.ofDays(100), sc.getMaxAge());
            assertEquals(StorageType.Memory, sc.getStorageType());
            assertEquals(DiscardPolicy.New, sc.getDiscardPolicy());
            assertEquals(1, sc.getReplicas());
            assertTrue(sc.getNoAck());
            assertEquals(Duration.ofMinutes(3), sc.getDuplicateWindow());
            assertNull(sc.getTemplateOwner());
        });
    }

    @Test
    public void testAddUpdateStreamInvalids() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            StreamConfiguration scNoName = StreamConfiguration.builder().build();
            assertThrows(IllegalArgumentException.class, () -> jsm.addStream(null));
            assertThrows(IllegalArgumentException.class, () -> jsm.addStream(scNoName));
            assertThrows(IllegalArgumentException.class, () -> jsm.updateStream(null));
            assertThrows(IllegalArgumentException.class, () -> jsm.updateStream(scNoName));

            // cannot update non existent stream
            StreamConfiguration sc = getTestStreamConfiguration();
            // stream not added yet
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(sc));

            // add the stream
            jsm.addStream(sc);

            // cannot change storage type
            StreamConfiguration scMemToFile = getTestStreamConfigurationBuilder()
                .storageType(StorageType.File)
                .build();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(scMemToFile));

            // cannot change MaxConsumers
            StreamConfiguration scMaxCon = getTestStreamConfigurationBuilder()
                    .maxConsumers(2)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(scMaxCon));

            // cannot change RetentionPolicy
            StreamConfiguration scReten = getTestStreamConfigurationBuilder()
                    .retentionPolicy(RetentionPolicy.Interest)
                    .build();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(scReten));

            jsm.deleteStream(STREAM);

            jsm.addStream(getTestStreamConfigurationBuilder().storageType(StorageType.File).build());
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(getTestStreamConfiguration()));
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

            String[] subjects = new String[6];
            for (int x = 0; x < 5; x++) {
                subjects[x] = subject(x);
            }
            subjects[5] = "foo.>";
            createMemoryStream(jsm, STREAM, subjects);

            List<PublishAck> packs = new ArrayList<>();
            JetStream js = nc.jetStream();
            for (int x = 0; x < 5; x++) {
                jsPublish(js, subject(x), x + 1);
                PublishAck pa = jsPublish(js, subject(x), data(x + 2));
                packs.add(pa);
                jsm.deleteMessage(STREAM, pa.getSeqno());
            }
            jsPublish(js, "foo.bar", 6);

            StreamInfo si = jsm.getStreamInfo(STREAM);
            assertEquals(STREAM, si.getConfiguration().getName());
            assertEquals(6, si.getStreamState().getSubjectCount());
            assertNull(si.getStreamState().getSubjects());
            assertEquals(5, si.getStreamState().getDeletedCount());
            assertEquals(0, si.getStreamState().getDeleted().size());

            si = jsm.getStreamInfo(STREAM, StreamInfoOptions.builder().allSubjects().deletedDetails().build());
            assertEquals(STREAM, si.getConfiguration().getName());
            assertEquals(6, si.getStreamState().getSubjectCount());
            List<Subject> list = si.getStreamState().getSubjects();
            assertNotNull(list);
            assertEquals(5, si.getStreamState().getDeletedCount());
            assertEquals(5, si.getStreamState().getDeleted().size());
            assertEquals(6, list.size());
            Map<String, Subject> map = new HashMap<>();
            for (Subject su : list) {
                map.put(su.getName(), su);
            }
            for (int x = 0; x < 5; x++) {
                Subject s = map.get(subject(x));
                assertNotNull(s);
                assertEquals(x + 1, s.getCount());
            }
            Subject sf = map.get("foo.bar");
            assertNotNull(sf);
            assertEquals(6, sf.getCount());

            for (PublishAck pa : packs) {
                assertTrue(si.getStreamState().getDeleted().contains(pa.getSeqno()));
            }

            jsPublish(js, "foo.baz", 2);
            sleep(100);

            si = jsm.getStreamInfo(STREAM, StreamInfoOptions.builder().filterSubjects("foo.>").deletedDetails().build());
            assertEquals(7, si.getStreamState().getSubjectCount());
            list = si.getStreamState().getSubjects();
            assertNotNull(list);
            assertEquals(2, list.size());
            map = new HashMap<>();
            for (Subject su : list) {
                map.put(su.getName(), su);
            }
            Subject s = map.get("foo.bar");
            assertNotNull(s);
            assertEquals(6, s.getCount());
            s = map.get("foo.baz");
            assertNotNull(s);
            assertEquals(2, s.getCount());
        });
    }

    @Test
    public void testDeleteStream() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStreamApiException jsapiEx =
                    assertThrows(JetStreamApiException.class, () -> jsm.deleteStream(STREAM));
            assertEquals(10059, jsapiEx.getApiErrorCode());

            createDefaultTestStream(jsm);
            assertNotNull(jsm.getStreamInfo(STREAM));
            assertTrue(jsm.deleteStream(STREAM));

            jsapiEx = assertThrows(JetStreamApiException.class, () -> jsm.getStreamInfo(STREAM));
            assertEquals(10059, jsapiEx.getApiErrorCode());

            jsapiEx = assertThrows(JetStreamApiException.class, () -> jsm.deleteStream(STREAM));
            assertEquals(10059, jsapiEx.getApiErrorCode());
        });
    }

    @Test
    public void testPurgeStreamAndOptions() throws Exception {
        runInJsServer(nc -> {
            // invalid to have both keep and seq
            assertThrows(IllegalArgumentException.class,
                () -> PurgeOptions.builder().keep(1).sequence(1).build());

            JetStreamManagement jsm = nc.jetStreamManagement();

            // error to purge a stream that does not exist
            assertThrows(JetStreamApiException.class, () -> jsm.purgeStream(STREAM));

            createMemoryStream(jsm, STREAM, subject(1), subject(2));

            StreamInfo si = jsm.getStreamInfo(STREAM);
            assertEquals(0, si.getStreamState().getMsgCount());

            jsPublish(nc, subject(1), 10);
            si = jsm.getStreamInfo(STREAM);
            assertEquals(10, si.getStreamState().getMsgCount());

            PurgeOptions options = PurgeOptions.builder().keep(7).build();
            PurgeResponse pr = jsm.purgeStream(STREAM, options);
            assertTrue(pr.isSuccess());
            assertEquals(3, pr.getPurged());

            options = PurgeOptions.builder().sequence(9).build();
            pr = jsm.purgeStream(STREAM, options);
            assertTrue(pr.isSuccess());
            assertEquals(5, pr.getPurged());
            si = jsm.getStreamInfo(STREAM);
            assertEquals(2, si.getStreamState().getMsgCount());

            pr = jsm.purgeStream(STREAM);
            assertTrue(pr.isSuccess());
            assertEquals(2, pr.getPurged());
            si = jsm.getStreamInfo(STREAM);
            assertEquals(0, si.getStreamState().getMsgCount());

            jsPublish(nc, subject(1), 10);
            jsPublish(nc, subject(2), 10);
            si = jsm.getStreamInfo(STREAM);
            assertEquals(20, si.getStreamState().getMsgCount());
            jsm.purgeStream(STREAM, PurgeOptions.subject(subject(1)));
            si = jsm.getStreamInfo(STREAM);
            assertEquals(10, si.getStreamState().getMsgCount());

            options = PurgeOptions.builder().subject(subject(1)).sequence(1).build();
            assertEquals(subject(1), options.getSubject());
            assertEquals(1, options.getSequence());

            options = PurgeOptions.builder().subject(subject(1)).keep(2).build();
            assertEquals(2, options.getKeep());
        });
    }

    @Test
    public void testAddDeleteConsumer() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(jsm, STREAM, subject(0), subject(1));

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
    public void testValidConsumerUpdates() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(jsm, STREAM, SUBJECT_GT);

            ConsumerConfiguration cc = prepForUpdateTest(jsm);
            cc = ConsumerConfiguration.builder(cc).deliverSubject(deliver(2)).build();
            assertValidAddOrUpdate(jsm, cc);

            cc = prepForUpdateTest(jsm);
            cc = ConsumerConfiguration.builder(cc).ackWait(Duration.ofSeconds(5)).build();
            assertValidAddOrUpdate(jsm, cc);

            cc = prepForUpdateTest(jsm);
            cc = ConsumerConfiguration.builder(cc).rateLimit(100).build();
            assertValidAddOrUpdate(jsm, cc);

            cc = prepForUpdateTest(jsm);
            cc = ConsumerConfiguration.builder(cc).maxAckPending(100).build();
            assertValidAddOrUpdate(jsm, cc);

            cc = prepForUpdateTest(jsm);
            cc = ConsumerConfiguration.builder(cc).maxDeliver(4).build();
            assertValidAddOrUpdate(jsm, cc);

            if (nc.getServerInfo().isNewerVersionThan("2.8.4")) {
                cc = prepForUpdateTest(jsm);
                cc = ConsumerConfiguration.builder(cc).filterSubject(SUBJECT_STAR).build();
                assertValidAddOrUpdate(jsm, cc);
            }
        });
    }

    @Test
    public void testInvalidConsumerUpdates() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(jsm, STREAM, SUBJECT_GT);

            ConsumerConfiguration cc = prepForUpdateTest(jsm);
            cc = ConsumerConfiguration.builder(cc).deliverPolicy(DeliverPolicy.New).build();
            assertInvalidConsumerUpdate(jsm, cc);

            if (nc.getServerInfo().isSameOrOlderThanVersion("2.8.4")) {
                cc = prepForUpdateTest(jsm);
                cc = ConsumerConfiguration.builder(cc).filterSubject(SUBJECT_STAR).build();
                assertInvalidConsumerUpdate(jsm, cc);
            }

            cc = prepForUpdateTest(jsm);
            cc = ConsumerConfiguration.builder(cc).idleHeartbeat(Duration.ofMillis(111)).build();
            assertInvalidConsumerUpdate(jsm, cc);
        });
    }

    private ConsumerConfiguration prepForUpdateTest(JetStreamManagement jsm) throws IOException, JetStreamApiException {
        try {
            jsm.deleteConsumer(STREAM, durable(1));
        }
        catch (Exception e) { /* ignore */ }

        ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(durable(1))
                .ackPolicy(AckPolicy.Explicit)
                .deliverSubject(deliver(1))
                .maxDeliver(3)
                .filterSubject(SUBJECT_GT)
                .build();
        assertValidAddOrUpdate(jsm, cc);
        return cc;
    }

    private void assertInvalidConsumerUpdate(JetStreamManagement jsm, ConsumerConfiguration cc) {
        JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> jsm.addOrUpdateConsumer(STREAM, cc));
        assertEquals(10012, e.getApiErrorCode());
        assertEquals(500, e.getErrorCode());
    }

    private void assertValidAddOrUpdate(JetStreamManagement jsm, ConsumerConfiguration cc) throws IOException, JetStreamApiException {
        ConsumerInfo ci = jsm.addOrUpdateConsumer(STREAM, cc);
        ConsumerConfiguration cicc = ci.getConsumerConfiguration();
        assertEquals(cc.getDurable(), ci.getName());
        assertEquals(cc.getDurable(), cicc.getDurable());
        assertEquals(cc.getDeliverSubject(), cicc.getDeliverSubject());
        assertEquals(cc.getMaxDeliver(), cicc.getMaxDeliver());
        assertEquals(cc.getDeliverPolicy(), cicc.getDeliverPolicy());

        List<String> consumers = jsm.getConsumerNames(STREAM);
        assertEquals(1, consumers.size());
        assertEquals(cc.getDurable(), consumers.get(0));
    }

    @Test
    public void testCreateConsumersWithFilters() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);

            // plain subject
            ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder().durable(DURABLE);
            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(SUBJECT).build());

            assertThrows(JetStreamApiException.class,
                    () -> jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("not-match")).build()));

            // wildcard subject
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, SUBJECT_STAR);

            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("A")).build());

            if (nc.getServerInfo().isSameOrOlderThanVersion("2.8.4")) {
                assertThrows(JetStreamApiException.class,
                    () -> jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("not-match")).build()));
            }

            // gt subject
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, SUBJECT_GT);

            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("A")).build());

            // try to filter against durable with mismatch, pull

            jsm.addOrUpdateConsumer(STREAM, ConsumerConfiguration.builder()
                    .durable(durable(42))
                    .filterSubject(subjectDot("F"))
                    .build()
            );
        });
    }

    @Test
    public void testGetConsumerInfo() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createDefaultTestStream(jsm);
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
            createMemoryStream(jsm, STREAM, subject(0), subject(1));

            addConsumers(jsm, STREAM, 600, "A", null); // getConsumers pages at 256

            List<ConsumerInfo> list = jsm.getConsumers(STREAM);
            assertEquals(600, list.size());

            addConsumers(jsm, STREAM, 500, "B", null); // getConsumerNames pages at 1024
            List<String> names = jsm.getConsumerNames(STREAM);
            assertEquals(1100, names.size());
        });
    }

    private void addConsumers(JetStreamManagement jsm, String stream, int count, String durableVary, String filterSubject) throws IOException, JetStreamApiException {
        for (int x = 1; x <= count; x++) {
            String dur = durable(durableVary, x);
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(dur)
                    .filterSubject(filterSubject)
                    .build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(stream, cc);
            assertEquals(dur, ci.getName());
            assertEquals(dur, ci.getConsumerConfiguration().getDurable());
            assertNull(ci.getConsumerConfiguration().getDeliverSubject());
        }
    }

    @Test
    public void testGetStreams() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            addStreams(jsm, 600, 0); // getStreams pages at 256

            List<StreamInfo> list = jsm.getStreams();
            assertEquals(600, list.size());

            addStreams(jsm, 500, 600); // getStreamNames pages at 1024

            List<String> names = jsm.getStreamNames();
            assertEquals(1100, names.size());
        });
    }

    private void addStreams(JetStreamManagement jsm, int count, int adj) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            createMemoryStream(jsm, stream(x + adj), subject(x + adj));
        }
    }

    @Test
    public void testGetAndDeleteMessage() throws Exception {
        MessageDeleteRequest mdr = new MessageDeleteRequest(1, true);
        assertEquals("{\"seq\":1}", mdr.toJson());
        assertEquals(1, mdr.getSequence());
        assertTrue(mdr.isErase());
        assertFalse(mdr.isNoErase());

        mdr = new MessageDeleteRequest(1, false);
        assertEquals("{\"seq\":1,\"no_erase\":true}", mdr.toJson());
        assertEquals(1, mdr.getSequence());
        assertFalse(mdr.isErase());
        assertTrue(mdr.isNoErase());

        runInJsServer(nc -> {
            createDefaultTestStream(nc);
            JetStream js = nc.jetStream();

            Headers h = new Headers();
            h.add("foo", "bar");

            ZonedDateTime beforeCreated = ZonedDateTime.now();
            js.publish(NatsMessage.builder().subject(SUBJECT).headers(h).data(dataBytes(1)).build());
            js.publish(NatsMessage.builder().subject(SUBJECT).build());

            JetStreamManagement jsm = nc.jetStreamManagement();

            MessageInfo mi = jsm.getMessage(STREAM, 1);
            assertNotNull(mi.toString());
            assertEquals(SUBJECT, mi.getSubject());
            assertEquals(data(1), new String(mi.getData()));
            assertEquals(1, mi.getSeq());
            assertTrue(mi.getTime().toEpochSecond() >= beforeCreated.toEpochSecond());
            assertNotNull(mi.getHeaders());
            assertEquals("bar", mi.getHeaders().get("foo").get(0));

            mi = jsm.getMessage(STREAM, 2);
            assertNotNull(mi.toString());
            assertEquals(SUBJECT, mi.getSubject());
            assertNull(mi.getData());
            assertEquals(2, mi.getSeq());
            assertTrue(mi.getTime().toEpochSecond() >= beforeCreated.toEpochSecond());
            assertNull(mi.getHeaders());

            assertTrue(jsm.deleteMessage(STREAM, 1, false)); // added coverage for use of erase (no_erase) flag.
            assertThrows(JetStreamApiException.class, () -> jsm.deleteMessage(STREAM, 1));
            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(STREAM, 1));
            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(STREAM, 3));
            assertThrows(JetStreamApiException.class, () -> jsm.deleteMessage(stream(999), 1));
            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(stream(999), 1));
        });
    }

    @Test
    public void testAuthCreateUpdateStream() throws Exception {

        try (NatsTestServer ts = new NatsTestServer("src/test/resources/js_authorization.conf", false)) {
            Options optionsSrc = new Options.Builder().server(ts.getURI())
                .userInfo("serviceup".toCharArray(), "uppass".toCharArray()).build();

            try (Connection nc = Nats.connect(optionsSrc)) {
                JetStreamManagement jsm = nc.jetStreamManagement();

                // add streams with both account
                StreamConfiguration sc = StreamConfiguration.builder()
                    .name(STREAM)
                    .storageType(StorageType.Memory)
                    .subjects(subject(1))
                    .build();
                StreamInfo si = jsm.addStream(sc);

                sc = StreamConfiguration.builder(si.getConfiguration())
                    .addSubjects(subject(2))
                    .build();

                jsm.updateStream(sc);
            }
        }
    }

    @Test
    public void testSealed() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            StreamInfo si = createMemoryStream(jsm, STREAM, SUBJECT);
            assertFalse(si.getConfiguration().getSealed());

            StreamConfiguration sc = new StreamConfiguration.Builder(si.getConfiguration()) {
                @Override
                public StreamConfiguration build() {
                    sealed(true);
                    return super.build();
                }
            }.build();
            si = jsm.updateStream(sc);
            assertTrue(si.getConfiguration().getSealed());
        });
    }

    @Test
    public void testStorageTypeCoverage() {
        assertEquals(StorageType.File, StorageType.get("file"));
        assertEquals(StorageType.File, StorageType.get("FILE"));
        assertEquals(StorageType.Memory, StorageType.get("memory"));
        assertEquals(StorageType.Memory, StorageType.get("MEMORY"));
        assertNull(StorageType.get("nope"));
    }

    @Test
    public void testGetStreamNamesBySubjectFilter() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createMemoryStream(jsm, stream(1), "foo");
            createMemoryStream(jsm, stream(2), "bar");
            createMemoryStream(jsm, stream(3), "a.a");
            createMemoryStream(jsm, stream(4), "a.b");

            List<String> list = jsm.getStreamNamesBySubjectFilter("*");
            assertStreamNameList(list, 1, 2);

            list = jsm.getStreamNamesBySubjectFilter(">");
            assertStreamNameList(list, 1, 2, 3, 4);

            list = jsm.getStreamNamesBySubjectFilter("*.*");
            assertStreamNameList(list, 3, 4);

            list = jsm.getStreamNamesBySubjectFilter("a.>");
            assertStreamNameList(list, 3, 4);

            list = jsm.getStreamNamesBySubjectFilter("a.*");
            assertStreamNameList(list, 3, 4);

            list = jsm.getStreamNamesBySubjectFilter("foo");
            assertStreamNameList(list, 1);

            list = jsm.getStreamNamesBySubjectFilter("a.a");
            assertStreamNameList(list, 3);

            list = jsm.getStreamNamesBySubjectFilter("nomatch");
            assertStreamNameList(list);
        });
    }

    private void assertStreamNameList(List<String> list, int... ids) {
        assertNotNull(list);
        assertEquals(ids.length, list.size());
        for (int id : ids) {
            assertTrue(list.contains(stream(id)));
        }
    }

    @Test
    public void testConsumerReplica() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            createMemoryStream(jsm, STREAM, subject(0), subject(1));

            final ConsumerConfiguration cc0 = ConsumerConfiguration.builder()
                    .durable(durable(0))
                    .build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(STREAM, cc0);
            // server returns 0 when value is not set
            assertEquals(0, ci.getConsumerConfiguration().getNumReplicas());

            final ConsumerConfiguration cc1 = ConsumerConfiguration.builder()
                    .durable(durable(0))
                    .numReplicas(1)
                    .build();
            ci = jsm.addOrUpdateConsumer(STREAM, cc1);
            assertEquals(1, ci.getConsumerConfiguration().getNumReplicas());
        });
    }

    @Test
    public void testGetDirect() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            StreamConfiguration sc = StreamConfiguration.builder()
                .name(STREAM)
                .storageType(StorageType.Memory)
                .subjects(subject(1), subject(2))
                .allowDirect(true)
                .build();

            jsm.addStream(sc);

            JetStream js = nc.jetStream();
            js.publish(NatsMessage.builder().subject(subject(1)).data("s1-q1").build());
            js.publish(NatsMessage.builder().subject(subject(2)).data("s2-q2").build());
            js.publish(NatsMessage.builder().subject(subject(1)).data("s1-q3").build());
            js.publish(NatsMessage.builder().subject(subject(2)).data("s2-q4").build());
            js.publish(NatsMessage.builder().subject(subject(1)).data("s1-q5").build());
            js.publish(NatsMessage.builder().subject(subject(2)).data("s2-q6").build());

            assertDirect(1, 1, jsm.getMessageDirect(STREAM, MessageGetRequest.forSequence(1)));
            assertDirect(1, 5, jsm.getMessageDirect(STREAM, MessageGetRequest.lastForSubject(subject(1))));
            assertDirect(2, 6, jsm.getMessageDirect(STREAM, MessageGetRequest.lastForSubject(subject(2))));

            assertDirect(1, 1, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(-1, subject(1))));
            assertDirect(2, 2, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(-1, subject(2))));
            assertDirect(1, 1, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(0, subject(1))));
            assertDirect(2, 2, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(0, subject(2))));
            assertDirect(1, 1, jsm.getMessageDirect(STREAM, MessageGetRequest.firstForSubject(subject(1))));
            assertDirect(2, 2, jsm.getMessageDirect(STREAM, MessageGetRequest.firstForSubject(subject(2))));

            assertDirect(1, 1, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(1, subject(1))));
            assertDirect(2, 2, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(1, subject(2))));

            assertDirect(1, 3, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(2, subject(1))));
            assertDirect(2, 2, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(2, subject(2))));

            assertDirect(1, 5, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(5, subject(1))));
            assertDirect(2, 6, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(5, subject(2))));

            assertStatus(408, jsm.getMessageDirect(STREAM, MessageGetRequest.forSequence(-1)));
            assertStatus(408, jsm.getMessageDirect(STREAM, MessageGetRequest.forSequence(0)));
            assertStatus(404, jsm.getMessageDirect(STREAM, MessageGetRequest.forSequence(9)));
            assertStatus(404, jsm.getMessageDirect(STREAM, MessageGetRequest.lastForSubject("not-a-subject")));
            assertStatus(404, jsm.getMessageDirect(STREAM, MessageGetRequest.firstForSubject("not-a-subject")));
            assertStatus(404, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(9, subject(1))));
            assertStatus(404, jsm.getMessageDirect(STREAM, MessageGetRequest.nextForSubject(1, "not-a-subject")));

            // can't do direct on stream if if wasn't configured with allowDirect
/* commenting this out for now until I know what the actual expected behavior is
            sc = StreamConfiguration.builder()
                .name(stream(3))
                .storageType(StorageType.Memory)
                .subjects(subject(3))
                .build();

            jsm.addStream(sc);

            js.publish(NatsMessage.builder().subject(subject(3)).data("data1").build());
            js.publish(NatsMessage.builder().subject(subject(3)).data("data2").build());
            js.publish(NatsMessage.builder().subject(subject(3)).data("data3").build());

            assertThrows(IOException.class, () -> jsm.getMessageDirect(stream(3), MessageGetRequest.forSequence(-1)));
            assertThrows(IOException.class, () -> jsm.getMessageDirect(stream(3), MessageGetRequest.forSequence(1)));
            assertThrows(IOException.class, () -> jsm.getMessageDirect(stream(3), MessageGetRequest.lastForSubject(subject(3))));
            assertThrows(IOException.class, () -> jsm.getMessageDirect(stream(3), MessageGetRequest.firstForSubject(subject(3))));
            assertThrows(IOException.class, () -> jsm.getMessageDirect(stream(3), MessageGetRequest.nextForSubject(1, subject(3))));
*/
            // coverage for deprecated methods
            MessageGetRequest.seqBytes(1);
            MessageGetRequest.lastBySubjectBytes(SUBJECT);
            new MessageGetRequest(1);
            new MessageGetRequest(SUBJECT);
        });
    }

    private void assertStatus(int status, Message statusMsg) {
        assertTrue(statusMsg.isStatusMessage());
        assertEquals(status, statusMsg.getStatus().getCode());
    }

    private void assertDirect(int subj, long seq, Message m) {
        Headers h = m.getHeaders();
        assertNotNull(h);
        assertEquals(STREAM, h.getFirst(NATS_STREAM));
        assertEquals(subject(subj), h.getFirst(NATS_SUBJECT));
        assertEquals("" + seq, h.getFirst(NATS_SEQUENCE));
        assertNotNull(h.getFirst(NATS_TIMESTAMP));
        assertEquals("s" + subj + "-q" + seq, new String(m.getData()));
    }
}
