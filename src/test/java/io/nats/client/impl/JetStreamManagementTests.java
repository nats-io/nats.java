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
import io.nats.client.support.DateTimeUtils;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.nats.client.support.DateTimeUtils.DEFAULT_TIME;
import static io.nats.client.support.DateTimeUtils.ZONE_ID_GMT;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.utils.ResourceUtils.dataAsString;
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
            assertNotNull(si.getStreamState().toString()); // coverage
            assertTrue(now <= si.getCreateTime().toEpochSecond());

            assertNotNull(si.getConfiguration());
            sc = si.getConfiguration();
            assertEquals(STREAM, sc.getName());

            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject(0), sc.getSubjects().get(0));
            assertEquals(subject(1), sc.getSubjects().get(1));
            assertTrue(subject(0).compareTo(subject(1)) != 0); // coverage

            assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(StorageType.Memory, sc.getStorageType());

            assertNotNull(si.getStreamState());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxMsgs());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaxMsgSize()); // COVERAGE for deprecated
            assertEquals(-1, sc.getMaximumMessageSize());
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

            if (nc.getServerInfo().isSameOrNewerThanVersion("2.10")) {
                JetStream js = jsm.jetStream();
                String stream = stream();
                sc = StreamConfiguration.builder()
                    .name(stream)
                    .storageType(StorageType.Memory)
                    .firstSequence(42)
                    .subjects("test-first-seq").build();
                si = jsm.addStream(sc);
                assertNotNull(si.getTimestamp());
                assertEquals(42, si.getConfiguration().getFirstSequence());
                PublishAck pa = js.publish("test-first-seq", null);
                assertEquals(42, pa.getSeqno());
            }
        });
    }

    @Test
    public void testStreamMetadata() throws Exception {
        jsServer.run(TestBase::atLeast2_9_0, nc -> {
            Map<String, String> metaData = new HashMap<>(); metaData.put(META_KEY, META_VALUE);
            JetStreamManagement jsm = nc.jetStreamManagement();

            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream())
                .storageType(StorageType.Memory)
                .subjects(subject())
                .metadata(metaData)
                .build();

            StreamInfo si = jsm.addStream(sc);
            assertNotNull(si.getConfiguration());
            assertMetaData(si.getConfiguration().getMetadata());
        });
    }

    @Test
    public void testStreamCreateWithNoSubject() throws Exception {
        jsServer.run(nc -> {
            long now = ZonedDateTime.now().toEpochSecond();

            JetStreamManagement jsm = nc.jetStreamManagement();

            String stream = stream();
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .storageType(StorageType.Memory)
                .build();

            StreamInfo si = jsm.addStream(sc);
            assertTrue(now <= si.getCreateTime().toEpochSecond());

            sc = si.getConfiguration();
            assertEquals(stream, sc.getName());

            assertEquals(1, sc.getSubjects().size());
            assertEquals(stream, sc.getSubjects().get(0));

            assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(StorageType.Memory, sc.getStorageType());

            assertNotNull(si.getConfiguration());
            assertNotNull(si.getStreamState());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxMsgs());
            assertEquals(-1, sc.getMaxBytes());
            assertEquals(-1, sc.getMaximumMessageSize());
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
            assertEquals(-1, sc.getMaximumMessageSize());
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
                .maximumMessageSize(44)
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
            assertEquals(44, sc.getMaximumMessageSize());
            assertEquals(45, sc.getMaxMsgsPerSubject());
            assertEquals(Duration.ofDays(100), sc.getMaxAge());
            assertEquals(StorageType.Memory, sc.getStorageType());
            assertEquals(DiscardPolicy.New, sc.getDiscardPolicy());
            assertEquals(1, sc.getReplicas());
            assertTrue(sc.getNoAck());
            assertEquals(Duration.ofMinutes(3), sc.getDuplicateWindow());
            assertNull(sc.getTemplateOwner());

            // allowed to change Allow Direct
            jsm.deleteStream(STREAM);
            jsm.addStream(getTestStreamConfigurationBuilder().allowDirect(false).build());
            jsm.updateStream(getTestStreamConfigurationBuilder().allowDirect(true).build());
            jsm.updateStream(getTestStreamConfigurationBuilder().allowDirect(false).build());

            // allowed to change Mirror Direct
            jsm.deleteStream(STREAM);
            jsm.addStream(getTestStreamConfigurationBuilder().mirrorDirect(false).build());
            jsm.updateStream(getTestStreamConfigurationBuilder().mirrorDirect(true).build());
            jsm.updateStream(getTestStreamConfigurationBuilder().mirrorDirect(false).build());
        });
    }

    @Test
    public void testAddStreamInvalids() throws Exception {
        jsServer.run(TestBase::atLeast2_10, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            assertThrows(IllegalArgumentException.class, () -> jsm.addStream(null));

            String stream = stream();

            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream)
                .description(variant())
                .storageType(StorageType.Memory)
                .subjects(subject())
                .build();
            jsm.addStream(sc);

            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).subjects(subject()).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).description(variant()).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).retentionPolicy(RetentionPolicy.Interest).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).retentionPolicy(RetentionPolicy.WorkQueue).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).compressionOption(CompressionOption.S2).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).maxConsumers(1).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).maxMessages(1).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).maxMessagesPerSubject(1).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).maxAge(Duration.ofSeconds(1L)).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).maxMsgSize(1).build())); // COVERAGE for deprecated
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).maximumMessageSize(1).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).storageType(StorageType.File).build()));

            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).noAck(true).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).discardPolicy(DiscardPolicy.New).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).duplicateWindow(Duration.ofSeconds(1L)).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).allowRollup(true).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).allowDirect(true).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).denyDelete(true).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).denyPurge(true).build()));
            assert10058(() -> jsm.addStream(StreamConfiguration.builder(sc).firstSequence(100).build()));
        });
    }

    // io.nats.client.JetStreamApiException: stream name already in use with a different configuration [10058]
    private void assert10058(Executable executable) {
        assertEquals(10058, assertThrows(JetStreamApiException.class, executable).getApiErrorCode());
    }

    @Test
    public void testUpdateStreamInvalids() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            assertThrows(IllegalArgumentException.class, () -> jsm.updateStream(null));

            String stream = stream();
            String[] subjects = new String[]{subject(), subject()};

            // cannot update non existent stream
            StreamConfiguration sc = getTestStreamConfiguration(stream, subjects);
            // stream not added yet
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(sc));

            // add the stream
            jsm.addStream(sc);

            // cannot change storage type
            StreamConfiguration scMemToFile = getTestStreamConfigurationBuilder(stream, subjects)
                .storageType(StorageType.File)
                .build();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(scMemToFile));

            // cannot change MaxConsumers
            StreamConfiguration scMaxCon = getTestStreamConfigurationBuilder()
                .maxConsumers(2)
                .build();
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(scMaxCon));

            StreamConfiguration scReten = getTestStreamConfigurationBuilder(stream, subjects)
                .retentionPolicy(RetentionPolicy.Interest)
                .build();
            if (nc.getServerInfo().isOlderThanVersion("2.10")) {
                // cannot change RetentionPolicy
                assertThrows(JetStreamApiException.class, () -> jsm.updateStream(scReten));
            }
            else {
                jsm.updateStream(scReten);
            }

            jsm.deleteStream(stream);

            jsm.addStream(getTestStreamConfigurationBuilder(stream, subjects).storageType(StorageType.File).build());
            assertThrows(JetStreamApiException.class, () -> jsm.updateStream(getTestStreamConfiguration(stream, subjects)));
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

    private static StreamConfiguration getTestStreamConfiguration(String stream, String... subjects) {
        return getTestStreamConfigurationBuilder(stream, subjects).build();
    }

    private static StreamConfiguration.Builder getTestStreamConfigurationBuilder() {
        return getTestStreamConfigurationBuilder(STREAM);
    }

    private static StreamConfiguration.Builder getTestStreamConfigurationBuilder(String stream, String... subjects) {
        if (subjects == null || subjects.length == 0) {
            subjects = new String[]{subject(0), subject(1)};
        }

        return StreamConfiguration.builder()
            .name(stream)
            .storageType(StorageType.Memory)
            .subjects(subjects);
    }

    @Test
    public void testGetStreamInfo() throws Exception {
        jsServer.run(nc -> {
            String stream = stream();

            JetStreamManagement jsm = nc.jetStreamManagement();
            assertThrows(JetStreamApiException.class, () -> jsm.getStreamInfo(stream));

            JetStream js = nc.jetStream();

            String[] subjects = new String[6];
            String subjectIx5 = subject();
            for (int x = 0; x < 5; x++) {
                subjects[x] = subject() + x + 1;
            }
            subjects[5] = subjectIx5 + ".>";

            createMemoryStream(jsm, stream, subjects);

            StreamInfo si = jsm.getStreamInfo(stream);
            assertEquals(stream, si.getConfiguration().getName());
            assertEquals(0, si.getStreamState().getSubjectCount());
            assertEquals(0, si.getStreamState().getSubjects().size());
            assertEquals(0, si.getStreamState().getDeletedCount());
            assertEquals(0, si.getStreamState().getDeleted().size());
            assertTrue(si.getStreamState().getSubjectMap().isEmpty());

            if (nc.getServerInfo().isOlderThanVersion("2.10")) {
                assertNull(si.getTimestamp());
            }
            else {
                assertNotNull(si.getTimestamp());
            }
            assertEquals(1, si.getConfiguration().getFirstSequence());

            List<PublishAck> packs = new ArrayList<>();
            for (int x = 0; x < 5; x++) {
                jsPublish(js, subjects[x], x + 1);
                PublishAck pa = jsPublish(js, subjects[x], data(x + 2));
                packs.add(pa);
                jsm.deleteMessage(stream, pa.getSeqno());
            }
            jsPublish(js, subjectIx5 + ".bar", 6);

            si = jsm.getStreamInfo(stream);
            assertEquals(stream, si.getConfiguration().getName());
            assertEquals(6, si.getStreamState().getSubjectCount());
            assertEquals(0, si.getStreamState().getSubjects().size());
            assertEquals(5, si.getStreamState().getDeletedCount());
            assertEquals(0, si.getStreamState().getDeleted().size());
            assertTrue(si.getStreamState().getSubjectMap().isEmpty());

            si = jsm.getStreamInfo(stream, StreamInfoOptions.builder().allSubjects().deletedDetails().build());
            assertEquals(stream, si.getConfiguration().getName());
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
                Subject s = map.get(subjects[x]);
                assertNotNull(s);
                assertEquals(x + 1, s.getCount());
            }
            Subject sf = map.get(subjectIx5 + ".bar");
            assertNotNull(sf);
            assertEquals(6, sf.getCount());
            assertEquals(6, si.getStreamState().getSubjectMap().size());

            for (PublishAck pa : packs) {
                assertTrue(si.getStreamState().getDeleted().contains(pa.getSeqno()));
            }

            jsPublish(js, subjectIx5 + ".baz", 2);
            sleep(100);

            si = jsm.getStreamInfo(stream, StreamInfoOptions.builder().filterSubjects(subjectIx5 + ".>").deletedDetails().build());
            assertEquals(7, si.getStreamState().getSubjectCount());
            list = si.getStreamState().getSubjects();
            assertNotNull(list);
            assertEquals(2, list.size());
            map = new HashMap<>();
            for (Subject su : list) {
                map.put(su.getName(), su);
            }
            Subject s = map.get(subjectIx5 + ".bar");
            assertNotNull(s);
            assertEquals(6, s.getCount());
            s = map.get(subjectIx5 + ".baz");
            assertNotNull(s);
            assertEquals(2, s.getCount());

            si = jsm.getStreamInfo(stream, StreamInfoOptions.builder().filterSubjects(subjects[4]).build());
            list = si.getStreamState().getSubjects();
            assertNotNull(list);
            assertEquals(1, list.size());
            s = list.get(0);
            assertEquals(subjects[4], s.getName());
            assertEquals(5, s.getCount());
        });
    }

    @Test
    public void testGetStreamInfoSubjectPagination() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/pagination.conf", false, true)) {
            try (Connection nc = standardConnection(ts.getURI())) {
                if (nc.getServerInfo().isNewerVersionThan("2.8.4")) {
                    JetStreamManagement jsm = nc.jetStreamManagement();
                    JetStream js = nc.jetStream();

                    long rounds = 101;
                    long size = 1000;
                    long count = rounds * size;
                    jsm.addStream(StreamConfiguration.builder()
                        .name(stream(1))
                        .storageType(StorageType.Memory)
                        .subjects("s.*.*")
                        .build());

                    jsm.addStream(StreamConfiguration.builder()
                        .name(stream(2))
                        .storageType(StorageType.Memory)
                        .subjects("t.*.*")
                        .build());

                    for (int x = 1; x <= rounds; x++) {
                        for (int y = 1; y <= size; y++) {
                            js.publish("s." + x + "." + y, null);
                        }
                    }

                    for (int y = 1; y <= size; y++) {
                        js.publish("t.7." + y, null);
                    }

                    StreamInfo si = jsm.getStreamInfo(stream(1));
                    validateStreamInfo(si.getStreamState(), 0, 0, count);

                    si = jsm.getStreamInfo(stream(1), StreamInfoOptions.allSubjects());
                    validateStreamInfo(si.getStreamState(), count, count, count);

                    si = jsm.getStreamInfo(stream(1), StreamInfoOptions.filterSubjects("s.7.*"));
                    validateStreamInfo(si.getStreamState(), size, size, count);

                    si = jsm.getStreamInfo(stream(1), StreamInfoOptions.filterSubjects("s.7.1"));
                    validateStreamInfo(si.getStreamState(), 1L, 1, count);

                    si = jsm.getStreamInfo(stream(2), StreamInfoOptions.filterSubjects("t.7.*"));
                    validateStreamInfo(si.getStreamState(), size, size, size);

                    si = jsm.getStreamInfo(stream(2), StreamInfoOptions.filterSubjects("t.7.1"));
                    validateStreamInfo(si.getStreamState(), 1L, 1, size);

                    List<StreamInfo> infos = jsm.getStreams();
                    assertEquals(2, infos.size());
                    si = infos.get(0);
                    if (si.getConfiguration().getSubjects().get(0).equals("s.*.*")) {
                        validateStreamInfo(si.getStreamState(), 0, 0, count);
                        validateStreamInfo(infos.get(1).getStreamState(), 0, 0, size);
                    }
                    else {
                        validateStreamInfo(si.getStreamState(), 0, 0, size);
                        validateStreamInfo(infos.get(1).getStreamState(), 0, 0, count);
                    }

                    infos = jsm.getStreams(">");
                    assertEquals(2, infos.size());

                    infos = jsm.getStreams("*.7.*");
                    assertEquals(2, infos.size());

                    infos = jsm.getStreams("*.7.1");
                    assertEquals(2, infos.size());

                    infos = jsm.getStreams("s.7.*");
                    assertEquals(1, infos.size());
                    assertEquals("s.*.*", infos.get(0).getConfiguration().getSubjects().get(0));

                    infos = jsm.getStreams("t.7.1");
                    assertEquals(1, infos.size());
                    assertEquals("t.*.*", infos.get(0).getConfiguration().getSubjects().get(0));
                }
            }
        }
    }

    private void validateStreamInfo(StreamState streamState, long subjectsList, long filteredCount, long subjectCount) {
        assertEquals(subjectsList, streamState.getSubjects().size());
        assertEquals(filteredCount, streamState.getSubjects().size());
        assertEquals(subjectCount, streamState.getSubjectCount());
    }

    @Test
    public void testGetStreamInfoOrNamesPaginationFilter() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            // getStreams pages at 256
            // getStreamNames pages at 1024

            addStreams(jsm, 300, 0, "x256");

            List<StreamInfo> list = jsm.getStreams();
            assertEquals(300, list.size());

            List<String> names = jsm.getStreamNames();
            assertEquals(300, names.size());

            addStreams(jsm, 1100, 300, "x1024");

            list = jsm.getStreams();
            assertEquals(1400, list.size());

            names = jsm.getStreamNames();
            assertEquals(1400, names.size());

            list = jsm.getStreams("*.x256.*");
            assertEquals(300, list.size());

            names = jsm.getStreamNames("*.x256.*");
            assertEquals(300, names.size());

            list = jsm.getStreams("*.x1024.*");
            assertEquals(1100, list.size());

            names = jsm.getStreamNames("*.x1024.*");
            assertEquals(1100, names.size());
        });
    }

    private void addStreams(JetStreamManagement jsm, int count, int adj, String div) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            createMemoryStream(jsm, "stream-" + (x + adj), "sub" + (x + adj) + "." + div + ".*");
        }
    }

    @Test
    public void testGetStreamNamesBySubjectFilter() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createMemoryStream(jsm, stream(1), "foo");
            createMemoryStream(jsm, stream(2), "bar");
            createMemoryStream(jsm, stream(3), "a.a");
            createMemoryStream(jsm, stream(4), "a.b");

            List<String> list = jsm.getStreamNames("*");
            assertStreamNameList(list, 1, 2);

            list = jsm.getStreamNames(">");
            assertStreamNameList(list, 1, 2, 3, 4);

            list = jsm.getStreamNames("*.*");
            assertStreamNameList(list, 3, 4);

            list = jsm.getStreamNames("a.>");
            assertStreamNameList(list, 3, 4);

            list = jsm.getStreamNames("a.*");
            assertStreamNameList(list, 3, 4);

            list = jsm.getStreamNames("foo");
            assertStreamNameList(list, 1);

            list = jsm.getStreamNames("a.a");
            assertStreamNameList(list, 3);

            list = jsm.getStreamNames("nomatch");
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
    public void testDeleteStream() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStreamApiException jsapiEx =
                assertThrows(JetStreamApiException.class, () -> jsm.deleteStream(stream()));
            assertEquals(10059, jsapiEx.getApiErrorCode());

            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            assertNotNull(jsm.getStreamInfo(tsc.stream));
            assertTrue(jsm.deleteStream(tsc.stream));

            jsapiEx = assertThrows(JetStreamApiException.class, () -> jsm.getStreamInfo(tsc.stream));
            assertEquals(10059, jsapiEx.getApiErrorCode());

            jsapiEx = assertThrows(JetStreamApiException.class, () -> jsm.deleteStream(tsc.stream));
            assertEquals(10059, jsapiEx.getApiErrorCode());
        });
    }

    @Test
    public void testPurgeStreamAndOptions() throws Exception {
        jsServer.run(nc -> {
            // invalid to have both keep and seq
            assertThrows(IllegalArgumentException.class,
                () -> PurgeOptions.builder().keep(1).sequence(1).build());

            JetStreamManagement jsm = nc.jetStreamManagement();

            // error to purge a stream that does not exist
            assertThrows(JetStreamApiException.class, () -> jsm.purgeStream(stream()));

            TestingStreamContainer tsc = new TestingStreamContainer(nc, 2);
            createMemoryStream(jsm, tsc.stream, tsc.subject(0), tsc.subject(1));

            StreamInfo si = jsm.getStreamInfo(tsc.stream);
            assertEquals(0, si.getStreamState().getMsgCount());

            jsPublish(nc, tsc.subject(0), 10);
            si = jsm.getStreamInfo(tsc.stream);
            assertEquals(10, si.getStreamState().getMsgCount());

            PurgeOptions options = PurgeOptions.builder().keep(7).build();
            PurgeResponse pr = jsm.purgeStream(tsc.stream, options);
            assertTrue(pr.isSuccess());
            assertEquals(3, pr.getPurged());

            options = PurgeOptions.builder().sequence(9).build();
            pr = jsm.purgeStream(tsc.stream, options);
            assertTrue(pr.isSuccess());
            assertEquals(5, pr.getPurged());
            si = jsm.getStreamInfo(tsc.stream);
            assertEquals(2, si.getStreamState().getMsgCount());

            pr = jsm.purgeStream(tsc.stream);
            assertTrue(pr.isSuccess());
            assertEquals(2, pr.getPurged());
            si = jsm.getStreamInfo(tsc.stream);
            assertEquals(0, si.getStreamState().getMsgCount());

            jsPublish(nc, tsc.subject(0), 10);
            jsPublish(nc, tsc.subject(1), 10);
            si = jsm.getStreamInfo(tsc.stream);
            assertEquals(20, si.getStreamState().getMsgCount());
            jsm.purgeStream(tsc.stream, PurgeOptions.subject(tsc.subject(0)));
            si = jsm.getStreamInfo(tsc.stream);
            assertEquals(10, si.getStreamState().getMsgCount());

            options = PurgeOptions.builder().subject(tsc.subject(0)).sequence(1).build();
            assertEquals(tsc.subject(0), options.getSubject());
            assertEquals(1, options.getSequence());

            options = PurgeOptions.builder().subject(tsc.subject(0)).keep(2).build();
            assertEquals(2, options.getKeep());
        });
    }

    @Test
    public void testAddDeleteConsumer() throws Exception {
        runInJsServer(nc -> {
            boolean atLeast2dot9 = nc.getServerInfo().isSameOrNewerThanVersion("2.9");

            JetStreamManagement jsm = nc.jetStreamManagement();

            createMemoryStream(jsm, STREAM, subjectDot(">"));

            List<ConsumerInfo> list = jsm.getConsumers(STREAM);
            assertEquals(0, list.size());

            final ConsumerConfiguration cc = ConsumerConfiguration.builder().build();
            IllegalArgumentException iae =
                assertThrows(IllegalArgumentException.class, () -> jsm.addOrUpdateConsumer(null, cc));
            assertTrue(iae.getMessage().contains("Stream cannot be null or empty"));
            iae = assertThrows(IllegalArgumentException.class, () -> jsm.addOrUpdateConsumer(STREAM, null));
            assertTrue(iae.getMessage().contains("Config cannot be null"));

            // durable and name can both be null
            ConsumerInfo ci = jsm.addOrUpdateConsumer(STREAM, cc);
            assertNotNull(ci.getName());

            // threshold can be set for durable
            final ConsumerConfiguration cc2 = ConsumerConfiguration.builder().durable(DURABLE).inactiveThreshold(10000).build();
            ci = jsm.addOrUpdateConsumer(STREAM, cc2);
            assertEquals(10000, ci.getConsumerConfiguration().getInactiveThreshold().toMillis());

            // prep for next part of test
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, subjectDot(">"));

            // with and w/o deliver subject for push/pull
            addConsumer(jsm, atLeast2dot9, 1, false, null, ConsumerConfiguration.builder()
                .durable(durable(1))
                .build());

            addConsumer(jsm, atLeast2dot9, 2, true, null, ConsumerConfiguration.builder()
                .durable(durable(2))
                .deliverSubject(deliver(2))
                .build());

            // test delete here
            List<String> consumers = jsm.getConsumerNames(STREAM);
            assertEquals(2, consumers.size());
            assertTrue(jsm.deleteConsumer(STREAM, durable(1)));
            consumers = jsm.getConsumerNames(STREAM);
            assertEquals(1, consumers.size());
            assertThrows(JetStreamApiException.class, () -> jsm.deleteConsumer(STREAM, durable(1)));

            // some testing of new name
            if (atLeast2dot9) {
                addConsumer(jsm, true, 3, false, null, ConsumerConfiguration.builder()
                    .durable(durable(3))
                    .name(durable(3))
                    .build());

                addConsumer(jsm, true, 4, true, null, ConsumerConfiguration.builder()
                    .durable(durable(4))
                    .name(durable(4))
                    .deliverSubject(deliver(4))
                    .build());

                addConsumer(jsm, true, 5, false, ">", ConsumerConfiguration.builder()
                    .durable(durable(5))
                    .filterSubject(">")
                    .build());

                addConsumer(jsm, true, 6, false, subjectDot(">"), ConsumerConfiguration.builder()
                    .durable(durable(6))
                    .filterSubject(subjectDot(">"))
                    .build());

                addConsumer(jsm, true, 7, false, subjectDot("foo"), ConsumerConfiguration.builder()
                    .durable(durable(7))
                    .filterSubject(subjectDot("foo"))
                    .build());
            }
        });
    }

    @Test
    public void testAddPausedConsumer() throws Exception {
        jsServer.run(TestBase::atLeast2_11, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            List<ConsumerInfo> list = jsm.getConsumers(tsc.stream);
            assertEquals(0, list.size());

            ZonedDateTime pauseUntil = ZonedDateTime.now(ZONE_ID_GMT).plusMinutes(2);
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(tsc.consumerName())
                    .pauseUntil(pauseUntil)
                    .build();

            // Consumer should be paused on creation.
            ConsumerInfo ci = jsm.addOrUpdateConsumer(tsc.stream, cc);
            assertTrue(ci.getPaused());
            assertTrue(ci.getPauseRemaining().toMillis() > 60_000);
            assertEquals(pauseUntil, ci.getConsumerConfiguration().getPauseUntil());
        });
    }

    @Test
    public void testPauseResumeConsumer() throws Exception {
        jsServer.run(TestBase::atLeast2_11, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            List<ConsumerInfo> list = jsm.getConsumers(tsc.stream);
            assertEquals(0, list.size());

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(tsc.consumerName())
                    .build();

            // durable and name can both be null
            ConsumerInfo ci = jsm.addOrUpdateConsumer(tsc.stream, cc);
            assertNotNull(ci.getName());

            // pause consumer
            ZonedDateTime pauseUntil = ZonedDateTime.now(ZONE_ID_GMT).plusMinutes(2);
            ConsumerPauseResponse pauseResponse = jsm.pauseConsumer(tsc.stream, ci.getName(), pauseUntil);
            assertTrue(pauseResponse.isPaused());
            assertEquals(pauseUntil, pauseResponse.getPauseUntil());

            ci = jsm.getConsumerInfo(tsc.stream, ci.getName());
            assertTrue(ci.getPaused());
            assertTrue(ci.getPauseRemaining().toMillis() > 60_000);

            // resume consumer
            boolean isResumed = jsm.resumeConsumer(tsc.stream, ci.getName());
            assertTrue(isResumed);

            ci = jsm.getConsumerInfo(tsc.stream, ci.getName());
            assertFalse(ci.getPaused());

            // pause again
            pauseResponse = jsm.pauseConsumer(tsc.stream, ci.getName(), pauseUntil);
            assertTrue(pauseResponse.isPaused());
            assertEquals(pauseUntil, pauseResponse.getPauseUntil());

            // resume via pause with no date
            pauseResponse = jsm.pauseConsumer(tsc.stream, ci.getName(), null);
            assertFalse(pauseResponse.isPaused());
            assertEquals(DEFAULT_TIME, pauseResponse.getPauseUntil());

            ci = jsm.getConsumerInfo(tsc.stream, ci.getName());
            assertFalse(ci.getPaused());

            assertThrows(JetStreamApiException.class, () -> jsm.pauseConsumer(stream(), tsc.consumerName(), pauseUntil));
            assertThrows(JetStreamApiException.class, () -> jsm.pauseConsumer(tsc.stream, name(), pauseUntil));
            assertThrows(JetStreamApiException.class, () -> jsm.resumeConsumer(stream(), tsc.consumerName()));
            assertThrows(JetStreamApiException.class, () -> jsm.resumeConsumer(tsc.stream, name()));
        });
    }

    private static void addConsumer(JetStreamManagement jsm, boolean atLeast2dot9, int id, boolean deliver, String fs, ConsumerConfiguration cc) throws IOException, JetStreamApiException {
        ConsumerInfo ci = jsm.addOrUpdateConsumer(STREAM, cc);
        assertEquals(durable(id), ci.getName());
        if (atLeast2dot9) {
            assertEquals(durable(id), ci.getConsumerConfiguration().getName());
        }
        assertEquals(durable(id), ci.getConsumerConfiguration().getDurable());
        if (fs == null) {
            assertNull(ci.getConsumerConfiguration().getFilterSubject());
        }
        if (deliver) {
            assertEquals(deliver(id), ci.getConsumerConfiguration().getDeliverSubject());
        }
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
    public void testConsumerMetadata() throws Exception {
        jsServer.run(nc -> {
            Map<String, String> metaData = new HashMap<>(); metaData.put(META_KEY, META_VALUE);
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(tsc.consumerName())
                .metadata(metaData)
                .build();

            ConsumerInfo ci = jsm.addOrUpdateConsumer(tsc.stream, cc);
            assertMetaData(ci.getConsumerConfiguration().getMetadata());
        });
    }

    @Test
    public void testCreateConsumersWithFilters() throws Exception {
        runInJsServer(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            createDefaultTestStream(jsm);

            // plain subject
            ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder().durable(DURABLE);
            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(SUBJECT).build());
            List<ConsumerInfo> cis = jsm.getConsumers(STREAM);
            assertEquals(SUBJECT, cis.get(0).getConsumerConfiguration().getFilterSubject());

            if (nc.getServerInfo().isSameOrNewerThanVersion("2.10")) {
                // 2.10 and later you can set the filter to something that does not match
                jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("two-ten-allows-not-matching")).build());
                cis = jsm.getConsumers(STREAM);
                assertEquals(subjectDot("two-ten-allows-not-matching"), cis.get(0).getConsumerConfiguration().getFilterSubject());
            }
            else {
                assertThrows(JetStreamApiException.class,
                    () -> jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("not-match")).build()));
            }

            // wildcard subject
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, SUBJECT_STAR);

            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("A")).build());
            cis = jsm.getConsumers(STREAM);
            assertEquals(subjectDot("A"), cis.get(0).getConsumerConfiguration().getFilterSubject());

            // gt subject
            jsm.deleteStream(STREAM);
            createMemoryStream(jsm, STREAM, SUBJECT_GT);

            jsm.addOrUpdateConsumer(STREAM, builder.filterSubject(subjectDot("A")).build());
            cis = jsm.getConsumers(STREAM);
            assertEquals(subjectDot("A"), cis.get(0).getConsumerConfiguration().getFilterSubject());
        });
    }

    @Test
    public void testGetConsumerInfo() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);
            assertThrows(JetStreamApiException.class, () -> jsm.getConsumerInfo(tsc.stream, tsc.consumerName()));
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(tsc.consumerName()).build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(tsc.stream, cc);
            assertEquals(tsc.stream, ci.getStreamName());
            assertEquals(tsc.consumerName(), ci.getName());
            ci = jsm.getConsumerInfo(tsc.stream, tsc.consumerName());
            assertEquals(tsc.stream, ci.getStreamName());
            assertEquals(tsc.consumerName(), ci.getName());
            assertThrows(JetStreamApiException.class, () -> jsm.getConsumerInfo(tsc.stream, durable(999)));
            if (nc.getServerInfo().isSameOrNewerThanVersion("2.10")) {
                assertNotNull(ci.getTimestamp());
            }
            else {
                assertNull(ci.getTimestamp());
            }
        });
    }

    @Test
    public void testGetConsumers() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(jsm);

            addConsumers(jsm, tsc.stream, 600, "A"); // getConsumers pages at 256

            List<ConsumerInfo> list = jsm.getConsumers(tsc.stream);
            assertEquals(600, list.size());

            addConsumers(jsm, tsc.stream, 500, "B"); // getConsumerNames pages at 1024
            List<String> names = jsm.getConsumerNames(tsc.stream);
            assertEquals(1100, names.size());
        });
    }

    private void addConsumers(JetStreamManagement jsm, String stream, int count, String durableVary) throws IOException, JetStreamApiException {
        for (int x = 1; x <= count; x++) {
            String dur = durable(durableVary, x);
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(dur)
                .build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(stream, cc);
            assertEquals(dur, ci.getName());
            assertEquals(dur, ci.getConsumerConfiguration().getDurable());
            assertNull(ci.getConsumerConfiguration().getDeliverSubject());
        }
    }

    @Test
    public void testDeleteMessage() throws Exception {
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

            ZonedDateTime timeBeforeCreated = ZonedDateTime.now();
            js.publish(NatsMessage.builder().subject(SUBJECT).headers(h).data(dataBytes(1)).build());
            js.publish(NatsMessage.builder().subject(SUBJECT).build());

            JetStreamManagement jsm = nc.jetStreamManagement();

            MessageInfo mi = jsm.getMessage(STREAM, 1);
            assertNotNull(mi.toString());
            assertEquals(SUBJECT, mi.getSubject());
            assertEquals(data(1), new String(mi.getData()));
            assertEquals(1, mi.getSeq());
            assertTrue(mi.getTime().toEpochSecond() >= timeBeforeCreated.toEpochSecond());
            assertNotNull(mi.getHeaders());
            assertEquals("bar", mi.getHeaders().get("foo").get(0));

            mi = jsm.getMessage(STREAM, 2);
            assertNotNull(mi.toString());
            assertEquals(SUBJECT, mi.getSubject());
            assertNull(mi.getData());
            assertEquals(2, mi.getSeq());
            assertTrue(mi.getTime().toEpochSecond() >= timeBeforeCreated.toEpochSecond());
            assertTrue(mi.getHeaders() == null || mi.getHeaders().isEmpty());

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
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            TestingStreamContainer tsc = new TestingStreamContainer(nc);
            assertFalse(tsc.si.getConfiguration().getSealed());

            JetStream js = nc.jetStream();
            js.publish(tsc.subject(), "data1".getBytes());

            StreamConfiguration sc = new StreamConfiguration.Builder(tsc.si.getConfiguration())
                .seal().build();
            StreamInfo si = jsm.updateStream(sc);
            assertTrue(si.getConfiguration().getSealed());

            assertThrows(JetStreamApiException.class, () -> js.publish(tsc.subject(), "data2".getBytes()));
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
    public void testConsumerReplica() throws Exception {
        jsServer.run(nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            TestingStreamContainer tsc = new TestingStreamContainer(nc);

            final ConsumerConfiguration cc0 = ConsumerConfiguration.builder()
                .durable(tsc.consumerName())
                .build();
            ConsumerInfo ci = jsm.addOrUpdateConsumer(tsc.stream, cc0);
            // server returns 0 when value is not set
            assertEquals(0, ci.getConsumerConfiguration().getNumReplicas());

            final ConsumerConfiguration cc1 = ConsumerConfiguration.builder()
                .durable(tsc.consumerName())
                .numReplicas(1)
                .build();
            ci = jsm.addOrUpdateConsumer(tsc.stream, cc1);
            assertEquals(1, ci.getConsumerConfiguration().getNumReplicas());
        });
    }

    @Test
    public void testGetMessage() throws Exception {
        jsServer.run(nc -> {
            if (nc.getServerInfo().isNewerVersionThan("2.8.4")) {
                JetStreamManagement jsm = nc.jetStreamManagement();
                JetStream js = nc.jetStream();

                TestingStreamContainer tsc = new TestingStreamContainer(nc, 2);
                assertFalse(tsc.si.getConfiguration().getAllowDirect());

                ZonedDateTime timeBeforeCreated = ZonedDateTime.now();
                sleep(100);
                js.publish(buildTestGetMessage(tsc, 0, 1));
                js.publish(buildTestGetMessage(tsc, 1, 2));
                js.publish(buildTestGetMessage(tsc, 0, 3));
                js.publish(buildTestGetMessage(tsc, 1, 4));
                js.publish(buildTestGetMessage(tsc, 0, 5));
                js.publish(buildTestGetMessage(tsc, 1, 6));

                validateGetMessage(jsm, tsc, timeBeforeCreated);

                StreamConfiguration sc = StreamConfiguration.builder(tsc.si.getConfiguration()).allowDirect(true).build();
                StreamInfo si = jsm.updateStream(sc);
                assertTrue(si.getConfiguration().getAllowDirect());
                validateGetMessage(jsm, tsc, timeBeforeCreated);

                // error case stream doesn't exist
                assertThrows(JetStreamApiException.class, () -> jsm.getMessage(stream(999), 1));
            }
        });
    }

    private static NatsMessage buildTestGetMessage(TestingStreamContainer tsc, int s, int q) {
        String data = "s" + s + "-q" + q;
        return NatsMessage.builder()
            .subject(tsc.subject(s))
            .data("d-" + data)
            .headers(new Headers().put("h", "h-" + data))
            .build();
    }

    private void validateGetMessage(JetStreamManagement jsm, TestingStreamContainer tsc, ZonedDateTime timeBeforeCreated) throws IOException, JetStreamApiException {

        assertMessageInfo(tsc, 0, 1, jsm.getMessage(tsc.stream, 1), timeBeforeCreated);
        assertMessageInfo(tsc, 0, 5, jsm.getLastMessage(tsc.stream, tsc.subject(0)), timeBeforeCreated);
        assertMessageInfo(tsc, 1, 6, jsm.getLastMessage(tsc.stream, tsc.subject(1)), timeBeforeCreated);

        assertMessageInfo(tsc, 0, 1, jsm.getNextMessage(tsc.stream, -1, tsc.subject(0)), timeBeforeCreated);
        assertMessageInfo(tsc, 1, 2, jsm.getNextMessage(tsc.stream, -1, tsc.subject(1)), timeBeforeCreated);
        assertMessageInfo(tsc, 0, 1, jsm.getNextMessage(tsc.stream, 0, tsc.subject(0)), timeBeforeCreated);
        assertMessageInfo(tsc, 1, 2, jsm.getNextMessage(tsc.stream, 0, tsc.subject(1)), timeBeforeCreated);
        assertMessageInfo(tsc, 0, 1, jsm.getFirstMessage(tsc.stream, tsc.subject(0)), timeBeforeCreated);
        assertMessageInfo(tsc, 1, 2, jsm.getFirstMessage(tsc.stream, tsc.subject(1)), timeBeforeCreated);
        assertMessageInfo(tsc, 0, 1, jsm.getFirstMessage(tsc.stream, timeBeforeCreated), timeBeforeCreated);
        assertMessageInfo(tsc, 0, 1, jsm.getFirstMessage(tsc.stream, timeBeforeCreated, tsc.subject(0)), timeBeforeCreated);
        assertMessageInfo(tsc, 1, 2, jsm.getFirstMessage(tsc.stream, timeBeforeCreated, tsc.subject(1)), timeBeforeCreated);

        assertMessageInfo(tsc, 0, 1, jsm.getNextMessage(tsc.stream, 1, tsc.subject(0)), timeBeforeCreated);
        assertMessageInfo(tsc, 1, 2, jsm.getNextMessage(tsc.stream, 1, tsc.subject(1)), timeBeforeCreated);

        assertMessageInfo(tsc, 0, 3, jsm.getNextMessage(tsc.stream, 2, tsc.subject(0)), timeBeforeCreated);
        assertMessageInfo(tsc, 1, 2, jsm.getNextMessage(tsc.stream, 2, tsc.subject(1)), timeBeforeCreated);

        assertMessageInfo(tsc, 0, 5, jsm.getNextMessage(tsc.stream, 5, tsc.subject(0)), timeBeforeCreated);
        assertMessageInfo(tsc, 1, 6, jsm.getNextMessage(tsc.stream, 5, tsc.subject(1)), timeBeforeCreated);

        assertStatus(10003, assertThrows(JetStreamApiException.class, () -> jsm.getMessage(tsc.stream, -1)));
        assertStatus(10003, assertThrows(JetStreamApiException.class, () -> jsm.getMessage(tsc.stream, 0)));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getMessage(tsc.stream, 9)));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getLastMessage(tsc.stream, "not-a-subject")));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getFirstMessage(tsc.stream, "not-a-subject")));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getNextMessage(tsc.stream, 9, tsc.subject(0))));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getNextMessage(tsc.stream, 1, "not-a-subject")));
    }

    private void assertStatus(int apiErrorCode, JetStreamApiException jsae) {
        assertEquals(apiErrorCode, jsae.getApiErrorCode());
    }

    private void assertMessageInfo(TestingStreamContainer tsc, int subj, long seq, MessageInfo mi, ZonedDateTime timeBeforeCreated) {
        assertEquals(tsc.stream, mi.getStream());
        assertEquals(tsc.subject(subj), mi.getSubject());
        assertEquals(seq, mi.getSeq());
        assertNotNull(mi.getTime());
        assertTrue(mi.getTime().toEpochSecond() >= timeBeforeCreated.toEpochSecond());
        String expectedData = "s" + subj + "-q" + seq;
        assertEquals("d-" + expectedData, new String(mi.getData()));
        assertEquals("h-" + expectedData, mi.getHeaders().getFirst("h"));
        assertNull(mi.getHeaders().getFirst(NATS_SUBJECT));
        assertNull(mi.getHeaders().getFirst(NATS_SEQUENCE));
        assertNull(mi.getHeaders().getFirst(NATS_TIMESTAMP));
        assertNull(mi.getHeaders().getFirst(NATS_STREAM));
        assertNull(mi.getHeaders().getFirst(NATS_LAST_SEQUENCE));
    }

    @Test
    public void testMessageGetRequestObject() {
        ZonedDateTime zdt = ZonedDateTime.now();
        validateMessageGetRequestObject(1, null, null, null, MessageGetRequest.forSequence(1));
        validateMessageGetRequestObject(-1, "last", null, null, MessageGetRequest.lastForSubject("last"));
        validateMessageGetRequestObject(-1, null, "first", null, MessageGetRequest.firstForSubject("first"));
        validateMessageGetRequestObject(1, null, "first", null, MessageGetRequest.nextForSubject(1, "first"));
        validateMessageGetRequestObject(-1, null, null, zdt, MessageGetRequest.firstForStartTime(zdt));
        validateMessageGetRequestObject(-1, null, "first", zdt, MessageGetRequest.firstForStartTimeAndSubject(zdt, "first"));

        // coverage for MessageInfo
        Message m = new NatsMessage("sub", null, new Headers()
            .put(NATS_SUBJECT, "sub")
            .put(NATS_SEQUENCE, "1")
            .put(NATS_LAST_SEQUENCE, "1")
            .put(NATS_TIMESTAMP, DateTimeUtils.toRfc3339(ZonedDateTime.now())),
            null);
        MessageInfo mi = new MessageInfo(m, "stream", true);
        assertEquals(1, mi.getLastSeq());
        assertTrue(mi.toString().contains("last_seq"));
        assertNotNull(mi.toString());
    }

    private void validateMessageGetRequestObject(
        long seq, String lastBySubject, String nextBySubject, ZonedDateTime zdt, MessageGetRequest mgr) {
        assertEquals(seq, mgr.getSequence());
        assertEquals(lastBySubject, mgr.getLastBySubject());
        assertEquals(nextBySubject, mgr.getNextBySubject());
        assertEquals(seq > 0 && nextBySubject == null, mgr.isSequenceOnly());
        assertEquals(lastBySubject != null, mgr.isLastBySubject());
        assertEquals(nextBySubject != null, mgr.isNextBySubject());
        assertEquals(zdt, mgr.getStartTime());
    }

    @Test
    public void testMessageGetRequestObjectDeprecatedMethods() {
        // coverage for deprecated methods
        MessageGetRequest.seqBytes(1);
        MessageGetRequest.lastBySubjectBytes(SUBJECT);
        new MessageGetRequest(1);
        new MessageGetRequest(SUBJECT);

        // coverage for MessageInfo, has error
        String json = dataAsString("GenericErrorResponse.json");
        NatsMessage m = new NatsMessage("sub", null, json.getBytes(StandardCharsets.US_ASCII));
        MessageInfo mi = new MessageInfo(m);
        assertTrue(mi.hasError());
        assertEquals(-1, mi.getLastSeq());
        assertFalse(mi.toString().contains("last_seq"));
    }

    @Test
    public void testDirectMessageRepublishedSubject() throws Exception {
        jsServer.run(TestBase::atLeast2_9_0, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            String streamBucketName = "sb-" + variant(null);
            String subject = subject();
            String streamSubject = subject + ".>";
            String publishSubject1 = subject + ".one";
            String publishSubject2 = subject + ".two";
            String publishSubject3 = subject + ".three";
            String republishDest = "$KV." + streamBucketName + ".>";

            StreamConfiguration sc = StreamConfiguration.builder()
                .name(streamBucketName)
                .storageType(StorageType.Memory)
                .subjects(streamSubject)
                .republish(Republish.builder().source(">").destination(republishDest).build())
                .build();
            jsm.addStream(sc);

            KeyValueConfiguration kvc = KeyValueConfiguration.builder().name(streamBucketName).build();
            nc.keyValueManagement().create(kvc);
            KeyValue kv = nc.keyValue(streamBucketName);

            nc.publish(publishSubject1, "uno".getBytes());
            nc.jetStream().publish(publishSubject2, "dos".getBytes());
            kv.put(publishSubject3, "tres");

            KeyValueEntry kve1 = kv.get(publishSubject1);
            assertEquals(streamBucketName, kve1.getBucket());
            assertEquals(publishSubject1, kve1.getKey());
            assertEquals("uno", kve1.getValueAsString());

            KeyValueEntry kve2 = kv.get(publishSubject2);
            assertEquals(streamBucketName, kve2.getBucket());
            assertEquals(publishSubject2, kve2.getKey());
            assertEquals("dos", kve2.getValueAsString());

            KeyValueEntry kve3 = kv.get(publishSubject3);
            assertEquals(streamBucketName, kve3.getBucket());
            assertEquals(publishSubject3, kve3.getKey());
            assertEquals("tres", kve3.getValueAsString());
        });
    }

    @Test
    public void testCreateConsumerUpdateConsumer() throws Exception {
        jsServer.run(TestBase::atLeast2_9_0, nc -> {
            String streamPrefix = variant();
            JetStreamManagement jsmNew = nc.jetStreamManagement();
            JetStreamManagement jsmPre290 = nc.jetStreamManagement(JetStreamOptions.builder().optOut290ConsumerCreate(true).build());

            // --------------------------------------------------------
            // New without filter
            // --------------------------------------------------------
            String stream1 = streamPrefix + "-new";
            String name = name();
            String subject = name();
            createMemoryStream(jsmNew, stream1, subject + ".*");

            ConsumerConfiguration cc11 = ConsumerConfiguration.builder().name(name).build();

            // update no good when not exist
            JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> jsmNew.updateConsumer(stream1, cc11));
            assertEquals(10149, e.getApiErrorCode());

            // initial create ok
            ConsumerInfo ci = jsmNew.createConsumer(stream1, cc11);
            assertEquals(name, ci.getName());
            assertNull(ci.getConsumerConfiguration().getFilterSubject());

            // any other create no good
            e = assertThrows(JetStreamApiException.class, () -> jsmNew.createConsumer(stream1, cc11));
            assertEquals(10148, e.getApiErrorCode());

            // update ok when exists
            ConsumerConfiguration cc12 = ConsumerConfiguration.builder().name(name).description(variant()).build();
            ci = jsmNew.updateConsumer(stream1, cc12);
            assertEquals(name, ci.getName());
            assertNull(ci.getConsumerConfiguration().getFilterSubject());

            // --------------------------------------------------------
            // New with filter subject
            // --------------------------------------------------------
            String stream2 = streamPrefix + "-new-fs";
            name = name();
            subject = name();
            String fs1 = subject + ".A";
            String fs2 = subject + ".B";
            createMemoryStream(jsmNew, stream2, subject + ".*");

            ConsumerConfiguration cc21 = ConsumerConfiguration.builder().name(name).filterSubject(fs1).build();

            // update no good when not exist
            e = assertThrows(JetStreamApiException.class, () -> jsmNew.updateConsumer(stream2, cc21));
            assertEquals(10149, e.getApiErrorCode());

            // initial create ok
            ci = jsmNew.createConsumer(stream2, cc21);
            assertEquals(name, ci.getName());
            assertEquals(fs1, ci.getConsumerConfiguration().getFilterSubject());

            // any other create no good
            e = assertThrows(JetStreamApiException.class, () -> jsmNew.createConsumer(stream2, cc21));
            assertEquals(10148, e.getApiErrorCode());

            // update ok when exists
            ConsumerConfiguration cc22 = ConsumerConfiguration.builder().name(name).filterSubjects(fs2).build();
            ci = jsmNew.updateConsumer(stream2, cc22);
            assertEquals(name, ci.getName());
            assertEquals(fs2, ci.getConsumerConfiguration().getFilterSubject());

            // --------------------------------------------------------
            // Pre 290 durable pathway
            // --------------------------------------------------------
            String stream3 = streamPrefix + "-old-durable";
            name = name();
            subject = name();
            fs1 = subject + ".A";
            fs2 = subject + ".B";
            String fs3 = subject + ".C";
            createMemoryStream(jsmPre290, stream3, subject + ".*");

            ConsumerConfiguration cc31 = ConsumerConfiguration.builder().durable(name).filterSubject(fs1).build();

            // update no good when not exist
            e = assertThrows(JetStreamApiException.class, () -> jsmPre290.updateConsumer(stream3, cc31));
            assertEquals(10149, e.getApiErrorCode());

            // initial create ok
            ci = jsmPre290.createConsumer(stream3, cc31);
            assertEquals(name, ci.getName());
            assertEquals(fs1, ci.getConsumerConfiguration().getFilterSubject());

            // opt out of 209, create on existing ok
            // This is not exactly the same behavior as with the new consumer create api, but it's what the server does
            jsmPre290.createConsumer(stream3, cc31);

            ConsumerConfiguration cc32 = ConsumerConfiguration.builder().durable(name).filterSubject(fs2).build();
            e = assertThrows(JetStreamApiException.class, () -> jsmPre290.createConsumer(stream3, cc32));
            assertEquals(10148, e.getApiErrorCode());

            // update ok when exists
            ConsumerConfiguration cc33 = ConsumerConfiguration.builder().durable(name).filterSubjects(fs3).build();
            ci = jsmPre290.updateConsumer(stream3, cc33);
            assertEquals(name, ci.getName());
            assertEquals(fs3, ci.getConsumerConfiguration().getFilterSubject());

            // --------------------------------------------------------
            // Pre 290 ephemeral pathway
            // --------------------------------------------------------
            subject = name();

            String stream4 = streamPrefix + "-old-ephemeral";
            fs1 = subject + ".A";
            createMemoryStream(jsmPre290, stream4, subject + ".*");

            ConsumerConfiguration cc4 = ConsumerConfiguration.builder().filterSubject(fs1).build();

            // update no good when not exist
            e = assertThrows(JetStreamApiException.class, () -> jsmPre290.updateConsumer(stream4, cc4));
            assertEquals(10149, e.getApiErrorCode());

            // initial create ok
            ci = jsmPre290.createConsumer(stream4, cc4);
            assertEquals(fs1, ci.getConsumerConfiguration().getFilterSubject());
        });
    }

    @Test
    public void testNoRespondersWhenConsumerDeleted() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        jsServer.run(new Options.Builder().errorListener(listener), TestBase::atLeast2_10_26, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();
            JetStream js = nc.jetStream();

            String stream = stream();
            String subject = subject();

            assertThrows(JetStreamApiException.class, () -> jsm.getMessage(stream, 1));

            createMemoryStream(jsm, stream, subject);

            for (int x = 0; x < 5; x++) {
                js.publish(subject, null);
            }

            String consumer = create1026Consumer(jsm, stream, subject);
            PullSubscribeOptions so = PullSubscribeOptions.fastBind(stream, consumer);
            JetStreamSubscription sub = js.subscribe(null, so);
            jsm.deleteConsumer(stream, consumer);
            sub.pull(5);
            validate1026(sub.nextMessage(500), listener, false);

            ConsumerContext context = setupFor1026Simplification(nc, jsm, listener, stream, subject);
            validate1026(context.next(1000), listener, true); // simplification next never raises warnings, so empty = true

            context = setupFor1026Simplification(nc, jsm, listener, stream, subject);
            //noinspection resource
            FetchConsumer fc = context.fetch(FetchConsumeOptions.builder().maxMessages(1).raiseStatusWarnings(false).build());
            validate1026(fc.nextMessage(), listener, true); // we said not to raise status warnings in the FetchConsumeOptions

            context = setupFor1026Simplification(nc, jsm, listener, stream, subject);
            //noinspection resource
            fc = context.fetch(FetchConsumeOptions.builder().maxMessages(1).raiseStatusWarnings().build());
            validate1026(fc.nextMessage(), listener, false); // we said raise status warnings in the FetchConsumeOptions

            context = setupFor1026Simplification(nc, jsm, listener, stream, subject);
            IterableConsumer ic = context.iterate(ConsumeOptions.builder().raiseStatusWarnings(false).build());
            validate1026(ic.nextMessage(1000), listener, true); // we said not to raise status warnings in the ConsumeOptions

            context = setupFor1026Simplification(nc, jsm, listener, stream, subject);
            ic = context.iterate(ConsumeOptions.builder().raiseStatusWarnings().build());
            validate1026(ic.nextMessage(1000), listener, false); // we said raise status warnings in the ConsumeOptions

            AtomicInteger count = new AtomicInteger();
            MessageHandler handler = m -> count.incrementAndGet();

            context = setupFor1026Simplification(nc, jsm, listener, stream, subject);
            //noinspection resource
            context.consume(ConsumeOptions.builder().raiseStatusWarnings(false).build(), handler);
            Thread.sleep(100); // give time to get a message
            assertEquals(0, count.get());
            validate1026(null, listener, true);

            context = setupFor1026Simplification(nc, jsm, listener, stream, subject);
            //noinspection resource
            context.consume(ConsumeOptions.builder().raiseStatusWarnings().build(), handler);
            Thread.sleep(100); // give time to get a message
            assertEquals(0, count.get());
            validate1026(null, listener, false);
        });
    }

    private static void validate1026(Message m, ListenerForTesting listener, boolean empty) {
        assertNull(m);
        sleep(100); // give time for the message to get there
        assertEquals(empty, listener.getPullStatusWarnings().isEmpty());
    }

    private static ConsumerContext setupFor1026Simplification(Connection nc, JetStreamManagement jsm, ListenerForTesting listener, String stream, String subject) throws IOException, JetStreamApiException {
        listener.reset();
        String consumer = create1026Consumer(jsm, stream, subject);
        ConsumerContext cCtx = nc.getConsumerContext(stream, consumer);
        jsm.deleteConsumer(stream, consumer);
        return cCtx;
    }

    private static String create1026Consumer(JetStreamManagement jsm, String stream, String subject) throws IOException, JetStreamApiException {
        String consumer = name();
        jsm.addOrUpdateConsumer(stream, ConsumerConfiguration.builder()
            .durable(consumer)
            .filterSubject(subject)
            .build());
        return consumer;
    }

    @Test
    public void testMessageDeleteRequest() {
        MessageDeleteRequest mdr = new MessageDeleteRequest(1, true);
        assertEquals(1, mdr.getSequence());
        assertTrue(mdr.isErase());
        assertFalse(mdr.isNoErase());
        assertTrue(mdr.toJson().contains("\"seq\""));
        assertFalse(mdr.toJson().contains("\"no_erase\""));

        mdr = new MessageDeleteRequest(2, false);
        assertEquals(2, mdr.getSequence());
        assertFalse(mdr.isErase());
        assertTrue(mdr.isNoErase());
        assertTrue(mdr.toJson().contains("\"seq\""));
        assertTrue(mdr.toJson().contains("\"no_erase\""));

        mdr = MessageDeleteRequest.builder().build();
        assertEquals(-1, mdr.getSequence());
        assertTrue(mdr.isErase());
        assertFalse(mdr.isNoErase());
        assertFalse(mdr.toJson().contains("\"seq\""));
        assertFalse(mdr.toJson().contains("\"no_erase\""));

        mdr = MessageDeleteRequest.builder().sequence(3).build();
        assertEquals(3, mdr.getSequence());
        assertTrue(mdr.isErase());
        assertFalse(mdr.isNoErase());
        assertTrue(mdr.toJson().contains("\"seq\""));
        assertFalse(mdr.toJson().contains("\"no_erase\""));

        mdr = MessageDeleteRequest.builder().noErase().erase().build();
        assertEquals(-1, mdr.getSequence());
        assertTrue(mdr.isErase());
        assertFalse(mdr.isNoErase());
        assertFalse(mdr.toJson().contains("\"seq\""));
        assertFalse(mdr.toJson().contains("\"no_erase\""));

        mdr = MessageDeleteRequest.builder().erase().noErase().build();
        assertEquals(-1, mdr.getSequence());
        assertFalse(mdr.isErase());
        assertTrue(mdr.isNoErase());
        assertFalse(mdr.toJson().contains("\"seq\""));
        assertTrue(mdr.toJson().contains("\"no_erase\""));
    }

    @Test
    public void testStreamPersistMode() throws Exception {
        jsServer.run(TestBase::atLeast2_12, nc -> {
            JetStreamManagement jsm = nc.jetStreamManagement();

            StreamConfiguration sc = StreamConfiguration.builder()
                .name(stream())
                .storageType(StorageType.File)
                .subjects(subject())
                .build();

            StreamInfo si = jsm.addStream(sc);
            assertTrue(si.getConfiguration().getPersistMode() == null || si.getConfiguration().getPersistMode() == PersistMode.Default);

            sc = StreamConfiguration.builder()
                .name(stream())
                .storageType(StorageType.File)
                .subjects(subject())
                .persistMode(PersistMode.Default)
                .build();

            si = jsm.addStream(sc);
            assertTrue(si.getConfiguration().getPersistMode() == null || si.getConfiguration().getPersistMode() == PersistMode.Default);

            sc = StreamConfiguration.builder()
                .name(stream())
                .storageType(StorageType.File)
                .subjects(subject())
                .persistMode(PersistMode.Async)
                .build();

            si = jsm.addStream(sc);
            assertSame(PersistMode.Async, si.getConfiguration().getPersistMode());
        });
    }
}
