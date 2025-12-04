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
import io.nats.client.support.Listener;
import io.nats.client.utils.VersionUtils;
import org.junit.jupiter.api.Test;

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
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class JetStreamManagementTests extends JetStreamTestBase {

    @Test
    public void testStreamCreate() throws Exception {
        long now = ZonedDateTime.now().toEpochSecond();

        runInSharedCustom((nc, ctx) -> {
            StreamConfiguration sc = ctx.scBuilder(2).build();
            String subject0 = ctx.subject(0);
            String subject1 = ctx.subject(1);

            StreamInfo si = ctx.createOrReplaceStream(sc);

            assertNotNull(si.getStreamState().toString()); // coverage
            assertTrue(now <= si.getCreateTime().toEpochSecond());

            assertNotNull(si.getConfiguration());
            sc = si.getConfiguration();
            assertEquals(ctx.stream, sc.getName());

            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject0, sc.getSubjects().get(0));
            assertEquals(subject1, sc.getSubjects().get(1));
            assertTrue(subject0.compareTo(subject1) != 0); // coverage

            assertEquals(RetentionPolicy.Limits, sc.getRetentionPolicy());
            assertEquals(DiscardPolicy.Old, sc.getDiscardPolicy());
            assertEquals(StorageType.Memory, sc.getStorageType());

            assertNotNull(si.getStreamState());
            assertEquals(-1, sc.getMaxConsumers());
            assertEquals(-1, sc.getMaxMsgs());
            assertEquals(-1, sc.getMaxBytes());
            //noinspection deprecation
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
        });
    }

    @Test
    public void testStreamCreate210() throws Exception {
        runInSharedCustom(VersionUtils::atLeast2_10, (nc, ctx) -> {
            StreamConfiguration sc = ctx.scBuilder(1)
                .firstSequence(42).build();
            StreamInfo si = ctx.createOrReplaceStream(sc);
            assertNotNull(si.getTimestamp());
            assertEquals(42, si.getConfiguration().getFirstSequence());
            PublishAck pa = ctx.js.publish(ctx.subject(), null);
            assertEquals(42, pa.getSeqno());
        });
    }

    @Test
    public void testStreamMetadata() throws Exception {
        runInSharedCustom(VersionUtils::atLeast2_9_0, (nc, ctx) -> {
            Map<String, String> metaData = new HashMap<>(); metaData.put(META_KEY, META_VALUE);
            StreamConfiguration sc = ctx.scBuilder(1).metadata(metaData).build();
            StreamInfo si = ctx.createOrReplaceStream(sc);
            assertNotNull(si.getConfiguration());
            assertMetaData(si.getConfiguration().getMetadata());
        });
    }

    @Test
    public void testStreamCreateWithNoSubject() throws Exception {
        long now = ZonedDateTime.now().toEpochSecond();
        runInSharedCustom((nc, ctx) -> {
            StreamConfiguration sc = ctx.scBuilder().subjects().build();
            StreamInfo si = ctx.addStream(sc);
            assertTrue(now <= si.getCreateTime().toEpochSecond());

            sc = si.getConfiguration();
            assertEquals(ctx.stream, sc.getName());

            assertEquals(1, sc.getSubjects().size());
            assertEquals(ctx.stream, sc.getSubjects().get(0));

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
        runInSharedCustom((nc, ctx) -> {
            ctx.createOrReplaceStream(2);
            String subject0 = ctx.subject(0);
            String subject1 = ctx.subject(1);

            StreamConfiguration sc = ctx.si.getConfiguration();
            assertNotNull(sc);
            assertEquals(ctx.stream, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(2, sc.getSubjects().size());
            assertEquals(subject0, sc.getSubjects().get(0));
            assertEquals(subject1, sc.getSubjects().get(1));
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

            sc = ctx.scBuilder(3)
                .maxMessages(42)
                .maxBytes(43)
                .maximumMessageSize(44)
                .maxAge(Duration.ofDays(100))
                .discardPolicy(DiscardPolicy.New)
                .noAck(true)
                .duplicateWindow(Duration.ofMinutes(3))
                .maxMessagesPerSubject(45)
                .build();
            StreamInfo si = ctx.jsm.updateStream(sc);
            assertNotNull(si);
            String subject2 = ctx.subject(2);

            sc = si.getConfiguration();
            assertNotNull(sc);
            assertEquals(ctx.stream, sc.getName());
            assertNotNull(sc.getSubjects());
            assertEquals(3, sc.getSubjects().size());
            assertEquals(subject0, sc.getSubjects().get(0));
            assertEquals(subject1, sc.getSubjects().get(1));
            assertEquals(subject2, sc.getSubjects().get(2));
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
            ctx.jsm.updateStream(StreamConfiguration.builder(sc).allowDirect(true).build());
            ctx.jsm.updateStream(StreamConfiguration.builder(sc).allowDirect(false).build());

            // allowed to change Mirror Direct
            ctx.createOrReplaceStream(StreamConfiguration.builder(sc).mirrorDirect(false).build());
            ctx.jsm.updateStream(StreamConfiguration.builder(sc).mirrorDirect(true).build());
            ctx.jsm.updateStream(StreamConfiguration.builder(sc).mirrorDirect(false).build());
        });
    }

    @Test
    public void testStreamExceptions() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            assertThrows(IllegalArgumentException.class, () -> ctx.jsm.addStream(null));

            // stream isn't even created yet
            assertStatus(10059, assertThrows(JetStreamApiException.class, () -> ctx.jsm.getMessage(ctx.stream, 1)));

            StreamConfiguration sc = ctx.scBuilder(1)
                .description(random())
                .build();
            ctx.createOrReplaceStream(sc);

            // no messages yet
            assertStatus(10037, assertThrows(JetStreamApiException.class, () -> ctx.jsm.getMessage(ctx.stream, 1)));

            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).subjects(random()).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).description(random()).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).retentionPolicy(RetentionPolicy.Interest).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).retentionPolicy(RetentionPolicy.WorkQueue).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).compressionOption(CompressionOption.S2).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).maxConsumers(1).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).maxMessages(1).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).maxMessagesPerSubject(1).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).maxAge(Duration.ofSeconds(1L)).build())));
            //noinspection deprecation
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).maxMsgSize(1).build()))); // COVERAGE for deprecated
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).maximumMessageSize(1).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).storageType(StorageType.File).build())));

            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).noAck(true).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).discardPolicy(DiscardPolicy.New).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).duplicateWindow(Duration.ofSeconds(1L)).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).allowRollup(true).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).allowDirect(true).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).denyDelete(true).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).denyPurge(true).build())));
            assertStatus(10058, assertThrows(JetStreamApiException.class, () -> ctx.jsm.addStream(StreamConfiguration.builder(sc).firstSequence(100).build())));
        });
    }

    @Test
    public void testUpdateStreamInvalids() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            assertThrows(IllegalArgumentException.class, () -> ctx.jsm.updateStream(null));

            StreamConfiguration sc = ctx.scBuilder(2).build();
            // cannot update non existent stream
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.updateStream(sc));

            // add the stream
            ctx.createOrReplaceStream(sc);

            // cannot change storage type
            StreamConfiguration scMemToFile = ctx.scBuilder(2)
                .storageType(StorageType.File)
                .build();
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.updateStream(scMemToFile));

            // cannot change MaxConsumers
            StreamConfiguration scMaxCon = ctx.scBuilder(2)
                .maxConsumers(2)
                .build();
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.updateStream(scMaxCon));

            StreamConfiguration scReten = ctx.scBuilder(2)
                .retentionPolicy(RetentionPolicy.Interest)
                .build();
            if (nc.getServerInfo().isOlderThanVersion("2.10")) {
                // cannot change RetentionPolicy
                assertThrows(JetStreamApiException.class, () -> ctx.jsm.updateStream(scReten));
            }
            else {
                ctx.jsm.updateStream(scReten);
            }
        });
    }

    @Test
    public void testGetStreamInfo() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.getStreamInfo(ctx.stream));

            String[] subjects = new String[6];
            String subjectIx5 = random();
            for (int x = 0; x < 5; x++) {
                subjects[x] = random() + x + 1;
            }
            subjects[5] = subjectIx5 + ".>";

            ctx.createOrReplaceStream(subjects);

            StreamInfo si = ctx.jsm.getStreamInfo(ctx.stream);
            assertEquals(ctx.stream, si.getConfiguration().getName());
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
                jsPublish(ctx.js, subjects[x], x + 1);
                PublishAck pa = jsPublish(ctx.js, subjects[x], data(x + 2));
                packs.add(pa);
                ctx.jsm.deleteMessage(ctx.stream, pa.getSeqno());
            }
            jsPublish(ctx.js, subjectIx5 + ".bar", 6);

            si = ctx.jsm.getStreamInfo(ctx.stream);
            assertEquals(ctx.stream, si.getConfiguration().getName());
            assertEquals(6, si.getStreamState().getSubjectCount());
            assertEquals(0, si.getStreamState().getSubjects().size());
            assertEquals(5, si.getStreamState().getDeletedCount());
            assertEquals(0, si.getStreamState().getDeleted().size());
            assertTrue(si.getStreamState().getSubjectMap().isEmpty());

            si = ctx.jsm.getStreamInfo(ctx.stream, StreamInfoOptions.builder().allSubjects().deletedDetails().build());
            assertEquals(ctx.stream, si.getConfiguration().getName());
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

            jsPublish(ctx.js, subjectIx5 + ".baz", 2);
            sleep(100);

            si = ctx.jsm.getStreamInfo(ctx.stream, StreamInfoOptions.builder().filterSubjects(subjectIx5 + ".>").deletedDetails().build());
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

            si = ctx.jsm.getStreamInfo(ctx.stream, StreamInfoOptions.builder().filterSubjects(subjects[4]).build());
            list = si.getStreamState().getSubjects();
            assertNotNull(list);
            assertEquals(1, list.size());
            s = list.get(0);
            assertEquals(subjects[4], s.getName());
            assertEquals(5, s.getCount());
        });
    }

    @Test
    public void testGetStreamInfoOrNamesPaginationFilter() throws Exception {
        runInOwnJsServer((nc, jsm, js) -> {
            // getStreams pages at 256
            // getStreamNames pages at 1024
            String prefix = random();
            addStreams(jsm, prefix, 300, 0, "x256");

            List<StreamInfo> list = jsm.getStreams();
            assertEquals(300, list.size());

            List<String> names = jsm.getStreamNames();
            assertEquals(300, names.size());

            addStreams(jsm, prefix, 1100, 300, "x1024");

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

    private void addStreams(JetStreamManagement jsm, String prefix, int count, int adj, String div) throws IOException, JetStreamApiException {
        for (int x = 0; x < count; x++) {
            createMemoryStream(jsm, prefix + "-" + (x + adj), "sub" + (x + adj) + "." + div + ".*");
        }
    }

    @Test
    public void testGetStreamNamesBySubjectFilter() throws Exception {
        runInOwnJsServer((nc, jsm, js) -> {
            String stream1 = random();
            String stream2 = random();
            String stream3 = random();
            String stream4 = random();
            createMemoryStream(jsm, stream1, "foo");
            createMemoryStream(jsm, stream2, "bar");
            createMemoryStream(jsm, stream3, "a.a");
            createMemoryStream(jsm, stream4, "a.b");

            List<String> list = jsm.getStreamNames("*");
            assertStreamNameList(list, stream1, stream2);

            list = jsm.getStreamNames(">");
            assertStreamNameList(list, stream1, stream2, stream3, stream4);

            list = jsm.getStreamNames("*.*");
            assertStreamNameList(list, stream3, stream4);

            list = jsm.getStreamNames("a.>");
            assertStreamNameList(list, stream3, stream4);

            list = jsm.getStreamNames("a.*");
            assertStreamNameList(list, stream3, stream4);

            list = jsm.getStreamNames("foo");
            assertStreamNameList(list, stream1);

            list = jsm.getStreamNames("a.a");
            assertStreamNameList(list, stream3);

            list = jsm.getStreamNames("nomatch");
            assertStreamNameList(list);
        });
    }

    private void assertStreamNameList(List<String> list, String... streams) {
        assertNotNull(list);
        assertEquals(streams.length, list.size());
        for (String s : streams) {
            assertTrue(list.contains(s));
        }
    }

    @Test
    public void testDeleteStream() throws Exception {
        runInShared((nc, ctx) -> {
            JetStreamApiException jsapiEx =
                assertThrows(JetStreamApiException.class, () -> ctx.jsm.deleteStream(random()));
            assertEquals(10059, jsapiEx.getApiErrorCode());

            assertNotNull(ctx.jsm.getStreamInfo(ctx.stream));
            assertTrue(ctx.deleteStream());

            jsapiEx = assertThrows(JetStreamApiException.class, () -> ctx.jsm.getStreamInfo(ctx.stream));
            assertEquals(10059, jsapiEx.getApiErrorCode());

            jsapiEx = assertThrows(JetStreamApiException.class, () -> ctx.deleteStream());
            assertEquals(10059, jsapiEx.getApiErrorCode());
        });
    }

    @Test
    public void testPurgeStreamAndOptions() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            // invalid to have both keep and seq
            assertThrows(IllegalArgumentException.class,
                () -> PurgeOptions.builder().keep(1).sequence(1).build());

            // error to purge a stream that does not exist
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.purgeStream(random()));

            ctx.createOrReplaceStream(2);
            StreamInfo si = ctx.jsm.getStreamInfo(ctx.stream);
            assertEquals(0, si.getStreamState().getMsgCount());

            jsPublish(nc, ctx.subject(0), 10);
            si = ctx.jsm.getStreamInfo(ctx.stream);
            assertEquals(10, si.getStreamState().getMsgCount());

            PurgeOptions options = PurgeOptions.builder().keep(7).build();
            PurgeResponse pr = ctx.jsm.purgeStream(ctx.stream, options);
            assertTrue(pr.isSuccess());
            assertEquals(3, pr.getPurged());

            options = PurgeOptions.builder().sequence(9).build();
            pr = ctx.jsm.purgeStream(ctx.stream, options);
            assertTrue(pr.isSuccess());
            assertEquals(5, pr.getPurged());
            si = ctx.jsm.getStreamInfo(ctx.stream);
            assertEquals(2, si.getStreamState().getMsgCount());

            pr = ctx.jsm.purgeStream(ctx.stream);
            assertTrue(pr.isSuccess());
            assertEquals(2, pr.getPurged());
            si = ctx.jsm.getStreamInfo(ctx.stream);
            assertEquals(0, si.getStreamState().getMsgCount());

            jsPublish(nc, ctx.subject(0), 10);
            jsPublish(nc, ctx.subject(1), 10);
            si = ctx.jsm.getStreamInfo(ctx.stream);
            assertEquals(20, si.getStreamState().getMsgCount());
            ctx.jsm.purgeStream(ctx.stream, PurgeOptions.subject(ctx.subject(0)));
            si = ctx.jsm.getStreamInfo(ctx.stream);
            assertEquals(10, si.getStreamState().getMsgCount());

            options = PurgeOptions.builder().subject(ctx.subject(0)).sequence(1).build();
            assertEquals(ctx.subject(0), options.getSubject());
            assertEquals(1, options.getSequence());

            options = PurgeOptions.builder().subject(ctx.subject(0)).keep(2).build();
            assertEquals(2, options.getKeep());
        });
    }

    @Test
    public void testAddDeleteConsumerPart1() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            String subject = random();
            ctx.createOrReplaceStream(subjectGt(subject));

            List<ConsumerInfo> list = ctx.jsm.getConsumers(ctx.stream);
            assertEquals(0, list.size());

            final ConsumerConfiguration cc = ConsumerConfiguration.builder().build();
            IllegalArgumentException iae =
                assertThrows(IllegalArgumentException.class, () -> ctx.jsm.addOrUpdateConsumer(null, cc));
            assertTrue(iae.getMessage().contains("Stream cannot be null or empty"));
            iae = assertThrows(IllegalArgumentException.class, () -> ctx.jsm.addOrUpdateConsumer(ctx.stream, null));
            assertTrue(iae.getMessage().contains("Config cannot be null"));

            // durable and name can both be null
            ConsumerInfo ci = ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);
            assertNotNull(ci.getName());

            // threshold can be set for durable
            final ConsumerConfiguration cc2 = ConsumerConfiguration.builder().durable(random()).inactiveThreshold(10000).build();
            ci = ctx.jsm.addOrUpdateConsumer(ctx.stream, cc2);
            assertNotNull(ci.getName());
            ConsumerConfiguration cc2cc = ci.getConsumerConfiguration();
            assertNotNull(cc2cc);
            Duration duration = ci.getConsumerConfiguration().getInactiveThreshold();
            assertNotNull(duration);
            assertEquals(10000, duration.toMillis());
        });
    }

    @Test
    public void testAddDeleteConsumerPart2() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            boolean atLeast2dot9 = nc.getServerInfo().isSameOrNewerThanVersion("2.9");
            String subject = random();
            ctx.createOrReplaceStream(subjectGt(subject));

            // with and w/o deliver subject for push/pull
            String dur0 = random();
            addConsumer(ctx.jsm, ctx.stream, atLeast2dot9, dur0, null, null, ConsumerConfiguration.builder()
                .durable(dur0)
                .build());

            String dur = random();
            String deliver = random();
            addConsumer(ctx.jsm, ctx.stream, atLeast2dot9, dur, deliver, null, ConsumerConfiguration.builder()
                .durable(dur)
                .deliverSubject(deliver)
                .build());

            // test delete here
            List<String> consumers = ctx.jsm.getConsumerNames(ctx.stream);
            assertEquals(2, consumers.size());
            assertTrue(ctx.jsm.deleteConsumer(ctx.stream, dur0));
            consumers = ctx.jsm.getConsumerNames(ctx.stream);
            assertEquals(1, consumers.size());
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.deleteConsumer(ctx.stream, dur0));

            // some testing of new name
            if (atLeast2dot9) {
                dur = random();
                addConsumer(ctx.jsm, ctx.stream, true, dur, null, null, ConsumerConfiguration.builder()
                    .durable(dur)
                    .name(dur)
                    .build());

                dur = random();
                deliver = random();
                addConsumer(ctx.jsm, ctx.stream, true, dur, deliver, null, ConsumerConfiguration.builder()
                    .durable(dur)
                    .name(dur)
                    .deliverSubject(deliver)
                    .build());

                dur = random();
                addConsumer(ctx.jsm, ctx.stream, true, dur, null, ">", ConsumerConfiguration.builder()
                    .durable(dur)
                    .filterSubject(">")
                    .build());

                dur = random();
                addConsumer(ctx.jsm, ctx.stream, true, dur, null, subjectGt(subject), ConsumerConfiguration.builder()
                    .durable(dur)
                    .filterSubject(subjectGt(subject))
                    .build());

                dur = random();
                addConsumer(ctx.jsm, ctx.stream, true, dur, null, subjectDot(subject, "foo"), ConsumerConfiguration.builder()
                    .durable(dur)
                    .filterSubject(subjectDot(subject, "foo"))
                    .build());
            }
        });
    }

    @Test
    public void testAddPausedConsumer() throws Exception {
        runInShared(VersionUtils::atLeast2_11, (nc, ctx) -> {
            List<ConsumerInfo> list = ctx.jsm.getConsumers(ctx.stream);
            assertEquals(0, list.size());

            ZonedDateTime pauseUntil = ZonedDateTime.now(ZONE_ID_GMT).plusMinutes(2);
            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(ctx.consumerName())
                    .pauseUntil(pauseUntil)
                    .build();

            // Consumer should be paused on creation.
            ConsumerInfo ci = ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);
            assertTrue(ci.getPaused());
            assertNotNull(ci.getPauseRemaining());
            assertTrue(ci.getPauseRemaining().toMillis() > 60_000);
            assertEquals(pauseUntil, ci.getConsumerConfiguration().getPauseUntil());
        });
    }

    @Test
    public void testPauseResumeConsumer() throws Exception {
        runInShared(VersionUtils::atLeast2_11, (nc, ctx) -> {
            List<ConsumerInfo> list = ctx.jsm.getConsumers(ctx.stream);
            assertEquals(0, list.size());

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                    .durable(ctx.consumerName())
                    .build();

            // durable and name can both be null
            ConsumerInfo ci = ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);
            assertNotNull(ci.getName());

            // pause consumer
            ZonedDateTime pauseUntil = ZonedDateTime.now(ZONE_ID_GMT).plusMinutes(2);
            ConsumerPauseResponse pauseResponse = ctx.jsm.pauseConsumer(ctx.stream, ci.getName(), pauseUntil);
            assertTrue(pauseResponse.isPaused());
            assertEquals(pauseUntil, pauseResponse.getPauseUntil());

            ci = ctx.jsm.getConsumerInfo(ctx.stream, ci.getName());
            assertTrue(ci.getPaused());
            assertNotNull(ci.getPauseRemaining());
            assertTrue(ci.getPauseRemaining().toMillis() > 60_000);

            // resume consumer
            boolean isResumed = ctx.jsm.resumeConsumer(ctx.stream, ci.getName());
            assertTrue(isResumed);

            ci = ctx.jsm.getConsumerInfo(ctx.stream, ci.getName());
            assertFalse(ci.getPaused());

            // pause again
            pauseResponse = ctx.jsm.pauseConsumer(ctx.stream, ci.getName(), pauseUntil);
            assertTrue(pauseResponse.isPaused());
            assertEquals(pauseUntil, pauseResponse.getPauseUntil());

            // resume via pause with no date
            pauseResponse = ctx.jsm.pauseConsumer(ctx.stream, ci.getName(), null);
            assertFalse(pauseResponse.isPaused());
            assertEquals(DEFAULT_TIME, pauseResponse.getPauseUntil());

            ci = ctx.jsm.getConsumerInfo(ctx.stream, ci.getName());
            assertFalse(ci.getPaused());

            assertThrows(JetStreamApiException.class, () -> ctx.jsm.pauseConsumer(random(), ctx.consumerName(), pauseUntil));
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.pauseConsumer(ctx.stream, random(), pauseUntil));
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.resumeConsumer(random(), ctx.consumerName()));
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.resumeConsumer(ctx.stream, random()));
        });
    }

    private static void addConsumer(JetStreamManagement jsm, String stream, boolean atLeast2dot9, String name, String deliver, String fs, ConsumerConfiguration cc) throws IOException, JetStreamApiException {
        ConsumerInfo ci = jsm.addOrUpdateConsumer(stream, cc);
        assertEquals(name, ci.getName());
        if (atLeast2dot9) {
            assertEquals(name, ci.getConsumerConfiguration().getName());
        }
        assertEquals(name, ci.getConsumerConfiguration().getDurable());
        if (fs == null) {
            assertNull(ci.getConsumerConfiguration().getFilterSubject());
        }
        if (deliver != null) {
            assertEquals(deliver, ci.getConsumerConfiguration().getDeliverSubject());
        }
    }

    @Test
    public void testValidConsumerUpdates() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            String subject = random();
            String subjectGt = subjectGt(subject);
            ctx.createOrReplaceStream(subjectGt);

            ConsumerConfiguration cc = prepForUpdateTest(ctx.jsm, ctx.stream, subjectGt, null);
            cc = ConsumerConfiguration.builder(cc).deliverSubject(random()).build();
            assertValidAddOrUpdate(ctx.jsm, cc, ctx.stream);

            cc = prepForUpdateTest(ctx.jsm, ctx.stream, subjectGt, cc.getDurable());
            cc = ConsumerConfiguration.builder(cc).ackWait(Duration.ofSeconds(5)).build();
            assertValidAddOrUpdate(ctx.jsm, cc, ctx.stream);

            cc = prepForUpdateTest(ctx.jsm, ctx.stream, subjectGt, cc.getDurable());
            cc = ConsumerConfiguration.builder(cc).rateLimit(100).build();
            assertValidAddOrUpdate(ctx.jsm, cc, ctx.stream);

            cc = prepForUpdateTest(ctx.jsm, ctx.stream, subjectGt, cc.getDurable());
            cc = ConsumerConfiguration.builder(cc).maxAckPending(100).build();
            assertValidAddOrUpdate(ctx.jsm, cc, ctx.stream);

            cc = prepForUpdateTest(ctx.jsm, ctx.stream, subjectGt, cc.getDurable());
            cc = ConsumerConfiguration.builder(cc).maxDeliver(4).build();
            assertValidAddOrUpdate(ctx.jsm, cc, ctx.stream);

            cc = prepForUpdateTest(ctx.jsm, ctx.stream, subjectGt, cc.getDurable());
            cc = ConsumerConfiguration.builder(cc).filterSubject(subjectStar(subject)).build();
            assertValidAddOrUpdate(ctx.jsm, cc, ctx.stream);
        });
    }

    @Test
    public void testInvalidConsumerUpdates() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            String subject = random();
            String subjectGt = subjectGt(subject);
            ctx.createOrReplaceStream(subjectGt);

            ConsumerConfiguration cc = prepForUpdateTest(ctx.jsm, ctx.stream, subjectGt, null);
            cc = ConsumerConfiguration.builder(cc).deliverPolicy(DeliverPolicy.New).build();
            assertInvalidConsumerUpdate(ctx.jsm, cc, ctx.stream);

            cc = prepForUpdateTest(ctx.jsm, ctx.stream, subjectGt, cc.getDurable());
            cc = ConsumerConfiguration.builder(cc).idleHeartbeat(Duration.ofMillis(111)).build();
            assertInvalidConsumerUpdate(ctx.jsm, cc, ctx.stream);
        });
    }

    private ConsumerConfiguration prepForUpdateTest(JetStreamManagement jsm, String stream, String subjectGt, String durableToDelete) throws IOException, JetStreamApiException {
        try {
            if (durableToDelete != null) {
                jsm.deleteConsumer(stream, durableToDelete);
            }
        }
        catch (Exception e) { /* ignore */ }
        ConsumerConfiguration cc = ConsumerConfiguration.builder()
            .durable(random())
            .ackPolicy(AckPolicy.Explicit)
            .deliverSubject(random())
            .maxDeliver(3)
            .filterSubject(subjectGt)
            .build();
        assertValidAddOrUpdate(jsm, cc, stream);
        return cc;
    }

    private void assertInvalidConsumerUpdate(JetStreamManagement jsm, ConsumerConfiguration cc, String stream) {
        JetStreamApiException e = assertThrows(JetStreamApiException.class, () -> jsm.addOrUpdateConsumer(stream, cc));
        assertEquals(10012, e.getApiErrorCode());
        assertEquals(500, e.getErrorCode());
    }

    private void assertValidAddOrUpdate(JetStreamManagement jsm, ConsumerConfiguration cc, String stream) throws IOException, JetStreamApiException {
        ConsumerInfo ci = jsm.addOrUpdateConsumer(stream, cc);
        ConsumerConfiguration cicc = ci.getConsumerConfiguration();
        assertEquals(cc.getDurable(), ci.getName());
        assertEquals(cc.getDurable(), cicc.getDurable());
        assertEquals(cc.getDeliverSubject(), cicc.getDeliverSubject());
        assertEquals(cc.getMaxDeliver(), cicc.getMaxDeliver());
        assertEquals(cc.getDeliverPolicy(), cicc.getDeliverPolicy());

        List<String> consumers = jsm.getConsumerNames(stream);
        assertEquals(1, consumers.size());
        assertEquals(cc.getDurable(), consumers.get(0));
    }

    @Test
    public void testConsumerMetadata() throws Exception {
        runInShared((nc, ctx) -> {
            Map<String, String> metaData = new HashMap<>(); metaData.put(META_KEY, META_VALUE);

            ConsumerConfiguration cc = ConsumerConfiguration.builder()
                .durable(random())
                .metadata(metaData)
                .build();

            ConsumerInfo ci = ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);
            assertMetaData(ci.getConsumerConfiguration().getMetadata());
        });
    }

    @Test
    public void testCreateConsumersWithFilters() throws Exception {
        runInShared((nc, ctx) -> {
            String subject = ctx.subject();

            // plain subject
            ConsumerConfiguration.Builder builder = ConsumerConfiguration.builder().durable(random());
            ctx.jsm.addOrUpdateConsumer(ctx.stream, builder.filterSubject(subject).build());
            List<ConsumerInfo> cis = ctx.jsm.getConsumers(ctx.stream);
            assertEquals(subject, cis.get(0).getConsumerConfiguration().getFilterSubject());

            if (nc.getServerInfo().isSameOrNewerThanVersion("2.10")) {
                // 2.10 and later you can set the filter to something that does not match
                ctx.jsm.addOrUpdateConsumer(ctx.stream, builder.filterSubject(subjectDot(subject, "two-ten-allows-not-matching")).build());
                cis = ctx.jsm.getConsumers(ctx.stream);
                assertEquals(subjectDot(subject, "two-ten-allows-not-matching"), cis.get(0).getConsumerConfiguration().getFilterSubject());
            }
            else {
                assertThrows(JetStreamApiException.class,
                    () -> ctx.jsm.addOrUpdateConsumer(ctx.stream, builder.filterSubject(subjectDot(subject, "not-match")).build()));
            }

            // wildcard subject
            ctx.createOrReplaceStream(subjectStar(subject));

            String subjectA = subjectDot(subject, "A");
            ctx.jsm.addOrUpdateConsumer(ctx.stream, builder.filterSubject(subjectA).build());
            cis = ctx.jsm.getConsumers(ctx.stream);
            assertEquals(subjectA, cis.get(0).getConsumerConfiguration().getFilterSubject());

            // gt subject
            ctx.createOrReplaceStream(subjectGt(subject));

            ctx.jsm.addOrUpdateConsumer(ctx.stream, builder.filterSubject(subjectA).build());
            cis = ctx.jsm.getConsumers(ctx.stream);
            assertEquals(subjectA, cis.get(0).getConsumerConfiguration().getFilterSubject());
        });
    }

    @Test
    public void testGetConsumerInfo() throws Exception {
        runInShared((nc, ctx) -> {
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.getConsumerInfo(ctx.stream, ctx.consumerName()));
            ConsumerConfiguration cc = ConsumerConfiguration.builder().durable(ctx.consumerName()).build();
            ConsumerInfo ci = ctx.jsm.addOrUpdateConsumer(ctx.stream, cc);
            assertEquals(ctx.stream, ci.getStreamName());
            assertEquals(ctx.consumerName(), ci.getName());
            ci = ctx.jsm.getConsumerInfo(ctx.stream, ctx.consumerName());
            assertEquals(ctx.stream, ci.getStreamName());
            assertEquals(ctx.consumerName(), ci.getName());
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.getConsumerInfo(ctx.stream, random()));
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
        runInShared((nc, ctx) -> {
            addConsumers(ctx.jsm, ctx.stream, 600); // getConsumers pages at 256

            List<ConsumerInfo> list = ctx.jsm.getConsumers(ctx.stream);
            assertEquals(600, list.size());

            addConsumers(ctx.jsm, ctx.stream, 500); // getConsumerNames pages at 1024
            List<String> names = ctx.jsm.getConsumerNames(ctx.stream);
            assertEquals(1100, names.size());
        });
    }

    private void addConsumers(JetStreamManagement jsm, String stream, int count) throws IOException, JetStreamApiException {
        String base = random() ;
        for (int x = 1; x <= count; x++) {
            String dur = base + "-" + x;
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

        runInShared((nc, ctx) -> {
            Headers h = new Headers();
            h.add("foo", "bar");

            ZonedDateTime timeBeforeCreated = ZonedDateTime.now();
            ctx.js.publish(NatsMessage.builder().subject(ctx.subject()).headers(h).data(dataBytes(1)).build());
            ctx.js.publish(NatsMessage.builder().subject(ctx.subject()).build());

            MessageInfo mi = ctx.jsm.getMessage(ctx.stream, 1);
            assertNotNull(mi.toString());
            assertEquals(ctx.subject(), mi.getSubject());
            assertNotNull(mi.getData());
            assertEquals(data(1), new String(mi.getData()));
            assertEquals(1, mi.getSeq());
            assertNotNull(mi.getTime());
            assertTrue(mi.getTime().toEpochSecond() >= timeBeforeCreated.toEpochSecond());
            assertNotNull(mi.getHeaders());
            List<String> foos = mi.getHeaders().get("foo");
            assertNotNull(foos);
            assertEquals("bar", foos.get(0));

            mi = ctx.jsm.getMessage(ctx.stream, 2);
            assertNotNull(mi.toString());
            assertEquals(ctx.subject(), mi.getSubject());
            assertNull(mi.getData());
            assertEquals(2, mi.getSeq());
            assertNotNull(mi.getTime());
            assertTrue(mi.getTime().toEpochSecond() >= timeBeforeCreated.toEpochSecond());
            assertTrue(mi.getHeaders() == null || mi.getHeaders().isEmpty());

            assertTrue(ctx.jsm.deleteMessage(ctx.stream, 1, false)); // added coverage for use of erase (no_erase) flag.
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.deleteMessage(ctx.stream, 1));
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.getMessage(ctx.stream, 1));
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.getMessage(ctx.stream, 3));
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.deleteMessage(random(), 1));
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.getMessage(random(), 1));
        });
    }

    @Test
    public void testSealed() throws Exception {
        runInShared((nc, ctx) -> {
            assertFalse(ctx.si.getConfiguration().getSealed());

            ctx.js.publish(ctx.subject(), "data1".getBytes());

            StreamConfiguration sc = new StreamConfiguration.Builder(ctx.si.getConfiguration())
                .seal()
                .build();
            StreamInfo si = ctx.jsm.updateStream(sc);
            assertTrue(si.getConfiguration().getSealed());

            assertThrows(JetStreamApiException.class, () -> ctx.js.publish(ctx.subject(), "data2".getBytes()));
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
        runInShared((nc, ctx) -> {
            final ConsumerConfiguration cc0 = ConsumerConfiguration.builder()
                .durable(ctx.consumerName())
                .build();
            ConsumerInfo ci = ctx.jsm.addOrUpdateConsumer(ctx.stream, cc0);
            // server returns 0 when value is not set
            assertEquals(0, ci.getConsumerConfiguration().getNumReplicas());

            final ConsumerConfiguration cc1 = ConsumerConfiguration.builder()
                .durable(ctx.consumerName())
                .numReplicas(1)
                .build();
            ci = ctx.jsm.addOrUpdateConsumer(ctx.stream, cc1);
            assertEquals(1, ci.getConsumerConfiguration().getNumReplicas());
        });
    }

    @Test
    public void testGetMessage() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            ctx.createOrReplaceStream(2);
            assertFalse(ctx.si.getConfiguration().getAllowDirect());

            ZonedDateTime timeBeforeCreated = ZonedDateTime.now();
            sleep(100);
            ctx.js.publish(buildTestGetMessage(ctx, 0, 1));
            ctx.js.publish(buildTestGetMessage(ctx, 1, 2));
            ctx.js.publish(buildTestGetMessage(ctx, 0, 3));
            ctx.js.publish(buildTestGetMessage(ctx, 1, 4));
            ctx.js.publish(buildTestGetMessage(ctx, 0, 5));
            ctx.js.publish(buildTestGetMessage(ctx, 1, 6));

            validateGetMessage(ctx.jsm, ctx, timeBeforeCreated);

            StreamConfiguration sc = StreamConfiguration.builder(ctx.si.getConfiguration()).allowDirect(true).build();
            StreamInfo si = ctx.jsm.updateStream(sc);
            assertTrue(si.getConfiguration().getAllowDirect());
            validateGetMessage(ctx.jsm, ctx, timeBeforeCreated);

            // error case stream doesn't exist
            assertThrows(JetStreamApiException.class, () -> ctx.jsm.getMessage(random(), 1));
        });
    }

    private static NatsMessage buildTestGetMessage(JetStreamTestingContext ctx, int n, int q) {
        String data = "s" + n + "-q" + q;
        return NatsMessage.builder()
            .subject(ctx.subject(n))
            .data("d-" + data)
            .headers(new Headers().put("h", "h-" + data))
            .build();
    }

    private void validateGetMessage(JetStreamManagement jsm, JetStreamTestingContext ctx, ZonedDateTime timeBeforeCreated) throws IOException, JetStreamApiException {
        assertMessageInfo(ctx, 0, 1, jsm.getMessage(ctx.stream, 1), timeBeforeCreated);
        assertMessageInfo(ctx, 0, 5, jsm.getLastMessage(ctx.stream, ctx.subject(0)), timeBeforeCreated);
        assertMessageInfo(ctx, 1, 6, jsm.getLastMessage(ctx.stream, ctx.subject(1)), timeBeforeCreated);

        assertMessageInfo(ctx, 0, 1, jsm.getNextMessage(ctx.stream, -1, ctx.subject(0)), timeBeforeCreated);
        assertMessageInfo(ctx, 1, 2, jsm.getNextMessage(ctx.stream, -1, ctx.subject(1)), timeBeforeCreated);
        assertMessageInfo(ctx, 0, 1, jsm.getNextMessage(ctx.stream, 0, ctx.subject(0)), timeBeforeCreated);
        assertMessageInfo(ctx, 1, 2, jsm.getNextMessage(ctx.stream, 0, ctx.subject(1)), timeBeforeCreated);
        assertMessageInfo(ctx, 0, 1, jsm.getFirstMessage(ctx.stream, ctx.subject(0)), timeBeforeCreated);
        assertMessageInfo(ctx, 1, 2, jsm.getFirstMessage(ctx.stream, ctx.subject(1)), timeBeforeCreated);
        assertMessageInfo(ctx, 0, 1, jsm.getFirstMessage(ctx.stream, timeBeforeCreated), timeBeforeCreated);
        assertMessageInfo(ctx, 0, 1, jsm.getFirstMessage(ctx.stream, timeBeforeCreated, ctx.subject(0)), timeBeforeCreated);
        assertMessageInfo(ctx, 1, 2, jsm.getFirstMessage(ctx.stream, timeBeforeCreated, ctx.subject(1)), timeBeforeCreated);

        assertMessageInfo(ctx, 0, 1, jsm.getNextMessage(ctx.stream, 1, ctx.subject(0)), timeBeforeCreated);
        assertMessageInfo(ctx, 1, 2, jsm.getNextMessage(ctx.stream, 1, ctx.subject(1)), timeBeforeCreated);

        assertMessageInfo(ctx, 0, 3, jsm.getNextMessage(ctx.stream, 2, ctx.subject(0)), timeBeforeCreated);
        assertMessageInfo(ctx, 1, 2, jsm.getNextMessage(ctx.stream, 2, ctx.subject(1)), timeBeforeCreated);

        assertMessageInfo(ctx, 0, 5, jsm.getNextMessage(ctx.stream, 5, ctx.subject(0)), timeBeforeCreated);
        assertMessageInfo(ctx, 1, 6, jsm.getNextMessage(ctx.stream, 5, ctx.subject(1)), timeBeforeCreated);

        assertStatus(10003, assertThrows(JetStreamApiException.class, () -> jsm.getMessage(ctx.stream, -1)));
        assertStatus(10003, assertThrows(JetStreamApiException.class, () -> jsm.getMessage(ctx.stream, 0)));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getMessage(ctx.stream, 9)));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getLastMessage(ctx.stream, "not-a-subject")));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getFirstMessage(ctx.stream, "not-a-subject")));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getNextMessage(ctx.stream, 9, ctx.subject(0))));
        assertStatus(10037, assertThrows(JetStreamApiException.class, () -> jsm.getNextMessage(ctx.stream, 1, "not-a-subject")));
    }

    private void assertStatus(int apiErrorCode, JetStreamApiException jsae) {
        assertEquals(apiErrorCode, jsae.getApiErrorCode());
    }

    private void assertMessageInfo(JetStreamTestingContext ctx, int subj, long seq, MessageInfo mi, ZonedDateTime timeBeforeCreated) {
        assertEquals(ctx.stream, mi.getStream());
        assertEquals(ctx.subject(subj), mi.getSubject());
        assertEquals(seq, mi.getSeq());
        assertNotNull(mi.getTime());
        assertTrue(mi.getTime().toEpochSecond() >= timeBeforeCreated.toEpochSecond());
        String expectedData = "s" + subj + "-q" + seq;
        assertNotNull(mi.getData());
        assertEquals("d-" + expectedData, new String(mi.getData()));
        assertNotNull(mi.getHeaders());
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
        //noinspection deprecation
        MessageGetRequest.seqBytes(1);
        //noinspection deprecation
        MessageGetRequest.lastBySubjectBytes(random());
        //noinspection deprecation
        new MessageGetRequest(1);
        //noinspection deprecation
        new MessageGetRequest(random());

        // coverage for MessageInfo, has error
        String json = dataAsString("GenericErrorResponse.json");
        NatsMessage m = new NatsMessage("sub", null, json.getBytes(StandardCharsets.US_ASCII));
        //noinspection deprecation
        MessageInfo mi = new MessageInfo(m);
        assertTrue(mi.hasError());
        assertEquals(-1, mi.getLastSeq());
        assertFalse(mi.toString().contains("last_seq"));
    }

    @Test
    public void testDirectMessageRepublishedSubject() throws Exception {
        runInSharedCustom(VersionUtils::atLeast2_9_0, (nc, ctx) -> {
            String streamName = random();
            String bucketName = random();
            String subject = random();
            String streamSubject = subject + ".>";
            String publishSubject1 = subject + ".one";
            String publishSubject2 = subject + ".two";
            String publishSubject3 = subject + ".three";
            String republishDest = "$KV." + bucketName + ".>";

            StreamConfiguration sc = ctx.scBuilder(streamSubject)
                .republish(Republish.builder()
                    .source(">")
                    .destination(republishDest)
                    .build())
                .build();
            ctx.createOrReplaceStream(sc);

            ctx.kvCreate(bucketName);
            KeyValue kv = nc.keyValue(bucketName);

            nc.publish(publishSubject1, "uno".getBytes());
            nc.jetStream().publish(publishSubject2, "dos".getBytes());
            kv.put(publishSubject3, "tres");

            KeyValueEntry kve1 = kv.get(publishSubject1);
            assertEquals(bucketName, kve1.getBucket());
            assertEquals(publishSubject1, kve1.getKey());
            assertEquals("uno", kve1.getValueAsString());

            KeyValueEntry kve2 = kv.get(publishSubject2);
            assertEquals(bucketName, kve2.getBucket());
            assertEquals(publishSubject2, kve2.getKey());
            assertEquals("dos", kve2.getValueAsString());

            KeyValueEntry kve3 = kv.get(publishSubject3);
            assertEquals(bucketName, kve3.getBucket());
            assertEquals(publishSubject3, kve3.getKey());
            assertEquals("tres", kve3.getValueAsString());
        });
    }

    @Test
    public void testCreateConsumerUpdateConsumer() throws Exception {
        runInOwnJsServer(VersionUtils::atLeast2_9_0, (nc, jsm, js) -> {
            String streamPrefix = random();
            JetStreamManagement jsmNew = nc.jetStreamManagement();
            JetStreamManagement jsmPre290 = nc.jetStreamManagement(JetStreamOptions.builder().optOut290ConsumerCreate(true).build());

            // --------------------------------------------------------
            // New without filter
            // --------------------------------------------------------
            String stream1 = streamPrefix + "-new";
            String name = random();
            String subject = random();
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
            ConsumerConfiguration cc12 = ConsumerConfiguration.builder().name(name).description(random()).build();
            ci = jsmNew.updateConsumer(stream1, cc12);
            assertEquals(name, ci.getName());
            assertNull(ci.getConsumerConfiguration().getFilterSubject());

            // --------------------------------------------------------
            // New with filter subject
            // --------------------------------------------------------
            String stream2 = streamPrefix + "-new-fs";
            name = random();
            subject = random();
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
            name = random();
            subject = random();
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
            subject = random();

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
        Listener listener = new Listener();
        runInSharedOwnNc(listener, VersionUtils::atLeast2_10_26, (nc, ctx) -> {
            String subject = ctx.subject();
            for (int x = 0; x < 5; x++) {
                ctx.js.publish(subject, null);
            }

            String consumer = create1026Consumer(ctx.jsm, ctx.stream, subject);
            PullSubscribeOptions so = PullSubscribeOptions.fastBind(ctx.stream, consumer);
            JetStreamSubscription sub = ctx.js.subscribe(null, so);
            ctx.jsm.deleteConsumer(ctx.stream, consumer);
            sub.pull(5);
            validate1026(sub.nextMessage(500), listener, false);

            ConsumerContext context = setupFor1026Simplification(nc, ctx.jsm, listener, ctx.stream, subject);
            validate1026(context.next(1000), listener, true); // simplification next never raises warnings, so empty = true

            context = setupFor1026Simplification(nc, ctx.jsm, listener, ctx.stream, subject);
            //noinspection resource
            FetchConsumer fc = context.fetch(FetchConsumeOptions.builder().maxMessages(1).raiseStatusWarnings(false).build());
            validate1026(fc.nextMessage(), listener, true); // we said not to raise status warnings in the FetchConsumeOptions

            context = setupFor1026Simplification(nc, ctx.jsm, listener, ctx.stream, subject);
            //noinspection resource
            fc = context.fetch(FetchConsumeOptions.builder().maxMessages(1).raiseStatusWarnings().build());
            validate1026(fc.nextMessage(), listener, false); // we said raise status warnings in the FetchConsumeOptions

            context = setupFor1026Simplification(nc, ctx.jsm, listener, ctx.stream, subject);
            IterableConsumer ic = context.iterate(ConsumeOptions.builder().raiseStatusWarnings(false).build());
            validate1026(ic.nextMessage(1000), listener, true); // we said not to raise status warnings in the ConsumeOptions

            context = setupFor1026Simplification(nc, ctx.jsm, listener, ctx.stream, subject);
            ic = context.iterate(ConsumeOptions.builder().raiseStatusWarnings().build());
            validate1026(ic.nextMessage(1000), listener, false); // we said raise status warnings in the ConsumeOptions

            AtomicInteger count = new AtomicInteger();
            MessageHandler handler = m -> count.incrementAndGet();

            context = setupFor1026Simplification(nc, ctx.jsm, listener, ctx.stream, subject);
            //noinspection resource
            context.consume(ConsumeOptions.builder().raiseStatusWarnings(false).build(), handler);
            Thread.sleep(100); // give time to get a message
            assertEquals(0, count.get());
            validate1026(null, listener, true);

            context = setupFor1026Simplification(nc, ctx.jsm, listener, ctx.stream, subject);
            //noinspection resource
            context.consume(ConsumeOptions.builder().raiseStatusWarnings().build(), handler);
            Thread.sleep(100); // give time to get a message
            assertEquals(0, count.get());
            validate1026(null, listener, false);
        });
    }

    private static void validate1026(Message m, Listener listener, boolean empty) {
        assertNull(m);
        sleep(250); // give time for the message to get there
        if (empty) {
            assertEquals(0, listener.getPullStatusWarningsCount());
        }
        else {
            assertTrue(listener.getPullStatusWarningsCount() > 0);
        }
    }

    private static ConsumerContext setupFor1026Simplification(Connection nc, JetStreamManagement jsm, Listener listener, String stream, String subject) throws IOException, JetStreamApiException {
        listener.reset();
        String consumer = create1026Consumer(jsm, stream, subject);
        ConsumerContext cCtx = nc.getConsumerContext(stream, consumer);
        jsm.deleteConsumer(stream, consumer);
        return cCtx;
    }

    private static String create1026Consumer(JetStreamManagement jsm, String stream, String subject) throws IOException, JetStreamApiException {
        String consumer = random();
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
        runInOwnJsServer(VersionUtils::atLeast2_12, (nc, jsm, js) -> {
            StreamConfiguration sc = StreamConfiguration.builder()
                .name(random())
                .storageType(StorageType.File)
                .subjects(random())
                .build();

            StreamInfo si = jsm.addStream(sc);
            assertTrue(si.getConfiguration().getPersistMode() == null || si.getConfiguration().getPersistMode() == PersistMode.Default);

            sc = StreamConfiguration.builder()
                .name(random())
                .storageType(StorageType.File)
                .subjects(random())
                .persistMode(PersistMode.Default)
                .build();

            si = jsm.addStream(sc);
            assertTrue(si.getConfiguration().getPersistMode() == null || si.getConfiguration().getPersistMode() == PersistMode.Default);

            sc = StreamConfiguration.builder()
                .name(random())
                .storageType(StorageType.File)
                .subjects(random())
                .persistMode(PersistMode.Async)
                .build();

            si = jsm.addStream(sc);
            assertSame(PersistMode.Async, si.getConfiguration().getPersistMode());
        });
    }
}
