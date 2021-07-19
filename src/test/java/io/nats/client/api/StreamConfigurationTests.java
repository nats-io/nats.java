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

package io.nats.client.api;

import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static io.nats.client.support.JsonUtils.EMPTY_JSON;
import static org.junit.jupiter.api.Assertions.*;

public class StreamConfigurationTests extends JetStreamTestBase {

    private StreamConfiguration getTestConfiguration() {
        String json = ResourceUtils.dataAsString("StreamConfiguration.json");
        return StreamConfiguration.instance(json);
    }

    @Test
    public void testConstruction() {
        StreamConfiguration testSc = getTestConfiguration();
        // from json
        validate(testSc);

        // test toJson
        validate(StreamConfiguration.instance(testSc.toJson()));

        // copy constructor
        validate(StreamConfiguration.builder(testSc).build());

        // builder
        StreamConfiguration.Builder builder = StreamConfiguration.builder()
                .name(testSc.getName())
                .subjects(testSc.getSubjects())
                .retentionPolicy(testSc.getRetentionPolicy())
                .maxConsumers(testSc.getMaxConsumers())
                .maxMessages(testSc.getMaxMsgs())
                .maxBytes(testSc.getMaxBytes())
                .maxAge(testSc.getMaxAge())
                .maxMsgSize(testSc.getMaxMsgSize())
                .storageType(testSc.getStorageType())
                .replicas(testSc.getReplicas())
                .noAck(testSc.getNoAck())
                .templateOwner(testSc.getTemplateOwner())
                .discardPolicy(testSc.getDiscardPolicy())
                .duplicateWindow(testSc.getDuplicateWindow())
                .placement(testSc.getPlacement())
                .mirror(testSc.getMirror())
                .sources(testSc.getSources());
        validate(builder.build());
        validate(builder.addSources((Source)null).build());

        List<Source> sources = new ArrayList<>(testSc.getSources());
        sources.add(null);
        Source copy = new Source(sources.get(0).toJson());
        sources.add(copy);
        validate(builder.addSources(sources).build());

        // equals and hashcode coverage
        External external = copy.getExternal();

        assertEquals(sources.get(0), copy);
        assertEquals(sources.get(0).hashCode(), copy.hashCode());
        assertEquals(sources.get(0).getExternal(), external);
        assertEquals(sources.get(0).getExternal().hashCode(), external.hashCode());

        List<String> lines = ResourceUtils.dataAsLines("SourceBaseJson.txt");
        for (String l1 : lines) {
            Mirror m1 = new Mirror(l1);
            assertEquals(m1, m1);
            assertNotEquals(m1, null);
            assertNotEquals(m1, new Object());
            for (String l2 : lines) {
                Mirror m2 = new Mirror(l2);
                if (l1.equals(l2)) {
                    assertEquals(m1, m2);
                }
                else {
                    assertNotEquals(m1, m2);
                }
            }
        }

        lines = ResourceUtils.dataAsLines("ExternalJson.txt");
        for (String l1 : lines) {
            External e1 = new External(l1);
            assertEquals(e1, e1);
            assertNotEquals(e1, null);
            assertNotEquals(e1, new Object());
            for (String l2 : lines) {
                External e2 = new External(l2);
                if (l1.equals(l2)) {
                    assertEquals(e1, e2);
                }
                else {
                    assertNotEquals(e1, e2);
                }
            }
        }

        // coverage for null StreamConfiguration, millis maxAge, millis duplicateWindow
        StreamConfiguration scCov = StreamConfiguration.builder(null)
                .maxAge(1111)
                .duplicateWindow(2222)
                .build();

        assertNull(scCov.getName());
        assertEquals(Duration.ofMillis(1111), scCov.getMaxAge());
        assertEquals(Duration.ofMillis(2222), scCov.getDuplicateWindow());
    }

    @Test
    public void testSourceBase() {
        StreamConfiguration sc = getTestConfiguration();
        Mirror m = sc.getMirror();

        String json = m.toJson();
        Source s1 = new Source(json);
        Source s2 = new Source(json);
        assertEquals(s1, s2);
        assertNotEquals(s1, null);
        assertNotEquals(s1, new Object());
        Mirror m1 = new Mirror(json);
        Mirror m2 = new Mirror(json);
        assertEquals(m1, m2);
        assertNotEquals(m1, null);
        assertNotEquals(m1, new Object());

        Source.Builder sb = Source.builder();
        Mirror.Builder mb = Mirror.builder();

        // STEPPED like this so the equals returns false on different portion of the object
        // by the end I've built an equal object
        assertNotEqualsEqualsHashcode(s1, m1, sb.startSeq(999), mb.startSeq(999));
        assertNotEqualsEqualsHashcode(s1, m1, sb.startSeq(m.getStartSeq()), mb.startSeq(m.getStartSeq()));

        assertNotEqualsEqualsHashcode(s1, m1, sb.sourceName(null), mb.sourceName(null));
        assertNotEqualsEqualsHashcode(s1, m1, sb.sourceName("not"), mb.sourceName("not"));
        assertNotEqualsEqualsHashcode(s1, m1, sb.sourceName(m.getSourceName()), mb.sourceName(m.getSourceName()));

        assertNotEqualsEqualsHashcode(s1, m1, sb.startTime(null), mb.startTime(null));
        assertNotEqualsEqualsHashcode(s1, m1, sb.startTime(ZonedDateTime.now()), mb.startTime(ZonedDateTime.now()));
        assertNotEqualsEqualsHashcode(s1, m1, sb.startTime(m.getStartTime()), mb.startTime(m.getStartTime()));

        assertNotEqualsEqualsHashcode(s1, m1, sb.filterSubject(null), mb.filterSubject(null));
        assertNotEqualsEqualsHashcode(s1, m1, sb.filterSubject("not"), mb.filterSubject("not"));
        assertNotEqualsEqualsHashcode(s1, m1, sb.filterSubject(m.getFilterSubject()), mb.filterSubject(m.getFilterSubject()));

        assertNotEqualsEqualsHashcode(s1, m1, sb.external(null), mb.external(null));
        assertNotEqualsEqualsHashcode(s1, m1, sb.external(new External(EMPTY_JSON)), mb.external(new External(EMPTY_JSON)));

        sb.external(m.getExternal());
        mb.external(m.getExternal());

        assertEquals(s1, sb.build());
        assertEquals(m1, mb.build());
        assertEquals(s1.hashCode(), sb.build().hashCode());
        assertEquals(m1.hashCode(), mb.build().hashCode());
    }

    private void assertNotEqualsEqualsHashcode(Source s, Mirror m, Source.Builder sb, Mirror.Builder mb) {
        assertNotEquals(s, sb.build());
        assertNotEquals(m, mb.build());
        assertNotEquals(s.hashCode(), sb.build().hashCode());
        assertNotEquals(m.hashCode(), mb.build().hashCode());
        assertNotEquals(sb.build(), s);
        assertNotEquals(mb.build(), m);
        assertNotEquals(sb.build().hashCode(), s.hashCode());
        assertNotEquals(mb.build().hashCode(), m.hashCode());
    }

    @Test
    public void testSubjects() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder();

        // subjects(...) replaces
        builder.subjects(subject(0));
        assertSubjects(builder.build(), 0);

        // subjects(...) replaces
        builder.subjects();
        assertSubjects(builder.build());

        // subjects(...) replaces
        builder.subjects(subject(1));
        assertSubjects(builder.build(), 1);

        // subjects(...) replaces
        builder.subjects((String)null);
        assertSubjects(builder.build());

        // subjects(...) replaces
        builder.subjects(subject(2), subject(3));
        assertSubjects(builder.build(), 2, 3);

        // subjects(...) replaces
        builder.subjects(subject(101), null, subject(102));
        assertSubjects(builder.build(), 101, 102);

        // subjects(...) replaces
        builder.subjects(Arrays.asList(subject(4), subject(5)));
        assertSubjects(builder.build(), 4, 5);

        // addSubjects(...) adds unique
        builder.addSubjects(subject(5), subject(6));
        assertSubjects(builder.build(), 4, 5, 6);

        // addSubjects(...) adds unique
        builder.addSubjects(Arrays.asList(subject(6), subject(7), subject(8)));
        assertSubjects(builder.build(), 4, 5, 6, 7, 8);

        // addSubjects(...) null check
        builder.addSubjects((String[]) null);
        assertSubjects(builder.build(), 4, 5, 6, 7, 8);

        // addSubjects(...) null check
        builder.addSubjects((Collection<String>) null);
        assertSubjects(builder.build(), 4, 5, 6, 7, 8);
    }

    private void assertSubjects(StreamConfiguration sc, int... subIds) {
        assertEquals(subIds.length, sc.getSubjects().size());
        for (int subId : subIds) {
            assertTrue(sc.getSubjects().contains(subject(subId)));
        }
    }

    @Test
    public void testRetentionPolicy() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder();
        assertEquals(RetentionPolicy.Limits, builder.build().getRetentionPolicy());

        builder.retentionPolicy(RetentionPolicy.Interest);
        assertEquals(RetentionPolicy.Interest, builder.build().getRetentionPolicy());

        builder.retentionPolicy(RetentionPolicy.WorkQueue);
        assertEquals(RetentionPolicy.WorkQueue, builder.build().getRetentionPolicy());

        builder.retentionPolicy(null);
        assertEquals(RetentionPolicy.Limits, builder.build().getRetentionPolicy());
    }

    @Test
    public void testStorageType() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder();
        assertEquals(StorageType.File, builder.build().getStorageType());

        builder.storageType(StorageType.Memory);
        assertEquals(StorageType.Memory, builder.build().getStorageType());

        builder.storageType(null);
        assertEquals(StorageType.File, builder.build().getStorageType());
    }

    @Test
    public void testDiscardPolicy() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder();
        assertEquals(DiscardPolicy.Old, builder.build().getDiscardPolicy());

        builder.discardPolicy(DiscardPolicy.New);
        assertEquals(DiscardPolicy.New, builder.build().getDiscardPolicy());

        builder.discardPolicy(null);
        assertEquals(DiscardPolicy.Old, builder.build().getDiscardPolicy());
    }

    private void validate(StreamConfiguration sc) {
        assertEquals("sname", sc.getName());
        assertEquals(2, sc.getSubjects().size());
        assertEquals("foo", sc.getSubjects().get(0));
        assertEquals("bar", sc.getSubjects().get(1));

        assertSame(RetentionPolicy.Interest, sc.getRetentionPolicy());
        assertEquals(730, sc.getMaxConsumers());
        assertEquals(731, sc.getMaxMsgs());
        assertEquals(732, sc.getMaxBytes());
        assertEquals(Duration.ofNanos(42000000000L), sc.getMaxAge());
        assertEquals(734, sc.getMaxMsgSize());
        assertEquals(StorageType.Memory, sc.getStorageType());
        assertEquals(5, sc.getReplicas());
        assertFalse(sc.getNoAck());
        assertEquals("twnr", sc.getTemplateOwner());
        assertSame(DiscardPolicy.New, sc.getDiscardPolicy());
        assertEquals(Duration.ofNanos(73000000000L), sc.getDuplicateWindow());

        assertNotNull(sc.getPlacement());
        assertEquals("clstr", sc.getPlacement().getCluster());
        assertEquals(2, sc.getPlacement().getTags().size());
        assertEquals("tag1", sc.getPlacement().getTags().get(0));
        assertEquals("tag2", sc.getPlacement().getTags().get(1));

        ZonedDateTime zdt = DateTimeUtils.parseDateTime("2020-11-05T19:33:21.163377Z");

        assertNotNull(sc.getMirror());
        assertEquals("eman", sc.getMirror().getSourceName());
        assertEquals(736, sc.getMirror().getStartSeq());
        assertEquals(zdt, sc.getMirror().getStartTime());
        assertEquals("mfsub", sc.getMirror().getFilterSubject());

        assertNotNull(sc.getMirror().getExternal());
        assertEquals("apithing", sc.getMirror().getExternal().getApi());
        assertEquals("dlvrsub", sc.getMirror().getExternal().getDeliver());

        assertEquals(2, sc.getSources().size());

        validateSource(sc.getSources().get(0), "s0", 737, "s0sub", "s0api", "s0dlvrsub", zdt);
        validateSource(sc.getSources().get(1), "s1", 738, "s1sub", "s1api", "s1dlvrsub", zdt);
    }

    private void validateSource(Source source, String name, int seq, String filter, String api, String deliver, ZonedDateTime zdt) {
        assertEquals(name, source.getSourceName());
        assertEquals(seq, source.getStartSeq());
        assertEquals(zdt, source.getStartTime());
        assertEquals(filter, source.getFilterSubject());

        assertNotNull(source.getExternal());
        assertEquals(api, source.getExternal().getApi());
        assertEquals(deliver, source.getExternal().getDeliver());
    }

    @Test
    public void miscCoverage() {
        // COVERAGE
        String json = ResourceUtils.dataAsString("StreamConfiguration.json");
        assertNotNull(StreamConfiguration.instance(json).toString());
    }
}
