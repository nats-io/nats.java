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

import io.nats.client.JetStreamManagement;
import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonParseException;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import io.nats.client.utils.ResourceUtils;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;

import static io.nats.client.api.CompressionOption.None;
import static io.nats.client.api.CompressionOption.S2;
import static io.nats.client.api.ConsumerConfiguration.*;
import static io.nats.client.support.ApiConstants.*;
import static org.junit.jupiter.api.Assertions.*;

public class StreamConfigurationTests extends JetStreamTestBase {

    private StreamConfiguration getTestConfiguration() {
        String json = ResourceUtils.dataAsString("StreamConfiguration.json");
        StreamConfiguration sc = StreamConfiguration.instance(JsonParser.parseUnchecked(json));
        assertNotNull(sc.toString()); // coverage
        return sc;
    }

    @Test
    public void testRoundTrip() throws Exception {
        runInJsServer(si -> si.isNewerVersionThan("2.8.4"), nc -> {
            CompressionOption compressionOption = atLeast2_10(ensureRunServerInfo()) ? S2 : None;
            StreamConfiguration sc = StreamConfiguration.builder(getTestConfiguration())
                .mirror(null)
                .sources()
                .replicas(1)
                .templateOwner(null)
                .allowRollup(false)
                .allowDirect(false)
                .mirrorDirect(false)
                .sealed(false)
                .compressionOption(compressionOption)
                .build();
            JetStreamManagement jsm = nc.jetStreamManagement();
            validate(jsm.addStream(sc).getConfiguration(), true);
        });
    }

    @Test
    public void testSerializationDeserialization() throws Exception {
        String originalJson = ResourceUtils.dataAsString("StreamConfiguration.json");
        StreamConfiguration sc = StreamConfiguration.instance(originalJson);
        validate(sc, false);
        String serializedJson = sc.toJson();
        validate(StreamConfiguration.instance(serializedJson), false);
    }

    @Test
    public void testSerializationDeserializationDefaults() throws Exception {
        StreamConfiguration sc = StreamConfiguration.instance("");
        assertNotNull(sc);
        String serializedJson = sc.toJson();
        assertNotNull(StreamConfiguration.instance(serializedJson));
    }

    @Test
    public void testMissingJsonFields() throws Exception{
        List<String> streamConfigFields = new ArrayList<String>(){
            {
                add(RETENTION);
                add(COMPRESSION);
                add(STORAGE);
                add(DISCARD);
                add(NAME);
                add(DESCRIPTION);
                add(MAX_CONSUMERS);
                add(MAX_MSGS);
                add(MAX_MSGS_PER_SUB);
                add(MAX_BYTES);
                add(MAX_AGE);
                add(MAX_MSG_SIZE);
                add(NUM_REPLICAS);
                add(NO_ACK);
                add(TEMPLATE_OWNER);
                add(DUPLICATE_WINDOW);
                add(SUBJECTS);
                add(PLACEMENT);
                add(REPUBLISH);
                add(SUBJECT_TRANSFORM);
                add(CONSUMER_LIMITS);
                add(MIRROR);
                add(SOURCES);
                add(SEALED);
                add(ALLOW_ROLLUP_HDRS);
                add(ALLOW_DIRECT);
                add(MIRROR_DIRECT);
                add(DENY_DELETE);
                add(DENY_PURGE);
                add(DISCARD_NEW_PER_SUBJECT);
                add(METADATA);
                add(FIRST_SEQ);
                add(ALLOW_MSG_TTL);
                add(SUBJECT_DELETE_MARKER_TTL);
            }
        };

        String originalJson = ResourceUtils.dataAsString("StreamConfiguration.json");

        // Loops through each field in the StreamConfiguration JSON format and ensures that the
        // StreamConfiguration can be built without that field being present in the JSON
        for (String streamConfigFieldName: streamConfigFields) {
            JsonValue originalParsedJson = JsonParser.parse(originalJson);
            originalParsedJson.map.remove(streamConfigFieldName);
            StreamConfiguration sc = StreamConfiguration.instance(originalParsedJson.toJson());
            assertNotNull(sc);
        }
    }

    @Test
    public void testInvalidNameInJson() throws Exception{
        String originalJson = ResourceUtils.dataAsString("StreamConfiguration.json");
        JsonValue originalParsedJson = JsonParser.parse(originalJson);
        originalParsedJson.map.put(NAME, new JsonValue("Inavlid*Name"));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.instance(originalParsedJson.toJson()));
    }

    @Test
    public void testConstruction() {
        StreamConfiguration testSc = getTestConfiguration();
        // from json
        validate(testSc, false);

        // test toJson
        validate(StreamConfiguration.instance(JsonParser.parseUnchecked(testSc.toJson())), false);

        // copy constructor
        validate(StreamConfiguration.builder(testSc).build(), false);

        Map<String, String> metaData = new HashMap<>(); metaData.put(META_KEY, META_VALUE);

        // builder
        StreamConfiguration.Builder builder = StreamConfiguration.builder()
            .name(testSc.getName())
            .description(testSc.getDescription())
            .subjects(testSc.getSubjects())
            .retentionPolicy(testSc.getRetentionPolicy())
            .compressionOption(testSc.getCompressionOption())
            .maxConsumers(testSc.getMaxConsumers())
            .maxMessages(testSc.getMaxMsgs())
            .maxMessagesPerSubject(testSc.getMaxMsgsPerSubject())
            .maxBytes(testSc.getMaxBytes())
            .maxAge(testSc.getMaxAge())
            .maximumMessageSize(testSc.getMaximumMessageSize())
            .storageType(testSc.getStorageType())
            .replicas(testSc.getReplicas())
            .noAck(testSc.getNoAck())
            .templateOwner(testSc.getTemplateOwner())
            .discardPolicy(testSc.getDiscardPolicy())
            .duplicateWindow(testSc.getDuplicateWindow())
            .placement(testSc.getPlacement())
            .republish(testSc.getRepublish())
            .subjectTransform(testSc.getSubjectTransform())
            .mirror(testSc.getMirror())
            .sources(testSc.getSources())
            .sealed(testSc.getSealed())
            .allowRollup(testSc.getAllowRollup())
            .allowDirect(testSc.getAllowDirect())
            .mirrorDirect(testSc.getMirrorDirect())
            .denyDelete(testSc.getDenyDelete())
            .denyPurge(testSc.getDenyPurge())
            .discardNewPerSubject(testSc.isDiscardNewPerSubject())
            .metadata(testSc.getMetadata())
            .firstSequence(testSc.getFirstSequence())
            .consumerLimits(testSc.getConsumerLimits())
            .allowMessageTtl(testSc.isAllowMessageTtl())
            .subjectDeleteMarkerTtl(testSc.getSubjectDeleteMarkerTtl())
            ;
        validate(builder.build(), false);
        validate(builder.addSources((Source)null).build(), false);

        List<Source> sources = new ArrayList<>(testSc.getSources());
        sources.add(null);
        Source copy = new Source(JsonParser.parseUnchecked(sources.get(0).toJson()));
        sources.add(copy);
        validate(builder.addSources(sources).build(), false);

        // covering add a single source
        sources = new ArrayList<>(testSc.getSources());
        builder.sources(new ArrayList<>()); // clears the sources
        builder.addSource(null); // coverage
        for (Source source : sources) {
            builder.addSource(source);
        }
        builder.addSource(sources.get(0));
        validate(builder.build(), false);

        // equals and hashcode coverage
        External external = copy.getExternal();

        assertEquals(sources.get(0), copy);
        assertEquals(sources.get(0).hashCode(), copy.hashCode());
        assertEquals(sources.get(0).getExternal(), external);
        assertEquals(sources.get(0).getExternal().hashCode(), external.hashCode());

        List<String> lines = ResourceUtils.dataAsLines("MirrorsSources.json");
        for (String l1 : lines) {
            if (l1.startsWith("{")) {
                Mirror m1 = new Mirror(JsonParser.parseUnchecked(l1));
                assertEquals(m1, m1);
                assertEquals(m1, Mirror.builder(m1).build());
                Source s1 = new Source(JsonParser.parseUnchecked(l1));
                assertEquals(s1, s1);
                assertEquals(s1, Source.builder(s1).build());
                //this provides testing coverage
                //noinspection ConstantConditions,SimplifiableAssertion
                assertTrue(!m1.equals(null));
                assertNotEquals(m1, new Object());
                for (String l2 : lines) {
                    if (l2.startsWith("{")) {
                        Mirror m2 = new Mirror(JsonParser.parseUnchecked(l2));
                        Source s2 = new Source(JsonParser.parseUnchecked(l2));
                        if (l1.equals(l2)) {
                            assertEquals(m1, m2);
                            assertEquals(s1, s2);
                        }
                        else {
                            assertNotEquals(m1, m2);
                            assertNotEquals(s1, s2);
                        }
                    }
                }
            }
        }

        lines = ResourceUtils.dataAsLines("ExternalJson.txt");
        for (String l1 : lines) {
            External e1 = new External(JsonParser.parseUnchecked(l1));
            assertEquals(e1, e1);
            assertNotEquals(e1, null);
            assertNotEquals(e1, new Object());
            for (String l2 : lines) {
                External e2 = new External(JsonParser.parseUnchecked(l2));
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
    public void testConstructionInvalidsCoverage() {
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().name(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxConsumers(0));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxConsumers(-2));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxMessages(0));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxMessages(-2));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxMessagesPerSubject(0));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxMessagesPerSubject(-2));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxBytes(0));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxBytes(-2));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxAge(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxAge(-1));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxMsgSize(0)); // COVERAGE for deprecated
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxMsgSize(-2)); // COVERAGE for deprecated
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maxMsgSize((long)Integer.MAX_VALUE + 1)); // COVERAGE for deprecated, TOO LARGE A NUMBER
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maximumMessageSize(0));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().maximumMessageSize(-2));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().replicas(0));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().replicas(6));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().duplicateWindow(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().duplicateWindow(-1));
    }

    @Test
    public void testSourceBase() {
        StreamConfiguration sc = getTestConfiguration();
        Mirror m = sc.getMirror();

        JsonValue v = JsonParser.parseUnchecked(m.toJson());
        Source s1 = new Source(v);
        Source s2 = new Source(v);
        assertEquals(s1, s2);
        assertNotEquals(s1, null);
        assertNotEquals(s1, new Object());
        Mirror m1 = new Mirror(v);
        Mirror m2 = new Mirror(v);
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
        assertNotEqualsEqualsHashcode(s1, m1, sb.external(new External(JsonValue.NULL)), mb.external(new External(JsonValue.NULL)));

        sb.external(m.getExternal());
        mb.external(m.getExternal());

        sb.subjectTransforms(m.getSubjectTransforms());
        mb.subjectTransforms(m.getSubjectTransforms());

        assertEquals(s1, sb.build());
        assertEquals(m1, mb.build());
        assertEquals(s1.hashCode(), sb.build().hashCode());
        assertEquals(m1.hashCode(), mb.build().hashCode());
        assertEquals(sb.subjectTransforms, m1.getSubjectTransforms());

        // coverage
        m.getSubjectTransforms().get(0).toString();
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

        builder.retentionPolicy(RetentionPolicy.Limits);
        assertEquals(RetentionPolicy.Limits, builder.build().getRetentionPolicy());

        builder.retentionPolicy(null);
        assertEquals(RetentionPolicy.Limits, builder.build().getRetentionPolicy());

        builder.retentionPolicy(RetentionPolicy.Interest);
        assertEquals(RetentionPolicy.Interest, builder.build().getRetentionPolicy());

        builder.retentionPolicy(RetentionPolicy.WorkQueue);
        assertEquals(RetentionPolicy.WorkQueue, builder.build().getRetentionPolicy());
    }

    @Test
    public void testCompressionOption() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder();
        assertEquals(None, builder.build().getCompressionOption());

        builder.compressionOption(None);
        assertEquals(None, builder.build().getCompressionOption());

        builder.compressionOption(null);
        assertEquals(None, builder.build().getCompressionOption());
        assertFalse(builder.build().toJson().contains("\"compression\""));

        builder.compressionOption(S2);
        assertEquals(S2, builder.build().getCompressionOption());
        assertTrue(builder.build().toJson().contains("\"compression\":\"s2\""));
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

    private void validate(StreamConfiguration sc, boolean serverTest) {
        assertEquals("sname", sc.getName());
        assertEquals("blah blah", sc.getDescription());
        assertEquals(4, sc.getSubjects().size());
        assertEquals("foo", sc.getSubjects().get(0));
        assertEquals("bar", sc.getSubjects().get(1));
        assertEquals("repub.>", sc.getSubjects().get(2));
        assertEquals("st.>", sc.getSubjects().get(3));

        assertSame(RetentionPolicy.Interest, sc.getRetentionPolicy());
        assertEquals(730, sc.getMaxConsumers());
        assertEquals(731, sc.getMaxMsgs());
        assertEquals(741, sc.getMaxMsgsPerSubject());
        assertEquals(732, sc.getMaxBytes());
        assertEquals(731, sc.getMaxMsgs());
        assertEquals(732, sc.getMaxBytes());
        assertEquals(Duration.ofNanos(43000000000L), sc.getMaxAge());
        assertEquals(Duration.ofNanos(42000000000L), sc.getDuplicateWindow());
        assertEquals(734, sc.getMaxMsgSize()); // COVERAGE for deprecated
        assertEquals(734, sc.getMaximumMessageSize());
        assertEquals(StorageType.Memory, sc.getStorageType());
        assertSame(DiscardPolicy.New, sc.getDiscardPolicy());

        assertTrue(sc.isAllowMessageTtl());
        assertEquals(Duration.ofNanos(73000000000L), sc.getSubjectDeleteMarkerTtl());

        assertNotNull(sc.getPlacement());
        assertEquals("clstr", sc.getPlacement().getCluster());
        assertEquals(2, sc.getPlacement().getTags().size());
        assertEquals("tag1", sc.getPlacement().getTags().get(0));
        assertEquals("tag2", sc.getPlacement().getTags().get(1));

        assertNotNull(sc.getRepublish());
        assertEquals("repub.>", sc.getRepublish().getSource());
        assertEquals("dest.>", sc.getRepublish().getDestination());
        assertTrue(sc.getRepublish().isHeadersOnly());

        ZonedDateTime zdt = DateTimeUtils.parseDateTime("2020-11-05T19:33:21.163377Z");

        if (serverTest) {
            assertEquals(1, sc.getReplicas());
        }
        else {
            assertTrue(sc.getNoAck());
            assertTrue(sc.getSealed());
            assertTrue(sc.getDenyDelete());
            assertTrue(sc.getDenyPurge());
            assertTrue(sc.isDiscardNewPerSubject());
            assertTrue(sc.getAllowRollup());
            assertTrue(sc.getAllowDirect());
            assertTrue(sc.getMirrorDirect());

            assertEquals(5, sc.getReplicas());
            assertEquals("twnr", sc.getTemplateOwner());

            Mirror mirror = sc.getMirror();
            assertNotNull(mirror);
            assertEquals("eman", mirror.getSourceName());
            assertEquals(736, mirror.getStartSeq());
            assertEquals(zdt, mirror.getStartTime());
            assertEquals("mfsub", mirror.getFilterSubject());

            assertNotNull(mirror.getExternal());
            assertEquals("apithing", mirror.getExternal().getApi());
            assertEquals("dlvrsub", mirror.getExternal().getDeliver());

            validateSubjectTransforms(mirror.getSubjectTransforms(), 2, "m");

            assertEquals(2, sc.getSources().size());
            validateSource(sc.getSources().get(0), 0, zdt);
            validateSource(sc.getSources().get(1), 1, zdt);

            assertEquals(1, sc.getMetadata().size());
            assertEquals(META_VALUE, sc.getMetadata().get(META_KEY));
            assertEquals(82942, sc.getFirstSequence());

            assertSame(S2, sc.getCompressionOption());

            validateSubjectTransforms(Collections.singletonList(sc.getSubjectTransform()), 1, "sc");

            assertNotNull(sc.getConsumerLimits());
            assertEquals(Duration.ofSeconds(50), sc.getConsumerLimits().getInactiveThreshold());
            assertEquals(42, sc.getConsumerLimits().getMaxAckPending());
        }
    }

    private void validateSource(Source source, int index, ZonedDateTime zdt) {
        long seq = 737 + index;
        String name = "s" + index;
        assertEquals(name, source.getSourceName());
        assertEquals(seq, source.getStartSeq());
        assertEquals(zdt, source.getStartTime());
        assertEquals(name + "sub", source.getFilterSubject());

        assertNotNull(source.getExternal());
        assertEquals(name + "api", source.getExternal().getApi());
        assertEquals(name + "dlvrsub", source.getExternal().getDeliver());

        validateSubjectTransforms(source.getSubjectTransforms(), 2, name);
    }

    public static void validateSubjectTransforms(List<SubjectTransform> subjectTransforms, int count, String name) {
        assertNotNull(subjectTransforms);
        assertEquals(count, subjectTransforms.size());
        for (int x = 0; x < count; x++) {
            SubjectTransform st = subjectTransforms.get(x);
            assertEquals(name + "_st_src" + x, st.getSource());
            assertEquals(name + "_st_dest" + x, st.getDestination());
        }
    }

    @Test
    public void testPlacement() {
        assertFalse(Placement.builder().build().hasData());
        assertFalse(Placement.builder().cluster(null).build().hasData());
        assertFalse(Placement.builder().cluster("").build().hasData());
        assertFalse(Placement.builder().tags((List<String>)null).build().hasData());

        Placement p = Placement.builder().cluster("cluster").build();
        assertEquals("cluster", p.getCluster());
        assertNull(p.getTags());
        assertTrue(p.hasData());

        p = Placement.builder().cluster("cluster").tags("a", "b").build();
        assertEquals("cluster", p.getCluster());
        assertEquals(2, p.getTags().size());
        assertTrue(p.hasData());

        p = Placement.builder().cluster("cluster").tags(Arrays.asList("a", "b")).build();
        assertEquals("cluster", p.getCluster());
        assertEquals(2, p.getTags().size());
        assertTrue(p.hasData());
    }

    @Test
    public void testRepublish() {
        assertThrows(IllegalArgumentException.class, () -> Republish.builder().build());
        assertThrows(IllegalArgumentException.class, () -> Republish.builder().source("src.>").build());
        assertThrows(IllegalArgumentException.class, () -> Republish.builder().destination("dest.>").build());

        Republish r = Republish.builder().source("src.>").destination("dest.>").build();
        assertEquals("src.>", r.getSource());
        assertEquals("dest.>", r.getDestination());
        assertFalse(r.isHeadersOnly());

        r = Republish.builder().source("src.>").destination("dest.>").headersOnly(true).build();
        assertEquals("src.>", r.getSource());
        assertEquals("dest.>", r.getDestination());
        assertTrue(r.isHeadersOnly());
    }

    @Test
    public void testSubjectTransform() {
        SubjectTransform st = SubjectTransform.builder().source("src.>").destination("dest.>").build();
        assertEquals("src.>", st.getSource());
        assertEquals("dest.>", st.getDestination());

        st = SubjectTransform.builder().build();
        assertNull(st.getSource());
        assertNull(st.getDestination());
    }

    @Test
    public void testConsumerLimits() {
        ConsumerLimits cl = ConsumerLimits.builder().build();
        assertEquals(null, cl.getInactiveThreshold());
        assertEquals(INTEGER_UNSET, cl.getMaxAckPending());

        cl = ConsumerLimits.builder().inactiveThreshold(Duration.ofMillis(0)).build();
        assertEquals(Duration.ZERO, cl.getInactiveThreshold());

        cl = ConsumerLimits.builder().inactiveThreshold(0L).build();
        assertEquals(Duration.ZERO, cl.getInactiveThreshold());

        cl = ConsumerLimits.builder().inactiveThreshold(Duration.ofMillis(1)).build();
        assertEquals(Duration.ofMillis(1), cl.getInactiveThreshold());

        cl = ConsumerLimits.builder().inactiveThreshold(1L).build();
        assertEquals(Duration.ofMillis(1), cl.getInactiveThreshold());

        cl = ConsumerLimits.builder().inactiveThreshold(Duration.ofMillis(-1)).build();
        assertEquals(DURATION_UNSET, cl.getInactiveThreshold());

        cl = ConsumerLimits.builder().inactiveThreshold(-1).build();
        assertEquals(DURATION_UNSET, cl.getInactiveThreshold());

        cl = ConsumerLimits.builder().maxAckPending(STANDARD_MIN).build();
        assertEquals(STANDARD_MIN, cl.getMaxAckPending());

        cl = ConsumerLimits.builder().maxAckPending(INTEGER_UNSET).build();
        assertEquals(INTEGER_UNSET, cl.getMaxAckPending());

        cl = ConsumerLimits.builder().maxAckPending(-2).build();
        assertEquals(INTEGER_UNSET, cl.getMaxAckPending());

        cl = ConsumerLimits.builder().maxAckPending(Long.MAX_VALUE).build();
        assertEquals(Integer.MAX_VALUE, cl.getMaxAckPending());
    }

    @Test
    public void testExternal() {
        External e = External.builder().api("api").deliver("deliver").build();
        assertEquals("api", e.getApi());
        assertEquals("deliver", e.getDeliver());

        e = External.builder().build();
        assertNull(e.getApi());
        assertNull(e.getDeliver());
    }

    @Test
    public void equalsContract() {
        // really testing SourceBase
        EqualsVerifier.simple().forClass(Mirror.class).verify();
        EqualsVerifier.simple().forClass(Source.class).verify();
    }

    public static StreamConfiguration getStreamConfigurationFromJson(String jsonFile) throws JsonParseException {
        return StreamConfiguration.instance(JsonParser.parse(ResourceUtils.dataAsString(jsonFile)));
    }
}
