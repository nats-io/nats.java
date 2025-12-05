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
import static io.nats.client.utils.VersionUtils.atLeast2_10;
import static org.junit.jupiter.api.Assertions.*;

public class StreamConfigurationTests extends JetStreamTestBase {

    public static final String DEFAULT_STREAM_NAME = "sname";

    private static String STREAM_CONFIGURATION_JSON;
    private static String getStreamConfigurationJson() {
        if (STREAM_CONFIGURATION_JSON == null) {
            STREAM_CONFIGURATION_JSON = ResourceUtils.dataAsString("StreamConfiguration.json");
        }
        return STREAM_CONFIGURATION_JSON;
    }

    private StreamConfiguration getTestConfiguration() {
        String json = getStreamConfigurationJson();
        StreamConfiguration sc = StreamConfiguration.instance(JsonParser.parseUnchecked(json));
        assertNotNull(sc.toString()); // coverage
        return sc;
    }

    @Test
    public void testRoundTrip() throws Exception {
        runInSharedCustom((nc, ctx) -> {
            CompressionOption compressionOption = atLeast2_10() ? S2 : None;
            StreamConfiguration sc = StreamConfiguration.builder(getTestConfiguration())
                .name(ctx.stream)
                .mirror(null)
                .sources()
                .replicas(1)
                .templateOwner(null)
                .allowRollup(false)
                .allowDirect(false)
                .mirrorDirect(false)
                .sealed(false)
                .compressionOption(compressionOption)
                .allowMessageCounter(false)
                .persistMode(null)
                .build();
            validateTestStreamConfiguration(ctx.createOrReplaceStream(sc).getConfiguration(), true, ctx.stream);
        });
    }

    @Test
    public void testSerializationDeserialization() throws Exception {
        String originalJson = getStreamConfigurationJson();
        StreamConfiguration sc = StreamConfiguration.instance(originalJson);
        validateTestStreamConfiguration(sc, false, DEFAULT_STREAM_NAME);
        String serializedJson = sc.toJson();
        validateTestStreamConfiguration(StreamConfiguration.instance(serializedJson), false, DEFAULT_STREAM_NAME);
    }

    @Test
    public void testSerializationDeserializationDefaults() throws Exception {
        StreamConfiguration sc = StreamConfiguration.instance("{\"name\":\"name\"}");
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
                add(SUBJECT_DELETE_MARKER_TTL);
            }
        };

        String originalJson = getStreamConfigurationJson();

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
        String originalJson = getStreamConfigurationJson();
        JsonValue originalParsedJson = JsonParser.parse(originalJson);
        originalParsedJson.map.put("name", new JsonValue("Invalid*Name"));
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.instance(originalParsedJson.toJson()));
    }

    @Test
    public void testConstruction() {
        StreamConfiguration testSc = getTestConfiguration();
        // from json
        validateTestStreamConfiguration(testSc, false, DEFAULT_STREAM_NAME);

        // test toJson
        validateTestStreamConfiguration(StreamConfiguration.instance(JsonParser.parseUnchecked(testSc.toJson())), false, DEFAULT_STREAM_NAME);

        // copy constructor
        validateTestStreamConfiguration(StreamConfiguration.builder(testSc).build(), false, DEFAULT_STREAM_NAME);

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
            .subjectDeleteMarkerTtl(testSc.getSubjectDeleteMarkerTtl())
            .allowMessageTtl(testSc.getAllowMessageTtl())
            .allowMessageSchedules(testSc.getAllowMsgSchedules())
            .allowMessageCounter(testSc.getAllowMessageCounter())
            .allowAtomicPublish(testSc.getAllowAtomicPublish())
            .persistMode(testSc.getPersistMode())
            ;
        validateTestStreamConfiguration(builder.build(), false, DEFAULT_STREAM_NAME);
        validateTestStreamConfiguration(builder.addSources((Source)null).build(), false, DEFAULT_STREAM_NAME);


        // COVERAGE of builder methods since I know these to be true
        builder
            // clear the flags
            .allowMessageTtl(false)
            .allowMessageSchedules(false)
            .allowMessageCounter(false)
            .allowAtomicPublish(false)
            // set the flags
            .allowMessageTtl()
            .allowMessageSchedules()
            .allowMessageCounter()
            .allowAtomicPublish()
        ;

        // COVERAGE
        builder.subjectDeleteMarkerTtl(-1);
        assertNull(builder.build().getSubjectDeleteMarkerTtl());
        builder.subjectDeleteMarkerTtl(1000);
        Duration d = builder.build().getSubjectDeleteMarkerTtl();
        assertNotNull(d);
        assertEquals(1000, d.toMillis());
        builder.subjectDeleteMarkerTtl(testSc.getSubjectDeleteMarkerTtl()); // set it back for the rest of the test

        validateTestStreamConfiguration(builder.build(), false, DEFAULT_STREAM_NAME);
        validateTestStreamConfiguration(builder.addSources((Source)null).build(), false, DEFAULT_STREAM_NAME);

        assertNotNull(testSc.getSources());
        List<Source> sources = new ArrayList<>(testSc.getSources());
        sources.add(null);
        Source copy = new Source(JsonParser.parseUnchecked(sources.get(0).toJson()));
        assertEquals(sources.get(0).toString(), copy.toString());
        sources.add(copy);
        validateTestStreamConfiguration(builder.addSources(sources).build(), false, DEFAULT_STREAM_NAME);

        // covering add a single source
        sources = new ArrayList<>(testSc.getSources());
        builder.sources(new ArrayList<>()); // clears the sources
        builder.addSource(null); // COVERAGE
        for (Source source : sources) {
            builder.addSource(source);
        }
        builder.addSource(sources.get(0));
        validateTestStreamConfiguration(builder.build(), false, DEFAULT_STREAM_NAME);

        // equals and hashcode coverage
        External externalFromCopy = copy.getExternal();
        assertNotNull(externalFromCopy);

        assertEquals(sources.get(0), copy);
        assertEquals(sources.get(0).hashCode(), copy.hashCode());
        External externalSource = sources.get(0).getExternal();
        assertNotNull(externalSource);
        assertEquals(externalSource, externalFromCopy);
        assertEquals(externalSource.hashCode(), externalFromCopy.hashCode());

        List<String> lines = ResourceUtils.dataAsLines("MirrorsSources.json");
        for (String l1 : lines) {
            if (l1.startsWith("{")) {
                Mirror m1 = new Mirror(JsonParser.parseUnchecked(l1));
                //noinspection EqualsWithItself
                assertEquals(m1, m1);
                assertEquals(m1, Mirror.builder(m1).build());
                Source s1 = new Source(JsonParser.parseUnchecked(l1));
                //noinspection EqualsWithItself
                assertEquals(s1, s1);
                assertEquals(s1, Source.builder(s1).build());
                //this provides testing coverage
                //noinspection ConstantConditions,SimplifiableAssertion
                assertTrue(!m1.equals(null));
                //noinspection MisorderedAssertEqualsArguments
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
            //noinspection EqualsWithItself
            assertEquals(e1, e1);
            //noinspection MisorderedAssertEqualsArguments
            assertNotEquals(e1, null);
            //noinspection MisorderedAssertEqualsArguments
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
            .name(random())
            .maxAge(1111)
            .duplicateWindow(2222)
            .build();

        assertNotNull(scCov.getName());
        assertEquals(Duration.ofMillis(1111), scCov.getMaxAge());
        assertEquals(Duration.ofMillis(2222), scCov.getDuplicateWindow());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testConstructionInvalidsCoverage() {
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().name(null).build());
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
        assertThrows(IllegalArgumentException.class, () -> StreamConfiguration.builder().subjectDeleteMarkerTtl(1));
    }

    @Test
    public void testSourceBase() {
        StreamConfiguration sc = getTestConfiguration();
        Mirror m = sc.getMirror();
        assertNotNull(m);
        JsonValue v = JsonParser.parseUnchecked(m.toJson());
        Source s1 = new Source(v);
        Source s2 = new Source(v);
        assertEquals(s1, s2);
        //noinspection MisorderedAssertEqualsArguments
        assertNotEquals(s1, null);
        //noinspection MisorderedAssertEqualsArguments
        assertNotEquals(s1, new Object());
        Mirror m1 = new Mirror(v);
        Mirror m2 = new Mirror(v);
        assertEquals(m1, m2);
        //noinspection MisorderedAssertEqualsArguments
        assertNotEquals(m1, null);
        //noinspection MisorderedAssertEqualsArguments
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

        sb.subjectTransforms((SubjectTransform[]) null);
        mb.subjectTransforms((SubjectTransform[]) null);
        assertEquals(sb.subjectTransforms, mb.subjectTransforms);

        List<SubjectTransform> stList = null;
        sb.subjectTransforms(stList);
        mb.subjectTransforms(stList);
        assertEquals(sb.subjectTransforms, mb.subjectTransforms);

        sb.subjectTransforms(stList);
        mb.subjectTransforms(stList);
        assertEquals(sb.subjectTransforms, mb.subjectTransforms);

        stList = new ArrayList<>();
        sb.subjectTransforms(stList);
        mb.subjectTransforms(stList);
        assertEquals(sb.subjectTransforms, mb.subjectTransforms);

        stList.add(null);
        sb.subjectTransforms(stList);
        mb.subjectTransforms(stList);
        assertEquals(sb.subjectTransforms, mb.subjectTransforms);

        sb.subjectTransforms(m.getSubjectTransforms());
        mb.subjectTransforms(m.getSubjectTransforms());

        assertEquals(s1, sb.build());
        assertEquals(m1, mb.build());
        assertEquals(s1.hashCode(), sb.build().hashCode());
        assertEquals(m1.hashCode(), mb.build().hashCode());
        assertEquals(sb.subjectTransforms, m1.getSubjectTransforms());

        // coverage
        List<SubjectTransform> st = m.getSubjectTransforms();
        assertNotNull(st);
        String s = st.get(0).toString();
        assertTrue(s != null && !s.isEmpty());
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
        StreamConfiguration.Builder builder = StreamConfiguration.builder().name(random());

        String subject = random();
        // subjects(...) replaces
        builder.subjects(subject);
        assertSubjects(builder.build(), subject);

        // subjects(...) replaces
        builder.subjects();
        assertSubjects(builder.build());

        // subjects(...) replaces
        subject = random();
        builder.subjects(subject);
        assertSubjects(builder.build(), subject);

        // subjects(...) replaces
        builder.subjects((String)null);
        assertSubjects(builder.build());

        // subjects(...) replaces
        String subjectA = random();
        String subjectB = random();
        builder.subjects(subjectA, subjectB);
        assertSubjects(builder.build(), subjectA, subjectB);

        // subjects(...) replaces
        subjectA = random();
        subjectB = random();
        builder.subjects(subjectA, null, subjectB);
        assertSubjects(builder.build(), subjectA, subjectB);

        // subjects(...) replaces
        subjectA = random();
        subjectB = random();
        builder.subjects(Arrays.asList(subjectA, subjectB));
        assertSubjects(builder.build(), subjectA, subjectB);

        // addSubjects(...) adds unique
        String subjectC = random();
        builder.addSubjects(subjectB, subjectC);
        assertSubjects(builder.build(), subjectA, subjectB, subjectC);

        // addSubjects(...) adds unique
        String subjectD = random();
        String subjectE = random();
        builder.addSubjects(Arrays.asList(subjectC, subjectD, subjectE));
        assertSubjects(builder.build(), subjectA, subjectB, subjectC, subjectD, subjectE);

        // addSubjects(...) null check
        builder.addSubjects((String[]) null);
        assertSubjects(builder.build(), subjectA, subjectB, subjectC, subjectD, subjectE);

        // addSubjects(...) null check
        builder.addSubjects((Collection<String>) null);
        assertSubjects(builder.build(), subjectA, subjectB, subjectC, subjectD, subjectE);
    }

    private void assertSubjects(StreamConfiguration sc, String... subjects) {
        assertEquals(subjects.length, sc.getSubjects().size());
        for (String s : subjects) {
            assertTrue(sc.getSubjects().contains(s));
        }
    }

    @Test
    public void testRetentionPolicy() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder().name(random());
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
        StreamConfiguration.Builder builder = StreamConfiguration.builder().name(random());
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
        StreamConfiguration.Builder builder = StreamConfiguration.builder().name(random());
        assertEquals(StorageType.File, builder.build().getStorageType());

        builder.storageType(StorageType.Memory);
        assertEquals(StorageType.Memory, builder.build().getStorageType());

        builder.storageType(null);
        assertEquals(StorageType.File, builder.build().getStorageType());
    }

    @Test
    public void testDiscardPolicy() {
        StreamConfiguration.Builder builder = StreamConfiguration.builder().name(random());
        assertEquals(DiscardPolicy.Old, builder.build().getDiscardPolicy());

        builder.discardPolicy(DiscardPolicy.New);
        assertEquals(DiscardPolicy.New, builder.build().getDiscardPolicy());

        builder.discardPolicy(null);
        assertEquals(DiscardPolicy.Old, builder.build().getDiscardPolicy());
    }

    private void validateTestStreamConfiguration(StreamConfiguration sc, boolean serverTest, String name) {
        assertEquals(name, sc.getName());
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
        //noinspection deprecation
        assertEquals(734, sc.getMaxMsgSize()); // COVERAGE for deprecated
        assertEquals(734, sc.getMaximumMessageSize());
        assertEquals(StorageType.Memory, sc.getStorageType());
        assertSame(DiscardPolicy.New, sc.getDiscardPolicy());

        assertTrue(sc.getAllowMessageTtl());
        //noinspection deprecation
        assertTrue(sc.isAllowMessageTtl()); // COVERAGE

        assertEquals(Duration.ofNanos(73000000000L), sc.getSubjectDeleteMarkerTtl());

        assertNotNull(sc.getPlacement());
        assertEquals("clstr", sc.getPlacement().getCluster());
        assertNotNull(sc.getPlacement().getTags());
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

            assertNotNull(sc.getSources());
            assertEquals(2, sc.getSources().size());
            validateSource(sc.getSources().get(0), 0, zdt);
            validateSource(sc.getSources().get(1), 1, zdt);

            assertNotNull(sc.getMetadata());
            assertEquals(1, sc.getMetadata().size());
            assertEquals(META_VALUE, sc.getMetadata().get(META_KEY));
            assertEquals(82942, sc.getFirstSequence());

            assertSame(S2, sc.getCompressionOption());

            validateSubjectTransforms(Collections.singletonList(sc.getSubjectTransform()), 1, "sc");

            assertNotNull(sc.getConsumerLimits());
            assertEquals(Duration.ofSeconds(50), sc.getConsumerLimits().getInactiveThreshold());
            assertEquals(42, sc.getConsumerLimits().getMaxAckPending());

            assertTrue(sc.getAllowMsgSchedules());
            assertTrue(sc.getAllowMessageCounter());
            assertTrue(sc.getAllowAtomicPublish());
            assertNotNull(sc.getPersistMode());
            assertSame(PersistMode.Async, sc.getPersistMode());
            assertSame(PersistMode.Async.getMode(), sc.getPersistMode().getMode());
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
        assertFalse(Placement.builder().tags(new ArrayList<>()).build().hasData());
        assertFalse(Placement.builder().tags().build().hasData());
        assertFalse(Placement.builder().tags((String[])null).build().hasData());
        String[] a = new String[0];
        assertFalse(Placement.builder().tags(a).build().hasData());
        a = new String[2];
        a[0] = "";
        assertFalse(Placement.builder().tags(a).build().hasData());

        Placement p = Placement.builder().cluster("cluster").build();
        assertEquals("cluster", p.getCluster());
        assertNull(p.getTags());
        assertTrue(p.hasData());

        p = Placement.builder().cluster("cluster").build();
        assertEquals("cluster", p.getCluster());
        assertNull(p.getTags());
        assertTrue(p.hasData());

        p = Placement.builder().tags("a", "b").build();
        assertNull(p.getCluster());
        assertNotNull(p.getTags());
        assertEquals(2, p.getTags().size());
        assertTrue(p.hasData());

        p = Placement.builder().tags("a", "b").build();
        assertNull(p.getCluster());
        assertNotNull(p.getTags());
        assertEquals(2, p.getTags().size());
        assertTrue(p.hasData());

        p = Placement.builder().cluster("cluster").tags(Arrays.asList("a", "b")).build();
        assertEquals("cluster", p.getCluster());
        assertNotNull(p.getTags());
        assertEquals(2, p.getTags().size());
        assertTrue(p.hasData());

        String s = p.toString();
        assertTrue(s != null && !s.isEmpty()); // COVERAGE

        // COVERAGE
        p = new Placement("cluster", null);
        assertEquals("cluster", p.getCluster());
        assertNull(p.getTags());
        assertTrue(p.hasData());

        p = new Placement("cluster", new ArrayList<>());
        assertEquals("cluster", p.getCluster());
        assertNull(p.getTags());
        assertTrue(p.hasData());

        p = new Placement("cluster", Arrays.asList("a", "b"));
        assertEquals("cluster", p.getCluster());
        assertNotNull(p.getTags());
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

        assertThrows(IllegalArgumentException.class, () -> SubjectTransform.builder().build());
        assertThrows(IllegalArgumentException.class, () -> SubjectTransform.builder().source("source").build());
        assertThrows(IllegalArgumentException.class, () -> SubjectTransform.builder().destination("dest").build());

        EqualsVerifier.simple().forClass(SubjectTransform.class).verify();
    }

    @Test
    public void testConsumerLimits() {
        ConsumerLimits cl = ConsumerLimits.builder().build();
        assertNull(cl.getInactiveThreshold());
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

        cl = ConsumerLimits.builder().maxAckPending((Long)null).build();
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
