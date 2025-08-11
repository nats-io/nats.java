// Copyright 2022 The NATS Authors
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

import io.nats.client.impl.Headers;
import io.nats.client.impl.JetStreamTestBase;
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class ObjectStoreApiTests extends JetStreamTestBase {

    @Test
    public void testConfigurationConstruction() {
        Placement p = Placement.builder().cluster("cluster").tags("a", "b").build();

        // builder
        ObjectStoreConfiguration bc = ObjectStoreConfiguration.builder("bucketName")
            .description("bucketDesc")
            .maxBucketSize(555)
            .ttl(Duration.ofMillis(777))
            .storageType(StorageType.Memory)
            .replicas(2)
            .placement(p)
            .compression(true)
            .build();
        validate(bc);

        validate(ObjectStoreConfiguration.builder(bc).build());

        JsonValue jvSc = JsonParser.parseUnchecked(bc.getBackingConfig().toJson());
        validate(new ObjectStoreConfiguration(StreamConfiguration.instance(jvSc)));
    }

    private void validate(ObjectStoreConfiguration osc) {
        assertEquals("bucketName", osc.getBucketName());
        assertEquals("bucketDesc", osc.getDescription());
        assertEquals(555, osc.getMaxBucketSize());
        assertEquals(Duration.ofMillis(777), osc.getTtl());
        assertEquals(StorageType.Memory, osc.getStorageType());
        assertEquals(2, osc.getReplicas());
        assertNotNull(osc.getPlacement());
        assertEquals("cluster", osc.getPlacement().getCluster());
        assertNotNull(osc.getPlacement().getTags());
        assertEquals(2, osc.getPlacement().getTags().size());
        assertTrue(osc.isCompressed());

        assertTrue(osc.toString().contains("bucketName"));
    }

    @Test
    public void testObjectInfoConstruction() {
        String json = dataAsString("ObjectInfo.json");
        ZonedDateTime now = ZonedDateTime.now();
        ObjectInfo oi = new ObjectInfo(json.getBytes(), now);
        validateObjectInfo(oi, DateTimeUtils.toGmt(now));
        now = ZonedDateTime.now();
        oi = new ObjectInfo(oi.toJson().getBytes(), now);
        validateObjectInfo(oi, DateTimeUtils.toGmt(now));
    }

    private void validateObjectInfo(ObjectInfo oi, ZonedDateTime modified) {
        assertEquals(BUCKET, oi.getBucket());
        assertEquals("object-name", oi.getObjectName());
        assertEquals("object-desc", oi.getDescription());
        assertEquals(344000, oi.getSize());
        assertEquals(42, oi.getChunks());
        assertEquals("nuidnuidnuid", oi.getNuid());
        assertEquals("SHA-256=abcdefghijklmnopqrstuvwxyz=", oi.getDigest());
        assertTrue(oi.isDeleted());
        assertEquals(modified, oi.getModified());
        assertNotNull(oi.getObjectMeta().getObjectMetaOptions());
        assertEquals(8196, oi.getObjectMeta().getObjectMetaOptions().getChunkSize());
        assertNotNull(oi.getHeaders());
        assertEquals(2, oi.getHeaders().size());
        List<String> list = oi.getHeaders().get(key(1));
        assertEquals(1, list.size());
        assertEquals(data(1), oi.getHeaders().getFirst(key(1)));
        list = oi.getHeaders().get(key(2));
        assertEquals(2, list.size());
        assertTrue(list.contains(data(21)));
        assertTrue(list.contains(data(22)));
    }

    @Test
    public void testObjectInfoCoverage() {
        ObjectLink link1a = ObjectLink.object(BUCKET, "name");
        ObjectLink link1b = ObjectLink.object(BUCKET, "name");
        ObjectLink link2 = ObjectLink.object(BUCKET, "name2");
        ObjectLink blink1a = ObjectLink.bucket(BUCKET);
        ObjectLink blink1b = ObjectLink.bucket(BUCKET);
        ObjectLink blink2 = ObjectLink.bucket("bucket2");

        ObjectMetaOptions metaOptions = ObjectMetaOptions.builder().link(link1a).chunkSize(1024).build();
        ObjectMetaOptions metaOptions2 = ObjectMetaOptions.builder().link(link1a).chunkSize(2048).build();
        ObjectMetaOptions metaOptionsL = ObjectMetaOptions.builder().link(link1a).build();
        ObjectMetaOptions metaOptionsL2 = ObjectMetaOptions.builder().link(link2).build();
        ObjectMetaOptions metaOptionsC = ObjectMetaOptions.builder().chunkSize(1024).build();

        linkCoverage(link1a, link1b, link2, blink1a, blink1b, blink2);
        metaOptionsCoverage(metaOptions, metaOptions2, metaOptionsL, metaOptionsL2, metaOptionsC);
        metaCoverage(link1a, link2);
        infoCoverage(link1a, link2);
    }

    @SuppressWarnings({"SimplifiableAssertion", "ConstantConditions"})
    private void linkCoverage(ObjectLink link1a, ObjectLink link1b, ObjectLink link2, ObjectLink blink1a, ObjectLink blink1b, ObjectLink blink2) {
        assertTrue(link1a.isObjectLink());
        assertFalse(link1a.isBucketLink());
        assertFalse(blink1a.isObjectLink());
        assertTrue(blink1a.isBucketLink());

        //noinspection EqualsWithItself
        assertEquals(link1a, link1a);
        assertEquals(link1a, link1b);
        assertEquals(link1a, ObjectLink.object(link1a.getBucket(), link1a.getObjectName()));
        //noinspection EqualsWithItself
        assertEquals(blink1a, blink1a);
        assertEquals(blink1a, blink1b);
        assertFalse(link1a.equals(new Object()));
        assertFalse(link1a.equals(null));
        assertFalse(blink1a.equals(new Object()));
        assertFalse(blink1a.equals(null));
        assertNotEquals(link1a, link2);
        assertNotEquals(link1a, blink1a);
        assertNotEquals(blink1a, link1a);
        assertNotEquals(blink1a, link2);
        assertNotEquals(blink1a, blink2);

        assertTrue(link1a.toString().contains(link1a.getBucket()));

        assertTrue(link1a.hashCode() != 0);
        assertTrue(link2.hashCode() != 0);
        assertTrue(blink1a.hashCode() != 0);
        assertTrue(blink2.hashCode() != 0);
    }

    @SuppressWarnings({"SimplifiableAssertion", "ConstantConditions"})
    private void metaOptionsCoverage(ObjectMetaOptions metaOptions, ObjectMetaOptions metaOptions2, ObjectMetaOptions metaOptionsL, ObjectMetaOptions metaOptionsL2, ObjectMetaOptions metaOptionsC) {
        assertTrue(metaOptions.hasData());
        assertTrue(metaOptionsL.hasData());
        assertTrue(metaOptionsC.hasData());
        assertFalse(ObjectMetaOptions.builder().build().hasData());

        //noinspection EqualsWithItself
        assertEquals(metaOptions, metaOptions);
        assertFalse(metaOptions.equals(new Object()));
        assertFalse(metaOptions.equals(null));
        assertNotEquals(metaOptions, metaOptions2);
        assertNotEquals(metaOptions, metaOptionsL);
        assertNotEquals(metaOptions, metaOptionsC);
        assertNotEquals(metaOptionsC, metaOptionsL);
        assertNotEquals(metaOptionsL, metaOptionsC);
        assertNotEquals(metaOptionsL, metaOptionsL2);
        assertNotEquals(metaOptionsL2, metaOptionsL);

        assertTrue(metaOptions.toString().contains("1024"));

        assertTrue(metaOptions.hashCode() != 0); // coverage
        assertTrue(metaOptions2.hashCode() != 0); // coverage
        assertTrue(metaOptionsL.hashCode() != 0); // coverage
        assertTrue(metaOptionsC.hashCode() != 0); // coverage
    }

    @SuppressWarnings({"SimplifiableAssertion", "ConstantConditions"})
    private void metaCoverage(ObjectLink link, ObjectLink link2) {
        List<ObjectMeta> list = new ArrayList<>();
        ObjectMeta meta1a = ObjectMeta.objectName("meta"); list.add(meta1a);
        ObjectMeta meta1b = ObjectMeta.objectName("meta"); list.add(meta1b);
        ObjectMeta meta1c = ObjectMeta.objectName("diff"); list.add(meta1c);
        ObjectMeta meta2a = ObjectMeta.builder("meta").description("desc").build(); list.add(meta2a);
        ObjectMeta meta2b = ObjectMeta.builder("meta").description("desc").build(); list.add(meta2b);
        ObjectMeta meta2c = ObjectMeta.builder("meta").description("diff").build(); list.add(meta2c);
        ObjectMeta meta3a = ObjectMeta.builder("meta").headers(new Headers().put("key", "data")).build(); list.add(meta3a);
        ObjectMeta meta3b = ObjectMeta.builder("meta").headers(new Headers().put("key", "data")).build(); list.add(meta3b);
        ObjectMeta meta3c = ObjectMeta.builder("meta").headers(new Headers().put("key", "diff")).build(); list.add(meta3c);
        ObjectMeta meta4a = ObjectMeta.builder("meta").link(link).build(); list.add(meta4a);
        ObjectMeta meta4b = ObjectMeta.builder("meta").link(link).build(); list.add(meta4b);
        ObjectMeta meta4c = ObjectMeta.builder("meta").link(link2).build(); list.add(meta4c);

        ObjectMeta metaH = ObjectMeta.builder("meta").headers(new Headers().put("key", "data")).headers(null).build();
        assertEquals(0, metaH.getHeaders().size());

        //noinspection EqualsWithItself
        assertEquals(meta1a, meta1a);
        assertEquals(meta1a, meta1b);
        assertNotEquals(meta1a, meta1c);
        assertFalse(meta1a.equals(null));
        assertFalse(meta1a.equals(new Object()));
        assertNotEquals(meta1a, meta2a);
        assertNotEquals(meta1a, meta3a);
        assertNotEquals(meta1a, meta4a);

        //noinspection EqualsWithItself
        assertEquals(meta2a, meta2a);
        assertEquals(meta2a, meta2b);
        assertNotEquals(meta2a, meta2c);
        assertNotEquals(meta2a, meta1a);

        //noinspection EqualsWithItself
        assertEquals(meta3a, meta3a);
        assertEquals(meta3a, meta3b);
        assertNotEquals(meta3a, meta3c);
        assertNotEquals(meta3a, meta1a);

        //noinspection EqualsWithItself
        assertEquals(meta4a, meta4a);
        assertEquals(meta4a, meta4b);
        assertNotEquals(meta4a, meta4c);
        assertNotEquals(meta4a, meta1a);

        for (ObjectMeta meta : list) {
            assertNotNull(meta.toJson()); // coverage
            assertNotNull(meta.toString()); // coverage
            assertTrue(meta.hashCode() != 0); // coverage
        }
    }

    @SuppressWarnings({"SimplifiableAssertion", "ConstantConditions"})
    private void infoCoverage(ObjectLink link1, ObjectLink link2) {
        List<ObjectInfo> list = new ArrayList<>();

        ObjectInfo info1a = ObjectInfo.builder("buck", "name").build(); list.add(info1a);
        ObjectInfo info1b = ObjectInfo.builder("buck", "name").build(); list.add(info1b);
        ObjectInfo info1c = ObjectInfo.builder("buck2", "name").build(); list.add(info1c);
        ObjectInfo info1d = ObjectInfo.builder("buck", "name2").build(); list.add(info1d);

        ObjectInfo info2a = ObjectInfo.builder("buck", "name").size(1).build(); list.add(info2a);
        ObjectInfo info2b = ObjectInfo.builder("buck", "name").size(1).build(); list.add(info2b);
        ObjectInfo info2c = ObjectInfo.builder("buck", "name").size(2).build(); list.add(info2c);

        ObjectInfo info3a = ObjectInfo.builder("buck", "name").chunks(1).build(); list.add(info3a);
        ObjectInfo info3b = ObjectInfo.builder("buck", "name").chunks(1).build(); list.add(info3b);
        ObjectInfo info3c = ObjectInfo.builder("buck", "name").chunks(2).build(); list.add(info3c);

        ObjectInfo info4a = ObjectInfo.builder("buck", "name").deleted(true).build(); list.add(info4a);
        ObjectInfo info4b = ObjectInfo.builder("buck", "name").deleted(true).build(); list.add(info4b);
        ObjectInfo info4c = ObjectInfo.builder("buck", "name").deleted(false).build(); list.add(info4c);

        ObjectInfo info5a = ObjectInfo.builder("buck", "name").nuid("1").build(); list.add(info5a);
        ObjectInfo info5b = ObjectInfo.builder("buck", "name").nuid("1").build(); list.add(info5b);
        ObjectInfo info5c = ObjectInfo.builder("buck", "name").nuid("2").build(); list.add(info5c);

        ZonedDateTime mod1 = ZonedDateTime.now();
        ZonedDateTime mod2 = mod1.minusDays(1);
        ObjectInfo info6a = ObjectInfo.builder("buck", "name").modified(mod1).build(); list.add(info6a);
        ObjectInfo info6b = ObjectInfo.builder("buck", "name").modified(mod1).build(); list.add(info6b);
        ObjectInfo info6c = ObjectInfo.builder("buck", "name").modified(mod2).build(); list.add(info6c);

        ObjectInfo info7a = ObjectInfo.builder("buck", "name").digest("1").build(); list.add(info7a);
        ObjectInfo info7b = ObjectInfo.builder("buck", "name").digest("1").build(); list.add(info7b);
        ObjectInfo info7c = ObjectInfo.builder("buck", "name").digest("2").build(); list.add(info7c);

        ObjectInfo info8a = ObjectInfo.builder("buck", "name").link(link1).build(); list.add(info8a);
        ObjectInfo info8b = ObjectInfo.builder("buck", "name").link(link1).build(); list.add(info8b);
        ObjectInfo info8c = ObjectInfo.builder("buck", "name").link(link2).build(); list.add(info8c);

        ObjectInfo.builder("buck", "name").options(ObjectMetaOptions.builder().build()).build(); // coverage

        assertEquals(info1a, info1b);
        assertNotEquals(info1a, info1c);
        //noinspection EqualsWithItself
        assertEquals(info1a, info1a);
        assertFalse(info1a.equals(null));
        assertFalse(info1a.equals(new Object()));
        assertNotEquals(info1a, info2a);
        assertNotEquals(info1a, info3a);
        assertNotEquals(info1a, info4a);
        assertNotEquals(info1a, info5a);
        assertNotEquals(info1a, info6a);
        assertNotEquals(info1a, info7a);
        assertNotEquals(info1a, info8a);

        //noinspection EqualsWithItself
        assertEquals(info2a, info2a);
        assertEquals(info2a, info2b);
        assertNotEquals(info2a, info2c);
        assertNotEquals(info2a, info1a);

        //noinspection EqualsWithItself
        assertEquals(info3a, info3a);
        assertEquals(info3a, info3b);
        assertNotEquals(info3a, info3c);
        assertNotEquals(info3a, info1a);

        //noinspection EqualsWithItself
        assertEquals(info4a, info4a);
        assertEquals(info4a, info4b);
        assertNotEquals(info4a, info4c);
        assertNotEquals(info4a, info1a);

        //noinspection EqualsWithItself
        assertEquals(info5a, info5a);
        assertEquals(info5a, info5b);
        assertNotEquals(info5a, info5c);
        assertNotEquals(info5a, info1a);

        //noinspection EqualsWithItself
        assertEquals(info6a, info6a);
        assertEquals(info6a, info6b);
        assertNotEquals(info6a, info6c);
        assertNotEquals(info6a, info1a);

        //noinspection EqualsWithItself
        assertEquals(info7a, info7a);
        assertEquals(info7a, info7b);
        assertNotEquals(info7a, info7c);
        assertNotEquals(info7a, info1a);

        //noinspection EqualsWithItself
        assertEquals(info8a, info8a);
        assertEquals(info8a, info8b);
        assertNotEquals(info8a, info8c);
        assertNotEquals(info8a, info1a);

        for (ObjectInfo info : list) {
            assertNotNull(info.toJson()); // coverage
            assertNotNull(info.toString()); // coverage
            assertTrue(info.hashCode() != 0); // coverage
        }
    }

    @Test
    public void testConstructionInvalidsCoverage() {
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().build());
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().name(null));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().name(EMPTY));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().name(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().name(HAS_PRINTABLE));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().name(HAS_DOT));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().name(STAR_NOT_SEGMENT));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().name(GT_NOT_SEGMENT));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().name(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().name(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder(HAS_FWD_SLASH));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder(HAS_BACK_SLASH));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder(HAS_EQUALS));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder(HAS_TIC));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().maxBucketSize(0));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().maxBucketSize(-2));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().ttl(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().replicas(0));
        assertThrows(IllegalArgumentException.class, () -> ObjectStoreConfiguration.builder().replicas(6));
    }
}
