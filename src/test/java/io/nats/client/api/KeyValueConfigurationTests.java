// Copyright 2021 The NATS Authors
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
import io.nats.client.support.JsonParseException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static io.nats.client.support.NatsConstants.EMPTY;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public class KeyValueConfigurationTests extends JetStreamTestBase {

    @Test
    public void testConstruction() throws JsonParseException {
        Placement p = Placement.builder().cluster("cluster").tags("a", "b").build();
        Republish r = Republish.builder().source("src").destination("dest").headersOnly(true).build();
        Map<String, String> metadata = new HashMap<>();
        metadata.put("meta-key", "meta-value");
        metadata.put("meta-key2", "meta-value2");

        // builder
        //noinspection deprecation
        KeyValueConfiguration bc = KeyValueConfiguration.builder()
            .name("bucketName")
            .description("bucketDesc")
            .maxHistoryPerKey(44)
            .maxBucketSize(555)
            .maxValueSize(66666666) // deprecated
            .maximumValueSize(666)  // shows that order matters too
            .ttl(Duration.ofMillis(777))
            .storageType(StorageType.Memory)
            .replicas(2)
            .placement(p)
            .republish(r)
            .compression(true)
            .metadata(metadata)
            .limitMarker(8888)
            .build();
        validate(bc);

        validate(KeyValueConfiguration.builder(bc).build());

        // instance(String) parses a JSON representation of the KV builder configuration:
        // the bucket (simple) name and KV-domain field names, NOT the backing stream.
        validate(KeyValueConfiguration.instance(dataAsString("KeyValueConfig.json")));
        assertThrows(JsonParseException.class, () -> KeyValueConfiguration.instance("not json"));
        // valid json with no name -> clean validation error
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.instance("{}"));

        // instanceViaStreamConfig(String) builds from the full backing stream configuration JSON
        validate(KeyValueConfiguration.instanceViaStreamConfig(bc.getBackingConfig().toJson()));

        bc = KeyValueConfiguration.builder()
                .name("bucketName")
                .build();

        assertEquals(1, bc.getMaxHistoryPerKey());
    }

    private void validate(KeyValueConfiguration kvc) {
        assertEquals("bucketName", kvc.getBucketName());
        assertEquals("bucketDesc", kvc.getDescription());
        assertEquals(44, kvc.getMaxHistoryPerKey());
        assertEquals(555, kvc.getMaxBucketSize());
        //noinspection deprecation
        assertEquals(666, kvc.getMaxValueSize());
        assertEquals(666, kvc.getMaximumValueSize());
        assertEquals(Duration.ofMillis(777), kvc.getTtl());
        assertEquals(StorageType.Memory, kvc.getStorageType());
        assertEquals(2, kvc.getReplicas());
        assertNotNull(kvc.getPlacement());
        assertEquals("cluster", kvc.getPlacement().getCluster());
        assertNotNull(kvc.getPlacement().getTags());
        assertEquals(2, kvc.getPlacement().getTags().size());
        assertNotNull(kvc.getRepublish());
        assertEquals("src", kvc.getRepublish().getSource());
        assertEquals("dest", kvc.getRepublish().getDestination());
        assertTrue(kvc.getRepublish().isHeadersOnly());
        assertTrue(kvc.isCompressed());
        assertNotNull(kvc.getLimitMarkerTtl());
        assertEquals(8888, kvc.getLimitMarkerTtl().toMillis());
        assertNotNull(kvc.getMetadata());
        assertEquals(2, kvc.getMetadata().size());
        assertEquals("meta-value", kvc.getMetadata().get("meta-key"));
        assertEquals("meta-value2", kvc.getMetadata().get("meta-key2"));

        assertTrue(kvc.toString().contains("bucketName"));
    }

    @Test
    public void testInstanceMirrorAndSources() throws JsonParseException {
        // mirror and sources are mutually exclusive, so exercise each on its own config.
        // Both the builder-config instance(...) and the stream-config instanceViaStreamConfig(...)
        // paths must deserialize them.
        KeyValueConfiguration src = KeyValueConfiguration.builder()
            .name("bucketName")
            .sources(Source.builder().name("KV_sourceBucket").build())
            .build();
        validateSources(src);
        validateSources(KeyValueConfiguration.instance(dataAsString("KeyValueConfigSources.json")));
        validateSources(KeyValueConfiguration.instanceViaStreamConfig(src.getBackingConfig().toJson()));

        KeyValueConfiguration mir = KeyValueConfiguration.builder()
            .name("bucketName")
            .mirror(Mirror.builder().name("KV_mirrorBucket").build())
            .build();
        validateMirror(mir);
        validateMirror(KeyValueConfiguration.instance(dataAsString("KeyValueConfigMirror.json")));
        validateMirror(KeyValueConfiguration.instanceViaStreamConfig(mir.getBackingConfig().toJson()));
    }

    private void validateSources(KeyValueConfiguration kvc) {
        assertNull(kvc.getMirror());
        assertNotNull(kvc.getSources());
        assertEquals(1, kvc.getSources().size());
        assertEquals("KV_sourceBucket", kvc.getSources().get(0).getName());
    }

    private void validateMirror(KeyValueConfiguration kvc) {
        assertNotNull(kvc.getMirror());
        assertEquals("KV_mirrorBucket", kvc.getMirror().getName());
    }

    @Test
    public void testConstructionInvalidsCoverage() {
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().build());
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().name(null));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().name(EMPTY));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().name(HAS_SPACE));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().name(HAS_PRINTABLE));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().name(HAS_DOT));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().name(STAR_NOT_SEGMENT));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().name(GT_NOT_SEGMENT));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().name(HAS_DOLLAR));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().name(HAS_LOW));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder(HAS_127));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder(HAS_FWD_SLASH));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder(HAS_BACK_SLASH));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder(HAS_EQUALS));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder(HAS_TIC));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().maxHistoryPerKey(0));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().maxHistoryPerKey(-1));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().maxHistoryPerKey(65));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().maxBucketSize(0));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().maxBucketSize(-2));
        //noinspection deprecation
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().maxValueSize(0));
        //noinspection deprecation
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().maxValueSize(-2));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().maximumValueSize(0));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().maximumValueSize(-2));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().ttl(Duration.ofNanos(-1)));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().replicas(0));
        assertThrows(IllegalArgumentException.class, () -> KeyValueConfiguration.builder().replicas(6));
    }
}
