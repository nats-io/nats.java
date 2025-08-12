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
import io.nats.client.support.JsonParser;
import io.nats.client.support.JsonValue;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static io.nats.client.support.NatsConstants.EMPTY;
import static org.junit.jupiter.api.Assertions.*;

public class KeyValueConfigurationTests extends JetStreamTestBase {

    @Test
    public void testConstruction() {
        Placement p = Placement.builder().cluster("cluster").tags("a", "b").build();
        Republish r = Republish.builder().source("src").destination("dest").headersOnly(true).build();

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
            .limitMarker(8888)
            .build();
        validate(bc);

        validate(KeyValueConfiguration.builder(bc).build());

        JsonValue jvSc = JsonParser.parseUnchecked(bc.getBackingConfig().toJson());
        validate(new KeyValueConfiguration(StreamConfiguration.instance(jvSc)));

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

        assertTrue(kvc.toString().contains("bucketName"));
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
