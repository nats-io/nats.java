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
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

public class KeyValueConfigurationTests extends JetStreamTestBase {

    @Test
    public void testConstruction() {
        Placement p = Placement.builder().cluster("cluster").tags("a", "b").build();

        // builder
        KeyValueConfiguration bc = KeyValueConfiguration.builder()
            .name("bucketName")
            .description("bucketDesc")
            .maxHistoryPerKey(44)
            .maxBucketSize(555)
            .maxValueSize(666)
            .ttl(Duration.ofMillis(777))
            .storageType(StorageType.Memory)
            .replicas(2)
            .placement(p)
            .build();
        validate(bc);

        validate(KeyValueConfiguration.builder(bc).build());

        validate(KeyValueConfiguration.instance(bc.getBackingConfig().toJson()));

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
        assertEquals(666, kvc.getMaxValueSize());
        assertEquals(Duration.ofMillis(777), kvc.getTtl());
        assertEquals(StorageType.Memory, kvc.getStorageType());
        assertEquals(2, kvc.getReplicas());
        assertNotNull(kvc.getPlacement());
        assertEquals("cluster", kvc.getPlacement().getCluster());
        assertEquals(2, kvc.getPlacement().getTags().size());

        assertTrue(kvc.toString().contains("bucketName"));
    }
}
