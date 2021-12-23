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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KeyValueConfigurationTests extends JetStreamTestBase {

    @Test
    public void testConstruction() {

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
            .build();
        validate(bc);

        validate(KeyValueConfiguration.builder(bc).build());

        validate(KeyValueConfiguration.instance(bc.getBackingConfig().toJson()));

        bc = KeyValueConfiguration.builder()
                .name("bucketName")
                .build();

        assertEquals(1, bc.getMaxHistoryPerKey());
    }

    private void validate(KeyValueConfiguration bc) {
        assertEquals("bucketName", bc.getBucketName());
        assertEquals("bucketDesc", bc.getDescription());
        assertEquals(44, bc.getMaxHistoryPerKey());
        assertEquals(555, bc.getMaxBucketSize());
        assertEquals(666, bc.getMaxValueSize());
        assertEquals(Duration.ofMillis(777), bc.getTtl());
        assertEquals(StorageType.Memory, bc.getStorageType());
        assertEquals(2, bc.getReplicas());

        assertTrue(bc.toString().contains("bucketName"));
    }
}
