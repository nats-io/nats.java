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
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BucketConfigurationTests extends JetStreamTestBase {

    @Test
    public void testConstruction() {

        // builder
        BucketConfiguration bc = BucketConfiguration.builder()
                .name("bucketName")
                .maxValues(333)
                .maxHistory(444)
                .maxBucketSize(555)
                .maxValueSize(666)
                .ttl(Duration.ofMillis(777))
                .storageType(StorageType.Memory)
                .replicas(2)
                .duplicateWindow(Duration.ofMillis(888))
                .build();
        validate(bc);

        validate(BucketConfiguration.builder(bc).build());

        validate(BucketConfiguration.instance(bc.getBackingConfig().toJson()));

        bc = BucketConfiguration.builder()
                .name("bucketName")
                .duplicateWindow(999)
                .build();

        assertEquals(1, bc.getMaxHistory());
        assertEquals(Duration.ofMillis(999), bc.getDuplicateWindow());
    }

    private void validate(BucketConfiguration bc) {
        assertEquals("bucketName", bc.getName());
        assertEquals(333, bc.getMaxValues());
        assertEquals(444, bc.getMaxHistory());
        assertEquals(555, bc.getMaxBucketSize());
        assertEquals(666, bc.getMaxValueSize());
        assertEquals(Duration.ofMillis(777), bc.getTtl());
        assertEquals(StorageType.Memory, bc.getStorageType());
        assertEquals(2, bc.getReplicas());
        assertEquals(Duration.ofMillis(888), bc.getDuplicateWindow());
    }
}
