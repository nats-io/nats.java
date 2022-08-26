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

import io.nats.client.NUID;
import io.nats.client.impl.Headers;
import io.nats.client.impl.JetStreamTestBase;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;

import static io.nats.client.support.DateTimeUtils.ZONE_ID_GMT;
import static org.junit.jupiter.api.Assertions.*;

public class ObjectStoreApiTests extends JetStreamTestBase {

    @Test
    public void testConfigurationConstruction() {
        Placement p = Placement.builder().cluster("cluster").tags("a", "b").build();

        // builder
        ObjectStoreConfiguration bc = ObjectStoreConfiguration.builder()
            .name("bucketName")
            .description("bucketDesc")
            .maxBucketSize(555)
            .ttl(Duration.ofMillis(777))
            .storageType(StorageType.Memory)
            .replicas(2)
            .placement(p)
            .build();
        validate(bc);

        validate(ObjectStoreConfiguration.builder(bc).build());

        validate(ObjectStoreConfiguration.instance(bc.getBackingConfig().toJson()));
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
        assertEquals(2, osc.getPlacement().getTags().size());

        assertTrue(osc.toString().contains("bucketName"));
    }

    @Test
    public void testObjectInfoConstruction() throws Exception {
        ZonedDateTime modified = ZonedDateTime.now(ZONE_ID_GMT);
        String nuid = NUID.nextGlobal();
        Headers h = new Headers().put("foo", "bar").put("bada", "bing");

        ObjectStoreLink link = ObjectStoreLink.builder()
            .bucket("link-to-bucket")
            .objectName("link-to-name")
            .build();

        ObjectMetaOptions metaOptions = ObjectMetaOptions.builder()
            .link(link)
            .chunkSize(1024)
            .build();

        ObjectMeta meta1 = ObjectMeta.builder("object-name")
            .description("object-desc")
            .headers(h)
            .options(metaOptions)
            .build();

        ObjectMeta meta2 = ObjectMeta.builder("object-name")
            .description("object-desc")
            .headers(h)
            .chunkSize(1024)
            .link(link)
            .build();

        ObjectInfo info1 = ObjectInfo.builder("object-bucket", meta1)
            .nuid(nuid)
            .size(9999)
            .modified(modified)
            .chunks(42)
            .digest("SHA-256=abcdefghijklmnopqrstuvwxyz=")
            .deleted(true)
            .build();

        ObjectInfo info2 = ObjectInfo.builder("object-bucket", meta2)
            .nuid(nuid)
            .size(9999)
            .modified(modified)
            .chunks(42)
            .digest("SHA-256=abcdefghijklmnopqrstuvwxyz=")
            .deleted(true)
            .build();

        ObjectInfo info3 = new ObjectInfo(info1.toJson(), modified);
        assertEquals(info1, info2);
        assertEquals(info1, info3);
    }
}
