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
package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.*;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.JetStreamOptions.DEFAULT_JS_OPTIONS;
import static io.nats.client.support.JsonUtils.printFormatted;
import static org.junit.jupiter.api.Assertions.*;

public class ObjectStoreTests extends JetStreamTestBase {

    @Test
    public void testWorkflow() throws Exception {
        long now = ZonedDateTime.now().toEpochSecond();

        runInJsServer(nc -> {
            // get the kv management context
            ObjectStoreManagement osm = nc.objectStoreManagement();

            // create the bucket
            ObjectStoreConfiguration osc = ObjectStoreConfiguration.builder()
                .name(bucket(1))
                .description("desc1")
                .storageType(StorageType.Memory)
                .build();

            ObjectStoreStatus oss = osm.create(osc);
            validateStore(oss, bucket(1), "desc1");

            JetStreamManagement jsm = nc.jetStreamManagement();
            assertNotNull(jsm.getStreamInfo("OBJ_" + BUCKET));

            List<String> names = osm.getBucketNames();
            assertEquals(1, names.size());
            assertTrue(names.contains(BUCKET));

            ObjectStore os = nc.objectStore(BUCKET);
            ObjectMetaOptions metaOptions = ObjectMetaOptions.builder()
                .chunkSize(4096)
                .build();

            ObjectMeta meta = ObjectMeta.builder("object-name")
                .description("object-desc")
                .headers(new Headers().put("key1", "foo").put("key2", "bar").add("key2", "baz"))
                .options(metaOptions)
                .build();
            ObjectInfo oi = os.put(meta, "aaa");
            printFormatted(oi);

            oi = os.getInfo("object-name");
            printFormatted(oi);

            oi = os.getInfo("not-found");
            assertNull(oi);
        });
    }

    private void validateStore(ObjectStoreStatus oss, String bucket, String desc) {
        assertEquals(bucket, oss.getBucketName());
        assertEquals(desc, oss.getDescription());
        assertFalse(oss.isSealed());
    }

    @Test
    public void testManageGetBucketNames() throws Exception {
        runInJsServer(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();

            // create bucket 1
            osm.create(ObjectStoreConfiguration.builder()
                .name(bucket(1))
                .storageType(StorageType.Memory)
                .build());

            // create bucket 2
            osm.create(ObjectStoreConfiguration.builder()
                .name(bucket(2))
                .storageType(StorageType.Memory)
                .build());

            createMemoryStream(nc, stream(1));
            createMemoryStream(nc, stream(2));

            List<String> buckets = osm.getBucketNames();
            assertEquals(2, buckets.size());
            assertTrue(buckets.contains(bucket(1)));
            assertTrue(buckets.contains(bucket(2)));
        });
    }

    @Test
    public void testObjectStoreOptionsBuilderCoverage() {
        assertKvo(DEFAULT_JS_OPTIONS, ObjectStoreOptions.builder().build());
        assertKvo(DEFAULT_JS_OPTIONS, ObjectStoreOptions.builder().jetStreamOptions(DEFAULT_JS_OPTIONS).build());
        assertKvo(DEFAULT_JS_OPTIONS, ObjectStoreOptions.builder((ObjectStoreOptions) null).build());
        assertKvo(DEFAULT_JS_OPTIONS, ObjectStoreOptions.builder(ObjectStoreOptions.builder().build()).build());
        assertKvo(DEFAULT_JS_OPTIONS, ObjectStoreOptions.builder(DEFAULT_JS_OPTIONS).build());

        ObjectStoreOptions oso = ObjectStoreOptions.builder().jsPrefix("prefix").build();
        assertEquals("prefix.", oso.getJetStreamOptions().getPrefix());
        assertFalse(oso.getJetStreamOptions().isDefaultPrefix());

        oso = ObjectStoreOptions.builder().jsDomain("domain").build();
        assertEquals("$JS.domain.API.", oso.getJetStreamOptions().getPrefix());
        assertFalse(oso.getJetStreamOptions().isDefaultPrefix());

        oso = ObjectStoreOptions.builder().jsRequestTimeout(Duration.ofSeconds(10)).build();
        assertEquals(Duration.ofSeconds(10), oso.getJetStreamOptions().getRequestTimeout());
    }

    private void assertKvo(JetStreamOptions expected, ObjectStoreOptions oso) {
        JetStreamOptions jso = oso.getJetStreamOptions();
        assertEquals(expected.getRequestTimeout(), jso.getRequestTimeout());
        assertEquals(expected.getPrefix(), jso.getPrefix());
        assertEquals(expected.isDefaultPrefix(), jso.isDefaultPrefix());
        assertEquals(expected.isPublishNoAck(), jso.isPublishNoAck());
    }

    @Test
    public void testObjectList() throws Exception {
        runInJsServer(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();

            osm.create(ObjectStoreConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .build());

            ObjectStore os = nc.objectStore(BUCKET);

            os.put(ObjectMeta.objectName(key(1)), "11");
            os.put(ObjectMeta.objectName(key(2)), "21");
            os.put(ObjectMeta.objectName(key(3)), "31");
            ObjectInfo info = os.put(ObjectMeta.objectName(key(2)), "22");

            os.addLink(key(4), info);

            os.put(ObjectMeta.objectName(key(9)), "91");
            os.delete(key(9));

            List<ObjectInfo> list = os.getList();
            assertEquals(4, list.size());

            List<String> names = new ArrayList<>();
            for (int x = 1; x <= 4; x++) {
                names.add(list.get(x-1).getObjectName());
            }

            for (int x = 1; x <= 4; x++) {
                assertTrue(names.contains(key(x)));
            }
        });
    }
}
