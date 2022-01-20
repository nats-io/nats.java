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
import io.nats.client.support.NatsObjectStoreUtil;
import io.nats.client.support.RandomUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import static io.nats.client.support.NatsObjectStoreUtil.toChunkPrefix;
import static org.junit.jupiter.api.Assertions.*;

public class ObjectStoreTests extends JetStreamTestBase {

    @Test
    public void testWorkflow() throws Exception {
        runInJsServer(nc -> {
            // get the contexts
            JetStream js = nc.jetStream();
            ObjectStoreManagement osm = nc.objectStoreManagement();

            // create the bucket
            ObjectStoreConfiguration osc = ObjectStoreConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .build();

            assertStatusAfterCreate(osm.create(osc));
            assertStatusAfterCreate(osm.getObjectStoreInfo(BUCKET));

            // I will use a direct JetStream subscription to double check things.
            // after the store is created, the stream exists so I can subscribe
            String streamName = NatsObjectStoreUtil.toStreamName(BUCKET);
            String metaStreamSubject = NatsObjectStoreUtil.toMetaStreamSubject(BUCKET);
            String chunkStreamSubject = NatsObjectStoreUtil.toChunkStreamSubject(BUCKET);
            JetStreamSubscription subMeta = js.subscribe(metaStreamSubject, PushSubscribeOptions.stream(streamName));
            JetStreamSubscription subChunk = js.subscribe(chunkStreamSubject, PushSubscribeOptions.stream(streamName));

            ObjectStore os = nc.objectStore(BUCKET);
            assertStatusAfterCreate(os.getStatus());

            // using a small chunk size so I can see the it split the data
            ObjectMetaOptions options = ObjectMetaOptions.chunkSize(1024);
            ObjectMeta meta1 = ObjectMeta.builder()
                .name(object(1))
                .description(description(1))
                .options(options)
                .build();

            // data 1
            byte[] data1 = RandomUtils.nextBytes(2560);

            // put data with bad name
            ObjectMeta.name("");
            assertThrows(IllegalArgumentException.class, () -> os.put(ObjectMeta.name(""), (InputStream) null));
            assertThrows(IllegalArgumentException.class, () -> os.put(ObjectMeta.name(null), (InputStream) null));

            // put the data
            ObjectInfo infoPut = os.put(meta1, data1);
            String nuid1 = infoPut.getNuid();

            // first test directly getting the chunks. I'm going to do this later on after a put over this name
            String rawSubject = toChunkPrefix(BUCKET) + nuid1;
            JetStreamSubscription sub = nc.jetStream().subscribe(rawSubject);
            List<Message> list = readMessagesAck(sub);
            sub.unsubscribe();
            assertEquals(infoPut.getChunks(), list.size());

                // test the results
            assertGetInfoMatchesPutInfo(os, infoPut, 1, 2560, 3);

            // check the meta data messages
            assertDirectMeta(subMeta, infoPut);

            // Check the chunks manually / directly
            assertDirectChunk(subChunk, 1024);
            assertDirectChunk(subChunk, 1024);
            assertDirectChunk(subChunk, 512);

            // "get" the object
            assertGetTheObject(os, data1, 1);

            // Delete the object
            assertDeleteObject(os, subMeta, 1);

            // coverage: the string put
            ObjectMeta meta2 = ObjectMeta.builder()
                .name(object(2))
                .description(description(2))
                .options(options)
                .build();

            // data 2
            String data2 = data(2);
            infoPut = os.put(meta2, data2);

            // test the results
            assertGetInfoMatchesPutInfo(os, infoPut, 2, data2.length(), 1);

            // check the meta data messages
            assertDirectMeta(subMeta, infoPut);

            // Check the chunks
            assertDirectChunk(subChunk, data2.length());

            // "get" the object
            assertGetTheObject(os, data2.getBytes(StandardCharsets.UTF_8), 2);

            // test putting an existing object

            // data 3 over data 1
            String data3 = data(3);
            infoPut = os.put(meta1, data3);

            // test the results
            assertGetInfoMatchesPutInfo(os, infoPut, 1, data3.length(), 1);

            // also test directly getting the chunks of the old nuid to see that the old chunks are gone
            rawSubject = toChunkPrefix(BUCKET) + nuid1;
            sub = nc.jetStream().subscribe(rawSubject);
            list = readMessagesAck(sub);
            sub.unsubscribe();
            assertEquals(0, list.size());

            // test getting an object that does not exist
            assertThrows(IllegalArgumentException.class, () -> os.getBytes(object(99)));

            // delete the object store
            osm.delete(BUCKET);
            assertThrows(JetStreamApiException.class, () -> osm.delete(BUCKET));
            assertThrows(JetStreamApiException.class, () -> osm.getObjectStoreInfo(BUCKET));

            assertEquals(0, osm.getObjectStoreBucketNames().size());
        });
    }

    private void assertDeleteObject(ObjectStore os, JetStreamSubscription subMeta, int id) throws IOException, JetStreamApiException, InterruptedException {
        os.delete(object(id));
        ObjectInfo info = os.getInfo(object(id));
        assertEquals(0, info.getSize());
        assertEquals(0, info.getChunks());
        assertTrue(info.isDeleted());
        assertDirectMeta(subMeta, info);
    }

    private void assertGetTheObject(ObjectStore os, byte[] expected, int id) throws IOException, JetStreamApiException, InterruptedException {
        byte[] dataGet = os.getBytes(object(id));
        assertEquals(expected.length, dataGet.length);
        for (int x = 0; x < expected.length; x++) {
            assertEquals(expected[x], dataGet[x]);
        }
    }

    private void assertGetInfoMatchesPutInfo(ObjectStore os, ObjectInfo infoPut, int objectNameId, int size, int chunks) throws IOException, JetStreamApiException {
        ObjectInfo infoGet = os.getInfo(object(objectNameId));
        assertEquals(infoPut, infoGet);
        assertEquals(object(objectNameId), infoPut.getObjectMeta().getName());
        if (infoPut.getObjectMeta().getDescription() != null) {
            assertEquals(description(objectNameId), infoPut.getObjectMeta().getDescription());
        }
        assertEquals(size, infoPut.getSize());
        assertEquals(chunks, infoPut.getChunks());
    }

    private void assertDirectMeta(JetStreamSubscription subMeta, ObjectInfo expected) throws InterruptedException {
        Message m = subMeta.nextMessage(1000);
        assertNotNull(m);
        assertEquals(expected, new ObjectInfo(m));
    }

    private void assertDirectChunk(JetStreamSubscription subChunk, int expected) throws InterruptedException {
        Message m = subChunk.nextMessage(1000);
        assertNotNull(m);
        assertEquals(expected, m.getData().length);
    }

    private void assertStatusAfterCreate(ObjectStoreStatus status) {
        ObjectStoreConfiguration osc = status.getConfiguration();
        assertEquals(BUCKET, status.getStoreName());
        assertEquals(BUCKET, osc.getStoreName());
        assertEquals(NatsObjectStoreUtil.toStreamName(BUCKET), osc.getBackingConfig().getName());
        assertEquals(Duration.ZERO, status.getTtl());
        assertEquals(Duration.ZERO, osc.getTtl());
        assertEquals(StorageType.Memory, status.getStorageType());
        assertEquals(StorageType.Memory, osc.getStorageType());
        assertEquals(1, status.getReplicas());
        assertEquals(1, osc.getReplicas());
        assertFalse(status.isSealed());
        assertEquals("JetStream", status.getBackingStore());
    }

    @Test
    public void testManageGetObjectStoreBucketNames() throws Exception {
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

            List<String> stores = osm.getObjectStoreBucketNames();
            assertEquals(2, stores.size());
            assertTrue(stores.contains(bucket(1)));
            assertTrue(stores.contains(bucket(2)));
        });
    }

    @Test
    public void testLinks() throws Exception {
        runInJsServer(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();

            // create buckets
            osm.create(ObjectStoreConfiguration.builder()
                .name(bucket(1))
                .storageType(StorageType.Memory)
                .build());
            ObjectStore os1 = nc.objectStore(bucket(1));

            osm.create(ObjectStoreConfiguration.builder()
                .name(bucket(2))
                .storageType(StorageType.Memory)
                .build());
            ObjectStore os2 = nc.objectStore(bucket(2));

            // an object to link to
            ObjectInfo info1 = os1.put(ObjectMeta.name(object(1)), "111");
            assertGetInfoMatchesPutInfo(os1, info1, 1, 3, 1);

            // link to the object
            ObjectInfo linkToObj1 = os1.addLink(object(101), info1);
            assertGetInfoMatchesPutInfo(os1, linkToObj1, 101, 0, 0);
            assertLinkIsCorrect(linkToObj1, info1);

            ObjectInfo linkToLink1 = os1.addLink(object(10101), linkToObj1);
            assertGetInfoMatchesPutInfo(os1, linkToLink1, 10101, 0, 0);
            assertLinkIsCorrect(linkToLink1, linkToObj1);

            ObjectInfo crossBucketToObj1 = os2.addLink(object(201), info1);
            assertGetInfoMatchesPutInfo(os2, crossBucketToObj1, 201, 0, 0);
            assertLinkIsCorrect(crossBucketToObj1, info1);

            // test values
            assertEquals("111", new String(os1.getBytes(object(1))));
            assertEquals("111", new String(os1.getBytes(object(101))));
            assertEquals("111", new String(os1.getBytes(object(10101))));
            assertEquals("111", new String(os2.getBytes(object(201))));
        });
    }

    private void assertLinkIsCorrect(ObjectInfo link, ObjectInfo target) {
        assertEquals(target.getBucket(), link.getObjectMetaOptions().getLink().getBucket());
        assertEquals(target.getName(), link.getObjectMetaOptions().getLink().getName());
    }
}