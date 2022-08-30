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
package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.api.*;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.nats.client.JetStreamOptions.DEFAULT_JS_OPTIONS;
import static io.nats.client.api.ObjectStoreWatchOption.IGNORE_DELETE;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static org.junit.jupiter.api.Assertions.*;

public class ObjectStoreTests extends JetStreamTestBase {

    @Test
    public void testWorkflow() throws Exception {
        runInJsServer(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();

            // create the bucket
            ObjectStoreConfiguration osc = ObjectStoreConfiguration.builder(BUCKET)
                .description(PLAIN)
                .ttl(Duration.ofHours(24))
                .storageType(StorageType.Memory)
                .build();

            ObjectStoreStatus status = osm.create(osc);
            assertEquals(BUCKET, status.getBucketName());
            assertEquals(PLAIN, status.getDescription());
            assertFalse(status.isSealed());
            assertEquals(0, status.getSize());
            assertEquals(Duration.ofHours(24), status.getTtl());
            assertEquals(StorageType.Memory, status.getStorageType());
            assertEquals(1, status.getReplicas());
            assertNotNull(status.getConfiguration()); // coverage
            assertNotNull(status.getBackingStreamInfo()); // coverage
            assertEquals("JetStream", status.getBackingStore());
            assertNotNull(status.toString()); // coverage

            JetStreamManagement jsm = nc.jetStreamManagement();
            assertNotNull(jsm.getStreamInfo("OBJ_" + BUCKET));

            List<String> names = osm.getBucketNames();
            assertEquals(1, names.size());
            assertTrue(names.contains(BUCKET));

            // put some objects into the stores
            ObjectStore os = nc.objectStore(BUCKET);

            // object not found errors
            assertClientError(OsObjectNotFound, () -> os.get("notFound", new ByteArrayOutputStream()));
            assertClientError(OsObjectNotFound, () -> os.updateMeta("notFound", ObjectMeta.objectName("notFound")));
            assertClientError(OsObjectNotFound, () -> os.delete("notFound"));

            ObjectMeta meta = ObjectMeta.builder("object name")
                .description("object description")
                .headers(new Headers().put("key1", "foo").put("key2", "bar").add("key2", "baz"))
                .chunkSize(4096)
                .build();

            Object[] input = getInput(4096 * 10);
            long len = (long)input[0];
            long expectedChunks = len / 4096;
            if (expectedChunks * 4096 < len) {
                expectedChunks++;
            }
            ObjectInfo oi1 = validateObjectInfo(len, expectedChunks, os.put(meta, (InputStream)input[1]));

            ByteArrayOutputStream baos = validateGet(os, len, expectedChunks);
            byte[] bytes = baos.toByteArray();
            byte[] bytes4k = Arrays.copyOf(bytes, 4096);

            ObjectInfo oi2 = validateObjectInfo(4096, 1, os.put(meta, new ByteArrayInputStream(bytes4k)));
            validateGet(os, 4096, 1);

            assertNotEquals(oi1.getNuid(), oi2.getNuid());

            // update meta
            ObjectInfo oi3 = os.updateMeta(oi2.getObjectName(),
                ObjectMeta.builder("new object name")
                    .description("new object description")
                    .headers(new Headers().put("newkey", "newval")).build());
            assertEquals("new object name", oi3.getObjectName());
            assertEquals("new object description", oi3.getDescription());
            assertNotNull(oi3.getHeaders());
            assertEquals(1, oi3.getHeaders().size());
            assertEquals("newval", oi3.getHeaders().getFirst("newkey"));

            // check the old object is not found at all
            assertClientError(OsObjectNotFound, () -> os.get("object name", new ByteArrayOutputStream()));
            assertClientError(OsObjectNotFound, () -> os.updateMeta("object name", ObjectMeta.objectName("notFound")));

            // delete object, then try to update meta
            os.delete(oi3.getObjectName());
            assertClientError(OsObjectIsDeleted, () -> os.updateMeta(oi3.getObjectName(), ObjectMeta.objectName("notFound")));

            // can't update meta to an existing object
            os.put("another1", "another1".getBytes());
            os.put("another2", "another2".getBytes());
            assertClientError(OsObjectAlreadyExists, () -> os.updateMeta("another1", ObjectMeta.objectName("another2")));
        });
    }

    private ByteArrayOutputStream validateGet(ObjectStore os, long len, long expectedChunks) throws IOException, JetStreamApiException, InterruptedException, NoSuchAlgorithmException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectInfo oi = os.get("object name", baos);
        assertEquals(len, baos.size());
        validateObjectInfo(len, expectedChunks, oi);
        return baos;
    }

    private ObjectInfo validateObjectInfo(long len, long expectedChunks, ObjectInfo oi) {
        assertEquals(BUCKET, oi.getBucket());
        assertEquals("object name", oi.getObjectName());
        assertEquals("object description", oi.getDescription());
        assertEquals(len, oi.getSize());
        assertEquals(expectedChunks, oi.getChunks());
        assertNotNull(oi.getHeaders());
        assertEquals(2, oi.getHeaders().size());
        List<String> list = oi.getHeaders().get("key1");
        assertEquals(1, list.size());
        assertEquals("foo", oi.getHeaders().getFirst("key1"));
        list = oi.getHeaders().get("key2");
        assertEquals(2, list.size());
        assertTrue(list.contains("bar"));
        assertTrue(list.contains("baz"));
        return oi;
    }

    @SuppressWarnings("SameParameterValue")
    private static Object[] getInput(int size) throws IOException {
        File found = null;
        long foundLen = Long.MAX_VALUE;
        final String classPath = System.getProperty("java.class.path", ".");
        final String[] classPathElements = classPath.split(System.getProperty("path.separator"));
        for(final String element : classPathElements){
            File f = new File(element);
            if (f.isFile()) {
                long flen = f.length();
                if (flen == size) {
                    found = f;
                    break;
                }
                if (flen >= size && flen < foundLen){
                    foundLen = flen;
                    found = f;
                }
            }
        }
        //noinspection ConstantConditions,resource
        return new Object[] {foundLen, Files.newInputStream(found.toPath())};
    }

    @Test
    public void testManageGetBucketNames() throws Exception {
        runInJsServer(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();

            // create bucket 1
            osm.create(ObjectStoreConfiguration.builder(bucket(1))
                .storageType(StorageType.Memory)
                .build());

            // create bucket 2
            osm.create(ObjectStoreConfiguration.builder(bucket(2))
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
        assertOso(ObjectStoreOptions.builder().build());
        assertOso(ObjectStoreOptions.builder().jetStreamOptions(DEFAULT_JS_OPTIONS).build());
        assertOso(ObjectStoreOptions.builder((ObjectStoreOptions) null).build());
        assertOso(ObjectStoreOptions.builder(ObjectStoreOptions.builder().build()).build());
        assertOso(ObjectStoreOptions.builder(DEFAULT_JS_OPTIONS).build());

        ObjectStoreOptions oso = ObjectStoreOptions.builder().jsPrefix("prefix").build();
        assertEquals("prefix.", oso.getJetStreamOptions().getPrefix());
        assertFalse(oso.getJetStreamOptions().isDefaultPrefix());

        oso = ObjectStoreOptions.builder().jsDomain("domain").build();
        assertEquals("$JS.domain.API.", oso.getJetStreamOptions().getPrefix());
        assertFalse(oso.getJetStreamOptions().isDefaultPrefix());

        oso = ObjectStoreOptions.builder().jsRequestTimeout(Duration.ofSeconds(10)).build();
        assertEquals(Duration.ofSeconds(10), oso.getJetStreamOptions().getRequestTimeout());
    }

    private void assertOso(ObjectStoreOptions oso) {
        JetStreamOptions jso = oso.getJetStreamOptions();
        assertEquals(JetStreamOptions.DEFAULT_JS_OPTIONS.getRequestTimeout(), jso.getRequestTimeout());
        assertEquals(JetStreamOptions.DEFAULT_JS_OPTIONS.getPrefix(), jso.getPrefix());
        assertEquals(JetStreamOptions.DEFAULT_JS_OPTIONS.isDefaultPrefix(), jso.isDefaultPrefix());
        assertEquals(JetStreamOptions.DEFAULT_JS_OPTIONS.isPublishNoAck(), jso.isPublishNoAck());
    }

    @Test
    public void testObjectLinks() throws Exception {
        runInJsServer(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();

            osm.create(ObjectStoreConfiguration.builder(bucket(1)).storageType(StorageType.Memory).build());
            ObjectStore os1 = nc.objectStore(bucket(1));

            osm.create(ObjectStoreConfiguration.builder(bucket(2)).storageType(StorageType.Memory).build());
            ObjectStore os2 = nc.objectStore(bucket(2));

            os1.put(key(11), "11".getBytes());
            os1.put(key(12), "12".getBytes());

            ObjectInfo info11 = os1.getInfo(key(11));
            ObjectInfo info12 = os1.getInfo(key(12));

            // can't overwrite object with a link
            assertClientError(OsObjectAlreadyExists, () -> os1.addLink(info11.getObjectName(), info11)); // can't overwrite object with a link

            // can't overwrite object with a bucket link
            assertClientError(OsObjectAlreadyExists, () -> os1.addBucketLink(info11.getObjectName(), os1));

            // Link to individual object.
            ObjectInfo linkTo11 = os1.addLink("linkTo11", info11);
            validateLink(linkTo11, "linkTo11", info11, null);

            // link to a link
            assertClientError(OsCantLinkToLink, () -> os1.addLink("linkToLinkIsErr", linkTo11));

            os2.put(key(21), "21".getBytes());
            ObjectInfo info21 = os2.getInfo(key(21));

            ObjectInfo crossLink = os2.addLink("crossLink", info11);
            validateLink(crossLink, "crossLink", info11, null);

            ObjectInfo bucketLink = os2.addBucketLink("bucketLink", os1);
            validateLink(bucketLink, "bucketLink", null, os1);

            // getInfo targetWillBeDeleted still gets info b/c the link is there
            ObjectInfo targetWillBeDeleted = os2.addLink("targetWillBeDeleted", info21);
            validateLink(targetWillBeDeleted, "targetWillBeDeleted", info21, null);
            os2.delete(info21.getObjectName());
            ObjectInfo oiDeleted = os2.getInfo(info21.getObjectName());
            assertTrue(oiDeleted.isDeleted()); // object is deleted
            assertClientError(OsObjectIsDeleted, () -> os2.addLink("willException", oiDeleted));
            ObjectInfo oiLink = os2.getInfo("targetWillBeDeleted");
            assertNotNull(oiLink); // link is still there but link target is deleted
            assertClientError(OsObjectNotFound, () -> os2.get("targetWillBeDeleted", new ByteArrayOutputStream()));
            assertClientError(OsCantLinkToLink, () -> os2.addLink("willException", oiLink)); // can't link to link

            os2.addLink("cross", info12);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectInfo crossInfo12 = os2.get("cross", baos);
            assertEquals(info12, crossInfo12);
            assertEquals(2, baos.size());
            assertEquals("12", baos.toString());
        });
    }

    private void validateLink(ObjectInfo oiLink, String linkName, ObjectInfo targetStore, ObjectStore targetBucket) {
        assertEquals(linkName, oiLink.getObjectName());
        assertNotNull(oiLink.getNuid());
        assertNotNull(oiLink.getModified());

        ObjectLink link = oiLink.getLink();
        assertNotNull(link);
        if (targetStore == null) { // link to bucket
            assertNull(link.getObjectName());
            assertEquals(link.getBucket(), targetBucket.getBucketName());
        }
        else {
            assertEquals(link.getObjectName(), targetStore.getObjectName());
            assertEquals(link.getBucket(), targetStore.getBucket());
        }
    }

    @Test
    public void testObjectList() throws Exception {
        runInJsServer(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();
            osm.create(ObjectStoreConfiguration.builder(BUCKET).storageType(StorageType.Memory).build());

            ObjectStore os = nc.objectStore(BUCKET);

            os.put(key(1), "11".getBytes());
            os.put(key(2), "21".getBytes());
            os.put(key(3), "31".getBytes());
            ObjectInfo info = os.put(key(2), "22".getBytes());

            os.addLink(key(4), info);

            os.put(key(9), "91".getBytes());
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

    static class TestObjectStoreWatcher implements ObjectStoreWatcher {
        public String name;
        public List<ObjectInfo> entries = new ArrayList<>();
        public ObjectStoreWatchOption[] watchOptions;
        public boolean beforeWatcher;
        public int endOfDataReceived;
        public boolean endBeforeEntries;

        public TestObjectStoreWatcher(String name, boolean beforeWatcher, ObjectStoreWatchOption... watchOptions) {
            this.name = name;
            this.beforeWatcher = beforeWatcher;
            this.watchOptions = watchOptions;
        }

        @Override
        public String toString() {
            return "TestObjectStoreWatcher{" +
                "name='" + name + '\'' +
                ", beforeWatcher=" + beforeWatcher +
                ", watchOptions=" + Arrays.toString(watchOptions) +
                '}';
        }

        @Override
        public void watch(ObjectInfo oi) {
            entries.add(oi);
        }

        @Override
        public void endOfData() {
            if (++endOfDataReceived == 1 && entries.size() == 0) {
                endBeforeEntries = true;
            }
        }
    }

    static String TEST_WATCH_KEY_NULL = "key.nl";
    static String TEST_WATCH_KEY_1 = "key.1";
    static String TEST_WATCH_KEY_2 = "key.2";
    static int SIZE = 15;
    static int CHUNKS = 2;
    static int CHUNK_SIZE = 10;

    interface TestWatchSubSupplier {
        NatsObjectStoreWatchSubscription get(ObjectStore os) throws Exception;
    }

    @Test
    public void testWatch() throws Exception {
        TestObjectStoreWatcher fullB4Watcher = new TestObjectStoreWatcher("fullB4Watcher", true);
        TestObjectStoreWatcher delB4Watcher = new TestObjectStoreWatcher("delB4Watcher", true, IGNORE_DELETE);

        TestObjectStoreWatcher fullAfterWatcher = new TestObjectStoreWatcher("fullAfterWatcher", false);
        TestObjectStoreWatcher delAfterWatcher = new TestObjectStoreWatcher("delAfterWatcher", false, IGNORE_DELETE);

        runInJsServer(nc -> {
            _testWatch(nc, fullB4Watcher, new Object[]{"A", "B", null}, os -> os.watch(fullB4Watcher, fullB4Watcher.watchOptions));
            _testWatch(nc, delB4Watcher, new Object[]{"A", "B"}, os -> os.watch(delB4Watcher, delB4Watcher.watchOptions));
            _testWatch(nc, fullAfterWatcher, new Object[]{"B", null}, os -> os.watch(fullAfterWatcher, fullAfterWatcher.watchOptions));
            _testWatch(nc, delAfterWatcher, new Object[]{"B"}, os -> os.watch(delAfterWatcher, delAfterWatcher.watchOptions));
        });
    }

    private void _testWatch(Connection nc, TestObjectStoreWatcher watcher, Object[] expecteds, TestWatchSubSupplier supplier) throws Exception {
        ObjectStoreManagement osm = nc.objectStoreManagement();

        String bucket = watcher.name + "Bucket";
        osm.create(ObjectStoreConfiguration.builder(bucket).storageType(StorageType.Memory).build());

        ObjectStore os = nc.objectStore(bucket);

        NatsObjectStoreWatchSubscription sub = null;

        if (watcher.beforeWatcher) {
            sub = supplier.get(os);
        }

        byte[] A = "A23456789012345".getBytes(StandardCharsets.US_ASCII);
        byte[] B = "B23456789012345".getBytes(StandardCharsets.US_ASCII);

        os.put(ObjectMeta.builder("A").chunkSize(CHUNK_SIZE).build(), new ByteArrayInputStream(A));
        os.put(ObjectMeta.builder("B").chunkSize(CHUNK_SIZE).build(), new ByteArrayInputStream(B));
        os.delete("A");

        if (!watcher.beforeWatcher) {
            sub = supplier.get(os);
        }

        sleep(1500); // give time for the watches to get messages

        validateWatcher(expecteds, watcher);
        //noinspection ConstantConditions
        sub.unsubscribe();
        osm.delete(bucket);
    }

    private void validateWatcher(Object[] expecteds, TestObjectStoreWatcher watcher) {
        assertEquals(expecteds.length, watcher.entries.size());
        assertEquals(1, watcher.endOfDataReceived);

        if (expecteds.length > 0) {
            assertEquals(watcher.beforeWatcher, watcher.endBeforeEntries);
        }

        int aix = 0;
        ZonedDateTime lastMod = ZonedDateTime.of(2000, 4, 1, 0, 0, 0, 0, ZoneId.systemDefault());

        for (ObjectInfo oi : watcher.entries) {

            assertTrue(oi.getModified().isAfter(lastMod) || oi.getModified().isEqual(lastMod));
            lastMod = oi.getModified();

            assertNotNull(oi.getNuid());

            Object expected = expecteds[aix++];
            if (expected == null) {
                assertTrue(oi.isDeleted());
            }
            else {
                assertEquals(CHUNK_SIZE, oi.getObjectMeta().getObjectMetaOptions().getChunkSize());
                assertEquals(CHUNKS, oi.getChunks());
                assertEquals(SIZE, oi.getSize());
            }
        }
    }

    @Test
    public void testSeal() throws Exception {
        runInJsServer(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();
            osm.create(ObjectStoreConfiguration.builder(BUCKET)
                .storageType(StorageType.Memory)
                .build());

            ObjectStore os = nc.objectStore(BUCKET);
            ObjectInfo info = os.put("name", "data".getBytes());

            os.seal();

            assertThrows(JetStreamApiException.class, () -> os.put("another", "data".getBytes()));

            ObjectMeta meta = ObjectInfo.builder(info).description("changed").build().getObjectMeta();
            assertThrows(JetStreamApiException.class, () -> os.updateMeta("name", meta));
        });
    }
}
