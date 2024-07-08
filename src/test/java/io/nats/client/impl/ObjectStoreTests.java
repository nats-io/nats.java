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
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

import static io.nats.client.JetStreamOptions.DEFAULT_JS_OPTIONS;
import static io.nats.client.api.ObjectStoreWatchOption.IGNORE_DELETE;
import static io.nats.client.support.NatsJetStreamClientError.*;
import static io.nats.client.support.NatsObjectStoreUtil.DEFAULT_CHUNK_SIZE;
import static org.junit.jupiter.api.Assertions.*;

public class ObjectStoreTests extends JetStreamTestBase {

    @Test
    public void testWorkflow() throws Exception {
        jsServer.run(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();
            nc.objectStoreManagement(ObjectStoreOptions.builder(DEFAULT_JS_OPTIONS).build()); // coverage

            Map<String, String> metadata = new HashMap<>();
            metadata.put("meta-foo", "meta-bar");

            String bucket = bucket();
            String desc = variant();
            // create the bucket
            ObjectStoreConfiguration osc = ObjectStoreConfiguration.builder(bucket)
                .description(desc)
                .ttl(Duration.ofHours(24))
                .storageType(StorageType.Memory)
                .metadata(metadata)
                .build();

            ObjectStoreStatus status = osm.create(osc);
            validateStatus(status, bucket, desc);
            validateStatus(osm.getStatus(bucket), bucket, desc);

            JetStreamManagement jsm = nc.jetStreamManagement();
            assertNotNull(jsm.getStreamInfo("OBJ_" + bucket));

            List<String> names = osm.getBucketNames();
            assertEquals(1, names.size());
            assertTrue(names.contains(bucket));

            // put some objects into the stores
            ObjectStore os = nc.objectStore(bucket);
            nc.objectStore(bucket, ObjectStoreOptions.builder(DEFAULT_JS_OPTIONS).build()); // coverage;

            validateStatus(os.getStatus(), bucket, desc);

            // object not found errors
            assertClientError(OsObjectNotFound, () -> os.get("notFound", new ByteArrayOutputStream()));
            assertClientError(OsObjectNotFound, () -> os.updateMeta("notFound", ObjectMeta.objectName("notFound")));
            assertClientError(OsObjectNotFound, () -> os.delete("notFound"));

            String objectName = name();
            ObjectMeta meta = ObjectMeta.builder(objectName)
                .description("object-desc")
                .headers(new Headers().put(key(1), data(1)).put(key(2), data(21)).add(key(2), data(22)))
                .chunkSize(4096)
                .build();

            Object[] input = getInput(4096 * 10);
            long len = (long)input[0];
            long expectedChunks = len / 4096;
            if (expectedChunks * 4096 < len) {
                expectedChunks++;
            }
            File file = (File)input[1];
            InputStream in = Files.newInputStream(file.toPath());
            ObjectInfo oi1 = validateObjectInfo(os.put(meta, in), bucket, objectName, len, expectedChunks, 4096);

            ByteArrayOutputStream baos = validateGet(os, bucket, objectName, len, expectedChunks, 4096);
            byte[] bytes = baos.toByteArray();
            byte[] bytes4k = Arrays.copyOf(bytes, 4096);

            ObjectInfo oi2 = validateObjectInfo(os.put(meta, new ByteArrayInputStream(bytes4k)), bucket, objectName, 4096, 1, 4096);
            validateGet(os, bucket, objectName, 4096, 1, 4096);

            assertNotEquals(oi1.getNuid(), oi2.getNuid());

            // update meta changing name, desc, headers
            ObjectInfo oi3 = os.updateMeta(oi2.getObjectName(),
                ObjectMeta.builder("object name B")
                    .description("description B")
                    .headers(new Headers().put("newkey", "newval")).build());
            assertEquals("object name B", oi3.getObjectName());
            assertEquals("description B", oi3.getDescription());
            assertNotNull(oi3.getHeaders());
            assertEquals(1, oi3.getHeaders().size());
            assertEquals("newval", oi3.getHeaders().getFirst("newkey"));

            // update meta changing desc only for coverage
            oi3 = os.updateMeta(oi3.getObjectName(),
                ObjectMeta.builder(oi3.getObjectMeta()).description("description C").build());
            assertEquals("object name B", oi3.getObjectName());
            assertEquals("description C", oi3.getDescription());
            assertEquals(1, oi3.getHeaders().size());

            // check the old object is not found at all
            assertClientError(OsObjectNotFound, () -> os.get(objectName, new ByteArrayOutputStream()));
            assertClientError(OsObjectNotFound, () -> os.updateMeta(objectName, ObjectMeta.objectName("notFound")));

            // delete object, then try to update meta
            ObjectInfo delInfo = os.delete(oi3.getObjectName());
            assertEquals(oi3.getObjectName(), delInfo.getObjectName());
            assertTrue(delInfo.isDeleted());

            // delete an object a second time is fine
            delInfo = os.delete(oi3.getObjectName());
            assertEquals(oi3.getObjectName(), delInfo.getObjectName());
            assertTrue(delInfo.isDeleted());

            // try to update meta for deleted
            ObjectInfo oiWillError = oi3;
            assertClientError(OsObjectIsDeleted, () -> os.updateMeta(oiWillError.getObjectName(), ObjectMeta.objectName("notFound")));

            // can't update meta to an existing object
            ObjectInfo oi = os.put("another1", "another1".getBytes());
            os.put("another2", "another2".getBytes());
            assertClientError(OsObjectAlreadyExists, () -> os.updateMeta("another1", ObjectMeta.objectName("another2")));

            // but you can update a name to a deleted object's name
            os.updateMeta(oi.getObjectName(), ObjectMeta.objectName(oi3.getObjectName()));

            // alternate puts
            String altName = name();
            expectedChunks = len / DEFAULT_CHUNK_SIZE;
            if (expectedChunks * DEFAULT_CHUNK_SIZE < len) {
                expectedChunks++;
            }
            in = Files.newInputStream(file.toPath());
            validateObjectInfo(os.put(altName, in), bucket, altName, null, false, len, expectedChunks, DEFAULT_CHUNK_SIZE);

            altName = file.getName();
            validateObjectInfo(os.put(file), bucket, altName, null, false, len, expectedChunks, DEFAULT_CHUNK_SIZE);
        });
    }

    private static void validateStatus(ObjectStoreStatus status, String bucket, String desc) {
        assertEquals(bucket, status.getBucketName());
        assertEquals(desc, status.getDescription());
        assertFalse(status.isSealed());
        assertEquals(0, status.getSize());
        assertEquals(Duration.ofHours(24), status.getTtl());
        assertEquals(StorageType.Memory, status.getStorageType());
        assertEquals(1, status.getReplicas());
        assertNull(status.getPlacement());
        assertNotNull(status.getConfiguration()); // coverage
        assertNotNull(status.getConfiguration().toString()); // coverage
        assertNotNull(status.getBackingStreamInfo()); // coverage
        assertEquals("JetStream", status.getBackingStore());
        assertNotNull(status.toString()); // coverage

        if (atLeast2_10()) {
            assertEquals(1, status.getMetadata().size());
            assertEquals("meta-bar", status.getMetadata().get("meta-foo"));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private ByteArrayOutputStream validateGet(ObjectStore os, String bucket, String objectName, long len, long chunks, int chunkSize) throws IOException, JetStreamApiException, InterruptedException, NoSuchAlgorithmException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectInfo oi = os.get(objectName, baos);
        assertEquals(len, baos.size());
        validateObjectInfo(oi, bucket, objectName, len, chunks, chunkSize);
        return baos;
    }

    private ObjectInfo validateObjectInfo(ObjectInfo oi, String bucket, String objectName, long size, long chunks, int chunkSize) {
        return validateObjectInfo(oi, bucket, objectName, "object-desc", true, size, chunks, chunkSize);
    }

    private ObjectInfo validateObjectInfo(ObjectInfo oi, String bucket, String objectName, String objectDesc, boolean headers, long size, long chunks, int chunkSize) {
        assertEquals(bucket, oi.getBucket());
        assertEquals(objectName, oi.getObjectName());
        if (objectDesc == null) {
            assertNull(oi.getDescription());
        }
        else {
            assertEquals(objectDesc, oi.getDescription());
        }
        assertEquals(size, oi.getSize());
        assertEquals(chunks, oi.getChunks());
        assertNotNull(oi.getNuid());
        assertFalse(oi.isDeleted());
        assertNotNull(oi.getModified());
        if (chunkSize > 0) {
            assertEquals(chunkSize, oi.getObjectMeta().getObjectMetaOptions().getChunkSize());
        }
        if (headers) {
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
        return oi;
    }

    @SuppressWarnings("SameParameterValue")
    private static Object[] getInput(int size) {
        File found = null;
        long foundLen = Long.MAX_VALUE;
        final String classPath = System.getProperty("java.class.path", ".");
        final String[] classPathElements = classPath.split(File.pathSeparator);
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
        return new Object[] {foundLen, found};
    }

    @Test
    public void testManageGetBucketNamesStatuses() throws Exception {
        jsServer.run(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();

            // create bucket 1
            String bucket1 = bucket();
            osm.create(ObjectStoreConfiguration.builder(bucket1)
                .storageType(StorageType.Memory)
                .build());

            // create bucket 2
            String bucket2 = bucket();
            osm.create(ObjectStoreConfiguration.builder(bucket2)
                .storageType(StorageType.Memory)
                .build());

            List<ObjectStoreStatus> infos = osm.getStatuses();
            assertEquals(2, infos.size());
            List<String> buckets = new ArrayList<>();
            for (ObjectStoreStatus status : infos) {
                buckets.add(status.getBucketName());
            }
            assertEquals(2, buckets.size());
            assertTrue(buckets.contains(bucket1));
            assertTrue(buckets.contains(bucket2));

            buckets = osm.getBucketNames();
            assertTrue(buckets.contains(bucket1));
            assertTrue(buckets.contains(bucket2));
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
        jsServer.run(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();

            String bucket1 = bucket();
            String bucket2 = bucket();

            osm.create(ObjectStoreConfiguration.builder(bucket1).storageType(StorageType.Memory).build());
            ObjectStore os1 = nc.objectStore(bucket1);

            osm.create(ObjectStoreConfiguration.builder(bucket2).storageType(StorageType.Memory).build());
            ObjectStore os2 = nc.objectStore(bucket2);

            String name1 = name();
            String name2 = name();
            byte[] bytes2 = dataBytes();
            os1.put(name1, dataBytes());
            os1.put(name2, bytes2);

            ObjectInfo info11 = os1.getInfo(name1);
            ObjectInfo info12 = os1.getInfo(name2);

            // can't overwrite object with a link
            assertClientError(OsObjectAlreadyExists, () -> os1.addLink(info11.getObjectName(), info11));

            // can overwrite a deleted link or another link
            os1.put("overwrite", "over".getBytes());
            os1.delete("overwrite");
            os1.addLink("overwrite", info11);
            os1.addLink("overwrite", info11);

            // can't overwrite object with a bucket link
            assertClientError(OsObjectAlreadyExists, () -> os1.addBucketLink(info11.getObjectName(), os1));

            // can overwrite a deleted link or another link
            os1.delete("overwrite");
            os1.addBucketLink("overwrite", os1);
            os1.addBucketLink("overwrite", os1);

            // Link to individual object.
            ObjectInfo linkTo11 = os1.addLink("linkTo11", info11);
            validateLink(linkTo11, "linkTo11", info11, null);

            // link to a link
            assertClientError(OsCantLinkToLink, () -> os1.addLink("linkToLinkIsErr", linkTo11));

            String name3 = name();
            byte[] bytes3 = dataBytes();
            os2.put(name3, bytes3);
            ObjectInfo info21 = os2.getInfo(name3);

            String crossLinkName = name();
            ObjectInfo crossLink = os2.addLink(crossLinkName, info11);
            validateLink(crossLink, crossLinkName, info11, null);
            ByteArrayOutputStream baosCross = new ByteArrayOutputStream();
            ObjectInfo crossGet = os2.get(crossLink.getObjectName(), baosCross);
            assertEquals(info11, crossGet);
            byte[] bytes = baosCross.toByteArray();
            assertEquals(bytes3.length, bytes.length);

            ObjectInfo bucketLink = os2.addBucketLink("bucketLink", os1);
            validateLink(bucketLink, "bucketLink", null, os1);

            // can't get a bucket
            assertClientError(OsGetLinkToBucket, () -> os2.get(bucketLink.getObjectName(), new ByteArrayOutputStream()));

            // getInfo targetWillBeDeleted still gets info b/c the link is there
            ObjectInfo targetWillBeDeleted = os2.addLink("targetWillBeDeleted", info21);
            validateLink(targetWillBeDeleted, "targetWillBeDeleted", info21, null);
            os2.delete(info21.getObjectName());
            ObjectInfo oiDeleted = os2.getInfo(info21.getObjectName(), true);
            assertTrue(oiDeleted.isDeleted()); // object is deleted but includingDeleted = true
            assertNull(os2.getInfo(info21.getObjectName())); // does includingDeleted = false
            assertNull(os2.getInfo(info21.getObjectName(), false)); // explicit includingDeleted = false
            assertClientError(OsObjectIsDeleted, () -> os2.addLink("willException", oiDeleted));
            ObjectInfo oiLink = os2.getInfo("targetWillBeDeleted");
            assertNotNull(oiLink); // link is still there but link target is deleted
            assertClientError(OsObjectNotFound, () -> os2.get("targetWillBeDeleted", new ByteArrayOutputStream()));
            assertClientError(OsCantLinkToLink, () -> os2.addLink("willException", oiLink)); // can't link to link

            String crossName = name();
            os2.addLink(crossName, info12);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectInfo crossInfo12 = os2.get(crossName, baos);
            assertEquals(info12, crossInfo12);
            assertEquals(bytes2.length, baos.size());
            assertEquals(new String(bytes2), baos.toString());

            ObjectMeta linkInPut = ObjectMeta.builder("linkInPut").link(ObjectLink.object("na", "na")).build();
            assertClientError(OsLinkNotAllowOnPut, () -> os2.put(linkInPut, new ByteArrayInputStream("na".getBytes())));
        });
    }

    private void validateLink(ObjectInfo oiLink, String linkName, ObjectInfo targetInfo, ObjectStore targetBucket) {
        assertEquals(linkName, oiLink.getObjectName());
        assertNotNull(oiLink.getNuid());
        assertNotNull(oiLink.getModified());

        ObjectLink link = oiLink.getLink();
        assertNotNull(link);
        if (targetInfo == null) { // link to bucket
            assertTrue(link.isBucketLink());
            assertFalse(link.isObjectLink());
            assertNull(link.getObjectName());
            assertEquals(link.getBucket(), targetBucket.getBucketName());
        }
        else {
            assertFalse(link.isBucketLink());
            assertTrue(link.isObjectLink());
            assertEquals(link.getObjectName(), targetInfo.getObjectName());
            assertEquals(link.getBucket(), targetInfo.getBucket());
        }
    }

    @Test
    public void testList() throws Exception {
        jsServer.run(nc -> {
            ObjectStoreManagement osm = nc.objectStoreManagement();

            String bucket = bucket();
            osm.create(ObjectStoreConfiguration.builder(bucket).storageType(StorageType.Memory).build());

            ObjectStore os = nc.objectStore(bucket);

            String[] names = new String[] {name(), name(), name(), name(), name()};
            os.put(names[0], dataBytes());
            os.put(names[1], dataBytes());
            os.put(names[2], dataBytes());
            ObjectInfo info = os.put(names[1], dataBytes());

            os.addLink(names[3], info);

            os.put(names[4], dataBytes());
            os.delete(names[4]);

            List<ObjectInfo> list = os.getList();
            assertEquals(4, list.size());

            List<String> gotNames = new ArrayList<>();
            for (int x = 1; x <= 4; x++) {
                gotNames.add(list.get(x-1).getObjectName());
            }

            for (int x = 0; x < 4; x++) {
                assertTrue(gotNames.contains(names[x]));
            }
        });
    }

    @Test
    public void testSeal() throws Exception {
        jsServer.run(nc -> {
            String bucket = bucket();
            ObjectStoreManagement osm = nc.objectStoreManagement();
            osm.create(ObjectStoreConfiguration.builder(bucket)
                .storageType(StorageType.Memory)
                .build());

            ObjectStore os = nc.objectStore(bucket);
            os.put("name", "data".getBytes());

            ObjectStoreStatus status = os.seal();

            assertTrue(status.isSealed());

            assertThrows(JetStreamApiException.class, () -> os.put("another", "data".getBytes()));

            ObjectMeta meta = ObjectMeta.builder("change").build();
            assertThrows(JetStreamApiException.class, () -> os.updateMeta("name", meta));
        });
    }

    @Test
    public void testCompression() throws Exception {
        jsServer.run(TestBase::atLeast2_10, nc -> {
            String bucket = bucket();
            ObjectStoreManagement osm = nc.objectStoreManagement();
            osm.create(ObjectStoreConfiguration.builder(bucket)
                .storageType(StorageType.Memory)
                .compression(true)
                .build());
            ObjectStore os = nc.objectStore(bucket);
            ObjectStoreStatus oss = os.getStatus();
            assertTrue(oss.isCompressed());
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
            if (++endOfDataReceived == 1 && entries.isEmpty()) {
                endBeforeEntries = true;
            }
        }
    }

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

        assertEquals(watcher.beforeWatcher, watcher.endBeforeEntries);

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
    public void testObjectStoreDomains() throws Exception {
        runInJsHubLeaf((hubNc, leafNc) -> {
            ObjectStoreManagement hubOsm = hubNc.objectStoreManagement();
            ObjectStoreManagement leafOsm = leafNc.objectStoreManagement(ObjectStoreOptions.builder().jsDomain(HUB_DOMAIN).build());

            // Create main OS on HUB
            String bucketName = bucket();
            ObjectStoreStatus hubStatus = hubOsm.create(ObjectStoreConfiguration.builder()
                .name(bucketName)
                .storageType(StorageType.Memory)
                .build());
            assertEquals(0, hubStatus.getSize());

            validateStatus(hubOsm, leafOsm, bucketName);

            ObjectStore hubOs = hubNc.objectStore(bucketName);
            ObjectStore leafOs = leafNc.objectStore(bucketName, ObjectStoreOptions.builder().jsDomain(HUB_DOMAIN).build());

            String objectName = name();
            ObjectMeta meta = ObjectMeta.builder(objectName).chunkSize(8 * 1024).build();

            Object[] input = getInput(4 * 8 * 1024);
            File file = (File)input[1];

            // put from hub
            InputStream in = Files.newInputStream(file.toPath());
            hubOs.put(meta, in);
            validateStatus(hubOsm, leafOsm, bucketName);
            validateInfo(hubOs, leafOs, objectName, false);
            validateGets(hubOs, leafOs, objectName, false);

            // delete from leaf
            leafOs.delete(objectName);
            validateStatus(hubOsm, leafOsm, bucketName);
            validateInfo(hubOs, leafOs, objectName, true);
            validateGets(hubOs, leafOs, objectName, true);

            // put from leaf
            in = Files.newInputStream(file.toPath());
            leafOs.put(meta, in);
            validateStatus(hubOsm, leafOsm, bucketName);
            validateInfo(hubOs, leafOs, objectName, false);
            validateGets(hubOs, leafOs, objectName, false);

            // delete from hub
            hubOs.delete(objectName);
            validateStatus(hubOsm, leafOsm, bucketName);
            validateInfo(hubOs, leafOs, objectName, true);
            validateGets(hubOs, leafOs, objectName, true);
        });
    }

    private static void validateStatus(ObjectStoreManagement hubOsm, ObjectStoreManagement leafOsm, String bucketName) throws IOException, JetStreamApiException {
        ObjectStoreStatus hubStatus = hubOsm.getStatus(bucketName);
        ObjectStoreStatus leafStatus = leafOsm.getStatus(bucketName);
        assertEquals(hubStatus.getSize(), leafStatus.getSize());
    }

    private static void validateInfo(ObjectStore hubOs, ObjectStore leafOs, String objectName, boolean deleted) throws IOException, JetStreamApiException {
        ObjectInfo hubInfo = hubOs.getInfo(objectName);
        ObjectInfo leafInfo = leafOs.getInfo(objectName);
        if (deleted) {
            assertNull(hubInfo);
            assertNull(leafInfo);
        }
        else {
            assertEquals(hubInfo.getNuid(), leafInfo.getNuid());
            assertEquals(hubInfo.getSize(), leafInfo.getSize());
            assertEquals(hubInfo.getObjectMeta().getObjectName(), leafInfo.getObjectMeta().getObjectName());
        }
    }

    private static void validateGets(ObjectStore hubOs, ObjectStore leafOs, String objectName, boolean deleted) throws IOException, JetStreamApiException, InterruptedException, NoSuchAlgorithmException {
        ByteArrayOutputStream hubOut = new ByteArrayOutputStream();
        ByteArrayOutputStream leafOut = new ByteArrayOutputStream();

        if (deleted) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> hubOs.get(objectName, hubOut));
            assertTrue(e.getMessage().contains("OS-90201"));
            e = assertThrows(IllegalArgumentException.class, () -> leafOs.get(objectName, hubOut));
            assertTrue(e.getMessage().contains("OS-90201"));
        }
        else {
            hubOs.get(objectName, hubOut);
            leafOs.get(objectName, leafOut);

            byte[] hubBytes = hubOut.toByteArray();
            byte[] leafBytes = leafOut.toByteArray();

            assertArrayEquals(hubBytes, leafBytes);
        }
    }
}
