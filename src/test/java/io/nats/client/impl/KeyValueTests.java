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
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.nats.client.support.NatsKeyValueUtil.streamName;
import static io.nats.client.support.NatsKeyValueUtil.streamSubject;
import static org.junit.jupiter.api.Assertions.*;

public class KeyValueTests extends JetStreamTestBase {

    @Test
    public void testWorkflow() throws Exception {
        long now = ZonedDateTime.now().toEpochSecond();

        String byteKey = "byteKey";
        String stringKey = "stringKey";
        String longKey = "longKey";
        String notFoundKey = "notFound";
        String byteValue1 = "Byte Value 1";
        String byteValue2 = "Byte Value 2";
        String stringValue1 = "String Value 1";
        String stringValue2 = "String Value 2";

        runInJsServer(nc -> {
            // get the kv management context
            KeyValueManagement kvm = nc.keyValueManagement(JetStreamOptions.DEFAULT_JS_OPTIONS); // use options here for coverage

            // create the bucket
            KeyValueConfiguration kvc = KeyValueConfiguration.builder()
                    .name(BUCKET)
                    .maxHistoryPerKey(3)
                    .storageType(StorageType.Memory)
                    .build();

            KeyValueStatus status = kvm.create(kvc);

            kvc = status.getConfiguration();
            assertEquals(BUCKET, status.getBucketName());
            assertEquals(BUCKET, kvc.getBucketName());
            assertEquals(streamName(BUCKET), kvc.getBackingConfig().getName());
            assertEquals(-1, kvc.getMaxValues());
            assertEquals(3, status.getMaxHistoryPerKey());
            assertEquals(3, kvc.getMaxHistoryPerKey());
            assertEquals(-1, kvc.getMaxBucketSize());
            assertEquals(-1, kvc.getMaxValueBytes());
            assertEquals(Duration.ZERO, status.getTtl());
            assertEquals(Duration.ZERO, kvc.getTtl());
            assertEquals(StorageType.Memory, kvc.getStorageType());
            assertEquals(1, kvc.getReplicas());
            assertEquals(0, status.getEntryCount());
            assertEquals("JetStream", status.getBackingStore());

            // get the kv context for the specific bucket
            KeyValue kv = nc.keyValue(BUCKET, JetStreamOptions.DEFAULT_JS_OPTIONS); // use options here for coverage

            // Put some keys. Each key is put in a subject in the bucket (stream)
            // The put returns the sequence number in the bucket (stream)
            assertEquals(1, kv.put(byteKey, byteValue1.getBytes()));
            assertEquals(2, kv.put(stringKey, stringValue1));
            assertEquals(3, kv.put(longKey, 1));

            // retrieve the values. all types are stored as bytes
            // so you can always get the bytes directly
            assertEquals(byteValue1, new String(kv.get(byteKey).getValue()));
            assertEquals(stringValue1, new String(kv.get(stringKey).getValue()));
            assertEquals("1", new String(kv.get(longKey).getValue()));

            // if you know the value is not binary and can safely be read
            // as a UTF-8 string, the getStringValue method is ok to use
            assertEquals(byteValue1, kv.get(byteKey).getValueAsString());
            assertEquals(stringValue1, kv.get(stringKey).getValueAsString());
            assertEquals("1", kv.get(longKey).getValueAsString());

            // if you know the value is a long, you can use
            // the getLongValue method
            // if it's not a number a NumberFormatException is thrown
            assertEquals(1, kv.get(longKey).getValueAsLong());
            assertThrows(NumberFormatException.class, () -> kv.get(stringKey).getValueAsLong());

            // going to manually track history for verification later
            List<KeyValueEntry> byteHistory = new ArrayList<>();
            List<KeyValueEntry> stringHistory = new ArrayList<>();
            List<KeyValueEntry> longHistory = new ArrayList<>();

            // entry gives detail about latest entry of the key
            byteHistory.add(
                    assertEntry(BUCKET, byteKey, KeyValueOperation.PUT, 1, byteValue1, now, kv.get(byteKey)));

            stringHistory.add(
                    assertEntry(BUCKET, stringKey, KeyValueOperation.PUT, 2, stringValue1, now, kv.get(stringKey)));

            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 3, "1", now, kv.get(longKey)));

            // history gives detail about the key
            assertHistory(byteHistory, kv.history(byteKey));
            assertHistory(stringHistory, kv.history(stringKey));
            assertHistory(longHistory, kv.history(longKey));

            // let's check the bucket info
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(3, status.getEntryCount());
            assertEquals(3, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // delete a key. Its entry will still exist, but it's value is null
            kv.delete(byteKey);

            byteHistory.add(
                    assertEntry(BUCKET, byteKey, KeyValueOperation.DELETE, 4, null, now, kv.get(byteKey)));
            assertHistory(byteHistory, kv.history(byteKey));

            // hashCode coverage
            assertEquals(byteHistory.get(0).hashCode(), byteHistory.get(0).hashCode());
            assertNotEquals(byteHistory.get(0).hashCode(), byteHistory.get(1).hashCode());

            // let's check the bucket info
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(4, status.getEntryCount());
            assertEquals(4, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // if the key has been deleted
            // all varieties of get will return null
            assertNull(kv.get(byteKey).getValue());
            assertNull(kv.get(byteKey).getValueAsString());
            assertNull(kv.get(byteKey).getValueAsLong());

            // if the key does not exist (no history) there is no entry
            assertNull(kv.get(notFoundKey));

            // Update values. You can even update a deleted key
            assertEquals(5, kv.put(byteKey, byteValue2.getBytes()));
            assertEquals(6, kv.put(stringKey, stringValue2));
            assertEquals(7, kv.put(longKey, 2));

            // values after updates
            assertEquals(byteValue2, new String(kv.get(byteKey).getValue()));
            assertEquals(stringValue2, kv.get(stringKey).getValueAsString());
            assertEquals(2, kv.get(longKey).getValueAsLong());

            // entry and history after update
            byteHistory.add(
                    assertEntry(BUCKET, byteKey, KeyValueOperation.PUT, 5, byteValue2, now, kv.get(byteKey)));
            assertHistory(byteHistory, kv.history(byteKey));

            stringHistory.add(
                    assertEntry(BUCKET, stringKey, KeyValueOperation.PUT, 6, stringValue2, now, kv.get(stringKey)));
            assertHistory(stringHistory, kv.history(stringKey));

            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 7, "2", now, kv.get(longKey)));
            assertHistory(longHistory, kv.history(longKey));

            // let's check the bucket info
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(7, status.getEntryCount());
            assertEquals(7, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // make sure it only keeps the correct amount of history
            assertEquals(8, kv.put(longKey, 3));
            assertEquals(3, kv.get(longKey).getValueAsLong());

            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 8, "3", now, kv.get(longKey)));
            assertHistory(longHistory, kv.history(longKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(8, status.getEntryCount());
            assertEquals(8, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // this would be the 4th entry for the longKey
            // sp the total records will stay the same
            assertEquals(9, kv.put(longKey, 4));
            assertEquals(4, kv.get(longKey).getValueAsLong());

            // history only retains 3 records
            longHistory.remove(0);
            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 9, "4", now, kv.get(longKey)));
            assertHistory(longHistory, kv.history(longKey));

            // record count does not increase
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(8, status.getEntryCount());
            assertEquals(9, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // should have exactly these 3 keys
            assertKeys(kv.keys(), byteKey, stringKey, longKey);

            // purge
            kv.purge(longKey);
            longHistory.clear();
            longHistory.add(
                assertEntry(BUCKET, longKey, KeyValueOperation.PURGE, 10, null, now, kv.get(longKey)));
            assertHistory(longHistory, kv.history(longKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(6, status.getEntryCount()); // includes 1 purge
            assertEquals(10, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // only 2 keys now
            assertKeys(kv.keys(), byteKey, stringKey);

            kv.purge(byteKey);
            byteHistory.clear();
            byteHistory.add(
                assertEntry(BUCKET, byteKey, KeyValueOperation.PURGE, 11, null, now, kv.get(byteKey)));
            assertHistory(byteHistory, kv.history(byteKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(4, status.getEntryCount()); // includes 2 purges
            assertEquals(11, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // only 1 key now
            assertKeys(kv.keys(), stringKey);

            kv.purge(stringKey);
            stringHistory.clear();
            stringHistory.add(
                assertEntry(BUCKET, stringKey, KeyValueOperation.PURGE, 12, null, now, kv.get(stringKey)));
            assertHistory(stringHistory, kv.history(stringKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(3, status.getEntryCount()); // 3 purges
            assertEquals(12, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // no more keys left
            assertKeys(kv.keys());

            // clear things
            kv.purgeDeletes();
            status = kvm.getBucketInfo(BUCKET);
            assertEquals(0, status.getEntryCount()); // purges are all gone
            assertEquals(12, status.getBackingStreamInfo().getStreamState().getLastSequence());

            longHistory.clear();
            assertHistory(longHistory, kv.history(longKey));

            stringHistory.clear();
            assertHistory(stringHistory, kv.history(stringKey));

            // put some more
            assertEquals(13, kv.put(longKey, 110));
            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 13, "110", now, kv.get(longKey)));

            assertEquals(14, kv.put(longKey, 111));
            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 14, "111", now, kv.get(longKey)));

            assertEquals(15, kv.put(longKey, 112));
            longHistory.add(
                    assertEntry(BUCKET, longKey, KeyValueOperation.PUT, 15, "112", now, kv.get(longKey)));

            assertEquals(16, kv.put(stringKey, stringValue1));
            stringHistory.add(
                    assertEntry(BUCKET, stringKey, KeyValueOperation.PUT, 16, stringValue1, now, kv.get(stringKey)));

            assertEquals(17, kv.put(stringKey, stringValue2));
            stringHistory.add(
                    assertEntry(BUCKET, stringKey, KeyValueOperation.PUT, 17, stringValue2, now, kv.get(stringKey)));

            assertHistory(longHistory, kv.history(longKey));
            assertHistory(stringHistory, kv.history(stringKey));

            status = kvm.getBucketInfo(BUCKET);
            assertEquals(5, status.getEntryCount());
            assertEquals(17, status.getBackingStreamInfo().getStreamState().getLastSequence());

            // delete the bucket
            kvm.delete(BUCKET);
            assertThrows(JetStreamApiException.class, () -> kvm.delete(BUCKET));
            assertThrows(JetStreamApiException.class, () -> kvm.getBucketInfo(BUCKET));

            assertEquals(0, kvm.getBucketsNames().size());
        });
    }

    @Test
    public void testKeys() throws Exception {
        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create bucket 1
            kvm.create(KeyValueConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .build());

            KeyValue kv = nc.keyValue(BUCKET);
            for (int x = 1; x <= 10; x++) {
                kv.put("k" + x, x);
            }

            List<String> keys = kv.keys();
            assertEquals(10, keys.size());

            kv.delete("k1");
            kv.delete("k3");
            kv.delete("k5");
            kv.purge("k7");
            kv.purge("k9");

            keys = kv.keys();
            assertEquals(5, keys.size());

            for (int x = 2; x <= 10; x += 2) {
                assertTrue(keys.contains("k" + x));
            }
        });
    }

    @Test
    public void testHistoryDeletePurge() throws Exception {
        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create bucket
            kvm.create(KeyValueConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .maxHistoryPerKey(64)
                .build());

            KeyValue kv = nc.keyValue(BUCKET);
            kv.put(KEY, "a");
            kv.put(KEY, "b");
            kv.put(KEY, "c");
            List<KeyValueEntry> list = kv.history(KEY);
            assertEquals(3, list.size());

            kv.delete(KEY);
            list = kv.history(KEY);
            assertEquals(4, list.size());

            kv.purge(KEY);
            list = kv.history(KEY);
            assertEquals(1, list.size());
        });
    }

    @Test
    public void testPurgeDeletes() throws Exception {
        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create bucket
            kvm.create(KeyValueConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .maxHistoryPerKey(64)
                .build());

            KeyValue kv = nc.keyValue(BUCKET);
            kv.put(key(1), "a");
            kv.delete(key(1));
            kv.put(key(2), "b");
            kv.put(key(3), "c");
            kv.put(key(4), "d");
            kv.purge(key(4));

            JetStream js = nc.jetStream();

            JetStreamSubscription sub = js.subscribe(streamSubject(BUCKET));

            Message m = sub.nextMessage(1000);
            assertEquals("a", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertEquals(0, m.getData().length);

            m = sub.nextMessage(1000);
            assertEquals("b", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertEquals("c", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertEquals(0, m.getData().length);

            sub.unsubscribe();

            kv.purgeDeletes();
            sub = js.subscribe(streamSubject(BUCKET));

            m = sub.nextMessage(1000);
            assertEquals("b", new String(m.getData()));

            m = sub.nextMessage(1000);
            assertEquals("c", new String(m.getData()));

            sub.unsubscribe();
        });
    }

    @Test
    public void testManageGetBucketNames() throws Exception {
        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            // create bucket 1
            kvm.create(KeyValueConfiguration.builder()
                    .name(bucket(1))
                    .storageType(StorageType.Memory)
                    .build());

            // create bucket 2
            kvm.create(KeyValueConfiguration.builder()
                    .name(bucket(2))
                    .storageType(StorageType.Memory)
                    .build());

            createMemoryStream(nc, stream(1));
            createMemoryStream(nc, stream(2));

            List<String> buckets = kvm.getBucketsNames();
            assertEquals(2, buckets.size());
            assertTrue(buckets.contains(bucket(1)));
            assertTrue(buckets.contains(bucket(2)));
        });
    }

    private void assertKeys(List<String> apiKeys, String... manualKeys) {
        assertEquals(manualKeys.length, apiKeys.size());
        for (String k : manualKeys) {
            assertTrue(apiKeys.contains(k));
        }
    }

    private void assertHistory(List<KeyValueEntry> manualHistory, List<KeyValueEntry> apiHistory) {
        assertEquals(apiHistory.size(), manualHistory.size());
        for (int x = 0; x < apiHistory.size(); x++) {
            assertKvEquals(apiHistory.get(x), manualHistory.get(x));
        }
    }

    @SuppressWarnings("SameParameterValue")
    private KeyValueEntry assertEntry(String bucket, String key, KeyValueOperation op, long seq, String value, long now, KeyValueEntry entry) {
        assertEquals(bucket, entry.getBucket());
        assertEquals(key, entry.getKey());
        assertEquals(op, entry.getOperation());
        assertEquals(seq, entry.getRevision());
        assertEquals(0, entry.getDelta());
        if (op == KeyValueOperation.PUT) {
            assertEquals(value, new String(entry.getValue()));
        }
        else {
            assertNull(entry.getValue());
        }
        assertTrue(now <= entry.getCreated().toEpochSecond());

        // coverage
        assertNotNull(entry.toString());
        return entry;
    }

    private void assertKvEquals(KeyValueEntry kv1, KeyValueEntry kv2) {
        assertEquals(kv1.getOperation(), kv2.getOperation());
        assertEquals(kv1.getRevision(), kv2.getRevision());
        assertEquals(kv1.getBucket(), kv2.getBucket());
        assertEquals(kv1.getKey(), kv2.getKey());
        assertTrue(Arrays.equals(kv1.getValue(), kv2.getValue()));
        long es1 = kv1.getCreated().toEpochSecond();
        long es2 = kv2.getCreated().toEpochSecond();
        assertEquals(es1, es2);
    }

    static class TestKeyValueWatcher implements KeyValueWatcher {
        public List<KeyValueEntry> entries = new ArrayList<>();

        @Override
        public void watch(KeyValueEntry kve) {
            entries.add(kve);
        }
    }

    @Test
    public void testWatch() throws Exception {
        String keyNull = "key.nl";
        String key1 = "key.1";
        String key2 = "key.2";

        runInJsServer(nc -> {
            KeyValueManagement kvm = nc.keyValueManagement();

            kvm.create(KeyValueConfiguration.builder()
                .name(BUCKET)
                .storageType(StorageType.Memory)
                .build());

            KeyValue kv = nc.keyValue(BUCKET);

            TestKeyValueWatcher key1FullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher key1MetaWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher key2FullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher key2MetaWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher allPutWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher allDelWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher allPurgeWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher allDelPurgeWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher allFullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher allMetaWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher starFullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher starMetaWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher gtFullWatcher = new TestKeyValueWatcher();
            TestKeyValueWatcher gtMetaWatcher = new TestKeyValueWatcher();

            List<NatsKeyValueWatchSubscription> subs = new ArrayList<>();
            subs.add(kv.watch(key1, key1FullWatcher, false));
            subs.add(kv.watch(key1, key1MetaWatcher, true));
            subs.add(kv.watch(key2, key2FullWatcher, false));
            subs.add(kv.watch(key2, key2MetaWatcher, true));
            subs.add(kv.watchAll(allPutWatcher, false, KeyValueOperation.PUT));
            subs.add(kv.watchAll(allDelWatcher, true, KeyValueOperation.DELETE));
            subs.add(kv.watchAll(allPurgeWatcher, true, KeyValueOperation.PURGE));
            subs.add(kv.watchAll(allDelPurgeWatcher, true, KeyValueOperation.DELETE, KeyValueOperation.PURGE));
            subs.add(kv.watchAll(allFullWatcher, false));
            subs.add(kv.watchAll(allMetaWatcher, true));
            subs.add(kv.watch("key.*", starFullWatcher, false));
            subs.add(kv.watch("key.*", starMetaWatcher, true));
            subs.add(kv.watch("key.>", gtFullWatcher, false));
            subs.add(kv.watch("key.>", gtMetaWatcher, true));

            kv.put(key1, "a");
            kv.put(key1, "aa");
            kv.put(key2, "z");
            kv.put(key2, "zz");
            kv.delete(key1);
            kv.delete(key2);
            kv.put(key1, "aaa");
            kv.put(key2, "zzz");
            kv.delete(key1);
            kv.delete(key2);
            kv.purge(key1);
            kv.purge(key2);
            kv.put(keyNull, (byte[])null);

            sleep(2000); // give time for the watches to get messages

            Object[] key1Expecteds = new Object[] {
                "a", "aa", KeyValueOperation.DELETE, "aaa", KeyValueOperation.DELETE, KeyValueOperation.PURGE
            };

            Object[] key2Expecteds = new Object[] {
                "z", "zz", KeyValueOperation.DELETE, "zzz", KeyValueOperation.DELETE, KeyValueOperation.PURGE
            };

            Object[] allExpecteds = new Object[] {
                "a", "aa", "z", "zz",
                KeyValueOperation.DELETE, KeyValueOperation.DELETE,
                "aaa", "zzz",
                KeyValueOperation.DELETE, KeyValueOperation.DELETE,
                KeyValueOperation.PURGE, KeyValueOperation.PURGE,
                null
            };

            Object[] allPuts = new Object[] {
                "a", "aa", "z", "zz", "aaa", "zzz", null
            };

            Object[] allDels = new Object[] {
                KeyValueOperation.DELETE, KeyValueOperation.DELETE,
                KeyValueOperation.DELETE, KeyValueOperation.DELETE
            };

            Object[] allPurges = new Object[] {
                KeyValueOperation.PURGE, KeyValueOperation.PURGE
            };

            Object[] allDelsPurges = new Object[] {
                KeyValueOperation.DELETE, KeyValueOperation.DELETE,
                KeyValueOperation.DELETE, KeyValueOperation.DELETE,
                KeyValueOperation.PURGE, KeyValueOperation.PURGE
            };

            // unsubscribe so the watchers don't get any more messages
            for (NatsKeyValueWatchSubscription sub : subs) {
                sub.unsubscribe();
            }

            // put some more data which should not be seen by watches
            kv.put(key1, "aaaa");
            kv.put(key2, "zzzz");

            validateWatcher(key1Expecteds, key1FullWatcher, false);
            validateWatcher(key1Expecteds, key1MetaWatcher, true);
            validateWatcher(key2Expecteds, key2FullWatcher, false);
            validateWatcher(key2Expecteds, key2MetaWatcher, true);
            validateWatcher(allPuts, allPutWatcher, false);
            validateWatcher(allDels, allDelWatcher, true);
            validateWatcher(allPurges, allPurgeWatcher, true);
            validateWatcher(allDelsPurges, allDelPurgeWatcher, true);
            validateWatcher(allExpecteds, allFullWatcher, false);
            validateWatcher(allExpecteds, allMetaWatcher, true);
            validateWatcher(allExpecteds, starFullWatcher, false);
            validateWatcher(allExpecteds, starMetaWatcher, true);
            validateWatcher(allExpecteds, gtFullWatcher, false);
            validateWatcher(allExpecteds, gtMetaWatcher, true);
        });
    }

    private void validateWatcher(Object[] expecteds, TestKeyValueWatcher watcher, boolean metaOnly) {
        int aix = 0;
        ZonedDateTime lastCreated = ZonedDateTime.of(2000, 4, 1, 0, 0, 0, 0, ZoneId.systemDefault());
        long lastRevision = -1;
        for (KeyValueEntry kve : watcher.entries) {

            assertTrue(kve.getCreated().isAfter(lastCreated) || kve.getCreated().isEqual(lastCreated));
            lastCreated = kve.getCreated();

            assertTrue(lastRevision < kve.getRevision());
            lastRevision = kve.getRevision();

            Object expected = expecteds[aix++];
            if (expected == null) {
                assertSame(KeyValueOperation.PUT, kve.getOperation());
                assertTrue(kve.getValue() == null || kve.getValue().length == 0);
                assertEquals(0, kve.getDataLen());
            }
            else if (expected instanceof String) {
                assertSame(KeyValueOperation.PUT, kve.getOperation());
                String s = (String) expected;
                if (metaOnly) {
                    assertTrue(kve.getValue() == null || kve.getValue().length == 0);
                    assertEquals(s.length(), kve.getDataLen());
                }
                else {
                    assertNotNull(kve.getValue());
                    assertEquals(s.length(), kve.getDataLen());
                    assertEquals(s, kve.getValueAsString());
                }
            }
            else {
                assertTrue(kve.getValue() == null || kve.getValue().length == 0);
                assertEquals(0, kve.getDataLen());
                assertSame(expected, kve.getOperation());
            }
        }
    }
}