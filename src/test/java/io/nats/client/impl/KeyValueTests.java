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

package io.nats.client.impl;

import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.*;
import io.nats.client.support.NatsJetStreamConstants;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class KeyValueTests extends JetStreamTestBase {

    @Test
    public void testBasic() throws Exception {
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
            KeyValueManagement kvm = nc.keyValueManagement();

            // create the bucket
            BucketConfiguration bc = BucketConfiguration.builder()
                    .name(BUCKET)
                    .maxHistory(3)
                    .storageType(StorageType.Memory)
                    .build();

            BucketInfo bi = kvm.createBucket(bc);

            bc = bi.getConfiguration();
            assertEquals(BUCKET, bc.getName());
            assertEquals(NatsJetStreamConstants.KV_STREAM_PREFIX + BUCKET, bc.getBackingConfig().getName());
            assertEquals(-1, bc.getMaxValues());
            assertEquals(3, bc.getMaxHistory());
            assertEquals(-1, bc.getMaxBucketSize());
            assertEquals(-1, bc.getMaxValueSize());
            assertEquals(Duration.ZERO, bc.getTtl());
            assertEquals(StorageType.Memory, bc.getStorageType());
            assertEquals(1, bc.getReplicas());
            assertEquals(Duration.ofMinutes(2), bc.getDuplicateWindow());
            assertTrue(now <= bi.getCreateTime().toEpochSecond());

            assertEquals(0, bi.getRecordCount());
            assertEquals(0, bi.getByteCount());
            assertEquals(0, bi.getLastSequence());

            // get the kv context for the specific bucket
            KeyValue kv = nc.keyValue(BUCKET);

            // Put some keys. Each key is put in a subject in the bucket (stream)
            // The put returns the sequence number in the bucket (stream)
            assertEquals(1, kv.put(byteKey, byteValue1.getBytes()));
            assertEquals(2, kv.put(stringKey, stringValue1));
            assertEquals(3, kv.put(longKey, 1));

            // retrieve the values. all types are stored as bytes
            // so you can always get the bytes directly
            assertEquals(byteValue1, new String(kv.getValue(byteKey)));
            assertEquals(stringValue1, new String(kv.getValue(stringKey)));
            assertEquals(Long.toString(1), new String(kv.getValue(longKey)));

            // if you know the value is not binary and can safely be read
            // as a UTF-8 string, the getStringValue method is ok to use
            assertEquals(byteValue1, kv.getStringValue(byteKey));
            assertEquals(stringValue1, kv.getStringValue(stringKey));
            assertEquals(Long.toString(1), kv.getStringValue(longKey));

            // if you know the value is a long, you can use
            // the getLongValue method
            // if it's not a number a NumberFormatException is thrown
            assertEquals(1, kv.getLongValue(longKey));
            assertThrows(NumberFormatException.class, () -> kv.getLongValue(stringKey));

            // entry gives detail about latest entry of the key
            // this might mean
            assertEntry(BUCKET, byteKey, KvOperation.PUT, 1, byteValue1, now, kv.getEntry(byteKey));
            assertEntry(BUCKET, stringKey, KvOperation.PUT, 2, stringValue1, now, kv.getEntry(stringKey));
            assertEntry(BUCKET, longKey, KvOperation.PUT, 3, Long.toString(1), now, kv.getEntry(longKey));

            // let's check the bucket info
            bi = kvm.getBucketInfo(BUCKET);
            assertEquals(3, bi.getRecordCount());
            assertEquals(3, bi.getLastSequence());

            // delete a key
            assertEquals(4, kv.delete(byteKey));
            // it's value is now null
            assertNull(kv.getValue(byteKey));

            // but it's entry still exists
            assertEntry(BUCKET, byteKey, KvOperation.DEL, 4, null, now, kv.getEntry(byteKey));

            // let's check the bucket info
            bi = kvm.getBucketInfo(BUCKET);
            assertEquals(4, bi.getRecordCount());
            assertEquals(4, bi.getLastSequence());

            // if the key has been deleted or not found / never existed
            // all varieties of get will return null
            assertNull(kv.getValue(byteKey));
            assertNull(kv.getStringValue(byteKey));
            assertNull(kv.getLongValue(byteKey));
            assertNull(kv.getValue(notFoundKey));
            assertNull(kv.getStringValue(notFoundKey));
            assertNull(kv.getLongValue(notFoundKey));

            // Update values. You can even update a deleted key
            assertEquals(5, kv.put(byteKey, byteValue2.getBytes()));
            assertEquals(6, kv.put(stringKey, stringValue2));
            assertEquals(7, kv.put(longKey, 2));

            // values after updates
            assertEquals(byteValue2, new String(kv.getValue(byteKey)));
            assertEquals(stringValue2, kv.getStringValue(stringKey));
            assertEquals(2, kv.getLongValue(longKey));

            // entry after update
            assertEntry(BUCKET, byteKey, KvOperation.PUT, 5, byteValue2, now, kv.getEntry(byteKey));
            assertEntry(BUCKET, stringKey, KvOperation.PUT, 6, stringValue2, now, kv.getEntry(stringKey));
            assertEntry(BUCKET, longKey, KvOperation.PUT, 7, Long.toString(2), now, kv.getEntry(longKey));

            // let's check the bucket info
            bi = kvm.getBucketInfo(BUCKET);
            assertEquals(7, bi.getRecordCount());
            assertEquals(7, bi.getLastSequence());

            // make sure it only keeps the correct amount of history
            assertEquals(8, kv.put(longKey, 3));
            assertEquals(3, kv.getLongValue(longKey));

            bi = kvm.getBucketInfo(BUCKET);
            assertEquals(8, bi.getRecordCount());
            assertEquals(8, bi.getLastSequence());

            // this would be the 4th entry for the longKey
            // sp the total records will stay the same
            assertEquals(9, kv.put(longKey, 4));
            assertEquals(4, kv.getLongValue(longKey));

            bi = kvm.getBucketInfo(BUCKET);
            assertEquals(8, bi.getRecordCount());
            assertEquals(9, bi.getLastSequence());

            // delete the bucket
            kvm.deleteBucket(BUCKET);
            assertThrows(JetStreamApiException.class, () -> kvm.getBucketInfo(BUCKET));

            // coverage
            assertNotNull(bi.toString());
        });
    }

    @SuppressWarnings("SameParameterValue")
    private void assertEntry(String bucket, String key, KvOperation op, long seq, String value, long now, KvEntry entry) {
        assertEquals(bucket, entry.getBucket());
        assertEquals(key, entry.getKey());
        assertEquals(op, entry.getKvOperation());
        assertEquals(seq, entry.getSeq());
        if (op == KvOperation.DEL) {
            assertNull(entry.getData());
        }
        else {
            assertEquals(value, new String(entry.getData()));
        }
        assertTrue(now <= entry.getCreated().toEpochSecond());

        // coverage
        assertNotNull(entry.toString());
    }
}
