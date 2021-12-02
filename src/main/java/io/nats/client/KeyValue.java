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
package io.nats.client;

import io.nats.client.api.KeyValueEntry;
import io.nats.client.api.KeyValueOperation;
import io.nats.client.api.KeyValueStatus;
import io.nats.client.api.KeyValueWatcher;
import io.nats.client.impl.NatsKeyValueWatchSubscription;

import java.io.IOException;
import java.util.List;

/**
 * Key Value Store Management context for creation and access to key value buckets.
 */
public interface KeyValue {

    /**
     * Get the name of the bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @return the name
     */
    String getBucketName();

    /**
     * Get the byte[] value for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @return the value or null if not found or deleted
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    byte[] getValue(String key) throws IOException, JetStreamApiException;

    /**
     * Get the string value for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @return the value as UTF-8 string or null if not found or deleted
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    String getStringValue(String key) throws IOException, JetStreamApiException;

    /**
     * Get the number value for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @return the value as number or null if not found or deleted
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    Long getLongValue(String key) throws IOException, JetStreamApiException;

    /**
     * Get the full entry for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @return the KvEntry object or null if not found.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    KeyValueEntry getEntry(String key) throws IOException, JetStreamApiException;

    /**
     * Put a byte[] as the value for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @param value the bytes of the value
     * @return the sequence number for the PUT record
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    long put(String key, byte[] value) throws IOException, JetStreamApiException;

    /**
     * Put a string as the value for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @param value the UTF-8 string
     * @return the sequence number for the PUT record
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    long put(String key, String value) throws IOException, JetStreamApiException;

    /**
     * Put a long as the value for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @param value the number
     * @return the sequence number for the PUT record
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    long put(String key, long value) throws IOException, JetStreamApiException;

    /**
     * Deletes a key.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void delete(String key) throws IOException, JetStreamApiException;

    /**
     * Purge all values/history from the specific key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void purge(String key) throws IOException, JetStreamApiException;

    /**
     * Watch updates for a specific key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @param watcher the watcher
     * @param metaOnly flag to receive KeyValueEntry metadata only. Use false to receive both metadata and headers
     * @param operations the types of operations to watch for. If not supplied, all types will be watched for.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, boolean metaOnly, KeyValueOperation... operations) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Watch updates for all keys
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param watcher the watcher
     * @param metaOnly flag to receive KeyValueEntry metadata only. Use false to receive both metadata and headers
     * @param operations the types of operations to watch for. If not supplied, all types will be watched for.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, boolean metaOnly, KeyValueOperation... operations) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get the set of the keys in a bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @return Set of keys
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    List<String> keys() throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get the history (list of KvEntry) for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @return List of KvEntry
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    List<KeyValueEntry> history(String key) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Remove all current delete markers
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    void purgeDeletes() throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get the KeyValueStatus object
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @return the status object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    KeyValueStatus getStatus() throws IOException, JetStreamApiException, InterruptedException;
}
