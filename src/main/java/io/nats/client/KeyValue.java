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

import io.nats.client.api.*;
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
     * Get the entry for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @return the KvEntry object or null if not found.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    KeyValueEntry get(String key) throws IOException, JetStreamApiException;

    /**
     * Put a byte[] as the value for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @param value the bytes of the value
     * @return the revision number for the key
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
     * @return the revision number for the key
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
     * @return the revision number for the key
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    long put(String key, Number value) throws IOException, JetStreamApiException;

    /**
     * Put as the value for a key iff the key does not exist (there is no history)
     * or is deleted (history shows the key is deleted)
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @param value the bytes of the value
     * @return the revision number for the key
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    long create(String key, byte[] value) throws IOException, JetStreamApiException;

    /**
     * Put as the value for a key iff the key exists and its last revision matches the expected
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param key the key
     * @param value the bytes of the value
     * @param expectedRevision the expected last revision
     * @return the revision number for the key
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    long update(String key, byte[] value, long expectedRevision) throws IOException, JetStreamApiException;

    /**
     * Soft deletes the key by placing a delete marker.
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
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Watch updates for all keys
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param watcher the watcher
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get a list of the keys in a bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @return List of keys
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    List<String> keys() throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get the history (list of KeyValueEntry) for a key
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
     * Remove history from all keys that currently are deleted or purged.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    void purgeDeletes() throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Remove history from all keys that currently are deleted or purged, considering options.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    void purgeDeletes(KeyValuePurgeOptions options) throws IOException, JetStreamApiException, InterruptedException;

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
