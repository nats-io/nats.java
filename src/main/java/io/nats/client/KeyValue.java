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
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Key Value Store Management context for creation and access to key value buckets.
 */
public interface KeyValue {

    /**
     * Get the name of the bucket.
     * @return the name
     */
    String getBucketName();

    /**
     * Get the entry for a key
     * when the key exists and is live (not deleted and not purged)
     * @param key the key
     * @return the KvEntry object or null if not found.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    KeyValueEntry get(String key) throws IOException, JetStreamApiException;

    /**
     * Get the specific revision of an entry for a key
     * when the key exists and is live (not deleted and not purged)
     * @param key the key
     * @param revision the revision
     * @return the KvEntry object or null if not found.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    KeyValueEntry get(String key, long revision) throws IOException, JetStreamApiException;

    /**
     * Put a byte[] as the value for a key
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
     * Put a string as the value for a key iff the key exists and its last revision matches the expected
     * @param key the key
     * @param value the UTF-8 string
     * @param expectedRevision the expected last revision
     * @return the revision number for the key
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    long update(String key, String value, long expectedRevision) throws IOException, JetStreamApiException;

    /**
     * Soft deletes the key by placing a delete marker.
     * @param key the key
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void delete(String key) throws IOException, JetStreamApiException;

    /**
     * Soft deletes the key by placing a delete marker iff the key exists and its last revision matches the expected
     * @param key the key
     * @param expectedRevision the expected last revision
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void delete(String key, long expectedRevision) throws IOException, JetStreamApiException;

    /**
     * Purge all values/history from the specific key
     * @param key the key
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void purge(String key) throws IOException, JetStreamApiException;

    /**
     * Purge all values/history from the specific key iff the key exists and its last revision matches the expected
     * @param key the key
     * @param expectedRevision the expected last revision
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void purge(String key, long expectedRevision) throws IOException, JetStreamApiException;

    /**
     * Watch updates for a specific key.
     * @param key the key.
     * @param watcher the watcher the implementation to receive changes
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Watch updates for a specific key, starting at a specific revision.
     * @param key the key.
     * @param watcher the watcher the implementation to receive changes
     * @param fromRevision the revision to start from
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watch(String key, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Watch updates for specific keys.
     * @param keys the keys
     * @param watcher the watcher the implementation to receive changes
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Watch updates for specific keys, starting at a specific revision.
     * @param keys the keys
     * @param watcher the watcher the implementation to receive changes
     * @param fromRevision the revision to start from
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watch(List<String> keys, KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Watch updates for all keys.
     * @param watcher the watcher the implementation to receive changes
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Watch updates for all keys starting from a specific revision
     * @param watcher the watcher the implementation to receive changes
     * @param fromRevision the revision to start from
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return The KeyValueWatchSubscription
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsKeyValueWatchSubscription watchAll(KeyValueWatcher watcher, long fromRevision, KeyValueWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get a list of the keys in a bucket.
     * @return List of keys
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    List<String> keys() throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get a list of the keys in a bucket filtered by a
     * subject-like string, for instance "key" or "key.foo.*" or "key.&gt;"
     * @param filter the subject like key filter
     * @return List of keys
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    List<String> keys(String filter) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get a list of the keys in a bucket filtered by
     * subject-like strings, for instance "aaa.*", "bbb.*;"
     * @param filters the subject like key filters
     * @return List of keys
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    List<String> keys(List<String> filters) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get a list of keys in the bucket through a LinkedBlockingQueue.
     * A KeyResult with isDone being true or an exception signifies there are no more keys
     * @return the LinkedBlockingQueue from which to poll
     */
    LinkedBlockingQueue<KeyResult> consumeKeys();

    /**
     * Get a list of keys in the bucket through a LinkedBlockingQueue filtered by a
     * subject-like string, for instance "key" or "key.foo.*" or "key.&gt;"
     * A KeyResult with isDone being true or an exception signifies there are no more keys
     * @param filter the subject like key filter
     * @return the LinkedBlockingQueue from which to poll
     */
    LinkedBlockingQueue<KeyResult> consumeKeys(String filter);

    /**
     * Get a list of keys in the bucket through a LinkedBlockingQueue filtered by
     * subject-like strings, for instance "aaa.*", "bbb.*;"
     * A KeyResult with isDone being true or an exception signifies there are no more keys
     * @param filters the subject like key filters
     * @return the LinkedBlockingQueue from which to poll
     */
    LinkedBlockingQueue<KeyResult> consumeKeys(List<String> filters);

    /**
     * Get the history (list of KeyValueEntry) for a key
     * @param key the key
     * @return List of KvEntry
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    List<KeyValueEntry> history(String key) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Remove history from all keys that currently are deleted or purged
     * with using a default KeyValuePurgeOptions
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    void purgeDeletes() throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Remove history from all keys that currently are deleted or purged, considering options.
     * @param options the purge options
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    void purgeDeletes(KeyValuePurgeOptions options) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get the KeyValueStatus object
     * @return the status object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    KeyValueStatus getStatus() throws IOException, JetStreamApiException, InterruptedException;
}
