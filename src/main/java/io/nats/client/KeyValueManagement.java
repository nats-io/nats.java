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
package io.nats.client;

import io.nats.client.api.BucketConfiguration;
import io.nats.client.api.BucketInfo;
import io.nats.client.api.KvEntry;
import io.nats.client.api.PurgeResponse;

import java.io.IOException;
import java.util.List;

/**
 * Key Value Store Management context for creation and access to key value buckets.
 */
public interface KeyValueManagement {

    /**
     * Create a bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param config the bucket configuration
     * @return bucket info
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    BucketInfo createBucket(BucketConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Deletes an existing bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param bucketName the stream name to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return true if the delete succeeded
     */
    boolean deleteBucket(String bucketName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param bucketName the stream name to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return stream information
     */
    BucketInfo getBucketInfo(String bucketName) throws IOException, JetStreamApiException;

    /**
     * Purge all keys/values/history from the bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param bucketName the bucket name
     * @return PurgeResponse the purge response
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PurgeResponse purgeBucket(String bucketName) throws IOException, JetStreamApiException;

    /**
     * Purge all values/history from the specific key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param bucketName the bucket name
     * @param key the key
     * @return PurgeResponse the purge response
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    PurgeResponse purgeKey(String bucketName, String key) throws IOException, JetStreamApiException;

    /**
     * Get the history (list of KvEntry) for a key
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param bucketName the bucket name
     * @param key the key
     * @return List of KvEntry
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<KvEntry> history(String bucketName, String key) throws IOException, JetStreamApiException;

    /**
     * Get the list of the keys in a bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param bucketName the bucket name
     * @return List of keys
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> keys(String bucketName) throws IOException, JetStreamApiException;
}
