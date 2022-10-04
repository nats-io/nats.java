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

import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;

import java.io.IOException;
import java.util.List;

/**
 * Key Value Store Management context for creation and access to key value buckets.
 */
public interface KeyValueManagement {

    /**
     * Create a key value store.
     * @param config the key value configuration
     * @return bucket info
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    KeyValueStatus create(KeyValueConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Update a key value store configuration. Storage type cannot change.
     * @param config the key value configuration
     * @return bucket info
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    KeyValueStatus update(KeyValueConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Get the list of bucket names.
     * @return list of bucket names
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> getBucketNames() throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing bucket.
     * @deprecated Use {@link #getStatus(String)} instead.
     * @param bucketName the bucket name to use
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return the bucket status object
     */
    @Deprecated
    KeyValueStatus getBucketInfo(String bucketName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing bucket.
     * @param bucketName the bucket name to use
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return the bucket status object
     */
    KeyValueStatus getStatus(String bucketName) throws IOException, JetStreamApiException;

    /**
     * Get the info for all buckets
     * @return list of bucket statuses
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<KeyValueStatus> getStatuses() throws IOException, JetStreamApiException;

    /**
     * Deletes an existing bucket. Will throw a JetStreamApiException if the delete fails.
     * @param bucketName the stream name to use.
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void delete(String bucketName) throws IOException, JetStreamApiException;
}
