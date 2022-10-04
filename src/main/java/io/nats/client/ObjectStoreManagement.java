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

package io.nats.client;

import io.nats.client.api.ObjectStoreConfiguration;
import io.nats.client.api.ObjectStoreStatus;

import java.io.IOException;
import java.util.List;

/**
 * Object Store Management context for creation and access to object stores.
 * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
 */
public interface ObjectStoreManagement {

    /**
     * Create an object store.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param config the object store configuration
     * @return bucket info
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws IllegalArgumentException the server is not JetStream enabled
     */
    ObjectStoreStatus create(ObjectStoreConfiguration config) throws IOException, JetStreamApiException;

    /**
     * Get the list of object stores bucket names
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @return list of object stores bucket names
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    List<String> getBucketNames() throws IOException, JetStreamApiException;

    /**
     * Gets the info for an existing object store bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param bucketName the object store bucket name to get info for
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return the bucket status object
     */
    ObjectStoreStatus getStatus(String bucketName) throws IOException, JetStreamApiException;

    /**
     * Gets the info for an all object store buckets.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @return the bucket status object
     */
    List<ObjectStoreStatus> getStatuses() throws IOException, JetStreamApiException;

    /**
     * Deletes an existing object store. Will throw a JetStreamApiException if the delete fails.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @param bucketName the object store bucket name to delete
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    void delete(String bucketName) throws IOException, JetStreamApiException;
}
