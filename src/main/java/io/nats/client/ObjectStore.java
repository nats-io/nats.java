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

import io.nats.client.api.*;
import io.nats.client.impl.NatsObjectStoreWatchSubscription;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * Object Store Management context for creation and access to key value buckets.
 *
 * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL.
 */
public interface ObjectStore {

    /**
     * Get the name of the object store's bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @return the name
     */
    String getBucketName();

    ObjectInfo put(ObjectMeta meta, InputStream inputStream) throws IOException, JetStreamApiException, NoSuchAlgorithmException;
    ObjectInfo put(String name, InputStream inputStream) throws IOException, JetStreamApiException, NoSuchAlgorithmException;
    ObjectInfo put(String name, byte[] input) throws IOException, JetStreamApiException, NoSuchAlgorithmException;
    ObjectInfo put(File file) throws IOException, JetStreamApiException, NoSuchAlgorithmException;

    ObjectInfo get(String objectName, OutputStream outputStream) throws IOException, JetStreamApiException, InterruptedException, NoSuchAlgorithmException;

    ObjectInfo getInfo(String objectName) throws IOException, JetStreamApiException;
    ObjectInfo updateMeta(String objectName, ObjectMeta meta) throws IOException, JetStreamApiException;
    ObjectInfo delete(String objectName) throws IOException, JetStreamApiException;

    ObjectInfo addLink(String objectName, ObjectInfo toInfo) throws IOException, JetStreamApiException;
    ObjectInfo addBucketLink(String objectName, ObjectStore toStore) throws IOException, JetStreamApiException;

    void seal() throws IOException, JetStreamApiException;

    /**
     * Get a list of all object [infos] in the store.
     * @return the list of objects
     */
    List<ObjectInfo> getList() throws IOException, JetStreamApiException, InterruptedException;

    NatsObjectStoreWatchSubscription watch(ObjectStoreWatcher watcher, ObjectStoreWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get the ObjectStoreStatus object
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @return the status object
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ObjectStoreStatus getStatus() throws IOException, JetStreamApiException;
}
