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
 * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
 */
public interface ObjectStore {

    /**
     * Get the name of the object store's bucket.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @return the name
     */
    String getBucketName();

    /**
     * Place the contents of the input stream into a new object.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param meta the metadata for the object
     * @param inputStream the source input stream
     * @return the ObjectInfo for the saved object
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws NoSuchAlgorithmException if the Digest Algorithm is not known. Currently, the only supported algorithm is SHA-256 
     */
    ObjectInfo put(ObjectMeta meta, InputStream inputStream) throws IOException, JetStreamApiException, NoSuchAlgorithmException;

    /**
     * Place the contents of the input stream into a new object.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param objectName the name of the object
     * @param inputStream the source input stream
     * @return the ObjectInfo for the saved object
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws NoSuchAlgorithmException if the Digest Algorithm is not known. Currently, the only supported algorithm is SHA-256
     */
    ObjectInfo put(String objectName, InputStream inputStream) throws IOException, JetStreamApiException, NoSuchAlgorithmException;

    /**
     * Place the bytes into a new object.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param objectName the name of the object
     * @param input the bytes to store
     * @return the ObjectInfo for the saved object
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws NoSuchAlgorithmException if the Digest Algorithm is not known. Currently, the only supported algorithm is SHA-256
     */
    ObjectInfo put(String objectName, byte[] input) throws IOException, JetStreamApiException, NoSuchAlgorithmException;

    /**
     * Place the contents of the file into a new object using the file name as the object name.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param file the file to read
     * @return the ObjectInfo for the saved object
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws NoSuchAlgorithmException if the Digest Algorithm is not known. Currently, the only supported algorithm is SHA-256
     */
    ObjectInfo put(File file) throws IOException, JetStreamApiException, NoSuchAlgorithmException;

    /**
     * Get an object by name from the store, reading it into the output stream, if the object exists.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param objectName The name of the object
     * @param outputStream the destination stream.
     * @return the ObjectInfo for the object name or throw an exception if it does not exist or is deleted.
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     * @throws NoSuchAlgorithmException if the Digest Algorithm is not known. Currently, the only supported algorithm is SHA-256
     */
    ObjectInfo get(String objectName, OutputStream outputStream) throws IOException, JetStreamApiException, InterruptedException, NoSuchAlgorithmException;

    /**
     * Get the info for an object if the object exists / is not deleted.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param objectName The name of the object
     * @return the ObjectInfo for the object name or throw an exception if it does not exist.
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ObjectInfo getInfo(String objectName) throws IOException, JetStreamApiException;

    /**
     * Get the info for an object if the object exists, optionally including deleted.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param objectName The name of the object
     * @param includingDeleted whether to return info for deleted objects
     * @return the ObjectInfo for the object name or throw an exception if it does not exist.
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ObjectInfo getInfo(String objectName, boolean includingDeleted) throws IOException, JetStreamApiException;

    /**
     * Update the metadata of name, description or headers. All other changes are ignored.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param objectName The name of the object
     * @param meta the metadata with the new or unchanged name, description and headers.
     * @return the ObjectInfo after update
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ObjectInfo updateMeta(String objectName, ObjectMeta meta) throws IOException, JetStreamApiException;

    /**
     * Delete the object by name. A No-op if the object is already deleted.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param objectName The name of the object
     * @return the ObjectInfo after delete or throw an exception if it does not exist.
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ObjectInfo delete(String objectName) throws IOException, JetStreamApiException;

    /**
     * Add a link to another object. A link cannot be for another link.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param objectName The name of the object
     * @param toInfo the info object of the object to link to
     * @return the ObjectInfo for the link as saved or throws an exception
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ObjectInfo addLink(String objectName, ObjectInfo toInfo) throws IOException, JetStreamApiException;

    /**
     * Add a link to another object store (bucket).
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param objectName The name of the object
     * @param toStore the store object to link to
     * @return the ObjectInfo for the link as saved or throws an exception
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ObjectInfo addBucketLink(String objectName, ObjectStore toStore) throws IOException, JetStreamApiException;

    /**
     * Close (seal) the bucket to changes. The store (bucket) will be read only.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @return the status object
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    ObjectStoreStatus seal() throws IOException, JetStreamApiException;

    /**
     * Get a list of all object [infos] in the store.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @return the list of objects
     */
    List<ObjectInfo> getList() throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Create a watch on the store (bucket).
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @param watcher the implementation to receive changes.
     * @param watchOptions the watch options to apply. If multiple conflicting options are supplied, the last options wins.
     * @return the NatsObjectStoreWatchSubscription
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     * @throws InterruptedException if the thread is interrupted
     */
    NatsObjectStoreWatchSubscription watch(ObjectStoreWatcher watcher, ObjectStoreWatchOption... watchOptions) throws IOException, JetStreamApiException, InterruptedException;

    /**
     * Get the ObjectStoreStatus object.
     * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
     * @return the status object
     * @throws IOException covers various communication issues with the NATS server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data the request had an error related to the data
     */
    ObjectStoreStatus getStatus() throws IOException, JetStreamApiException;
}
