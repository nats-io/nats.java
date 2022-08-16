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


/**
 * Object Store Management context for creation and access to key value buckets.
 *
 * THIS IS A PLACEHOLDER FOR THE EXPERIMENTAL OBJECT STORE IMPLEMENTATION.
 */
public interface ObjectStore {

    /**
     * Get the name of the object store's bucket.
     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
     * @return the name
     */
//    String getBucketName();
//
//    ObjectInfo put(ObjectMeta meta, InputStream inputStream) throws IOException, JetStreamApiException;
//    ObjectInfo put(ObjectMeta meta, byte[] input) throws IOException, JetStreamApiException;
//    ObjectInfo put(ObjectMeta meta, String input) throws IOException, JetStreamApiException;
//
//    void get(String objectName, OutputStream outputStream) throws IOException, JetStreamApiException, InterruptedException;
//    byte[] getBytes(String objectName) throws IOException, JetStreamApiException, InterruptedException;
//
//    ObjectInfo getInfo(String objectName) throws IOException, JetStreamApiException;
//    ObjectInfo updateMeta(String objectName, ObjectMeta meta) throws IOException, JetStreamApiException;
//    ObjectInfo delete(String objectName) throws IOException, JetStreamApiException;
//
//    ObjectInfo addLink(String objectName, ObjectInfo toInfo) throws IOException, JetStreamApiException;
//    ObjectInfo addBucketLink(String objectName, ObjectStore toStore) throws IOException, JetStreamApiException;
//
//    void seal() throws IOException, JetStreamApiException;

//    /**
//     * Get the ObjectStoreStatus object
//     * THIS IS A BETA FEATURE AND SUBJECT TO CHANGE
//     * @return the status object
//     * @throws IOException covers various communication issues with the NATS
//     *         server such as timeout or interruption
//     * @throws JetStreamApiException the request had an error related to the data
//     * @throws InterruptedException if the thread is interrupted
//     */
//    ObjectStoreStatus getStatus() throws IOException, JetStreamApiException, InterruptedException;
}
