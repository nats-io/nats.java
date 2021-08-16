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

import io.nats.client.api.KvEntry;

import java.io.IOException;

/**
 * Key Value Store Management context for creation and access to key value buckets.
 */
public interface KeyValue {

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
    KvEntry getEntry(String key) throws IOException, JetStreamApiException;

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
     * @return the sequence number for the DEL record
     * @throws IOException covers various communication issues with the NATS
     *         server such as timeout or interruption
     * @throws JetStreamApiException the request had an error related to the data
     */
    long delete(String key) throws IOException, JetStreamApiException;
}
