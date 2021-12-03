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
package io.nats.client.api;

import io.nats.client.support.NatsKeyValueUtil;

import java.time.Duration;

import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;

/**
 * The BucketConfiguration class contains the configuration for of a Key Value bucket.
 */
public class KeyValueConfiguration {
    private final StreamConfiguration sc;
    private final String bucketName;

    static KeyValueConfiguration instance(String json) {
        return new KeyValueConfiguration(StreamConfiguration.instance(json));
    }

    KeyValueConfiguration(StreamConfiguration sc) {
        this.sc = sc;
        bucketName = extractBucketName(sc.getName());
    }

    public StreamConfiguration getBackingConfig() {
        return sc;
    }

    /**
     * Gets the name of this bucket.
     * @return the name of the bucket.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Gets the description of this bucket.
     * @return the description of the bucket.
     */
    public String getDescription() {
        return sc.getDescription();
    }

    /**
     * Gets the maximum number of values across all keys, including history values
     * @return the maximum number of values for the bucket
     */
    public long getMaxValues() {
        return sc.getMaxMsgs();
    }

    /**
     * Gets the maximum number of history for any one key. Includes the current value.
     * @return the maximum number of values for any one key.
     */
    public long getMaxHistoryPerKey() {
        return sc.getMaxMsgsPerSubject();
    }

    /**
     * Gets the maximum number of bytes for this bucket.
     * @return the maximum number of bytes for this bucket.
     */
    public long getMaxBucketSize() {
        return sc.getMaxBytes();
    }

    /**
     * Gets the maximum number of bytes for an individual value in the bucket.
     * @return the maximum bytes for a value.
     */      
    public long getMaxValueBytes() {
        return sc.getMaxMsgSize();
    }

    /**
     * Gets the maximum age for a value in this bucket.
     * @return the maximum age.
     */
    public Duration getTtl() {
        return sc.getMaxAge();
    }

    /**
     * Gets the storage type for this bucket.
     * @return the storage type for this stream.
     */
    public StorageType getStorageType() {
        return sc.getStorageType();
    }

    /**
     * Gets the number of replicas for this bucket.
     * @return the number of replicas
     */    
    public int getReplicas() {
        return sc.getReplicas();
    }

    @Override
    public String toString() {
        return "KeyValueConfiguration{" +
            "name='" + bucketName + '\'' +
            ", description='" + getDescription() + '\'' +
            ", maxValues=" + getMaxValues() +
            ", maxHistoryPerKey=" + getMaxHistoryPerKey() +
            ", maxBucketSize=" + getMaxBucketSize() +
            ", maxValueBytes=" + getMaxValueBytes() +
            ", ttl=" + getTtl() +
            ", storageType=" + getStorageType() +
            ", replicas=" + getReplicas() +
            '}';
    }

    /**
     * Creates a builder for the stream configuration.
     * @return a stream configuration builder
     */
    public static KeyValueConfiguration.Builder builder() {
        return new KeyValueConfiguration.Builder();
    }

    /**
     * Creates a builder to copy the stream configuration.
     * @param kvc an existing KeyValueConfiguration
     * @return a stream configuration builder
     */
    public static KeyValueConfiguration.Builder builder(KeyValueConfiguration kvc) {
        return new KeyValueConfiguration.Builder(kvc);
    }

    /**
     * BucketConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code new BucketConfiguration.Builder().build()} will create a new BucketConfiguration.
     *
     */
    public static class Builder {

        String name;
        StreamConfiguration.Builder scBuilder;

        /**
         * Default Builder
         */
        public Builder() {
            this(null);
        }

        /**
         * Update Builder, useful if you need to update a configuration
         * @param kvc the configuration to copy
         */
        public Builder(KeyValueConfiguration kvc) {
            if (kvc == null) {
                scBuilder = new StreamConfiguration.Builder();
                maxHistoryPerKey(1);
            }
            else {
                scBuilder = new StreamConfiguration.Builder(kvc.sc);
                name = NatsKeyValueUtil.extractBucketName(kvc.sc.getName());
            }
        }

        /**
         * Sets the name of the store.
         * @param name name of the store.
         * @return the builder
         */
        public KeyValueConfiguration.Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the description of the store.
         * @param description description of the store.
         * @return the builder
         */
        public KeyValueConfiguration.Builder description(String description) {
            scBuilder.description(description);
            return this;
        }

        /**
         * Sets the maximum number of values across all keys, including history values in the BucketConfiguration.
         * @param maxValues the maximum number of values
         * @return Builder
         */
        public KeyValueConfiguration.Builder maxValues(long maxValues) {
            scBuilder.maxMessages(validateMaxBucketValues(maxValues));
            return this;
        }

        /**
         * Sets the maximum number of history for any one key. Includes the current value.
         * @param maxHistoryPerKey the maximum number of messages
         * @return Builder
         */
        public KeyValueConfiguration.Builder maxHistoryPerKey(int maxHistoryPerKey) {
            scBuilder.maxMessagesPerSubject(validateMaxHistory(maxHistoryPerKey));
            return this;
        }

        /**
         * Sets the maximum number of bytes in the BucketConfiguration.
         * @param maxBucketSize the maximum number of bytes
         * @return Builder
         */
        public KeyValueConfiguration.Builder maxBucketSize(long maxBucketSize) {
            scBuilder.maxBytes(validateMaxBucketBytes(maxBucketSize));
            return this;
        }

        /**
         * Sets the maximum number of bytes for an individual value in the BucketConfiguration.
         * @param maxValueBytes the maximum number of bytes for a value
         * @return Builder
         */
        public KeyValueConfiguration.Builder maxValueBytes(long maxValueBytes) {
            scBuilder.maxMsgSize(validateMaxValueBytes(maxValueBytes));
            return this;
        }

        /**
         * Sets the maximum for a value in this BucketConfiguration.
         * @param ttl the maximum age
         * @return Builder
         */
        public KeyValueConfiguration.Builder ttl(Duration ttl) {
            scBuilder.maxAge(ttl);
            return this;
        }

        /**
         * Sets the storage type in the BucketConfiguration.
         * @param storageType the storage type
         * @return Builder
         */
        public KeyValueConfiguration.Builder storageType(StorageType storageType) {
            scBuilder.storageType(storageType);
            return this;
        }

        /**
         * Sets the number of replicas a message must be stored on in the BucketConfiguration.
         * @param replicas the number of replicas to store this message on
         * @return Builder
         */
        public KeyValueConfiguration.Builder replicas(int replicas) {
            scBuilder.replicas(replicas);
            return this;
        }

        /**
         * Builds the BucketConfiguration
         * @return a bucket configuration.
         */
        public KeyValueConfiguration build() {
            name = validateKvBucketNameRequired(name);
            scBuilder.name(streamName(name))
                .subjects(streamSubject(name))
                .allowRollup(true)
                .denyDelete(true);
            return new KeyValueConfiguration(scBuilder.build());
        }
    }
}
