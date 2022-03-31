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

import java.time.Duration;

import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;

/**
 * The KeyValueConfiguration class contains the configuration for a Key Value bucket.
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

    /**
     * Gets the stream configuration for the stream which backs the bucket
     * @return the stream configuration
     */
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
     * Gets the maximum size for an individual value in the bucket.
     * @return the maximum size for a value.
     */      
    public long getMaxValueSize() {
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
            ", maxHistoryPerKey=" + getMaxHistoryPerKey() +
            ", maxBucketSize=" + getMaxBucketSize() +
            ", maxValueSize=" + getMaxValueSize() +
            ", ttl=" + getTtl() +
            ", storageType=" + getStorageType() +
            ", replicas=" + getReplicas() +
            '}';
    }

    /**
     * Creates a builder for the Key Value Configuration.
     * @return a key value configuration builder
     */
    public static KeyValueConfiguration.Builder builder() {
        return new KeyValueConfiguration.Builder();
    }

    /**
     * Creates a builder to copy the key value configuration.
     * @param kvc an existing KeyValueConfiguration
     * @return a stream configuration builder
     */
    public static KeyValueConfiguration.Builder builder(KeyValueConfiguration kvc) {
        return new KeyValueConfiguration.Builder(kvc);
    }

    /**
     * KeyValueConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code new KeyValueConfiguration.Builder().build()} will create a new KeyValueConfiguration.
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
         * Construct the builder by copying another configuration
         * @param kvc the configuration to copy
         */
        public Builder(KeyValueConfiguration kvc) {
            if (kvc == null) {
                scBuilder = new StreamConfiguration.Builder();
                maxHistoryPerKey(1);
                replicas(1);
            }
            else {
                scBuilder = new StreamConfiguration.Builder(kvc.sc);
                name = extractBucketName(kvc.sc.getName());
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
         * Sets the maximum number of history for any one key. Includes the current value.
         * @param maxHistoryPerKey the maximum history
         * @return Builder
         */
        public KeyValueConfiguration.Builder maxHistoryPerKey(int maxHistoryPerKey) {
            scBuilder.maxMessagesPerSubject(validateMaxHistory(maxHistoryPerKey));
            return this;
        }

        /**
         * Sets the maximum number of bytes in the KeyValueConfiguration.
         * @param maxBucketSize the maximum number of bytes
         * @return Builder
         */
        public KeyValueConfiguration.Builder maxBucketSize(long maxBucketSize) {
            scBuilder.maxBytes(validateMaxBucketBytes(maxBucketSize));
            return this;
        }

        /**
         * Sets the maximum size for an individual value in the KeyValueConfiguration.
         * @param maxValueSize the maximum size for a value
         * @return Builder
         */
        public KeyValueConfiguration.Builder maxValueSize(long maxValueSize) {
            scBuilder.maxMsgSize(validateMaxValueSize(maxValueSize));
            return this;
        }

        /**
         * Sets the maximum age for a value in this KeyValueConfiguration.
         * @param ttl the maximum age
         * @return Builder
         */
        public KeyValueConfiguration.Builder ttl(Duration ttl) {
            scBuilder.maxAge(ttl);
            return this;
        }

        /**
         * Sets the storage type in the KeyValueConfiguration.
         * @param storageType the storage type
         * @return Builder
         */
        public KeyValueConfiguration.Builder storageType(StorageType storageType) {
            scBuilder.storageType(storageType);
            return this;
        }

        /**
         * Sets the number of replicas a message must be stored on in the KeyValueConfiguration.
         * @param replicas the number of replicas
         * @return Builder
         */
        public KeyValueConfiguration.Builder replicas(int replicas) {
            scBuilder.replicas(Math.max(replicas, 1));
            return this;
        }

        /**
         * Builds the KeyValueConfiguration
         * @return the KeyValueConfiguration.
         */
        public KeyValueConfiguration build() {
            name = validateKvBucketNameRequired(name);
            scBuilder.name(toStreamName(name))
                .subjects(toStreamSubject(name))
                .allowRollup(true)
                .discardPolicy(DiscardPolicy.New)
                .denyDelete(true);
            return new KeyValueConfiguration(scBuilder.build());
        }
    }
}
