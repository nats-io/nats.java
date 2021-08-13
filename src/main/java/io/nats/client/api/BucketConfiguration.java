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

package io.nats.client.api;

import java.time.Duration;

import static io.nats.client.support.NatsKeyValueUtil.*;
import static io.nats.client.support.Validator.*;

/**
 * The BucketConfiguration class contains the configuration for of a Key Value bucket.
 */
public class BucketConfiguration {
    private final StreamConfiguration sc;
    private final String name;

    static BucketConfiguration instance(String json) {
        return new BucketConfiguration(StreamConfiguration.instance(json));
    }

    BucketConfiguration(StreamConfiguration sc) {
        this.sc = sc;
        name = extractBucketName(sc.getName());
    }

    public StreamConfiguration getBackingConfig() {
        return sc;
    }

    /**
     * Gets the name of this bucket.
     * @return the name of the bucket.
     */
    public String getName() {
        return name;
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
    public long getMaxHistory() {
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

    /**
     * Gets the duplicate checking window bucket. Duration.ZERO
     * means duplicate checking is not enabled.
     * @return the duration of the window.
     */    
    public Duration getDuplicateWindow() {
        return sc.getDuplicateWindow();
    }

    @Override
    public String toString() {
        return "BucketConfiguration{" +
                "name='" + name + '\'' +
                ", maxValues=" + getMaxValues() +
                ", maxHistory=" + getMaxHistory() +
                ", maxBucketSize=" + getMaxBucketSize() +
                ", maxValueSize=" + getMaxValueSize() +
                ", ttl=" + getTtl() +
                ", storageType=" + getStorageType() +
                ", replicas=" + getReplicas() +
                ", duplicateWindow=" + getDuplicateWindow() +
                '}';
    }

    /**
     * Creates a builder for the stream configuration.
     * @return a stream configuration builder
     */
    public static BucketConfiguration.Builder builder() {
        return new BucketConfiguration.Builder();
    }

    /**
     * Creates a builder to copy the stream configuration.
     * @param bc an existing StreamConfiguration
     * @return a stream configuration builder
     */
    public static BucketConfiguration.Builder builder(BucketConfiguration bc) {
        return new BucketConfiguration.Builder(bc);
    }

    /**
     * StreamConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code new StreamConfiguration.Builder().build()} will create a new ConsumerConfiguration.
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
         * @param bc the configuration to copy
         */
        public Builder(BucketConfiguration bc) {
            if (bc == null) {
                scBuilder = new StreamConfiguration.Builder();
                maxHistory(1);
            }
            else {
                scBuilder = new StreamConfiguration.Builder(bc.sc);
                name = bc.sc.getName().substring(KV_STREAM_PREFIX_LEN);
            }
        }

        /**
         * Sets the name of the stream.
         * @param name name of the stream.
         * @return the builder
         */
        public BucketConfiguration.Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the maximum number of values across all keys, including history values in the BucketConfiguration.
         * @param maxValues the maximum number of values
         * @return Builder
         */
        public BucketConfiguration.Builder maxValues(long maxValues) {
            scBuilder.maxMessages(validateMaxBucketValues(maxValues));
            return this;
        }

        /**
         * Sets the maximum number of history for any one key. Includes the current value.
         * @param maxHistory the maximum number of messages
         * @return Builder
         */
        public BucketConfiguration.Builder maxHistory(long maxHistory) {
            scBuilder.maxMessagesPerSubject(validateMaxValuesPerKey(maxHistory));
            return this;
        }

        /**
         * Sets the maximum number of bytes in the BucketConfiguration.
         * @param maxBucketSize the maximum number of bytes
         * @return Builder
         */
        public BucketConfiguration.Builder maxBucketSize(long maxBucketSize) {
            scBuilder.maxBytes(validateMaxBucketBytes(maxBucketSize));
            return this;
        }

        /**
         * Sets the maximum number of bytes for an individual value in the BucketConfiguration.
         * @param maxValueSize the maximum number of bytes for a value
         * @return Builder
         */
        public BucketConfiguration.Builder maxValueSize(long maxValueSize) {
            scBuilder.maxMsgSize(validateMaxValueSize(maxValueSize));
            return this;
        }

        /**
         * Sets the maximum for a value in this BucketConfiguration.
         * @param ttl the maximum age
         * @return Builder
         */
        public BucketConfiguration.Builder ttl(Duration ttl) {
            scBuilder.maxAge(ttl);
            return this;
        }

        /**
         * Sets the storage type in the BucketConfiguration.
         * @param storageType the storage type
         * @return Builder
         */
        public BucketConfiguration.Builder storageType(StorageType storageType) {
            scBuilder.storageType(storageType);
            return this;
        }

        /**
         * Sets the number of replicas a message must be stored on in the BucketConfiguration.
         * @param replicas the number of replicas to store this message on
         * @return Builder
         */
        public BucketConfiguration.Builder replicas(int replicas) {
            scBuilder.replicas(replicas);
            return this;
        }

        /**
         * Sets the duplicate checking window in the the BucketConfiguration.  A Duration.Zero
         * disables duplicate checking.  Duplicate checking is disabled by default.
         * @param window duration to hold message ids for duplicate checking.
         * @return Builder
         */
        public BucketConfiguration.Builder duplicateWindow(Duration window) {
            scBuilder.duplicateWindow(window);
            return this;
        }

        /**
         * Sets the duplicate checking window in the the BucketConfiguration.  A Duration.Zero
         * disables duplicate checking.  Duplicate checking is disabled by default.
         * @param windowMillis duration to hold message ids for duplicate checking.
         * @return Builder
         */
        public BucketConfiguration.Builder duplicateWindow(long windowMillis) {
            scBuilder.duplicateWindow(windowMillis);
            return this;
        }

        /**
         * Builds the BucketConfiguration
         * @return a bucket configuration.
         */
        public BucketConfiguration build() {
            name = validateBucketNameRequired(name);
            scBuilder.name(streamName(name));
            scBuilder.subjects(streamSubject(name));
            return new BucketConfiguration(scBuilder.build());
        }
    }
}
