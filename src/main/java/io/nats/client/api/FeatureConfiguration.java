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

package io.nats.client.api;

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.Duration;
import java.util.Map;

import static io.nats.client.support.Validator.validateBucketName;
import static io.nats.client.support.Validator.validateMaxBucketBytes;

public abstract class FeatureConfiguration implements JsonSerializable {
    protected static final CompressionOption JS_COMPRESSION_YES = CompressionOption.S2;
    protected static final CompressionOption JS_COMPRESSION_NO = CompressionOption.None;

    protected final StreamConfiguration sc;
    protected final String bucketName;

    @Override
    @NotNull
    public String toJson() {
        return toJsonValue().toString();
    }

    @Override
    @NotNull
    public JsonValue toJsonValue() {
        JsonValueUtils.MapBuilder mb = new JsonValueUtils.MapBuilder();
        mb.put("name", bucketName);
        mb.put("description", getDescription());
        mb.put("maxBucketSize", getMaxBucketSize());
        mb.put("ttl", getTtl());
        mb.put("storageType", getStorageType());
        mb.put("replicas", getReplicas());
        mb.put("placement", getPlacement());
        mb.put("isCompressed", isCompressed());
        mb.put("metaData", getMetadata());
        return mb.toJsonValue();
    }

    public FeatureConfiguration(StreamConfiguration sc, String bucketName) {
        this.sc = sc;
        this.bucketName = bucketName;
    }

    /**
     * Gets the stream configuration for the stream which backs the bucket
     * @return the stream configuration
     */
    @NotNull
    public StreamConfiguration getBackingConfig() {
        return sc;
    }

    /**
     * Gets the name of this bucket.
     * @return the name of the bucket.
     */
    @NotNull
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Gets the description of this bucket.
     * @return the description of the bucket.
     */
    @Nullable
    public String getDescription() {
        return sc.getDescription();
    }

    /**
     * Gets the maximum number of bytes for this bucket.
     * @return the maximum number of bytes for this bucket.
     */
    public long getMaxBucketSize() {
        return sc.getMaxBytes();
    }

    /**
     * Gets the maximum age for a value in this bucket.
     * @return the maximum age.
     */
    @Nullable
    public Duration getTtl() {
        return sc.getMaxAge();
    }

    /**
     * Gets the storage type for this bucket.
     * @return the storage type for this stream.
     */
    @Nullable
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
     * Placement directives to consider when placing replicas of this stream,
     * random placement when unset
     * @return the placement [directive object]
     */
    @Nullable
    public Placement getPlacement() {
        return sc.getPlacement();
    }

    /**
     * Gets the state of compression
     * @return true if compression is used
     */
    public boolean isCompressed() {
        return sc.getCompressionOption() == JS_COMPRESSION_YES;
    }

    /**
     * Get the metadata for the feature
     * @return the metadata map. Might be null.
     */
    @Nullable
    public Map<String, String> getMetadata() {
        return sc.getMetadata();
    }

    protected static abstract class Builder<B, FC> {
        protected String name;
        protected Duration ttl = Duration.ZERO;
        protected StreamConfiguration.Builder scBuilder;
        protected abstract B getThis();

        /**
         * Sets the name of the store.
         * @param name name of the store.
         * @return the builder
         */
        protected B name(String name) {
            this.name = validateBucketName(name, true);
            return getThis();
        }

        /**
         * Sets the description of the store.
         * @param description description of the store.
         * @return the builder
         */
        protected B description(String description) {
            scBuilder.description(description);
            return getThis();
        }

        /**
         * Sets the maximum number of bytes in the configuration.
         * @param maxBucketSize the maximum number of bytes
         * @return Builder
         */
        protected B maxBucketSize(long maxBucketSize) {
            scBuilder.maxBytes(validateMaxBucketBytes(maxBucketSize));
            return getThis();
        }

        /**
         * Sets the maximum age for a value in this configuration.
         * @param ttl the maximum age
         * @return Builder
         */
        protected B ttl(Duration ttl) {
            this.ttl = ttl == null ? Duration.ZERO : ttl;
            scBuilder.maxAge(this.ttl);
            return getThis();
        }

        /**
         * Sets the storage type in the configuration.
         * @param storageType the storage type
         * @return Builder
         */
        protected B storageType(StorageType storageType) {
            scBuilder.storageType(storageType);
            return getThis();
        }

        /**
         * Sets the number of replicas a message must be stored on in the configuration.
         * @param replicas the number of replicas
         * @return Builder
         */
        protected B replicas(int replicas) {
            scBuilder.replicas(replicas);
            return getThis();
        }

        /**
         * Sets the placement directive object
         * @param placement the placement directive object
         * @return Builder
         */
        protected B placement(Placement placement) {
            scBuilder.placement(placement);
            return getThis();
        }

        /**
         * Sets whether to use compression for the configuration.
         * If set, will use the default compression algorithm of the feature backing.
         * @param compression whether to use compression in the configuration
         * @return Builder
         */
        protected B compression(boolean compression) {
            scBuilder.compressionOption(compression ? JS_COMPRESSION_YES : JS_COMPRESSION_NO);
            return getThis();
        }

        /**
         * Sets the metadata for the configuration
         * @param metadata the metadata map
         * @return Builder
         */
        public B metadata(Map<String, String> metadata) {
            scBuilder.metadata(metadata);
            return getThis();
        }

        /**
         * Builds the feature options.
         * @return feature options
         */
        public abstract FC build();
    }
}
