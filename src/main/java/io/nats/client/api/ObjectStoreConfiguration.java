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

import io.nats.client.support.NatsObjectStoreUtil;

import java.time.Duration;

import static io.nats.client.support.NatsObjectStoreUtil.*;
import static io.nats.client.support.Validator.*;

/**
 * The ObjectStoreStatus class contains information about an object store.
 * OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
 */
public class ObjectStoreConfiguration extends FeatureConfiguration {
    ObjectStoreConfiguration(StreamConfiguration sc) {
        super(sc, extractBucketName(sc.getName()));
    }

    @Override
    public String toString() {
        return "ObjectStoreConfiguration{" +
            "name='" + bucketName + '\'' +
            ", description='" + getDescription() + '\'' +
            ", maxBucketSize=" + getMaxBucketSize() +
            ", ttl=" + getTtl() +
            ", storageType=" + getStorageType() +
            ", replicas=" + getReplicas() +
            ", placement=" + getPlacement() +
            ", isCompressed=" + isCompressed() +
            '}';
    }

    /**
     * Creates a builder for the Object Store Configuration.
     * @return an ObjectStoreConfiguration Builder
     */
    public static ObjectStoreConfiguration.Builder builder() {
        return new ObjectStoreConfiguration.Builder((ObjectStoreConfiguration)null);
    }

    /**
     * Creates a builder for the Object Store Configuration.
     * @param name the name of the object store bucket
     * @return an ObjectStoreConfiguration Builder
     */
    public static ObjectStoreConfiguration.Builder builder(String name) {
        return new ObjectStoreConfiguration.Builder(name);
    }

    /**
     * Creates a builder to copy the Object Store configuration.
     * @param osc an existing ObjectStoreConfiguration
     * @return an ObjectStoreConfiguration Builder
     */
    public static ObjectStoreConfiguration.Builder builder(ObjectStoreConfiguration osc) {
        return new ObjectStoreConfiguration.Builder(osc);
    }

    /**
     * ObjectStoreConfiguration is created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     *
     * <p>{@code new ObjectStoreConfiguration.Builder().build()} will create a new ObjectStoreConfiguration.
     *
     */
    public static class Builder {
        String name;
        StreamConfiguration.Builder scBuilder;

        /**
         * Default Builder
         */
        public Builder() {
            this((ObjectStoreConfiguration)null);
        }

        /**
         * Builder accepting the object store bucket name.
         * @param name name of the store.
         */
        public Builder(String name) {
            this((ObjectStoreConfiguration)null);
            name(name);
        }

        /**
         * Construct the builder by copying another configuration
         * @param osc the configuration to copy
         */
        public Builder(ObjectStoreConfiguration osc) {
            if (osc == null) {
                scBuilder = new StreamConfiguration.Builder();
                replicas(1);
            }
            else {
                scBuilder = new StreamConfiguration.Builder(osc.sc);
                name = NatsObjectStoreUtil.extractBucketName(osc.sc.getName());
            }
        }

        /**
         * Sets the name of the store.
         * @param name name of the store.
         * @return the builder
         */
        public Builder name(String name) {
            this.name = validateBucketName(name, true);
            return this;
        }

        /**
         * Sets the description of the store.
         * @param description description of the store.
         * @return the builder
         */
        public Builder description(String description) {
            scBuilder.description(description);
            return this;
        }

        /**
         * Sets the maximum number of bytes in the ObjectStoreConfiguration.
         * @param maxBucketSize the maximum number of bytes
         * @return Builder
         */
        public Builder maxBucketSize(long maxBucketSize) {
            scBuilder.maxBytes(validateMaxBucketBytes(maxBucketSize));
            return this;
        }

        /**
         * Sets the maximum age for a value in this ObjectStoreConfiguration.
         * @param ttl the maximum age
         * @return Builder
         */
        public Builder ttl(Duration ttl) {
            scBuilder.maxAge(ttl);
            return this;
        }

        /**
         * Sets the storage type in the ObjectStoreConfiguration.
         * @param storageType the storage type
         * @return Builder
         */
        public Builder storageType(StorageType storageType) {
            scBuilder.storageType(storageType);
            return this;
        }

        /**
         * Sets the number of replicas a message must be stored on in the ObjectStoreConfiguration.
         * @param replicas the number of replicas
         * @return Builder
         */
        public Builder replicas(int replicas) {
            scBuilder.replicas(replicas);
            return this;
        }

        /**
         * Sets the placement directive object
         * @param placement the placement directive object
         * @return Builder
         */
        public Builder placement(Placement placement) {
            scBuilder.placement(placement);
            return this;
        }

        /**
         * Sets whether to use compression for the ObjectStoreConfiguration.
         * If set, will use the default compression algorithm of the Object Store backing.
         * @param compression whether to use compression in the ObjectStoreConfiguration
         * @return Builder
         */
        public Builder compression(boolean compression) {
            scBuilder.compressionOption(compression ? JS_COMPRESSION : CompressionOption.None);
            return this;
        }

        /**
         * Builds the ObjectStoreConfiguration
         * @return the ObjectStoreConfiguration.
         */
        public ObjectStoreConfiguration build() {
            name = required(name, "name");
            scBuilder.name(toStreamName(name))
                .subjects(toMetaStreamSubject(name), toChunkStreamSubject(name))
                .allowRollup(true)
                .allowDirect(true)
                .discardPolicy(DiscardPolicy.New);
            return new ObjectStoreConfiguration(scBuilder.build());
        }
    }
}
