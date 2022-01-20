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

import static io.nats.client.support.NatsObjectStoreUtil.*;
import static io.nats.client.support.Validator.validateObjectStoreBucketNameRequired;

/**
 * The ObjectStoreConfiguration class contains the configuration for Object Store
 */
public class ObjectStoreConfiguration {
    private final StreamConfiguration sc;
    private final String storeName;

    static ObjectStoreConfiguration instance(String json) {
        return new ObjectStoreConfiguration(StreamConfiguration.instance(json));
    }

    ObjectStoreConfiguration(StreamConfiguration sc) {
        this.sc = sc;
        storeName = extractBucketName(sc.getName());
    }

    /**
     * If true, indicates the stream is sealed and cannot be modified in any way
     * @return the stream configuration
     */
    public StreamConfiguration getBackingConfig() {
        return sc;
    }

    /**
     * Gets the name of this object store.
     * @return the name of the object store.
     */
    public String getStoreName() {
        return storeName;
    }

    /**
     * Gets the description of this object store.
     * @return the description of the object store.
     */
    public String getDescription() {
        return sc.getDescription();
    }

    /**
     * Gets the maximum age for a value in this object store.
     * @return the maximum age.
     */
    public Duration getTtl() {
        return sc.getMaxAge();
    }

    /**
     * Gets the storage type for this object store.
     * @return the storage type for this stream.
     */
    public StorageType getStorageType() {
        return sc.getStorageType();
    }

    /**
     * Gets the number of replicas for this object store.
     * @return the number of replicas
     */
    public int getReplicas() {
        return sc.getReplicas();
    }

    @Override
    public String toString() {
        return "ObjectStoreConfiguration{" +
            "name='" + storeName + '\'' +
            ", description='" + getDescription() + '\'' +
            ", ttl=" + getTtl() +
            ", storageType=" + getStorageType() +
            ", replicas=" + getReplicas() +
            '}';
    }

    /**
     * Creates a builder for the Key Value Configuration.
     * @return a key value configuration builder
     */
    public static ObjectStoreConfiguration.Builder builder() {
        return new ObjectStoreConfiguration.Builder();
    }

    /**
     * Creates a builder to copy the key value configuration.
     * @param osc an existing KeyValueConfiguration
     * @return a stream configuration builder
     */
    public static ObjectStoreConfiguration.Builder builder(ObjectStoreConfiguration osc) {
        return new ObjectStoreConfiguration.Builder(osc);
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
         * @param osc the configuration to copy
         */
        public Builder(ObjectStoreConfiguration osc) {
            if (osc == null) {
                scBuilder = new StreamConfiguration.Builder();
                replicas(1);
            }
            else {
                scBuilder = new StreamConfiguration.Builder(osc.sc);
                name = extractBucketName(osc.sc.getName());
            }
        }

        /**
         * Sets the name of the store.
         * @param name name of the store.
         * @return the builder
         */
        public ObjectStoreConfiguration.Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Sets the description of the store.
         * @param description description of the store.
         * @return the builder
         */
        public ObjectStoreConfiguration.Builder description(String description) {
            scBuilder.description(description);
            return this;
        }

        /**
         * Sets the maximum age for a value in this KeyValueConfiguration.
         * @param ttl the maximum age
         * @return Builder
         */
        public ObjectStoreConfiguration.Builder ttl(Duration ttl) {
            scBuilder.maxAge(ttl);
            return this;
        }

        /**
         * Sets the storage type in the KeyValueConfiguration.
         * @param storageType the storage type
         * @return Builder
         */
        public ObjectStoreConfiguration.Builder storageType(StorageType storageType) {
            scBuilder.storageType(storageType);
            return this;
        }

        /**
         * Sets the number of replicas a message must be stored on in the KeyValueConfiguration.
         * @param replicas the number of replicas
         * @return Builder
         */
        public ObjectStoreConfiguration.Builder replicas(int replicas) {
            scBuilder.replicas(Math.max(replicas, 1));
            return this;
        }

        /**
         * Builds the KeyValueConfiguration
         * @return the KeyValueConfiguration.
         */
        public ObjectStoreConfiguration build() {
            name = validateObjectStoreBucketNameRequired(name);
            scBuilder.name(toStreamName(name))
                .subjects(toMetaStreamSubject(name), toChunkStreamSubject(name))
                .discardPolicy(DiscardPolicy.New)
                .allowRollup(true);
            return new ObjectStoreConfiguration(scBuilder.build());
        }
    }
}
