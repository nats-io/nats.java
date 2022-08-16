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

import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;

import java.time.Duration;

/**
 * The ObjectStoreStatus class contains information about an object store.
 *
 * THIS IS A PLACEHOLDER FOR THE EXPERIMENTAL OBJECT STORE IMPLEMENTATION.
 */
public class ObjectStoreConfiguration {

    private final StreamInfo streamInfo;
    private final StreamConfiguration sc;
    private final String storeName;

    public ObjectStoreConfiguration(StreamInfo si) {
        streamInfo = si;
        sc = si.getConfiguration();
        // TODO name of store, other stuff, see KV config
        storeName = null;
    }

    /**
     * Get the name of the object store
     * @return the name
     */
    public String getStoreName() {
        return storeName;
    }

    /**
     * Gets the description of this bucket.
     * @return the description of the bucket.
     */
    public String getDescription() {
        return sc.getDescription();
    }

    /**
     * Gets the stream configuration for the stream which backs the bucket
     * @return the stream configuration
     */
    public StreamConfiguration getBackingConfig() {
        return sc;
    }

    /**
     * Get the combined size of all data in the bucket including metadata, in bytes
     * @return the size
     */
    public long getSize() {
        return streamInfo.getStreamState().getByteCount();
    }

    /**
     * If true, indicates the stream is sealed and cannot be modified in any way
     * @return the sealed setting
     */
    public boolean isSealed() {
        return getBackingConfig().getSealed();
    }

    /**
     * Gets the maximum age for a value in this store.
     * @return the maximum age.
     */
    public Duration getTtl() {
        // TODO
        return null;
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
     * Gets the name of the type of backing store, currently only "JetStream"
     * @return the name of the store, currently only "JetStream"
     */
    public String getBackingStore() {
        return "JetStream";
    }

    @Override
    public String toString() {
        return "ObjectStoreStatus{" +
            "name='" + getStoreName() + '\'' +
            ", description='" + getDescription() + '\'' +
            ", ttl=" + getTtl() +
            ", storageType=" + getStorageType() +
            ", replicas=" + getReplicas() +
            ", isSealed=" + isSealed() +
            '}';
    }
}
