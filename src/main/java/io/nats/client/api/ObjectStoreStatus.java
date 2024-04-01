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

import io.nats.client.support.JsonValueUtils;

import java.time.Duration;
import java.util.Map;

/**
 * The ObjectStoreStatus class contains information about an object store.
 */
public class ObjectStoreStatus {

    private final StreamInfo streamInfo;
    private final ObjectStoreConfiguration config;

    public ObjectStoreStatus(StreamInfo si) {
        streamInfo = si;
        config = new ObjectStoreConfiguration(streamInfo.getConfiguration());
    }

    /**
     * Get the name of the object store
     * @return the name
     */
    public String getBucketName() {
        return config.getBucketName();
    }

    /**
     * Gets the description of this bucket.
     * @return the description of the bucket.
     */
    public String getDescription() {
        return config.getDescription();
    }

    /**
     * Gets the info for the stream which backs the bucket. Valid for BackingStore "JetStream"
     * @return the stream info
     */
    public StreamInfo getBackingStreamInfo() {
        return streamInfo;
    }

    /**
     * Gets the configuration object directly
     * @return the configuration.
     */
    public ObjectStoreConfiguration getConfiguration() {
        return config;
    }

    /**
     * Get the combined size of all data in the bucket including metadata, in bytes
     * @return the size
     */
    public long getSize() {
        return streamInfo.getStreamState().getByteCount();
    }

    /**
     * Gets the maximum number of bytes for this bucket.
     * @return the maximum number of bytes for this bucket.
     */
    public long getMaxBucketSize() {
        return config.getMaxBucketSize();
    }

    /**
     * If true, indicates the store is sealed and cannot be modified in any way
     * @return the sealed setting
     */
    public boolean isSealed() {
        return config.isSealed();
    }

    /**
     * Gets the maximum age for a value in this store.
     * @return the maximum age.
     */
    public Duration getTtl() {
        return config.getTtl();
    }

    /**
     * Gets the storage type for this bucket.
     * @return the storage type for this stream.
     */
    public StorageType getStorageType() {
        return config.getStorageType();
    }

    /**
     * Gets the number of replicas for this store.
     * @return the number of replicas
     */
    public int getReplicas() {
        return config.getReplicas();
    }

    /**
     * Gets the placement directive for the store.
     * @return the placement
     */
    public Placement getPlacement() {
        return config.getPlacement();
    }

    /**
     * Gets the state of compression
     * @return true if compression is used
     */
    public boolean isCompressed() {
        return config.isCompressed();
    }

    /**
     * Get the metadata for the store
     * @return the metadata map. Might be null.
     */
    public Map<String, String> getMetadata() {
        return config.getMetadata();
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
        JsonValueUtils.MapBuilder mb = new JsonValueUtils.MapBuilder();
        mb.put("size", getSize());
        mb.put("isSealed", isSealed());
        mb.put("config", config);
        return "ObjectStoreStatus" + mb.toJsonValue().toJson();
    }
}
