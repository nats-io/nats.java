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

/**
 * The KeyValueStatus class contains information about a Key Value Bucket.
 */
public class KeyValueStatus {

    private final StreamInfo streamInfo;
    private final KeyValueConfiguration config;

    public KeyValueStatus(StreamInfo si) {
        streamInfo = si;
        config = new KeyValueConfiguration(streamInfo.getConfiguration());
    }

    /**
     * Get the name of the bucket
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
    public KeyValueConfiguration getConfiguration() {
        return config;
    }

    /**
     * Get the number of total entries in the bucket, including historical entries
     * @return the count of entries
     */
    public long getEntryCount() {
        return streamInfo.getStreamState().getMsgCount();
    }

    /**
     * Gets the maximum number of history for any one key. Includes the current value.
     * @return the maximum number of values for any one key.
     */
    public long getMaxHistoryPerKey() {
        return config.getMaxHistoryPerKey();
    }

    /**
     * Gets the maximum number of bytes for this bucket.
     * @return the maximum number of bytes for this bucket.
     */
    public long getMaxBucketSize() {
        return config.getMaxBucketSize();
    }

    /**
     * Gets the maximum size for an individual value in the bucket.
     * @return the maximum size a value.
     */
    public long getMaxValueSize() {
        return config.getMaxValueSize();
    }

    /**
     * Gets the maximum age for a value in this bucket.
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
     * Gets the number of replicas for this bucket.
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
     * Gets the republish configuration
     * @return the republish object
     */
    public Republish getRepublish() {
        return config.getRepublish();
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
        return "KeyValueStatus{" +
            "name='" + getBucketName() + '\'' +
            ", description='" + getDescription() + '\'' +
            ", entryCount=" + getEntryCount() +
            ", maxHistoryPerKey=" + getMaxHistoryPerKey() +
            ", maxBucketSize=" + getMaxBucketSize() +
            ", maxValueSize=" + getMaxValueSize() +
            ", ttl=" + getTtl() +
            ", storageType=" + getStorageType() +
            ", replicas=" + getReplicas() +
            '}';
    }
}
