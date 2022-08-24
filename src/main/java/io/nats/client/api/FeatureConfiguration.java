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

public abstract class FeatureConfiguration {
    protected final StreamConfiguration sc;
    protected final String bucketName;


    public FeatureConfiguration(StreamConfiguration sc, String bucketName) {
        this.sc = sc;
        this.bucketName = bucketName;
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
     * Placement directives to consider when placing replicas of this stream,
     * random placement when unset
     * @return the placement [directive object]
     */
    public Placement getPlacement() {
        return sc.getPlacement();
    }
}
