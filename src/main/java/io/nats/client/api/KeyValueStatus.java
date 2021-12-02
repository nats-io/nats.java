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

    public StreamInfo getBackingStreamInfo() {
        return streamInfo;
    }
    /**
     * Gets the stream configuration.
     * @return the stream configuration.
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
     * Gets the maximum age for a value in this bucket.
     * @return the maximum age.
     */
    public Duration getTtl() {
        return config.getTtl();
    }

    /**
     * Gets the name of the type of backing store for the
     * @return the name of the store, currently only "JetStream"
     */
    public String getBackingStore() {
        return "JetStream";
    }
}
