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

import java.time.ZonedDateTime;

import static io.nats.client.support.JsonUtils.objectString;

/**
 * The BucketInfo class contains information about a Key Value Bucket.
 */
public class BucketInfo {

    private final StreamInfo streamInfo;
    private final BucketConfiguration config;


    public BucketInfo(StreamInfo si) {
        streamInfo = si;
        config = new BucketConfiguration(streamInfo.getConfiguration());
    }
    
    /**
     * Gets the stream configuration.
     * @return the stream configuration.
     */
    public BucketConfiguration getConfiguration() {
        return config;
    }

    public long getRecordCount() {
        return streamInfo.getStreamState().getMsgCount();
    }

    public long getByteCount() {
        return streamInfo.getStreamState().getByteCount();
    }

    public long getLastSequence() {
        return streamInfo.getStreamState().getLastSequence();
    }

    /**
     * Gets the creation time of the stream.
     * @return the creation date and time.
     */
    public ZonedDateTime getCreateTime() {
        return streamInfo.getCreateTime();
    }

    @Override
    public String toString() {
        return "BucketInfo{" +
                "created=" + getCreateTime() +
                "recordCount=" + getRecordCount() +
                "byteCount=" + getByteCount() +
                "lastSequence=" + getLastSequence() +
                ", " + objectString("config", config) +
                '}';
    }
}
