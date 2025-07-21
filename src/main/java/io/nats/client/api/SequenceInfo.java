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

import io.nats.client.support.JsonValue;
import org.jspecify.annotations.Nullable;

import java.time.ZonedDateTime;

import static io.nats.client.support.ApiConstants.LAST_ACTIVE;
import static io.nats.client.support.JsonValueUtils.readDate;

/**
 * This class holds the sequence numbers for a consumer and
 * stream and .
 */
public class SequenceInfo extends SequencePair {

    private final ZonedDateTime lastActive;

    SequenceInfo(JsonValue vSequenceInfo) {
        super(vSequenceInfo);
        lastActive = readDate(vSequenceInfo, LAST_ACTIVE);
    }

    /**
     * The last time a message was delivered or acknowledged (for ack_floor)
     * @return the last active time
     */
    @Nullable
    public ZonedDateTime getLastActive() {
        return lastActive;
    }

    @Override
    public String toString() {
        return "SequenceInfo{" +
            "consumerSeq=" + consumerSeq +
            ", streamSeq=" + streamSeq +
            ", lastActive=" + lastActive +
            '}';
    }
}
