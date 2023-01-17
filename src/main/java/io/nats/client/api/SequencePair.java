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
import io.nats.client.support.JsonValueUtils;

import static io.nats.client.support.ApiConstants.CONSUMER_SEQ;
import static io.nats.client.support.ApiConstants.STREAM_SEQ;

/**
 * This class holds the sequence numbers for a consumer and
 * stream.
 */
public class SequencePair {
    protected final long consumerSeq;
    protected final long streamSeq;

    SequencePair(JsonValue vSequencePair) {
        consumerSeq = JsonValueUtils.readLong(vSequencePair, CONSUMER_SEQ, 0);
        streamSeq = JsonValueUtils.readLong(vSequencePair, STREAM_SEQ, 0);
    }

    /**
     * Gets the consumer sequence number.
     * @return sequence number.
     */
    public long getConsumerSequence() {
        return consumerSeq;
    }

    /**
     * Gets the stream sequence number.
     * @return sequence number.
     */
    public long getStreamSequence() {
        return streamSeq;
    }
}
