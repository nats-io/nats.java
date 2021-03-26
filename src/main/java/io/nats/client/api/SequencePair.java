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

import io.nats.client.support.JsonUtils;

import static io.nats.client.support.ApiConstants.CONSUMER_SEQ_RE;
import static io.nats.client.support.ApiConstants.STREAM_SEQ_RE;

/**
 * This class holds the sequence numbers for a consumer and
 * stream.
 */
public class SequencePair {
    private final long consumerSeq;
    private final long streamSeq;

    SequencePair(String json) {
        consumerSeq = JsonUtils.readLong(json, CONSUMER_SEQ_RE, 0);
        streamSeq = JsonUtils.readLong(json, STREAM_SEQ_RE, 0);
    }

    /**
     * Gets the consumer sequence number.
     *
     * @return seqence number.
     */
    public long getConsumerSequence() {
        return consumerSeq;
    }

    /**
     * Gets the stream sequence number.
     *
     * @return sequence number.
     */
    public long getStreamSequence() {
        return streamSeq;
    }

    @Override
    public String toString() {
        return "SequencePair{" +
                "consumerSeq=" + consumerSeq +
                ", streamSeq=" + streamSeq +
                '}';
    }
}
