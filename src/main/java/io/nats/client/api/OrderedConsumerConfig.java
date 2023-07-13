// Copyright 2023 The NATS Authors
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

import static io.nats.client.support.Validator.emptyAsNull;

public class OrderedConsumerConfig {
    private DeliverPolicy deliverPolicy;
    private Long startSequence;
    private ZonedDateTime startTime;
    private String filterSubject;
    private ReplayPolicy replayPolicy;
    private Boolean headersOnly;

    /**
     * OrderedConsumerConfig creation works like a builder.
     * The builder supports chaining and will create a default set of options if
     * no methods are calls, including setting the filter subject to "&gt;"
     */
    public OrderedConsumerConfig() {}

    /**
     * Sets the delivery policy of the OrderedConsumerConfig.
     * @param deliverPolicy the delivery policy.
     * @return Builder
     */
    public OrderedConsumerConfig deliverPolicy(DeliverPolicy deliverPolicy) {
        this.deliverPolicy = deliverPolicy;
        return this;
    }

    /**
     * Sets the start sequence of the OrderedConsumerConfig.
     * @param startSequence the start sequence
     * @return Builder
     */
    public OrderedConsumerConfig startSequence(long startSequence) {
        this.startSequence = startSequence < 1 ? null : startSequence;
        return this;
    }

    /**
     * Sets the start time of the OrderedConsumerConfig.
     * @param startTime the start time
     * @return Builder
     */
    public OrderedConsumerConfig startTime(ZonedDateTime startTime) {
        this.startTime = startTime;
        return this;
    }

    /**
     * Sets the filter subject of the OrderedConsumerConfig.
     * @param filterSubject the filter subject
     * @return Builder
     */
    public OrderedConsumerConfig filterSubject(String filterSubject) {
        this.filterSubject = emptyAsNull(filterSubject);
        return this;
    }

    /**
     * Sets the replay policy of the OrderedConsumerConfig.
     * @param replayPolicy the replay policy.
     * @return Builder
     */
    public OrderedConsumerConfig replayPolicy(ReplayPolicy replayPolicy) {
        this.replayPolicy = replayPolicy;
        return this;
    }

    /**
     * set the headers only flag saying to deliver only the headers of
     * messages in the stream and not the bodies
     * @param headersOnly the flag
     * @return Builder
     */
    public OrderedConsumerConfig headersOnly(Boolean headersOnly) {
        this.headersOnly = headersOnly;
        return this;
    }

    public DeliverPolicy getDeliverPolicy() {
        return deliverPolicy;
    }

    public Long getStartSequence() {
        return startSequence;
    }

    public ZonedDateTime getStartTime() {
        return startTime;
    }

    public String getFilterSubject() {
        return filterSubject;
    }

    public ReplayPolicy getReplayPolicy() {
        return replayPolicy;
    }

    public Boolean getHeadersOnly() {
        return headersOnly;
    }
}
