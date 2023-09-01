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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.nats.client.support.Validator.emptyAsNull;

public class OrderedConsumerConfiguration {

    public static String DEFAULT_FILTER_SUBJECT = ">";

    private final List<String> filterSubjects;
    private DeliverPolicy deliverPolicy;
    private Long startSequence;
    private ZonedDateTime startTime;
    private ReplayPolicy replayPolicy;
    private Boolean headersOnly;

    /**
     * OrderedConsumerConfiguration creation works like a builder.
     * The builder supports chaining and will create a default set of options if
     * no methods are calls, including setting the filter subject to "&gt;"
     */
    public OrderedConsumerConfiguration() {
        startSequence = ConsumerConfiguration.LONG_UNSET;
        filterSubjects = new ArrayList<>();
        filterSubjects.add(DEFAULT_FILTER_SUBJECT);
    }

    /**
     * Sets the filter subject of the OrderedConsumerConfiguration.
     * @param filterSubject the filter subject
     * @return Builder
     */
    public OrderedConsumerConfiguration filterSubject(String filterSubject) {
        return filterSubjects(Collections.singletonList(filterSubject));
    }

    /**
     * Sets the filter subjects of the OrderedConsumerConfiguration.
     * @param filterSubject the filter subject
     * @return Builder
     */
    public OrderedConsumerConfiguration filterSubjects(String... filterSubject) {
        return filterSubjects(Arrays.asList(filterSubject));
    }

    /**
     * Sets the filter subject of the OrderedConsumerConfiguration.
     * @param filterSubjects one or more filter subjects
     * @return Builder
     */
    public OrderedConsumerConfiguration filterSubjects(List<String> filterSubjects) {
        this.filterSubjects.clear();
        for (String fs : filterSubjects) {
            String fsean = emptyAsNull(fs);
            if (fsean != null) {
                this.filterSubjects.add(fsean);
            }
        }
        if (this.filterSubjects.isEmpty()) {
            this.filterSubjects.add(DEFAULT_FILTER_SUBJECT);
        }
        return this;
    }

    /**
     * Sets the delivery policy of the OrderedConsumerConfiguration.
     * @param deliverPolicy the delivery policy.
     * @return Builder
     */
    public OrderedConsumerConfiguration deliverPolicy(DeliverPolicy deliverPolicy) {
        this.deliverPolicy = deliverPolicy;
        return this;
    }

    /**
     * Sets the start sequence of the OrderedConsumerConfiguration.
     * @param startSequence the start sequence
     * @return Builder
     */
    public OrderedConsumerConfiguration startSequence(long startSequence) {
        this.startSequence = startSequence < 1 ? ConsumerConfiguration.LONG_UNSET : startSequence;
        return this;
    }

    /**
     * Sets the start time of the OrderedConsumerConfiguration.
     * @param startTime the start time
     * @return Builder
     */
    public OrderedConsumerConfiguration startTime(ZonedDateTime startTime) {
        this.startTime = startTime;
        return this;
    }

    /**
     * Sets the replay policy of the OrderedConsumerConfiguration.
     * @param replayPolicy the replay policy.
     * @return Builder
     */
    public OrderedConsumerConfiguration replayPolicy(ReplayPolicy replayPolicy) {
        this.replayPolicy = replayPolicy;
        return this;
    }

    /**
     * set the headers only flag saying to deliver only the headers of
     * messages in the stream and not the bodies
     * @param headersOnly the flag
     * @return Builder
     */
    public OrderedConsumerConfiguration headersOnly(Boolean headersOnly) {
        this.headersOnly = headersOnly != null && headersOnly ? true : null;
        return this;
    }

    public String getFilterSubject() {
        return filterSubjects.get(0);
    }

    public List<String> getFilterSubjects() {
        return filterSubjects;
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

    public ReplayPolicy getReplayPolicy() {
        return replayPolicy;
    }

    public Boolean getHeadersOnly() {
        return headersOnly;
    }
}
