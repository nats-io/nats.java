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

import io.nats.client.support.*;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static io.nats.client.support.ApiConstants.*;
import static io.nats.client.support.JsonUtils.beginJson;
import static io.nats.client.support.JsonUtils.endJson;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.support.NatsConstants.GREATER_THAN;
import static io.nats.client.support.Validator.emptyAsNull;
import static io.nats.client.support.Validator.nullOrEmpty;

public class OrderedConsumerConfiguration implements JsonSerializable {

    private final List<String> filterSubjects;
    private DeliverPolicy deliverPolicy;
    private Long startSequence;
    private ZonedDateTime startTime;
    private ReplayPolicy replayPolicy;
    private Boolean headersOnly;
    private String consumerNamePrefix;

    /**
     * OrderedConsumerConfiguration creation works like a builder.
     * The builder supports chaining and will create a default set of options if
     * no methods are calls, including setting the filter subject to &gt;
     */
    public OrderedConsumerConfiguration() {
        startSequence = ConsumerConfiguration.LONG_UNSET;
        filterSubjects = new ArrayList<>();
        filterSubjects.add(GREATER_THAN);
    }

    public OrderedConsumerConfiguration(@NonNull String json) throws JsonParseException {
        this(JsonParser.parse(json));
    }

    public OrderedConsumerConfiguration(@NonNull JsonValue v) throws JsonParseException {
        this();
        filterSubjects(readStringList(v, FILTER_SUBJECTS)); // readStringList won't return null but can return empty
        deliverPolicy(DeliverPolicy.get(readString(v, DELIVER_POLICY)));
        startSequence(readLong(v, OPT_START_SEQ, ConsumerConfiguration.LONG_UNSET));
        startTime(readDate(v, OPT_START_TIME));
        replayPolicy(ReplayPolicy.get(readString(v, REPLAY_POLICY)));
        headersOnly(readBoolean(v, HEADERS_ONLY, null));
    }

    /**
     * Returns a JSON representation of this ordered consumer configuration.
     * @return JSON ordered consumer configuration JSON string
     */
    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        JsonUtils.addStrings(sb, FILTER_SUBJECTS, filterSubjects); // filter will always have at least a GREATER_THAN
        if (deliverPolicy != null) {
            JsonUtils.addField(sb, DELIVER_POLICY, deliverPolicy.toString());
        }
        JsonUtils.addFieldWhenGtZero(sb, OPT_START_SEQ, startSequence);
        JsonUtils.addField(sb, OPT_START_TIME, startTime);
        if (replayPolicy != null) {
            JsonUtils.addField(sb, REPLAY_POLICY, replayPolicy.toString());
        }
        JsonUtils.addFldWhenTrue(sb, HEADERS_ONLY, headersOnly);
        return endJson(sb).toString();
    }

    /**
     * Sets the filter subject of the OrderedConsumerConfiguration.
     * null or empty string means no filter.
     * @param filterSubject the filter subject
     * @return The Builder
     */
    public OrderedConsumerConfiguration filterSubject(String filterSubject) {
        return emptyAsNull(filterSubject) == null
            ? filterSubjects((List<String>)null)
            : filterSubjects(Collections.singletonList(filterSubject));
    }

    /**
     * Sets the filter subjects of the OrderedConsumerConfiguration.
     * A null or empty array or no items that are not null or empty means no filter
     * @param filterSubjects the filter subject
     * @return The Builder
     */
    public OrderedConsumerConfiguration filterSubjects(String... filterSubjects) {
        if (nullOrEmpty(filterSubjects)) {
            this.filterSubjects.clear();
            this.filterSubjects.add(GREATER_THAN);
        }
        return _filterSubjects(Arrays.asList(filterSubjects));
    }

    /**
     * Sets the filter subject of the OrderedConsumerConfiguration.
     * A null or empty list or no items that are not null or empty means no filter
     * @param filterSubjects one or more filter subjects
     * @return The Builder
     */
    public OrderedConsumerConfiguration filterSubjects(List<String> filterSubjects) {
        if (nullOrEmpty(filterSubjects)) {
            this.filterSubjects.clear();
            this.filterSubjects.add(GREATER_THAN);
        }
        return _filterSubjects(filterSubjects);
    }

    private OrderedConsumerConfiguration _filterSubjects(@NonNull List<String> filterSubjects) {
        this.filterSubjects.clear();
        for (String fs : filterSubjects) {
            String fsEan = emptyAsNull(fs);
            if (fsEan != null) {
                this.filterSubjects.add(fsEan);
            }
        }
        if (this.filterSubjects.isEmpty()) {
            this.filterSubjects.add(GREATER_THAN);
        }
        return this;
    }

    /**
     * Sets the delivery policy of the OrderedConsumerConfiguration.
     * @param deliverPolicy the delivery policy.
     * @return The Builder
     */
    public OrderedConsumerConfiguration deliverPolicy(DeliverPolicy deliverPolicy) {
        this.deliverPolicy = deliverPolicy;
        return this;
    }

    /**
     * Sets the start sequence of the OrderedConsumerConfiguration.
     * @param startSequence the start sequence
     * @return The Builder
     */
    public OrderedConsumerConfiguration startSequence(long startSequence) {
        this.startSequence = startSequence < 1 ? ConsumerConfiguration.LONG_UNSET : startSequence;
        return this;
    }

    /**
     * Sets the start time of the OrderedConsumerConfiguration.
     * @param startTime the start time
     * @return The Builder
     */
    public OrderedConsumerConfiguration startTime(ZonedDateTime startTime) {
        this.startTime = startTime;
        return this;
    }

    /**
     * Sets the replay policy of the OrderedConsumerConfiguration.
     * @param replayPolicy the replay policy.
     * @return The Builder
     */
    public OrderedConsumerConfiguration replayPolicy(ReplayPolicy replayPolicy) {
        this.replayPolicy = replayPolicy;
        return this;
    }

    /**
     * set the headers only flag saying to deliver only the headers of
     * messages in the stream and not the bodies
     * @param headersOnly the flag
     * @return The Builder
     */
    public OrderedConsumerConfiguration headersOnly(Boolean headersOnly) {
        this.headersOnly = headersOnly;
        return this;
    }

    /**
     * Sets the consumer name prefix for consumers created by this configuration.
     * @param consumerNamePrefix the prefix or null to clear.
     * @return The Builder
     */
    public OrderedConsumerConfiguration consumerNamePrefix(String consumerNamePrefix) {
        this.consumerNamePrefix = emptyAsNull(consumerNamePrefix);
        return this;
    }

    @Nullable
    public String getFilterSubject() {
        return filterSubjects.size() != 1 ? null : filterSubjects.get(0);
    }

    @NonNull
    public List<String> getFilterSubjects() {
        return filterSubjects;
    }

    public boolean hasMultipleFilterSubjects() {
        return filterSubjects.size() > 1;
    }

    @Nullable
    public DeliverPolicy getDeliverPolicy() {
        return deliverPolicy;
    }

    @Nullable
    public Long getStartSequence() {
        return startSequence;
    }

    @Nullable
    public ZonedDateTime getStartTime() {
        return startTime;
    }

    @Nullable
    public ReplayPolicy getReplayPolicy() {
        return replayPolicy;
    }

    @Nullable
    public Boolean getHeadersOnly() {
        return headersOnly;
    }

    public boolean isHeadersOnly() {
        return headersOnly != null && headersOnly;
    }

    public String getConsumerNamePrefix() {
        return consumerNamePrefix;
    }
}
