// Copyright 2026 The NATS Authors
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

import io.nats.client.support.JsonSerializable;
import io.nats.client.support.JsonValue;
import io.nats.client.support.JsonValueUtils;
import org.jspecify.annotations.NonNull;

import java.util.Objects;

import static io.nats.client.support.ApiConstants.DELIVER_SUBJECT;
import static io.nats.client.support.ApiConstants.NAME;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.support.Validator.validateConsumerName;
import static io.nats.client.support.Validator.validateSubject;

/**
 * Consumer information for durable sourcing. Dictates that a durable consumer with a specific
 * name is used for sourcing.
 */
public class ConsumerSource implements JsonSerializable {
    private final String name;
    private final String deliverSubject;

    static ConsumerSource optionalInstance(JsonValue vConsumerSource) {
        return vConsumerSource == null ? null : new ConsumerSource(vConsumerSource);
    }

    ConsumerSource(JsonValue vConsumerSource) {
        name = JsonValueUtils.readString(vConsumerSource, NAME);
        deliverSubject = JsonValueUtils.readString(vConsumerSource, DELIVER_SUBJECT);
    }

    /**
     * Construct the ConsumerSource configuration
     * @param name the consumer name
     * @param deliverSubject the deliver subject
     */
    public ConsumerSource(@NonNull String name, @NonNull String deliverSubject) {
        this.name = validateConsumerName(name, true);
        this.deliverSubject = validateSubject(deliverSubject, true);
    }

    /**
     * Construct a ConsumerSource configuration copying the information from another ConsumerSource
     * @param consumerSource the source configuration
     */
    public ConsumerSource(@NonNull ConsumerSource consumerSource) {
        this.name = consumerSource.name;
        this.deliverSubject = consumerSource.deliverSubject;
    }

    @Override
    @NonNull
    public String toJson() {
        StringBuilder sb = beginJson();
        addField(sb, NAME, name);
        addField(sb, DELIVER_SUBJECT, deliverSubject);
        return endJson(sb).toString();
    }

    /**
     * The durable consumer name used for sourcing.
     * @return the consumer name
     */
    @NonNull
    public String getName() {
        return name;
    }

    /**
     * The subject to deliver messages to.
     * @return the deliver subject
     */
    @NonNull
    public String getDeliverSubject() {
        return deliverSubject;
    }

    @Override
    public String toString() {
        return "ConsumerSource{" +
                "name='" + name + '\'' +
                ", deliverSubject='" + deliverSubject + '\'' +
                '}';
    }

    @Override
    public final boolean equals(Object o) {
        if (!(o instanceof ConsumerSource)) return false;

        ConsumerSource that = (ConsumerSource) o;
        return Objects.equals(name, that.name) && Objects.equals(deliverSubject, that.deliverSubject);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(name);
        result = 31 * result + Objects.hashCode(deliverSubject);
        return result;
    }

    /**
     * Creates a builder for a ConsumerSource object.
     * @return the builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * ConsumerSource can be created using a Builder.
     */
    public static class Builder {
        private String name;
        private String deliverSubject;

        /**
         * Construct a builder for a ConsumerSource object
         */
        public Builder() {}

        /**
         * Set the consumer name.
         * @param name the consumer name
         * @return the builder
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * Set the deliver subject.
         * @param deliverSubject the deliver subject
         * @return the builder
         */
        public Builder deliverSubject(String deliverSubject) {
            this.deliverSubject = deliverSubject;
            return this;
        }

        /**
         * Build a ConsumerSource object
         * @return the ConsumerSource object
         */
        public ConsumerSource build() {
            return new ConsumerSource(name, deliverSubject);
        }
    }
}
