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

package io.nats.client;

import io.nats.client.api.ConsumerConfiguration;

import static io.nats.client.support.Validator.emptyAsNull;

/**
 * The PushSubscribeOptions class specifies the options for subscribing with JetStream enabled servers.
 * Options are created using the constructors or a {@link Builder}.
 */
public class PushSubscribeOptions extends SubscribeOptions {

    private PushSubscribeOptions(String stream, String durable, String deliverSubject, boolean bindMode, ConsumerConfiguration consumerConfig) {
        super(stream, durable, deliverSubject, bindMode, false, consumerConfig);
    }

    /**
     * Gets the deliver subject held in the consumer configuration.
     * @return the Deliver subject
     */
    public String getDeliverSubject() {
        return consumerConfig.getDeliverSubject();
    }

    /**
     * Macro to create a default PushSubscribeOptions except for
     * where you must specify the stream because
     * the subject could apply to both a stream and a mirror.
     * @deprecated
     * This method is no longer used as bind has a different meaning.
     * See {@link #stream(String)} instead.
     * @param stream the stream name
     * @return push subscribe options
     */
    @Deprecated
    public static PushSubscribeOptions bind(String stream) {
        return new Builder().stream(stream).build();
    }

    /**
     * Macro to create a default PushSubscribeOptions except for
     * where you must specify the stream because
     * the subject could apply to both a stream and a mirror.
     * @param stream the stream name
     * @return push subscribe options
     */
    public static PushSubscribeOptions stream(String stream) {
        return new Builder().stream(stream).build();
    }

    /**
     * Macro to create a PushSubscribeOptions where you are
     * binding to an existing stream and durable consumer.
     * @param stream the stream name
     * @param durable the durable name
     * @return push subscribe options
     */
    public static PushSubscribeOptions bind(String stream, String durable) {
        return new PushSubscribeOptions.Builder().stream(stream).durable(durable).bind(true).build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * PushSubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder
            extends SubscribeOptions.Builder<Builder, PushSubscribeOptions> {
        private String deliverSubject;

        @Override
        protected Builder getThis() {
            return this;
        }

        /**
         * Setting this specifies the push model to a delivery subject.
         * Null or empty clears the field.
         * @param deliverSubject the subject to deliver on.
         * @return the builder.
         */
        public Builder deliverSubject(String deliverSubject) {
            this.deliverSubject = emptyAsNull(deliverSubject);
            return this;
        }

        /**
         * Builds the push subscribe options.
         * @return push subscribe options
         */
        @Override
        public PushSubscribeOptions build() {
            return new PushSubscribeOptions(stream, durable, deliverSubject, isBind, consumerConfig);
        }
    }
}

