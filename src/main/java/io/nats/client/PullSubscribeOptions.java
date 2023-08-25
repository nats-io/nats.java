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

/**
 * The PullSubscribeOptions class specifies the options for subscribing with JetStream enabled servers.
 * Options are set using the {@link PullSubscribeOptions.Builder} or static helper methods.
 */
public class PullSubscribeOptions extends SubscribeOptions {
    public static final PullSubscribeOptions DEFAULT_PULL_OPTS = PullSubscribeOptions.builder().build();

    private PullSubscribeOptions(Builder builder) {
        super(builder, true, null, null, -1, -1);
    }

    /**
     * Macro to start a PullSubscribeOptions builder
     * @return push subscribe options builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Create PullSubscribeOptions for binding to
     * a specific stream and consumer by name.
     * The client validates regular (non-fast)
     * binds to ensure that provided consumer configuration
     * is consistent with the server version and that
     * consumer type (push versus pull) matches the subscription type.
     * and that it matches the subscription type.
     * @param stream the stream name to bind to
     * @param name the consumer name
     * @return push subscribe options
     */
    public static PullSubscribeOptions bind(String stream, String name) {
        return new Builder().stream(stream).name(name).bind(true).build();
    }

    /**
     * Create PullSubscribeOptions where you are fast-binding to
     * a specific stream and consumer by name.
     * The client does not validate that the provided consumer configuration
     * is consistent with the server version or that
     * consumer type (push versus pull) matches the subscription type.
     * An inconsistent consumer configuration for instance can result in
     * receiving messages from unexpected subjects.
     * A consumer type mismatch will result in an error from the server.
     * @param stream the stream name to bind to
     * @param name the consumer name, commonly the durable name
     * @return push subscribe options
     */
    public static PullSubscribeOptions fastBind(String stream, String name) {
        return new Builder().stream(stream).name(name).fastBind(true).build();
    }

    /**
     * PullSubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder
            extends SubscribeOptions.Builder<Builder, PullSubscribeOptions> {

        @Override
        protected Builder getThis() {
            return this;
        }

        /**
         * Builds the pull subscribe options.
         * @return pull subscribe options
         */
        @Override
        public PullSubscribeOptions build() {
            return new PullSubscribeOptions(this);
        }

        /**
         * Specify binding to an existing consumer via name.
         * The client does not validate that the provided consumer configuration
         * is consistent with the server version or that
         * consumer type (push versus pull) matches the subscription type.
         * An inconsistent consumer configuration for instance can result in
         * receiving messages from unexpected subjects.
         * A consumer type mismatch will result in an error from the server.
         * @return the builder
         * @param fastBind whether to fast bind or not
         */
        public Builder fastBind(boolean fastBind) {
            this.fastBind = fastBind;
            return this;
        }
    }
}
