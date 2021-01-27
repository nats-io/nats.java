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

import java.time.Duration;

import static io.nats.client.support.Validator.validateJetStreamPrefix;

/**
 * The JetStreamManagementOptions class specifies the general options for JetStream management.
 * Options are created using the  {@link JetStreamManagementOptions.Builder Builder}.
 */
public class JetStreamManagementOptions extends JetStreamOptions {

    private JetStreamManagementOptions(String prefix, Duration requestTimeout) {
        super(prefix, requestTimeout, false);
    }

    /**
     * SubscribeOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder {

        private String prefix = null;
        private Duration requestTimeout = Options.DEFAULT_CONNECTION_TIMEOUT;

        /**
         * Sets the request timeout for JetStream API calls.
         * @param timeout the duration to wait for responses.
         * @return the builder
         */
        public Builder requestTimeout(Duration timeout) {
            if (timeout != null) {
                this.requestTimeout = timeout;
            }
            return this;
        }

        /**
         * Sets the prefix for JetStream Management subjects. A prefix can be used in conjunction with
         * user permissions to restrict access to certain JetStream instances.  This must
         * match the prefix used in the server.
         * @param value the JetStream Management prefix
         * @return the builder.
         */
        public Builder prefix(String value) {
            this.prefix = validateJetStreamPrefix(value);
            return this;
        }

        /**
         * Builds the JetStream Management options.
         * @return JetStreamManagement options
         */
        public JetStreamManagementOptions build() {
            return new JetStreamManagementOptions(prefix, requestTimeout);
        }
    }

}
