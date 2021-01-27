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

/**
 * The JetStreamManagementOptions class specifies the general options for JetStream management.
 * Options are created using the  {@link JetStreamManagementOptions.Builder Builder}.
 */
public class JetStreamManagementOptions extends JetStreamOptions {

    private JetStreamManagementOptions(String prefix, Duration requestTimeout) {
        super(prefix, requestTimeout, false);
    }

    /**
     * JetStreamManagementOptions can be created using a Builder. The builder supports chaining and will
     * create a default set of options if no methods are calls.
     */
    public static class Builder extends JetStreamOptions.Builder {
        public Builder() {
            direct(true);
        }

        /**
         * Direct mode is always on for management and is always true.
         * @param value ignored
         * @return the builder.
         */
        public JetStreamOptions.Builder direct(boolean value) {
            // no-op, direct is always true
            return this;
        }
    }
}
