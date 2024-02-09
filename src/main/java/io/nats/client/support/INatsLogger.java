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

package io.nats.client.support;

import java.util.logging.Level;

/**
 * This interface class represents the methods that need to be implemented by a custom Logger Implementation for
 * nats.java library consumers.
 * An exemplary default implementation of this interface logging to STDOUT can be found in this repo.
 */
public interface INatsLogger {

    /**
     * Simple provisions a preformatted log line to be handled as request
     * Note that this method is called from within the nats.java thread and a long-lasting logging operation blocks
     * nats.java message handling threads or similar.
     * @param natsLogEvent The event to be logged containing e.g., level , message, originating class name and throwable all optional
     */
    void log (final NatsLogEvent natsLogEvent);
}
