// Copyright 2020-2023 The NATS Authors
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

import org.jspecify.annotations.Nullable;

/**
 * The Ordered Consumer and it's context provide a simplification interface to the ordered consumer behavior.
 */
public interface OrderedConsumerContext extends BaseConsumerContext {
    /**
     * Gets the consumer name created for the underlying Ordered Consumer
     * This will return null until the first consume (next, iterate, fetch, consume)
     * is executed because the JetStream consumer, which carries the name,
     * has not been created yet.
     * <p>
     * The consumer name is subject to change for 2 reasons.
     * 1. Any time next(...) is called
     * 2. Anytime a message is received out of order for instance because of a disconnection
     * </p>
     * <p>If your OrderedConsumerConfiguration has a consumerNamePrefix,
     * the consumer name will always start with the prefix
     * </p>
     * @return the consumer name or null
     */
    @Override
    @Nullable
    String getConsumerName();
}
