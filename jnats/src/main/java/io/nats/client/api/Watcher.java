// Copyright 2022 The NATS Authors
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

import org.jspecify.annotations.Nullable;

/**
 * Use the Watcher interface to watch for updates
 */
public interface Watcher<T> {

    /**
     * Called when an object has been updated.
     * @param t The watched object
     */
    void watch(T t);

    /**
     * Called once if there is no data when the watch is created
     * or if there is data, the first time the watch exhausts all existing data.
     */
    void endOfData();

    /**
     * The watcher can supply a prefix to use on the consumer name
     * that is generated when creating the internal watch consumer.
     * This can be useful for monitoring the consumer.
     * @return the name, or null if not needed, which is the default interface implementation.
     */
    @Nullable
    default String getConsumerNamePrefix() {
        return null;
    }
}
