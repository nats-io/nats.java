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

package io.nats.client.jetstream;

/**
 * The Jetstream Account Statistics
 */
public interface AccountStatistics {

    /**
     * Gets the amount of memory used by the Jetstream deployment.
     *
     * @return bytes
     */
    long getMemory();

    /**
     * Gets the amount of storage used by  the Jetstream deployment.
     *
     * @return bytes
     */
    long getStorage();

    /**
     * Gets the number of streams used by the Jetstream deployment.
     *
     * @return stream maximum count
     */
    long getStreams();

    /**
     * Gets the number of consumers used by the Jetstream deployment.
     *
     * @return consumer maximum count
     */
    long getConsumers();
}
