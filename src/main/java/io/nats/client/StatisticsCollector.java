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

package io.nats.client;

// TODO: Javadoc -- class and methods and indicate which stats are advanced stats
public interface StatisticsCollector extends Statistics {
    /**
     * Sets whether advanced stats are/should be tracked.
     */
    void setAdvancedTracking(boolean trackAdvanced);

    void incrementPingCount();

    /**
     * Increments the total number of times this connection has tried to reconnect.
     */
    void incrementReconnects();

    /**
     * Increments the total number of messages dropped by this connection across all slow consumers.
     */
    void incrementDroppedCount();

    void incrementOkCount();

    void incrementErrCount();

    void incrementExceptionCount();

    void incrementRequestsSent();

    void incrementRepliesReceived();

    /**
     * Increments the total number of duplicate replies received by this connection.
     * <p>
     * NOTE: This is only counted if advanced stats are enabled.
     */
    void incrementDuplicateRepliesReceived();

    /**
     * Increments the total number of orphan replies received by this connection.
     * <p>
     * NOTE: This is only counted if advanced stats are enabled.
     */
    void incrementOrphanRepliesReceived();

    /**
     * Increments the total number of messages that have come in to this connection.
     */
    void incrementInMsgs();

    /**
     * Increments the total number of messages that have gone out of this connection.
     */
    void incrementOutMsgs();

    void incrementInBytes(long bytes);

    void incrementOutBytes(long bytes);

    void incrementFlushCounter();

    void incrementOutstandingRequests();

    void decrementOutstandingRequests();

    /**
     * Registers a Socket read by this connection.
     * <p>
     * NOTE: Implementations should only count this if advanced stats are enabled.
     */
    void registerRead(long bytes);

    /**
     * Registers a Socket write by this connection.
     * <p>
     * NOTE: Implementations should only count this if advanced stats are enabled.
     */
    void registerWrite(long bytes);
}
