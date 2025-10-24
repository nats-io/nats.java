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

/**
 * A collector for connection metrics.
 * <p>
 * Information about key metrics is incremented on this collector by the connection.
 * <p>
 * See {@link Statistics} for accessing the collected metrics.
 */
public interface StatisticsCollector extends Statistics {
    /**
     * Sets whether advanced stats are/should be tracked.
     * @param trackAdvanced the advanced tracking flag. set to true to turn on advanced tracking
     */
    void setAdvancedTracking(boolean trackAdvanced);

    /**
     * Increments the total number of pings that have been sent from this connection.
     */
    void incrementPingCount();

    /**
     * Increments the total number of times this connection has tried to reconnect.
     */
    void incrementReconnects();

    /**
     * Increments the total number of messages dropped by this connection across all slow consumers.
     */
    void incrementDroppedCount();

    /**
     * Increments the total number of op +OKs received by this connection.
     */
    void incrementOkCount();

    /**
     * Increments the total number of op -ERRs received by this connection.
     */
    void incrementErrCount();

    /**
     * Increments the total number of exceptions seen by this connection.
     */
    void incrementExceptionCount();

    /**
     * Increments the total number of requests sent by this connection.
     */
    void incrementRequestsSent();

    /**
     * Increments the total number of replies received by this connection.
     */
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
     * Increments the total number of messages that have come in to this connection
     * by 1 AND the number of bytes in the same call.
     * @param bytes the number of bytes
     */
    default void incrementIn(long bytes) {
        incrementInMsgs();
        incrementInBytes(bytes);
    }

    /**
     * Increments the total number of messages that have gone out of this connection.
     * by 1 AND the number of bytes in the same call.
     * @param bytes the number of bytes
     */
    default void incrementOut(long bytes) {
        incrementOutMsgs();
        incrementOutBytes(bytes);
    }

    /**
     * @deprecated Was always called with incrementInBytes. Replaced with incrementIn(long bytes)
     * Increments the total number of messages that have come in to this connection.
     */
    @Deprecated
    void incrementInMsgs();

    /**
     * @deprecated Was always called with incrementOutBytes. Replaced with incrementOut(long bytes)
     * Increments the total number of messages that have gone out of this connection.
     */
    @Deprecated
    void incrementOutMsgs();

    /**
     * @deprecated Was always called with incrementIn. Replaced with incrementIn(long bytes)
     * Increment the total number of message bytes that have come in to this connection.
     * @param bytes the number of bytes coming in
     */
    @Deprecated
    void incrementInBytes(long bytes);

    /**
     * @deprecated Was always called with incrementOut. Replaced with incrementOut(long bytes)
     * Increment the total number of message bytes that have gone out of this connection.
     * @param bytes the number of bytes going out
     */
    @Deprecated
    void incrementOutBytes(long bytes);

    /**
     * Increment the total number of outgoing message flushes by this connection.
     */
    void incrementFlushCounter();

    /**
     * Increments the count of outstanding of requests from this connection.
     */
    void incrementOutstandingRequests();

    /**
     * Decrements the count of outstanding of requests from this connection.
     */
    void decrementOutstandingRequests();

    /**
     * Registers a Socket read by this connection.
     * <p>NOTE: Implementations should only count this if advanced stats are enabled.</p>
     * @param bytes the number of bytes being read
     */
    void registerRead(long bytes);

    /**
     * Registers a Socket write by this connection.
     * <p>NOTE: Implementations should only count this if advanced stats are enabled.</p>
     * @param bytes the number of bytes being written
     */
    void registerWrite(long bytes);
}
