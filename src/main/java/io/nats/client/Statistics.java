// Copyright 2015-2018 The NATS Authors
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
 * Connections can provide an instance of Statistics, {@link Connection#getStatistics() getStatistics()}. The statistics
 * object provides information about key metrics related to the connection over its entire lifecycle.
 * 
 * <p>The Statistics toString() provides a summary of the statistics.
 */
public interface Statistics {

    // TODO: Javadoc -- methods and indicate which stats are advanced stats

    long getPings();

    /**
     * @return the total number of times this connection has tried to reconnect.
     */
    long getReconnects();

    /**
     * @return the total number of messages dropped by this connection across all slow consumers.
     */
    long getDroppedCount();

    long getOKs();

    long getErrs();

    long getExceptions();

    long getRequestsSent();

    long getRepliesReceived();

    long getDuplicateRepliesReceived();

    long getOrphanRepliesReceived();

    /**
     * @return the total number of messages that have come in to this connection.
     */
    long getInMsgs();

    /**
     * @return the total number of messages that have gone out of this connection.
     */
    long getOutMsgs();

    /**
     * @return the total number of message bytes that have come in to this connection.
     */
    long getInBytes();

    /**
     * @return the total number of message bytes that have gone out of to this connection.
     */
    long getOutBytes();

    long getFlushCounter();

    long getOutstandingRequests();
}
