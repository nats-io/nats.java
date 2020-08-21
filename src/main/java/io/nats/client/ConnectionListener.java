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
 * Applications can use a ConnectionListener to track the status of a {@link Connection Connection}. The 
 * listener is configured in the {@link Options Options} at creation time.
 */
public interface ConnectionListener {
    public enum Events {
        /** The connection has successfully completed the handshake with the nats-server. */
        CONNECTED("nats: connection opened"),
        /** The connection is permanently closed, either by manual action or failed reconnects. */
        CLOSED("nats: connection closed"),
        /** The connection lost its connection, but may try to reconnect if configured to. */
        DISCONNECTED("nats: connection disconnected"), 
        /** The connection was connected, lost its connection and successfully reconnected. */
        RECONNECTED("nats: connection reconnected"), 
        /** The connection was reconnected and the server has been notified of all subscriptions. */
        RESUBSCRIBED("nats: subscriptions re-established"),
        /** The connection was told about new servers from, from the current server. */ 
        DISCOVERED_SERVERS("nats: discovered servers"),
        /** Server Sent a lame duck mode. */
        LAME_DUCK("nats: lame duck mode");

        private String event;

        Events(String err) {
            this.event = err;
        }

        /**
         * @return the string value for this event
         */
        public String toString() {
            return this.event;
        }
    }

    /**
     * Connection related events that occur asynchronously in the client code are
     * sent to a ConnectionListener via a single method. The ConnectionListener can
     * use the event type to decide what to do about the problem.
     * 
     * @param conn the connection associated with the error
     * @param type the type of event that has occurred
     */
    public void connectionEvent(Connection conn, Events type);
}