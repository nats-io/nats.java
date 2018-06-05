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

public interface ConnectionHandler {
    public enum Events {
        CONNECTION_CLOSED     ("nats: connection closed"),
        DISCONNECTED          ("nats: connection disconnected"),
        RECONNECTED           ("nats: connection reconnected"),
        DISCOVERED_SERVERS    ("nats: discovered servers");

        private String event;

        Events(String err) {
            this.event = err;
        }

        public String getEvent() {
            return this.event;
        }
    }

    /**
     * Connection related events that occur asynchronously in the client code are sent to a
     * ConnectionHandler via a single method.
     * The ConnectionHandler can use the event type to decide what to do about the problem.
     * @param conn The connection associated with the error
     * @param type The type of event that has occured
     */
    public void connectionEvent(Connection conn, Events type);
}