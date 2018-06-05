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

public interface ErrorHandler {

    public enum Errors {
        ERR_CONNECTION_CLOSED      ("nats: connection closed"),
        ERR_SECURE_CONN_REQUIRED   ("nats: secure connection required"),
        ERR_SECURE_CONN_WANTED     ("nats: secure connection not available"),
        ERR_BAD_SUBSCRIPTION       ("nats: invalid subscription"),
        ERR_TYPE_SUBSCRIPTION      ("nats: invalid subscription type"),
        ERR_BAD_SUBJECT            ("nats: invalid subject"),
        ERR_SLOW_CONSUMER          ("nats: slow consumer, messages dropped"),
        ERR_TIMEOUT                ("nats: timeout"),
        ERR_BAD_TIMEOUT            ("nats: timeout invalid"),
        ERR_AUTHORIZATION          ("nats: authorization violation"),
        ERR_NO_SERVERS             ("nats: no servers available for connection"),
        ERR_JSON_PARSE             ("nats: connect message, json parse error"),
        ERR_CHAN_ARG               ("nats: argument needs to be a channel type"),
        ERR_MAX_PAYLOAD            ("nats: maximum payload exceeded"),
        ERR_MAX_MESSAGES           ("nats: maximum messages delivered"),
        ERR_SYNC_SUB_REQUIRED      ("nats: illegal call on an async subscription"),
        ERR_MULTIPLE_TLS_CONFIGS   ("nats: multiple tls.Configs not allowed"),
        ERR_NO_INFO_RECEIVED       ("nats: protocol exception, INFO not received"),
        ERR_RECONNECT_BUF_EXCEEDED ("nats: outbound buffer limit exceeded"),
        ERR_INVALID_CONNECTION     ("nats: invalid connection"),
        ERR_INVALID_MSG            ("nats: invalid message or message nil"),
        ERR_INVALID_ARG            ("nats: invalid argument"),
        ERR_INVALID_CONTEXT        ("nats: invalid context"),
        ERR_STALE_CONNECTION       ("nats: stale connection");

        private String err;

        Errors(String err) {
            this.err = err;
        }

        public String getErr() {
            return this.err;
        }
    }

    /**
     * Errors that occur asynchronously in the client code are sent to an ErrorHandler via a single method.
     * The ErrorHandler can use the error type to decide what to do about the problem.
     * @param conn The connection associated with the error
     * @param sub The subscriber associated with the error, can be null
     * @param type The type of error that has occured
     */
    public void errorOccurred(Connection conn, Subscription sub, Errors type);
}