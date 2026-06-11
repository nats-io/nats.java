// Copyright 2026 The NATS Authors
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

import io.nats.client.impl.RequestFailureMessage;

import java.io.IOException;
import java.time.Duration;

/**
 * Carries the detail of a request that came back without a response, but only when
 * {@link Options.Builder#advancedRequestBehavior()} is enabled. It wraps a
 * {@link RequestFailureMessage} and exposes a specific {@link RequestFailureReason} plus the connection
 * status and last protocol error captured at the moment the request failed, so the actual cause (timeout
 * vs. reconnect vs. permissions vs. no-responders) can be told apart. The original caught exception is
 * chained as the cause (an {@link java.util.concurrent.ExecutionException} is unwrapped to its cause;
 * see {@link RequestFailureMessage#getCause()}). Extends {@link IOException} so existing catch blocks and
 * signatures are unaffected.
 *
 * <p>JetStream requests (publish, stream/consumer management) throw this in place of the generic
 * "Timeout or no response waiting for NATS JetStream server" IOException. Core
 * {@link Connection#request(String, byte[], java.time.Duration) Connection.request(...)} calls instead
 * return the {@link RequestFailureMessage} carrier directly (it is a {@link Message}), which callers can
 * inspect or wrap in this exception.
 */
public class RequestFailureException extends IOException {

    private final RequestFailureMessage rfm;

    public RequestFailureException(RequestFailureMessage rfm) {
        super(buildMessage(rfm), rfm.getCause());
        this.rfm = rfm;
    }

    private static String buildMessage(RequestFailureMessage rfm) {
        StringBuilder sb = new StringBuilder("Request failed [reason=").append(rfm.getReason())
            .append(", connectionStatus=").append(rfm.getConnectionStatus())
            .append(", waited=").append(rfm.getWaited());
        String lastError = rfm.getLastError();
        if (lastError != null && !lastError.isEmpty()) {
            sb.append(", lastError=").append(lastError);
        }
        return sb.append(']').toString();
    }

    /**
     * @return the classified reason the request returned no response
     */
    public RequestFailureReason getReason() {
        return rfm.getReason();
    }

    /**
     * @return the connection status at the moment the request failed
     */
    public Connection.Status getConnectionStatus() {
        return rfm.getConnectionStatus();
    }

    /**
     * @return the last protocol error reported by the server (may be null or empty)
     */
    public String getLastError() {
        return rfm.getLastError();
    }

    /**
     * @return how long the request waited before failing
     */
    public Duration getWaited() {
        return rfm.getWaited();
    }
}
