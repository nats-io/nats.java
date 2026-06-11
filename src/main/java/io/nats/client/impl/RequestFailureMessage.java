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

package io.nats.client.impl;

import io.nats.client.Connection;
import io.nats.client.RequestFailureException;
import io.nats.client.RequestFailureReason;
import io.nats.client.support.NatsRequestCompletableFuture;

import java.time.Duration;
import java.util.concurrent.CancellationException;

/**
 * Internal carrier returned by {@link NatsConnection#requestInternal} in place of a null when a
 * request comes back without a response and {@link io.nats.client.Options#advancedRequestBehavior()}
 * is enabled. Construction classifies why the request came back empty (the {@link RequestFailureReason})
 * from the state captured at failure time, and carries enough detail for the JetStream layer to throw a
 * specific {@link RequestFailureException}. It is never published and never travels over the wire; the
 * public {@code Connection.request(...)} methods strip it back to null to preserve their null-on-timeout
 * contract.
 */
public class RequestFailureMessage extends NatsMessage {

    private final RequestFailureReason reason;
    private final Connection.Status connectionStatus;
    private final String lastError;
    private final Duration waited;
    private final Throwable cause;

    RequestFailureMessage(NatsRequestCompletableFuture future,
                                    Throwable cause,
                                    Duration waited,
                                    Connection.Status connectionStatus,
                                    String lastError) {
        super();
        this.cause = cause;
        this.waited = waited;
        this.connectionStatus = connectionStatus;
        this.lastError = lastError;
        this.reason = classify(future, cause, connectionStatus, lastError);
    }

    private static RequestFailureReason classify(NatsRequestCompletableFuture future,
                                                 Throwable cause,
                                                 Connection.Status connectionStatus,
                                                 String lastError) {
        if (future.wasCancelledClosing()
                || connectionStatus == Connection.Status.RECONNECTING
                || connectionStatus == Connection.Status.DISCONNECTED) {
            return RequestFailureReason.CONNECTION_CLOSING;
        }
        if (lastError != null && !lastError.isEmpty()) {
            return RequestFailureReason.PROTOCOL_ERROR;
        }
        if (cause instanceof CancellationException
                && !future.wasCancelledTimedOut()
                && future.getCancelAction() == NatsRequestCompletableFuture.CancelAction.CANCEL) {
            return RequestFailureReason.NO_RESPONDERS;
        }
        return RequestFailureReason.TIMEOUT;
    }

    public RequestFailureReason getReason() {
        return reason;
    }

    public Connection.Status getConnectionStatus() {
        return connectionStatus;
    }

    public String getLastError() {
        return lastError;
    }

    public Duration getWaited() {
        return waited;
    }

    /**
     * The exception caught while waiting for the response, kept so the underlying detail is never lost.
     * It is the {@link java.util.concurrent.TimeoutException} or
     * {@link java.util.concurrent.CancellationException} as caught; for a failure delivered as an
     * {@link java.util.concurrent.ExecutionException} it is that exception's
     * {@linkplain Throwable#getCause() cause} (the wrapper is unwrapped, so a timeout from the internal
     * cleanup task surfaces the same {@code TimeoutException} as a timeout from the request deadline).
     * @return the underlying cause of the failure (may be null)
     */
    public Throwable getCause() {
        return cause;
    }
}
