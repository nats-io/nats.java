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

/**
 * The classified reason a request returned no response, surfaced on a {@link RequestFailureException}
 * (JetStream requests) or carried by a {@link io.nats.client.impl.RequestFailureMessage} (core
 * requests) when {@link Options.Builder#advancedRequestBehavior()} is enabled.
 */
public enum RequestFailureReason {
    /** The request deadline elapsed with no reply (real server slowness or a short timeout). */
    TIMEOUT,
    /** The in-flight request was cancelled by a disconnect, reconnect, or connection close. */
    CONNECTION_CLOSING,
    /** A 503 No Responders - nothing was subscribed to the subject (e.g. no stream covers it). */
    NO_RESPONDERS,
    /** The server sent a protocol -ERR (permissions violation, max payload, ...); see lastError. */
    PROTOCOL_ERROR,
    /** The request was cancelled for an unclassified reason. */
    CANCELLED
}
