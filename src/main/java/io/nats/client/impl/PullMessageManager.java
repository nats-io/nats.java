// Copyright 2021 The NATS Authors
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

import io.nats.client.JetStreamStatusException;
import io.nats.client.Message;
import io.nats.client.PullRequestOptions;
import io.nats.client.support.Status;

import static io.nats.client.support.NatsJetStreamConstants.NATS_PENDING_BYTES;
import static io.nats.client.support.NatsJetStreamConstants.NATS_PENDING_MESSAGES;
import static io.nats.client.support.Status.*;

class PullMessageManager extends MessageManager {

    protected long pendingMessages;
    protected long pendingBytes;
    protected boolean trackingBytes;

    protected PullMessageManager(NatsConnection conn, boolean syncMode) {
        super(conn, syncMode);
        trackingBytes = false;
        pendingMessages = 0;
        pendingBytes = 0;
    }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        sub.setBeforeQueueProcessor(this::beforeQueueProcessorImpl);
    }

    @Override
    protected void startPullRequest(PullRequestOptions pro) {
        synchronized (stateChangeLock) {
            pendingMessages += pro.getBatchSize();
            pendingBytes += pro.getMaxBytes();
            trackingBytes = (pendingBytes > 0);
            configureIdleHeartbeat(pro.getIdleHeartbeat(), -1);
            if (hb) {
                initOrResetHeartbeatTimer();
            }
            else {
                shutdownHeartbeatTimer();
            }
        }
    }

    private void trackPending(long m, long b) {
        synchronized (stateChangeLock) {
            pendingMessages -= m;
            pendingBytes -= b;
            if (pendingMessages < 1 || (trackingBytes && pendingBytes < 1)) {
                pendingMessages = 0;
                pendingBytes = 0;
                trackingBytes = false;
                if (hb) {
                    shutdownHeartbeatTimer();
                }
            }
        }
    }

    @Override
    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        messageReceived(); // record message time. Used for heartbeat tracking

        Status status = msg.getStatus();

        // normal js message
        if (status == null) {
            trackPending(1, bytesInMessage(msg));
            return true;
        }

        // heartbeat just needed to be recorded
        if (status.isHeartbeat()) {
            return false;
        }

        Headers h = msg.getHeaders();
        if (h != null) {
            String s = h.getFirst(NATS_PENDING_MESSAGES);
            if (s != null) {
                try {
                    long m = Long.parseLong(s);
                    long b = Long.parseLong(h.getFirst(NATS_PENDING_BYTES));
                    trackPending(m, b);
                }
                catch (Exception ignore) {}
            }
        }

        // not found or timeout only have message/byte tracking, so no need for them to be queued (return false)
        // all other statuses are either warnings or errors and handled in manage
        int statusCode = status.getCode();
        return statusCode != NOT_FOUND_CODE && statusCode != REQUEST_TIMEOUT_CODE;
    }

    @Override
    protected boolean manage(Message msg) {
        Status status = msg.getStatus();

        // normal js message
        if (status == null) {
            trackJsMessage(msg);
            return false;
        }

        int statusCode = status.getCode();
        if (statusCode == CONFLICT_CODE) {
            // sometimes just a warning
            if (status.getMessage().contains("Exceed")) {
                conn.executeCallback((c, el) -> el.pullStatusWarning(c, sub, status));
                return true;
            }
            // fall through
        }

        // all others are errors
        conn.executeCallback((c, el) -> el.pullStatusError(c, sub, status));
        if (syncMode) {
            throw new JetStreamStatusException(sub, status);
        }

        return true; // all status are managed
    }

    private long bytesInMessage(Message msg) {
        NatsMessage nm = (NatsMessage) msg;
        return nm.subject.length()
            + nm.headerLen
            + nm.dataLen
            + (nm.replyTo == null ? 0 : nm.replyTo.length());
    }
}
