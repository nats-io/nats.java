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

import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.Status.*;

class PullMessageManager extends MessageManager {

    protected final Object pendingLock;
    protected Long pendingMessages;
    protected Long pendingBytes;
    protected boolean trackingBytes;

    protected PullMessageManager(NatsConnection conn, boolean syncMode) {
        super(conn, syncMode);
        pendingLock = new Object();
        trackingBytes = false;
        pendingMessages = 0L;
        pendingBytes = 0L;
    }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        sub.setBeforeQueueProcessor(this::beforeQueueProcessorImpl);
    }

    @Override
    protected void startPullRequest(PullRequestOptions pro) {
        synchronized (pendingLock) {
            pendingMessages += pro.getBatchSize();
            pendingBytes += pro.getMaxBytes();
            trackingBytes = (pendingBytes > 0);
        }
        initIdleHeartbeat(pro.getIdleHeartbeat(), -1);
        if (hb) {
            initOrResetHeartbeatTimer();
        }
        else {
            shutdownHeartbeatTimer();
        }
    }

    private void trackPending(long m, long b) {
        synchronized (pendingLock) {
            if (m > 0) {
                pendingMessages -= m;
            }
            if (trackingBytes && b > 0) {
                pendingBytes -= b;
            }
            if (pendingMessages < 1 || (trackingBytes && pendingBytes < 1)) {
                pendingMessages = 0L;
                pendingBytes = 0L;
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
            String s;
            long m = ((s = h.getFirst(NATS_PENDING_MESSAGES)) == null) ? -1 : Long.parseLong(s);
            long b = ((s = h.getFirst(NATS_PENDING_BYTES)) == null) ? -1 : Long.parseLong(s);
            trackPending(m, b);
        }

        int statusCode = status.getCode();
        // these codes are tracked and nothing else
        if (statusCode == NOT_FOUND_CODE || statusCode == REQUEST_TIMEOUT_CODE) {
            return false;
        }

        // CONFLICT_CODE + BATCH_COMPLETED is discarded b/c it is handled other ways (return false)
        // all other statuses are passed on (return true)
        return statusCode != CONFLICT_CODE || !status.getMessage().startsWith(BATCH_COMPLETED);
    }

    @Override
    protected boolean manage(Message msg) {
        if (msg.getStatus() == null) {
            trackJsMessage(msg);
            return false;
        }

        Status status = msg.getStatus();
        int statusCode = status.getCode();
        if (statusCode == CONFLICT_CODE) {
            // sometimes just a warning
            if (status.getMessage().contains("Exceed")) {
                conn.executeCallback((c, el) -> el.pullStatusWarning(c, sub, status));
                return true;
            }
            // fall through
        }

        // all others are fatal
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
