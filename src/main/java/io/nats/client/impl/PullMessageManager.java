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

import io.nats.client.Message;
import io.nats.client.PullRequestOptions;
import io.nats.client.SubscribeOptions;
import io.nats.client.support.Status;

import java.util.ArrayList;
import java.util.List;

import static io.nats.client.impl.MessageManager.ManageResult.*;
import static io.nats.client.support.NatsJetStreamConstants.NATS_PENDING_BYTES;
import static io.nats.client.support.NatsJetStreamConstants.NATS_PENDING_MESSAGES;
import static io.nats.client.support.Status.*;

class PullMessageManager extends MessageManager {

    protected int pendingMessages;
    protected long pendingBytes;
    protected boolean trackingBytes;
    protected boolean raiseStatusWarnings;
    protected PullManagerObserver pullManagerObserver;
    protected boolean initialized;
    protected List<String> unansweredPulls;

    protected PullMessageManager(NatsConnection conn, SubscribeOptions so, boolean syncMode) {
        super(conn, so, syncMode);
        initialized = false;
        unansweredPulls = new ArrayList<>();
        reset();
    }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        sub.setBeforeQueueProcessor(this::beforeQueueProcessorImpl);
    }

    @Override
    protected void startPullRequest(String pullSubject, PullRequestOptions pro, boolean raiseStatusWarnings, PullManagerObserver pullManagerObserver) {
        synchronized (stateChangeLock) {
            this.raiseStatusWarnings = raiseStatusWarnings;
            this.pullManagerObserver = pullManagerObserver;
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
            unansweredPulls.add(pullSubject);
        }
    }

    @Override
    protected void handleHeartbeatError() {
        super.handleHeartbeatError();
        reset();
        if (pullManagerObserver != null) {
            pullManagerObserver.heartbeatError();
        }
    }

    private void trackIncoming(int m, long b, String pullsubject) {
        synchronized (stateChangeLock) {
            // message time used for heartbeat tracking
            // subjects used to detect multiple failed heartbeats
            lastMsgReceived = System.currentTimeMillis();

            if (pullsubject == null) {
                unansweredPulls.clear();
            }
            else {
                unansweredPulls.remove(pullsubject);
            }

            if (m != Integer.MIN_VALUE) {
                pendingMessages -= m;
                boolean zero = pendingMessages < 1;
                if (trackingBytes) {
                    pendingBytes -= b;
                    zero |= pendingBytes < 1;
                }
                if (zero) {
                    reset();
                }

                if (pullManagerObserver != null) {
                    pullManagerObserver.pendingUpdated();
                }
            }
        }
    }

    protected void reset() {
        pendingMessages = 0;
        pendingBytes = 0;
        trackingBytes = false;
        if (initialized && hb) {
            shutdownHeartbeatTimer();
        }
        initialized = true;
    }

    protected boolean hasUnansweredPulls() {
        synchronized (stateChangeLock) {
            return !unansweredPulls.isEmpty();
        }
    }

    @Override
    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        Status status = msg.getStatus();

        // normal js message
        if (status == null) {
            trackIncoming(1, msg.consumeByteCount(), null);
            return true;
        }

        // heartbeat just needed to be recorded
        if (status.isHeartbeat()) {
            trackIncoming(Integer.MIN_VALUE, -1, msg.subject);
            return false;
        }

        Headers h = msg.getHeaders();
        int m = Integer.MIN_VALUE;
        long b = -1;
        if (h != null) {
            String s = h.getFirst(NATS_PENDING_MESSAGES);
            if (s != null) {
                try {
                    m = Integer.parseInt(s);
                    b = Long.parseLong(h.getFirst(NATS_PENDING_BYTES));
                }
                catch (NumberFormatException ignore) {
                    m = Integer.MIN_VALUE; // shouldn't happen but don't fail; make sure don't track m/b
                }
            }
        }
        trackIncoming(m, b, msg.subject);
        return true;
    }

    @Override
    protected ManageResult manage(Message msg) {
        // normal js message
        if (msg.getStatus() == null) {
            trackJsMessage(msg);
            return MESSAGE;
        }
        return manageStatus(msg);
    }

    protected ManageResult manageStatus(Message msg) {
        Status status = msg.getStatus();
        switch (status.getCode()) {
            case NOT_FOUND_CODE:
            case REQUEST_TIMEOUT_CODE:
                if (raiseStatusWarnings) {
                    conn.executeCallback((c, el) -> el.pullStatusWarning(c, sub, status));
                }
                return STATUS_TERMINUS;

            case CONFLICT_CODE:
                // sometimes just a warning
                String statMsg = status.getMessage();
                if (statMsg.startsWith("Exceeded Max")) {
                    if (raiseStatusWarnings) {
                        conn.executeCallback((c, el) -> el.pullStatusWarning(c, sub, status));
                    }
                    return STATUS_HANDLED;
                }

                if (statMsg.equals(BATCH_COMPLETED) ||
                    statMsg.equals(MESSAGE_SIZE_EXCEEDS_MAX_BYTES))
                {
                    return STATUS_TERMINUS;
                }
                break;
        }

        // all others are errors
        conn.executeCallback((c, el) -> el.pullStatusError(c, sub, status));
        return STATUS_ERROR;
    }

    protected boolean noMorePending() {
        return pendingMessages < 1 || (trackingBytes && pendingBytes < 1);
    }
}
