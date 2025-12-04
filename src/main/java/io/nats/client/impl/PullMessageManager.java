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

import static io.nats.client.impl.MessageManager.ManageResult.*;
import static io.nats.client.support.NatsJetStreamConstants.*;
import static io.nats.client.support.Status.*;

class PullMessageManager extends MessageManager {

    protected int pendingMessages;
    protected long pendingBytes;
    protected boolean trackingBytes;
    protected boolean raiseStatusWarnings;
    protected PullManagerObserver pullManagerObserver;
    protected String currentPinId;

    protected PullMessageManager(NatsConnection conn, SubscribeOptions so, boolean syncMode) {
        super(conn, so, syncMode);
        resetTracking();
    }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        sub.setBeforeQueueProcessor(this::beforeQueueProcessorImpl);
    }

    @Override
    protected void startPullRequest(String pullSubject, PullRequestOptions pro, boolean raiseStatusWarnings, PullManagerObserver pullManagerObserver) {
        stateChangeLock.lock();
        try {
            this.raiseStatusWarnings = raiseStatusWarnings;
            this.pullManagerObserver = pullManagerObserver;
            pendingMessages += pro.getBatchSize();
            pendingBytes += pro.getMaxBytes();
            trackingBytes = (pendingBytes > 0);
            configureIdleHeartbeat(pro.getIdleHeartbeat(), -1);
            if (hb.get()) {
                initOrResetHeartbeatTimer();
            }
            else {
                shutdownHeartbeatTimer(); // just in case the pull was changed from hb to non-hb
            }
        }
        finally {
            stateChangeLock.unlock();
        }
    }

    @Override
    protected void handleHeartbeatError() {
        super.handleHeartbeatError();
        resetTracking();
        if (pullManagerObserver != null) {
            pullManagerObserver.heartbeatError();
        }
    }

    private void trackIncoming(int m, long b) {
        stateChangeLock.lock();
        try {
            // message time used for heartbeat tracking
            updateLastMessageReceived();

            if (m != Integer.MIN_VALUE) {
                pendingMessages -= m;
                boolean zero = pendingMessages < 1;
                if (trackingBytes) {
                    pendingBytes -= b;
                    zero |= pendingBytes < 1;
                }
                if (zero) {
                    resetTracking();
                }
                if (pullManagerObserver != null) {
                    pullManagerObserver.pendingUpdated();
                }
            }
        }
        finally {
            stateChangeLock.unlock();
        }
    }

    protected void resetTracking() {
        pendingMessages = 0;
        pendingBytes = 0;
        trackingBytes = false;
        updateLastMessageReceived();
    }

    @Override
    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        Status status = msg.getStatus();

        // normal js message
        if (status == null) {
            trackIncoming(1, msg.consumeByteCount());
            return true;
        }

        // heartbeat just needed to be recorded
        if (status.isHeartbeat()) {
            updateLastMessageReceived(); // no need to call track incoming, this is all it does
            return false;
        }

        int m = Integer.MIN_VALUE;
        long b = Long.MIN_VALUE;
        Headers h = msg.getHeaders();
        if (h != null) {
            try {
                //noinspection DataFlowIssue WE ALREADY CATCH THE EXCEPTION
                m = Integer.parseInt(h.getFirst(NATS_PENDING_MESSAGES));
                //noinspection DataFlowIssue WE ALREADY CATCH THE EXCEPTION
                b = Long.parseLong(h.getFirst(NATS_PENDING_BYTES));
            }
            catch (NumberFormatException ignore) {
                m = Integer.MIN_VALUE; // shouldn't happen but don't fail; make sure don't track m/b
            }
        }
        trackIncoming(m, b);
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

    @Override
    protected void subTrackJsMessage(Message msg) {
        if (msg.hasHeaders()) {
            currentPinId = msg.getHeaders().getFirst(NATS_PIN_ID_HDR);
        }
    }

    protected ManageResult manageStatus(Message msg) {
        Status status = msg.getStatus();
        switch (status.getCode()) {
            case NOT_FOUND_CODE:
            case REQUEST_TIMEOUT_CODE:
            case NO_RESPONDERS_CODE:
                if (raiseStatusWarnings) {
                    conn.notifyErrorListener((c, el) -> el.pullStatusWarning(c, sub, status));
                }
                return STATUS_TERMINUS;

            case CONFLICT_CODE:
                // sometimes just a warning
                String statMsg = status.getMessage();
                if (statMsg.startsWith(EXCEEDED_MAX_PREFIX) || statMsg.equals(SERVER_SHUTDOWN))
                {
                    if (raiseStatusWarnings) {
                        conn.notifyErrorListener((c, el) -> el.pullStatusWarning(c, sub, status));
                    }
                    return STATUS_HANDLED;
                }

                if (statMsg.equals(BATCH_COMPLETED)
                    || statMsg.equals(LEADERSHIP_CHANGE)
                    || statMsg.equals(MESSAGE_SIZE_EXCEEDS_MAX_BYTES))
                {
                    if (raiseStatusWarnings) {
                        conn.notifyErrorListener((c, el) -> el.pullStatusWarning(c, sub, status));
                    }
                    return STATUS_TERMINUS;
                }
                break;

            case PIN_ERROR_CODE:
                currentPinId = null;
                return STATUS_TERMINUS;
        }

        // All unknown 409s are errors, since that basically means the client is not aware of them.
        // These known ones are also errors: "Consumer Deleted" and "Consumer is push based"
        conn.notifyErrorListener((c, el) -> el.pullStatusError(c, sub, status));
        return STATUS_ERROR;
    }

    protected boolean noMorePending() {
        return pendingMessages < 1 || (trackingBytes && pendingBytes < 1);
    }
}
