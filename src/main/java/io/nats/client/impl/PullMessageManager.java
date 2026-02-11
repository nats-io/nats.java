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

    protected boolean raiseStatusWarnings;
    protected PullManagerObserver pullManagerObserver;
    protected String currentPinId;

    protected PullMessageManager(NatsConnection conn, SubscribeOptions so, boolean syncMode) {
        super(conn, so, syncMode);
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
        if (pullManagerObserver != null) {
            pullManagerObserver.pullTerminatedByError();
        }
    }

    @Override
    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        updateLastMessageReceived();

        Status status = msg.getStatus();

        // normal js message
        if (status == null) {
            if (pullManagerObserver != null) {
                pullManagerObserver.messageReceived(msg);
            }
            return true;
        }

        // heartbeat just needed to updateLastMessageReceived
        if (status.isHeartbeat()) {
            return false;
        }

        // all other status messages return true, but some have work to do.

        // pin error or status with pending headers
        // pass that info on as soon as possible, before manage
        Headers h = msg.getHeaders();
        if (h != null) {
            try {
                //noinspection DataFlowIssue WE ALREADY CATCH THE EXCEPTION
                int m = Integer.parseInt(h.getFirst(NATS_PENDING_MESSAGES));
                //noinspection DataFlowIssue WE ALREADY CATCH THE EXCEPTION
                long b = Long.parseLong(h.getFirst(NATS_PENDING_BYTES));
                if (pullManagerObserver != null) {
                    pullManagerObserver.pullCompletedWithStatus(m, b);
                }
            }
            catch (NumberFormatException ignore) {}
        }
        return true;
    }

    @Override
    protected ManageResult manage(Message msg) {
        if (msg.isJetStream()) {
            trackJsMessage(msg);
            checkForPin(msg);
            return MESSAGE;
        }
        return manageStatus(msg);
    }

    protected void checkForPin(Message msg) {
        if (msg.hasHeaders()) {
            String pinId = msg.getHeaders().getFirst(NATS_PIN_ID_HDR);
            if (pinId != null) {
                currentPinId = pinId;
            }
        }
    }

    protected ManageResult manageStatus(Message msg) {
        Status status = msg.getStatus();
        switch (status.getCode()) {
            case PIN_ERROR_CODE:
                currentPinId = null;
                if (pullManagerObserver != null) {
                    pullManagerObserver.pullCompletedWithStatus(-1, -1);
                }
                // no break PIN_ERROR_CODE is STATUS_TERMINUS
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
        }

        // All unknown 409s are errors, since that basically means the client is not aware of them.
        // These known ones are also errors: "Consumer Deleted" and "Consumer is push based"
        conn.notifyErrorListener((c, el) -> el.pullStatusError(c, sub, status));
        return STATUS_ERROR;
    }
}
