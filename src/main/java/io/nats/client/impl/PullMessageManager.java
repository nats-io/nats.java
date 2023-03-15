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
import io.nats.client.support.PullStatus;
import io.nats.client.support.Status;

import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.NatsJetStreamConstants.NATS_PENDING_BYTES;
import static io.nats.client.support.NatsJetStreamConstants.NATS_PENDING_MESSAGES;
import static io.nats.client.support.Status.NOT_FOUND_CODE;
import static io.nats.client.support.Status.REQUEST_TIMEOUT_CODE;

class PullMessageManager extends MessageManager {

    protected final AtomicLong pendingMessages;
    protected final AtomicLong pendingBytes;

    protected PullMessageManager(NatsConnection conn, boolean syncMode) {
        super(conn, syncMode);
        this.pendingMessages = new AtomicLong(0);
        this.pendingBytes = new AtomicLong(0);
    }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        sub.setBeforeQueueProcessor(this::beforeQueueProcessorImpl);
    }

    @Override
    protected void startPullRequest(PullRequestOptions pro) {
        pendingMessages.addAndGet(pro.getBatchSize());
        pendingBytes.addAndGet(pro.getMaxBytes());
        initIdleHeartbeat(pro.getIdleHeartbeat(), -1);
        if (hb) {
            initOrResetHeartbeatTimer();
        }
        else {
            shutdownHeartbeatTimer();
        }
    }

    @Override
    protected PullStatus getPullStatus() {
        return new PullStatus(pendingMessages.get(), pendingBytes.get(), hb);
    }

    private void trackPending(long mo, long bo) {
        boolean reachedEnd = false;
        if (mo > 0) {
            if (pendingMessages.addAndGet(-mo) < 1) {
                reachedEnd = true;
            }
        }
        if (bo > 0) {
            if (pendingBytes.addAndGet(-bo) < 1) {
                reachedEnd = true;
            }
        }
        if (reachedEnd) {
            pendingMessages.set(0);
            pendingBytes.set(0);
            if (hb) {
                shutdownHeartbeatTimer();
            }
        }
    }

    @Override
    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        if (hb) {
            messageReceived();
            Status status = msg.getStatus();
            // only plain heartbeats do not get queued (return false == not queued)
            //     normal message || status but not hb
            return status == null || !status.isHeartbeat();
        }
        return true;
    }

    @Override
    protected boolean manage(Message msg) {
        if (msg.getStatus() == null) {
            trackJsMessage(msg);
            trackPending(1, bytesInMessage(msg));
            return false;
        }

        Status status = msg.getStatus();
        Headers h = msg.getHeaders();
        if (h != null) {
            String s;
            long mo = ((s = h.getFirst(NATS_PENDING_MESSAGES)) == null) ? -1 : Long.parseLong(s);
            long bo = ((s = h.getFirst(NATS_PENDING_BYTES)) == null) ? -1 : Long.parseLong(s);
            trackPending(mo, bo);
        }

        int statusCode = status.getCode();
        if (statusCode != NOT_FOUND_CODE && statusCode != REQUEST_TIMEOUT_CODE) {
            conn.executeCallback((c, el) -> el.errorPullStatus(c, sub, status));
            if (syncMode) {
                throw new JetStreamStatusException(sub, status);
            }
        }

        return true; // all status are managed
    }

    private long bytesInMessage(Message msg) {
        return msg.getSubject().length()
            + (msg.getReplyTo() == null ? 0 : msg.getReplyTo().length())
            + (msg.getData() == null ? 0 : msg.getData().length);
    }
}
