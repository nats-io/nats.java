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
import io.nats.client.support.Status;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

class PullMessageManager extends MessageManager {
    /*
        Known Pull Statuses
        -------------------------------------------
        409 Consumer is push based
        409 Exceeded MaxRequestBatch of %d
        409 Exceeded MaxRequestExpires of %v
        409 Exceeded MaxRequestMaxBytes of %v
        409 Message Size Exceeds MaxBytes
        409 Exceeded MaxWaiting
        404 No Messages
        408 Request Timeout
    */

    protected static final List<Integer> WARNINGS = Arrays.asList(404, 408);

    protected final NatsConnection conn;
    protected final boolean syncMode;
    protected final boolean hb;

    protected final long idleHeartbeatSetting;
    protected final long alarmPeriodSetting;

    protected long lastStreamSeq;
    protected long lastConsumerSeq;

    protected final AtomicLong lastMsgReceived;

    public PullMessageManager(NatsConnection conn,
                              boolean syncMode)
    {
        this.conn = conn;
        this.syncMode = syncMode;
        lastStreamSeq = -1;
        lastConsumerSeq = -1;
        lastMsgReceived = new AtomicLong();

        // for now...
        hb = false;
        idleHeartbeatSetting = 0;
        alarmPeriodSetting = 0;
    }

    @Override
    void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
//        if (hb) {
//            sub.setBeforeQueueProcessor(this::beforeQueueProcessor);
//            heartbeatTimer = new PushMessageManager.HeartbeatTimer();
//        }
    }

    NatsMessage beforeQueueProcessor(NatsMessage msg) {
        lastMsgReceived.set(System.currentTimeMillis());
        return msg;
    }

    boolean manage(Message msg) {
        if (msg.isStatusMessage()) {
            Status status = msg.getStatus();
            if ( !WARNINGS.contains(status.getCode()) ) {
                if (syncMode) {
                    throw new JetStreamStatusException(sub, status);
                }
                conn.getOptions().getErrorListener().unhandledStatus(conn, sub, status);
            }
            return true;
        }

        // JS Message
        lastStreamSeq = msg.metaData().streamSequence();
        lastConsumerSeq = msg.metaData().consumerSequence();
        subManage(msg);
        return false;
    }

    protected void subManage(Message msg) {}

    boolean isSyncMode() { return syncMode; }
    boolean isHb() { return hb; }

    long getIdleHeartbeatSetting() { return idleHeartbeatSetting; }
    long getAlarmPeriodSetting() { return alarmPeriodSetting; }

    long getLastStreamSequence() { return lastStreamSeq; }
    long getLastConsumerSequence() { return lastConsumerSeq; }
    long getLastMsgReceived() { return lastMsgReceived.get(); }
}
