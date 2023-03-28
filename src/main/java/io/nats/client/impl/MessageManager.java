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

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;

abstract class MessageManager {
    protected static final int THRESHOLD = 3;

    protected final NatsConnection conn;
    protected final boolean syncMode;

    protected NatsJetStreamSubscription sub;

    protected long lastStreamSeq;
    protected long lastConsumerSeq;
    protected long lastMsgReceived;

    // heartbeat stuff
    protected boolean hb;
    protected long idleHeartbeatSetting;
    protected long alarmPeriodSetting;

    protected TimerTask heartbeatTimerTask;
    protected Timer heartbeatTimer;

    protected MessageManager(NatsConnection conn, boolean syncMode) {
        this.conn = conn;
        this.syncMode = syncMode;
        lastStreamSeq = 0;
        lastConsumerSeq = 0;

        hb = false;
        idleHeartbeatSetting = 0;
        alarmPeriodSetting = 0;

        messageReceived(); // initializes lastMsgReceived;
    }

    protected boolean isSyncMode()           { return syncMode; }
    protected long getLastStreamSequence()   { return lastStreamSeq; }
    protected long getLastConsumerSequence() { return lastConsumerSeq; }
    protected long getLastMsgReceived()      { return lastMsgReceived; }
    protected boolean isHb()                 { return hb; }
    protected long getIdleHeartbeatSetting() { return idleHeartbeatSetting; }
    protected long getAlarmPeriodSetting()   { return alarmPeriodSetting; }

    protected void startup(NatsJetStreamSubscription sub) {
        this.sub = sub;
    }

    protected void shutdown() {
        shutdownHeartbeatTimer();
    }

    protected void startPullRequest(PullRequestOptions pullRequestOptions) {
        // does nothing - only implemented for pulls, but in base class since instance is always referenced as MessageManager, not subclass
    }

    protected void messageReceived() {
        lastMsgReceived = System.currentTimeMillis();
    }

    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        return true;
    }

    abstract protected boolean manage(Message msg);

    protected void trackJsMessage(Message msg) {
        NatsJetStreamMetaData meta = msg.metaData();
        lastStreamSeq = meta.streamSequence();
        lastConsumerSeq++;
    }

    protected void handleHeartbeatError() {
        conn.executeCallback((c, el) -> el.heartbeatAlarm(c, sub, lastStreamSeq, lastConsumerSeq));
    }

    protected void configureIdleHeartbeat(Duration configIdleHeartbeat, long configMessageAlarmTime) {
        idleHeartbeatSetting = configIdleHeartbeat == null ? 0 : configIdleHeartbeat.toMillis();
        if (idleHeartbeatSetting <= 0) {
            alarmPeriodSetting = 0;
            hb = false;
        }
        else {
            if (configMessageAlarmTime < idleHeartbeatSetting) {
                alarmPeriodSetting = idleHeartbeatSetting * THRESHOLD;
            }
            else {
                alarmPeriodSetting = configMessageAlarmTime;
            }
            hb = true;
        }
    }

    synchronized protected void initOrResetHeartbeatTimer() {
        shutdownHeartbeatTimer();
        heartbeatTimer = new Timer();
        heartbeatTimerTask = new TimerTask() {
            @Override
            public void run() {
                long sinceLast = System.currentTimeMillis() - lastMsgReceived;
                if (sinceLast > alarmPeriodSetting) {
                    handleHeartbeatError();
                }
            }
        };
        scheduleHeartbeatTimerTask();
    }

    synchronized private void scheduleHeartbeatTimerTask() {
        heartbeatTimer.schedule(heartbeatTimerTask, alarmPeriodSetting, alarmPeriodSetting);
    }

    synchronized protected void shutdownHeartbeatTimer() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel();
            heartbeatTimer = null;
        }
    }
}
