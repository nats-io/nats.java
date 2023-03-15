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
import io.nats.client.support.PullStatus;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

abstract class MessageManager {
    protected static final int THRESHOLD = 3;

    protected final NatsConnection conn;
    protected final boolean syncMode;

    protected NatsJetStreamSubscription sub;

    protected long lastStreamSeq;
    protected long internalConsumerSeq;

    protected final AtomicLong lastMsgReceived;

    // heartbeat stuff
    protected boolean hb;
    protected long idleHeartbeatSetting;
    protected long alarmPeriodSetting;
    protected HeartbeatTimer heartbeatTimer;

    protected MessageManager(NatsConnection conn, boolean syncMode) {
        this.conn = conn;
        this.syncMode = syncMode;
        lastStreamSeq = 0;
        internalConsumerSeq = 0;
        lastMsgReceived = new AtomicLong(System.currentTimeMillis());

        hb = false;
        idleHeartbeatSetting = 0;
        alarmPeriodSetting = 0;
    }

    protected boolean isSyncMode()               { return syncMode; }
    protected long getLastStreamSequence()       { return lastStreamSeq; }
    protected long getInternalConsumerSequence() { return internalConsumerSeq; }
    protected long getLastMsgReceived()          { return lastMsgReceived.get(); }
    protected boolean isHb()                     { return hb; }
    protected long getIdleHeartbeatSetting()     { return idleHeartbeatSetting; }
    protected long getAlarmPeriodSetting()       { return alarmPeriodSetting; }

    protected void startup(NatsJetStreamSubscription sub) {
        this.sub = sub;
    }

    protected void shutdown() {
        shutdownHeartbeatTimer();
    }

    protected void startPullRequest(PullRequestOptions pullRequestOptions) {
        // does nothing - only implemented for pulls, but in base class since instance is always referenced as MessageManager, not subclass
    }

    protected PullStatus getPullStatus() {
        return null;
    }

    protected void messageReceived() {
        lastMsgReceived.set(System.currentTimeMillis());
    }

    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        return true;
    }

    abstract protected boolean manage(Message msg);

    protected void trackJsMessage(Message msg) {
        NatsJetStreamMetaData meta = msg.metaData();
        lastStreamSeq = meta.streamSequence();
        internalConsumerSeq++;
    }

    protected void initIdleHeartbeat(Duration configIdleHeartbeat, long configMessageAlarmTime) {
        initIdleHeartbeat(configIdleHeartbeat == null ? 0 : configIdleHeartbeat.toMillis(), configMessageAlarmTime);
    }

    protected void initIdleHeartbeat(long configIdleHeartbeat, long configMessageAlarmTime) {
        idleHeartbeatSetting = configIdleHeartbeat;
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

    protected void initOrResetHeartbeatTimer() {
        if (heartbeatTimer == null) {
            heartbeatTimer = new HeartbeatTimer(alarmPeriodSetting);
        }
        else {
            heartbeatTimer.restart();
        }
    }

    protected void shutdownHeartbeatTimer() {
        if (heartbeatTimer != null) {
            heartbeatTimer.shutdown();
            heartbeatTimer = null;
        }
    }

    protected void handleHeartbeatError() {
        conn.executeCallback((c, el) -> el.heartbeatAlarm(c, sub, lastStreamSeq, internalConsumerSeq));
    }

    protected class HeartbeatTimer {
        protected Timer timer;
        protected boolean alive = true;
        protected long alarmPeriodSetting;

        protected class HeartbeatTimerTask extends TimerTask {
            @Override
            public void run() {
                long sinceLast = System.currentTimeMillis() - lastMsgReceived.get();
                if (sinceLast > alarmPeriodSetting) {
                    handleHeartbeatError();
                }
                restart();
            }
        }

        protected HeartbeatTimer(long alarmPeriodSetting) {
            this.alarmPeriodSetting = alarmPeriodSetting;
            restart();
        }

        synchronized protected void restart() {
            cancel();
            if (alive) {
                timer = new Timer();
                timer.schedule(new HeartbeatTimerTask(), alarmPeriodSetting);
            }
        }

        synchronized protected void shutdown() {
            alive = false;
            cancel();
        }

        private void cancel() {
            if (timer != null) {
                timer.cancel();
                timer.purge();
                timer = null;
            }
        }
    }
}
