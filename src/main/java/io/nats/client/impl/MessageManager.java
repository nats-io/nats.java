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

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

abstract class MessageManager {
    public enum ManageResult {MESSAGE, STATUS_HANDLED, STATUS_TERMINUS, STATUS_ERROR}

    protected static final int THRESHOLD = 3;

    protected final Object stateChangeLock;
    protected final NatsConnection conn;
    protected final SubscribeOptions so;
    protected final boolean syncMode;

    protected NatsJetStreamSubscription sub; // not final it is not set until after construction

    protected long lastStreamSeq;
    protected long lastConsumerSeq;
    protected AtomicLong lastMsgReceived;

    // heartbeat stuff
    protected boolean hb;
    protected long idleHeartbeatSetting;
    protected long alarmPeriodSetting;
    protected MmTimerTask heartbeatTimerTask;
    protected Timer heartbeatTimer;

    protected MessageManager(NatsConnection conn, SubscribeOptions so, boolean syncMode) {
        stateChangeLock = new Object();

        this.conn = conn;
        this.so = so;
        this.syncMode = syncMode;
        lastStreamSeq = 0;
        lastConsumerSeq = 0;

        hb = false;
        idleHeartbeatSetting = 0;
        alarmPeriodSetting = 0;
        lastMsgReceived = new AtomicLong(System.currentTimeMillis());
    }

    protected boolean isSyncMode()           { return syncMode; }
    protected long getLastStreamSequence()   { return lastStreamSeq; }
    protected long getLastConsumerSequence() { return lastConsumerSeq; }
    protected long getLastMsgReceived()      { return lastMsgReceived.get(); }
    protected boolean isHb()                 { return hb; }
    protected long getIdleHeartbeatSetting() { return idleHeartbeatSetting; }
    protected long getAlarmPeriodSetting()   { return alarmPeriodSetting; }

    protected void startup(NatsJetStreamSubscription sub) {
        this.sub = sub;
    }

    protected void shutdown() {
        shutdownHeartbeatTimer();
    }

    protected void startPullRequest(String pullSubject, PullRequestOptions pullRequestOptions, boolean raiseStatusWarnings, PullManagerObserver pullManagerObserver) {
        // does nothing - only implemented for pulls, but in base class since instance is referenced as MessageManager, not subclass
    }

    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        return true;
    }
    abstract protected ManageResult manage(Message msg);

    protected void trackJsMessage(Message msg) {
        synchronized (stateChangeLock) {
            NatsJetStreamMetaData meta = msg.metaData();
            lastStreamSeq = meta.streamSequence();
            lastConsumerSeq++;
        }
    }

    protected void handleHeartbeatError() {
        conn.executeCallback((c, el) -> el.heartbeatAlarm(c, sub, lastStreamSeq, lastConsumerSeq));
    }

    protected void configureIdleHeartbeat(Duration configIdleHeartbeat, long configMessageAlarmTime) {
        synchronized (stateChangeLock) {
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
    }

    protected void updateLastMessageReceived() {
        lastMsgReceived.set(System.currentTimeMillis());
    }

    class MmTimerTask extends TimerTask {
        long alarmPeriod;
        final AtomicBoolean alive;

        public MmTimerTask(long alarmPeriod) {
            this.alarmPeriod = alarmPeriod;
            alive = new AtomicBoolean(true);
        }

        public void shutdown() {
            alive.getAndSet(false);
        }

        @Override
        public void run() {
            if (alive.get()) {
                long sinceLast = System.currentTimeMillis() - lastMsgReceived.get();
                if (alive.get() && sinceLast > alarmPeriodSetting) {
                    handleHeartbeatError();
                }
            }
        }
    }

    protected void initOrResetHeartbeatTimer() {
        synchronized (stateChangeLock) {
            if (heartbeatTimer != null) {
                if (heartbeatTimerTask.alarmPeriod == alarmPeriodSetting) {
                    updateLastMessageReceived();
                    return;
                }
                shutdownHeartbeatTimer();
            }
            heartbeatTimer = new Timer();
            heartbeatTimerTask = new MmTimerTask(alarmPeriodSetting);
            heartbeatTimer.schedule(heartbeatTimerTask, alarmPeriodSetting, alarmPeriodSetting);
            updateLastMessageReceived();
        }
    }

    protected void shutdownHeartbeatTimer() {
        synchronized (stateChangeLock) {
            if (heartbeatTimer != null) {
                heartbeatTimerTask.shutdown();
                heartbeatTimerTask = null;
                heartbeatTimer.cancel();
                heartbeatTimer = null;
            }
        }
    }
}
