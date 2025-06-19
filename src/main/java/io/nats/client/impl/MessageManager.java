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
import io.nats.client.support.NatsConstants;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

abstract class MessageManager {
    public enum ManageResult {MESSAGE, STATUS_HANDLED, STATUS_TERMINUS, STATUS_ERROR}

    protected static final int THRESHOLD = 3;

    protected final ReentrantLock stateChangeLock;
    protected final NatsConnection conn;
    protected final SubscribeOptions so;
    protected final boolean syncMode;

    protected NatsJetStreamSubscription sub; // not final it is not set until after construction

    protected long lastStreamSeq;
    protected long lastConsumerSeq;
    protected AtomicLong lastMsgReceivedNanoTime;

    // heartbeat stuff
    protected boolean hb;
    protected long idleHeartbeatSetting;
    protected long alarmPeriodSettingNanos;
    protected MmTimerTask heartbeatTimerTask;
    protected Timer heartbeatTimer;

    protected MessageManager(NatsConnection conn, SubscribeOptions so, boolean syncMode) {
        stateChangeLock = new ReentrantLock();

        this.conn = conn;
        this.so = so;
        this.syncMode = syncMode;
        lastStreamSeq = 0;
        lastConsumerSeq = 0;

        hb = false;
        idleHeartbeatSetting = 0;
        alarmPeriodSettingNanos = 0;
        lastMsgReceivedNanoTime = new AtomicLong(System.nanoTime());
    }

    protected boolean isSyncMode()              { return syncMode; }
    protected long getLastStreamSequence()      { return lastStreamSeq; }
    protected long getLastConsumerSequence()    { return lastConsumerSeq; }
    protected long getLastMsgReceivedNanoTime() { return lastMsgReceivedNanoTime.get(); }
    protected boolean isHb()                    { return hb; }
    protected long getIdleHeartbeatSetting()    { return idleHeartbeatSetting; }
    protected long getAlarmPeriodSettingNanos() { return alarmPeriodSettingNanos; }

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
        stateChangeLock.lock();
        try {
            NatsJetStreamMetaData meta = msg.metaData();
            lastStreamSeq = meta.streamSequence();
            lastConsumerSeq++;
        }
        finally {
            stateChangeLock.unlock();
        }
    }

    protected void handleHeartbeatError() {
        conn.executeCallback((c, el) -> el.heartbeatAlarm(c, sub, lastStreamSeq, lastConsumerSeq));
    }

    protected void configureIdleHeartbeat(Duration configIdleHeartbeat, long configMessageAlarmTime) {
        stateChangeLock.lock();
        try {
            idleHeartbeatSetting = configIdleHeartbeat == null ? 0 : configIdleHeartbeat.toMillis();
            if (idleHeartbeatSetting <= 0) {
                alarmPeriodSettingNanos = 0;
                hb = false;
            }
            else {
                long alarmPeriodSetting;
                if (configMessageAlarmTime < idleHeartbeatSetting) {
                    alarmPeriodSetting = idleHeartbeatSetting * THRESHOLD;
                }
                else {
                    alarmPeriodSetting = configMessageAlarmTime;
                }
                alarmPeriodSettingNanos = alarmPeriodSetting * NatsConstants.NANOS_PER_MILLI;
                hb = true;
            }
        }
        finally {
            stateChangeLock.unlock();
        }
    }

    protected void updateLastMessageReceived() {
        lastMsgReceivedNanoTime.set(System.nanoTime());
    }

    class MmTimerTask extends TimerTask {
        long alarmPeriodNanos;
        final AtomicBoolean alive;

        public MmTimerTask(long alarmPeriodNanos) {
            this.alarmPeriodNanos = alarmPeriodNanos;
            alive = new AtomicBoolean(true);
        }

        public void reuse() {
            alive.getAndSet(true);
        }

        public void shutdown() {
            alive.getAndSet(false);
        }

        @Override
        public void run() {
            if (alive.get() && !Thread.interrupted()) {
                long sinceLast = System.nanoTime() - lastMsgReceivedNanoTime.get();
                if (alive.get() && sinceLast > alarmPeriodNanos) {
                    handleHeartbeatError();
                }
            }
        }

        @Override
        public String toString() {
            long sinceLastMillis = (System.nanoTime() - lastMsgReceivedNanoTime.get()) / NatsConstants.NANOS_PER_MILLI;
            return "MmTimerTask{" +
                ", alarmPeriod=" + (alarmPeriodNanos / NatsConstants.NANOS_PER_MILLI) +
                "ms, alive=" + alive.get() +
                ", sinceLast=" + sinceLastMillis + "ms}";
        }
    }

    protected void initOrResetHeartbeatTimer() {
        stateChangeLock.lock();
        try {
            if (heartbeatTimer != null) {
                // Same settings, just reuse the existing timer
                if (heartbeatTimerTask.alarmPeriodNanos == alarmPeriodSettingNanos) {
                    heartbeatTimerTask.reuse();
                    updateLastMessageReceived();
                    return;
                }

                // Replace timer since settings have changed
                shutdownHeartbeatTimer();
            }
            // replacement or new comes here
            heartbeatTimer = new Timer();
            heartbeatTimerTask = new MmTimerTask(alarmPeriodSettingNanos);
            long alarmPeriodSettingMillis = alarmPeriodSettingNanos / NatsConstants.NANOS_PER_MILLI;
            heartbeatTimer.schedule(heartbeatTimerTask, alarmPeriodSettingMillis, alarmPeriodSettingMillis);
            updateLastMessageReceived();
        }
        finally {
            stateChangeLock.unlock();
        }
    }

    protected void shutdownHeartbeatTimer() {
        stateChangeLock.lock();
        try {
            if (heartbeatTimer != null) {
                heartbeatTimerTask.shutdown();
                heartbeatTimerTask = null;
                heartbeatTimer.cancel();
                heartbeatTimer = null;
            }
        }
        finally {
            stateChangeLock.unlock();
        }
    }
}
