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
import io.nats.client.NatsSystemClock;
import io.nats.client.PullRequestOptions;
import io.nats.client.SubscribeOptions;
import io.nats.client.support.NatsConstants;
import io.nats.client.support.ScheduledTask;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
    protected final AtomicLong lastMsgReceivedNanoTime;

    // heartbeat stuff
    protected final AtomicBoolean hb;
    protected final AtomicLong idleHeartbeatSettingMillis;
    protected final AtomicLong alarmPeriodSettingNanos;
    protected final AtomicReference<ScheduledTask> heartbeatTaskRef;

    protected MessageManager(NatsConnection conn, SubscribeOptions so, boolean syncMode) {
        stateChangeLock = new ReentrantLock();

        this.conn = conn;
        this.so = so;
        this.syncMode = syncMode;
        lastStreamSeq = 0;
        lastConsumerSeq = 0;

        hb = new AtomicBoolean(false);
        idleHeartbeatSettingMillis = new AtomicLong();
        alarmPeriodSettingNanos = new AtomicLong();
        lastMsgReceivedNanoTime = new AtomicLong(NatsSystemClock.nanoTime());
        heartbeatTaskRef = new AtomicReference<>();
    }

    protected boolean isSyncMode()              { return syncMode; }
    protected long getLastStreamSequence()      { return lastStreamSeq; }
    protected long getLastConsumerSequence()    { return lastConsumerSeq; }
    protected long getLastMsgReceivedNanoTime() { return lastMsgReceivedNanoTime.get(); }
    protected boolean isHb()                    { return hb.get(); }
    protected long getIdleHeartbeatSetting()    { return idleHeartbeatSettingMillis.get(); }
    protected long getAlarmPeriodSettingNanos() { return alarmPeriodSettingNanos.get(); }

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
            long idleSettingMillis = configIdleHeartbeat == null ? 0 : configIdleHeartbeat.toMillis();
            idleHeartbeatSettingMillis.set(idleSettingMillis);
            if (idleSettingMillis <= 0) {
                alarmPeriodSettingNanos.set(0);
                hb.set(false);
            }
            else {
                long alarmPeriodSettingMillis;
                if (configMessageAlarmTime < idleSettingMillis) {
                    alarmPeriodSettingMillis = idleSettingMillis * THRESHOLD;
                }
                else {
                    alarmPeriodSettingMillis = configMessageAlarmTime;
                }
                alarmPeriodSettingNanos.set(alarmPeriodSettingMillis * NatsConstants.NANOS_PER_MILLI);
                hb.set(true);
            }
        }
        finally {
            stateChangeLock.unlock();
        }
    }

    protected void updateLastMessageReceived() {
        lastMsgReceivedNanoTime.set(NatsSystemClock.nanoTime());
    }

    protected void initOrResetHeartbeatTimer() {
        stateChangeLock.lock();
        try {
            ScheduledTask hbTask = heartbeatTaskRef.get();
            if (hbTask != null) {
                // we always want a fresh schedule because it will have the initial delay
                hbTask.shutdown();
            }

            // set the ref with a new ScheduledTask
            // reminder that ScheduledTask schedules itself, which is why we pass the executor
            heartbeatTaskRef.set(
                new ScheduledTask(conn.getScheduledExecutor(), alarmPeriodSettingNanos.get(), TimeUnit.NANOSECONDS,
                    () -> {
                        long sinceLast = NatsSystemClock.nanoTime() - lastMsgReceivedNanoTime.get();
                        if (sinceLast > alarmPeriodSettingNanos.get()) {
                            handleHeartbeatError();
                        }
                    })
            );

            // since we just scheduled, reset this otherwise it may alarm too soon
            updateLastMessageReceived();
        }
        finally {
            stateChangeLock.unlock();
        }
    }

    protected void shutdownHeartbeatTimer() {
        stateChangeLock.lock();
        try {
            ScheduledTask hbTask = heartbeatTaskRef.get();
            if (hbTask != null) {
                hbTask.shutdown();
                heartbeatTaskRef.set(null);
            }
        }
        finally {
            stateChangeLock.unlock();
        }
    }
}
