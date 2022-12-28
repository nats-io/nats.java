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

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

abstract class MessageManager {
    protected static final int THRESHOLD = 3;

    protected final NatsConnection conn;
    protected final NatsDispatcher dispatcher;
    protected final boolean syncMode;

    protected NatsJetStreamSubscription sub;
    protected boolean hb;
    protected long idleHeartbeatSetting;
    protected long alarmPeriodSetting;

    protected long lastStreamSeq;
    protected long lastConsumerSeq;

    protected final AtomicLong lastMsgReceived;
    protected HeartbeatTimer heartbeatTimer;

    public MessageManager(NatsConnection conn, NatsDispatcher dispatcher) {
        this.conn = conn;
        this.dispatcher = dispatcher;
        syncMode = dispatcher == null;
        hb = false;
        idleHeartbeatSetting = 0;
        alarmPeriodSetting = 0;
        lastStreamSeq = -1;
        lastConsumerSeq = -1;
        lastMsgReceived = new AtomicLong(System.currentTimeMillis());
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

    boolean isSyncMode()            { return syncMode; }
    boolean isHb()                  { return hb; }
    long getIdleHeartbeatSetting()  { return idleHeartbeatSetting; }
    long getAlarmPeriodSetting()    { return alarmPeriodSetting; }
    long getLastStreamSequence()    { return lastStreamSeq; }
    long getLastConsumerSequence()  { return lastConsumerSeq; }
    long getLastMsgReceived()       { return lastMsgReceived.get(); }

    void startup(NatsJetStreamSubscription sub) {
        this.sub = sub;
        if (hb) {
            sub.setBeforeQueueProcessor(this::beforeQueueProcessor);
            heartbeatTimer = new HeartbeatTimer();
        }
    }

    void shutdown() {
        if (heartbeatTimer != null) {
            heartbeatTimer.shutdown();
            heartbeatTimer = null;
        }
    }

    protected boolean manage(Message msg) {
        lastStreamSeq = msg.metaData().streamSequence();
        lastConsumerSeq = msg.metaData().consumerSequence();
        return false;
    }

    protected String extractFcSubject(Message msg) {
        return null;
    }

    protected NatsMessage beforeQueueProcessor(NatsMessage msg) {
        lastMsgReceived.set(System.currentTimeMillis());
        if (msg.isStatusMessage()
            && msg.getStatus().isHeartbeat()
            && extractFcSubject(msg) == null)
        {
            return null;
        }
        return msg;
    }


    protected void handleHeartbeatError() {
        conn.executeCallback((c, el) -> el.heartbeatAlarm(c, sub, lastStreamSeq, lastConsumerSeq));
    }

    protected class HeartbeatTimer {
        Timer timer;
        boolean alive = true;

        class HeartbeatTimerTask extends TimerTask {
            @Override
            public void run() {
                long sinceLast = System.currentTimeMillis() - lastMsgReceived.get();
                if (sinceLast > alarmPeriodSetting) {
                    handleHeartbeatError();
                }
                restart();
            }
        }

        public HeartbeatTimer() {
            restart();
        }

        synchronized void restart() {
            cancel();
            if (alive) {
                timer = new Timer();
                timer.schedule(new HeartbeatTimerTask(), alarmPeriodSetting);
            }
        }

        synchronized public void shutdown() {
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
