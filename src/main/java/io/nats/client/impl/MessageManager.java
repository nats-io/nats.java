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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;

abstract class MessageManager {
    protected static final int THRESHOLD = 3;

    protected final NatsConnection conn;
    protected final NatsDispatcher dispatcher;
    protected final boolean syncMode;

    protected NatsJetStreamSubscription sub;

    protected long lastStreamSeq;
    protected long internalConsumerSeq;

    protected final AtomicLong lastMsgReceived;

    public MessageManager(NatsConnection conn, NatsDispatcher dispatcher) {
        this.conn = conn;
        this.dispatcher = dispatcher;
        syncMode = dispatcher == null;
        lastStreamSeq = -1;
        internalConsumerSeq = 0;
        lastMsgReceived = new AtomicLong(System.currentTimeMillis());
    }

    boolean isSyncMode()                { return syncMode; }
    long getLastStreamSequence()        { return lastStreamSeq; }
    long getInternalConsumerSequence()  { return internalConsumerSeq; }
    long getLastMsgReceived()           { return lastMsgReceived.get(); }

    protected void startup(NatsJetStreamSubscription sub) {
        this.sub = sub;
    }

    protected void shutdown() {}

    abstract protected boolean manage(Message msg);

    protected void trackJsMessage(Message msg) {
        NatsJetStreamMetaData meta = msg.metaData();
        lastStreamSeq = meta.streamSequence();
        internalConsumerSeq++;
    }

    protected boolean hasFcSubject(Message msg) {
        return msg.getHeaders() != null && msg.getHeaders().containsKey(CONSUMER_STALLED_HDR);
    }

    protected String extractFcSubject(Message msg) {
        return msg.getHeaders() == null ? null : msg.getHeaders().getFirst(CONSUMER_STALLED_HDR);
    }

    protected void handleHeartbeatError() {
        conn.executeCallback((c, el) -> el.heartbeatAlarm(c, sub, lastStreamSeq, internalConsumerSeq));
    }

    protected class HeartbeatTimer {
        Timer timer;
        boolean alive = true;
        long alarmPeriodSetting;

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

        public HeartbeatTimer(long alarmPeriodSetting) {
            this.alarmPeriodSetting = alarmPeriodSetting;
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
