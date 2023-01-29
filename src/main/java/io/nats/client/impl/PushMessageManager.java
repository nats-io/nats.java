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

import io.nats.client.ErrorListener;
import io.nats.client.JetStreamStatusException;
import io.nats.client.Message;
import io.nats.client.SubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.support.Status;

import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;

class PushMessageManager extends MessageManager {

    protected static final List<Integer> PUSH_KNOWN_STATUS_CODES = Collections.singletonList(409);

    protected static final int THRESHOLD = 3;

    protected final NatsConnection conn;

    protected final NatsJetStream js;
    protected final String stream;
    protected final ConsumerConfiguration serverCC;
    protected final NatsDispatcher dispatcher;

    protected final boolean syncMode;
    protected final boolean queueMode;
    protected final boolean hb;
    protected final boolean fc;

    protected final long idleHeartbeatSetting;
    protected final long alarmPeriodSetting;

    protected String lastFcSubject;
    protected long lastStreamSeq;
    protected long lastConsumerSeq;

    protected final AtomicLong lastMsgReceived;
    protected HeartbeatTimer heartbeatTimer;

    PushMessageManager(NatsConnection conn,
                       NatsJetStream js,
                       String stream,
                       SubscribeOptions so,
                       ConsumerConfiguration serverCC,
                       boolean queueMode,
                       NatsDispatcher dispatcher)
    {
        this.conn = conn;
        this.js = js;
        this.stream = stream;
        this.serverCC = serverCC;
        this.dispatcher = dispatcher;
        this.syncMode = dispatcher == null;
        this.queueMode = queueMode;
        lastStreamSeq = -1;
        lastConsumerSeq = -1;
        lastMsgReceived = new AtomicLong(System.currentTimeMillis());

        if (queueMode) {
            hb = false;
            fc = false;
            idleHeartbeatSetting = 0;
            alarmPeriodSetting = 0;
        }
        else {
            idleHeartbeatSetting = serverCC.getIdleHeartbeat() == null ? 0 : serverCC.getIdleHeartbeat().toMillis();
            if (idleHeartbeatSetting <= 0) {
                alarmPeriodSetting = 0;
                hb = false;
            }
            else {
                long mat = so.getMessageAlarmTime();
                if (mat < idleHeartbeatSetting) {
                    alarmPeriodSetting = idleHeartbeatSetting * THRESHOLD;
                }
                else {
                    alarmPeriodSetting = mat;
                }
                hb = true;
            }
            fc = hb && serverCC.isFlowControl(); // can't have fc w/o heartbeat
        }
    }

    @Override
    void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        if (hb) {
            sub.setBeforeQueueProcessor(this::beforeQueueProcessor);
            heartbeatTimer = new HeartbeatTimer();
        }
    }

    @Override
    void shutdown() {
        if (heartbeatTimer != null) {
            heartbeatTimer.shutdown();
            heartbeatTimer = null;
        }
        super.shutdown();
    }

    protected void handleHeartbeatError() {
        conn.executeCallback((c, el) -> el.heartbeatAlarm(c, sub, lastStreamSeq, lastConsumerSeq));
    }

    class HeartbeatTimer {
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

    boolean isSyncMode() { return syncMode; }
    boolean isQueueMode() { return queueMode; }
    boolean isFc() { return fc; }
    boolean isHb() { return hb; }

    long getIdleHeartbeatSetting() { return idleHeartbeatSetting; }
    long getAlarmPeriodSetting() { return alarmPeriodSetting; }

    String getLastFcSubject() { return lastFcSubject; }
    long getLastStreamSequence() { return lastStreamSeq; }
    long getLastConsumerSequence() { return lastConsumerSeq; }
    long getLastMsgReceived() { return lastMsgReceived.get(); }

    NatsMessage beforeQueueProcessor(NatsMessage msg) {
        lastMsgReceived.set(System.currentTimeMillis());
        if (msg.isStatusMessage()
            && msg.getStatus().isHeartbeat()
            && extractFcSubject(msg) == null)
        {
            return null;
        }
        return msg;
    }

    protected boolean subManage(Message msg) {
        return false;
    }

    boolean manage(Message msg) {
        if (!sub.getSID().equals(msg.getSID())) {
            return true;
        }

        if (msg.isStatusMessage()) {
            // this checks fc, hb and unknown
            // only process fc and hb if those flags are set
            // otherwise they are simply known statuses
            Status status = msg.getStatus();
            if (status.isFlowControl()) {
                if (fc) {
                    _processFlowControl(msg.getReplyTo(), ErrorListener.FlowControlSource.FLOW_CONTROL);
                }
            }
            else if (status.isHeartbeat()) {
                if (fc) {
                    // status flowControlSubject is set in the beforeQueueProcessor
                    _processFlowControl(extractFcSubject(msg), ErrorListener.FlowControlSource.HEARTBEAT);
                }
            }
            else if (!PUSH_KNOWN_STATUS_CODES.contains(status.getCode())) {
                // If this status is unknown to us, always use the error handler.
                // If it's a sync call, also throw an exception
                conn.executeCallback((c, el) -> el.unhandledStatus(c, sub, status));
                if (syncMode) {
                    throw new JetStreamStatusException(sub, status);
                }
            }
            return true;
        }

        if (subManage(msg)) {
            return true;
        }

        // JS Message
        lastStreamSeq = msg.metaData().streamSequence();
        lastConsumerSeq = msg.metaData().consumerSequence();

        return false;
    }

    String extractFcSubject(Message msg) {
        return msg.getHeaders() == null ? null : msg.getHeaders().getFirst(CONSUMER_STALLED_HDR);
    }

    private void _processFlowControl(String fcSubject, ErrorListener.FlowControlSource source) {
        // we may get multiple fc/hb messages with the same reply
        // only need to post to that subject once
        if (fcSubject != null && !fcSubject.equals(lastFcSubject)) {
            conn.publishInternal(fcSubject, null, null, null);
            lastFcSubject = fcSubject; // set after publish in case the pub fails
            conn.executeCallback((c, el) -> el.flowControlProcessed(c, sub, fcSubject, source));
        }
    }
}
