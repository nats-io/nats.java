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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;

public class PushAutoStatusManager implements AutoStatusManager {

    private static final int THRESHOLD = 3;

    private final NatsConnection conn;
    private NatsJetStreamSubscription sub;

    private final boolean syncMode;
    private final boolean queueMode;
    private final boolean hb;
    private final boolean fc;

    private final long idleHeartbeatSetting;
    private final long alarmPeriodSetting;

    private String lastFcSubject;
    private long lastStreamSeq;
    private long lastConsumerSeq;

    private final AtomicLong lastMsgReceived;
    private AsmTimer asmTimer;

    PushAutoStatusManager(NatsConnection conn, SubscribeOptions so,
                          ConsumerConfiguration cc,
                          boolean queueMode, boolean syncMode)
    {
        this.conn = conn;
        this.syncMode = syncMode;
        this.queueMode = queueMode;
        lastStreamSeq = -1;
        lastConsumerSeq = -1;
        lastMsgReceived = new AtomicLong();

        if (queueMode) {
            hb = false;
            fc = false;
            idleHeartbeatSetting = 0;
            alarmPeriodSetting = 0;
        }
        else {
            idleHeartbeatSetting = cc.getIdleHeartbeat() == null ? 0 : cc.getIdleHeartbeat().toMillis();
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
            fc = hb && cc.isFlowControl(); // can't have fc w/o heartbeat
        }
    }

    // chicken or egg situation here. The handler needs the sub in case of error
    // but the sub needs the handler in order to be created
    public void setSub(NatsJetStreamSubscription sub) {
        this.sub = sub;
        if (hb) {
            conn.setBeforeQueueProcessor(this::beforeQueueProcessor);
            asmTimer = new AsmTimer();
        }
    }

    public void shutdown() {
        if (asmTimer != null) {
            asmTimer.shutdown();
        }
    }

    class AsmTimer {
        Timer timer;
        boolean alive = true;

        class AsmTimerTask extends TimerTask {
            @Override
            public void run() {
                long sinceLast = System.currentTimeMillis() - lastMsgReceived.get();
                if (sinceLast > alarmPeriodSetting) {
                    conn.getOptions().getErrorListener().heartbeatAlarm(conn, sub, lastStreamSeq, lastConsumerSeq);
                }
                restart();
            }
        }

        public AsmTimer() {
            restart();
        }

        synchronized void restart() {
            cancel();
            if (alive) {
                timer = new Timer();
                timer.schedule(new AsmTimerTask(), alarmPeriodSetting);
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
    public long getLastStreamSequence() { return lastStreamSeq; }
    public long getLastConsumerSequence() { return lastConsumerSeq; }
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

    public boolean manage(Message msg) {
        if (msg.isStatusMessage()) {
            // this checks fc, hb and unknown
            // only process fc and hb if those flags are set
            // otherwise they are simply known statuses
            Status status = msg.getStatus();
            if (status.isFlowControl()) {
                if (fc) {
                    _processFlowControl(msg.getReplyTo(), ErrorListener.FlowControlSource.FLOW_CONTROL);
                }
                return true;
            }

            if (status.isHeartbeat()) {
                if (fc) {
                    // status flowControlSubject is set in the beforeQueueProcessor
                    _processFlowControl(extractFcSubject(msg), ErrorListener.FlowControlSource.HEARTBEAT);
                }
                return true;
            }

            // this status is unknown to us, always use the error handler.
            // If it's a sync call, also throw an exception
            conn.getOptions().getErrorListener().unhandledStatus(conn, sub, status);
            if (syncMode) {
                throw new JetStreamStatusException(sub, status);
            }
            return true;
        }

        // JS Message
        lastStreamSeq = msg.metaData().streamSequence();
        lastConsumerSeq = msg.metaData().consumerSequence();
        return false;
    }

    private String extractFcSubject(Message msg) {
        return msg.getHeaders() == null ? null : msg.getHeaders().getFirst(CONSUMER_STALLED_HDR);
    }

    private void _processFlowControl(String fcSubject, ErrorListener.FlowControlSource source) {
        // we may get multiple fc/hb messages with the same reply
        // only need to post to that subject once
        if (fcSubject != null && !fcSubject.equals(lastFcSubject)) {
            conn.publishInternal(fcSubject, null, null, null, false);
            lastFcSubject = fcSubject; // set after publish in case the pub fails
            conn.getOptions().getErrorListener().flowControlProcessed(conn, sub, fcSubject, source);
        }
    }
}
