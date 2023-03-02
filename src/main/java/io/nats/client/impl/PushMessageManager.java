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
import io.nats.client.Message;
import io.nats.client.SubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.support.Status;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

class PushMessageManager extends MessageManager {

    protected static final List<Integer> PUSH_KNOWN_STATUS_CODES = Collections.singletonList(409);

    protected final NatsJetStream js;
    protected final String stream;
    protected final ConsumerConfiguration originalCc;

    protected final boolean queueMode;
    protected final boolean fc;
    protected boolean hb;
    protected long idleHeartbeatSetting;
    protected long alarmPeriodSetting;

    protected HeartbeatTimer heartbeatTimer;
    protected String lastFcSubject;

    PushMessageManager(NatsConnection conn,
                       NatsJetStream js,
                       String stream,
                       SubscribeOptions so,
                       ConsumerConfiguration originalCc,
                       boolean queueMode,
                       NatsDispatcher dispatcher)
    {
        super(conn, dispatcher);
        this.js = js;
        this.stream = stream;
        this.originalCc = originalCc;
        this.queueMode = queueMode;

        hb = false;
        idleHeartbeatSetting = 0;
        alarmPeriodSetting = 0;

        if (queueMode) {
            fc = false;
        }
        else {
            initIdleHeartbeat(originalCc.getIdleHeartbeat(), so.getMessageAlarmTime());
            fc = hb && originalCc.isFlowControl(); // can't have fc w/o heartbeat
        }
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

    boolean isQueueMode()           { return queueMode; }
    boolean isFc()                  { return fc; }
    boolean isHb()                  { return hb; }
    long getIdleHeartbeatSetting()  { return idleHeartbeatSetting; }
    long getAlarmPeriodSetting()    { return alarmPeriodSetting; }
    String getLastFcSubject()       { return lastFcSubject; }

    @Override
    protected void startup(NatsJetStreamSubscription sub) {
        super.startup(sub);
        if (hb) {
            sub.setBeforeQueueProcessor(this::beforeQueueProcessorImpl);
            heartbeatTimer = new HeartbeatTimer(alarmPeriodSetting);
        }
    }

    @Override
    protected void shutdown() {
        super.shutdown();
        if (heartbeatTimer != null) {
            heartbeatTimer.shutdown();
            heartbeatTimer = null;
        }
    }

    protected Boolean beforeQueueProcessorImpl(NatsMessage msg) {
        lastMsgReceived.set(System.currentTimeMillis());
        return msg.isStatusMessage() ? bqpStatus(msg) : true;
    }

    protected Boolean bqpStatus(NatsMessage msg) {
        // A heartbeat can be plain or include flow control info.
        // Plain ones do not have to be queued (return false)
        // Ones with fc subjects need to be queued on since they
        // are treated like normal fc and processed in turn.
        final Status status = msg.getStatus();
        if (status.isHeartbeat()) {
            return hasFcSubject(msg);
        }
        if (!PUSH_KNOWN_STATUS_CODES.contains(status.getCode())) {
            conn.executeCallback((c, el) -> el.unhandledStatus(c, sub, status));
            return false;
        }
        return true;
    }

    @Override
    protected boolean manage(Message msg) {
        if (msg.isStatusMessage()) {
            manageStatus(msg);
            return true; // all status are managed
        }

        trackJsMessage(msg);
        return false;
    }

    protected void manageStatus(Message msg) {
        // this checks fc, hb and unknown
        // only process fc and hb if those flags are set
        // otherwise they are simply known statuses
        Status status = msg.getStatus();
        if (status.isFlowControl()) {
            if (fc) {
                processFlowControl(msg.getReplyTo(), ErrorListener.FlowControlSource.FLOW_CONTROL);
            }
        }
        else if (status.isHeartbeat()) {
            if (fc) {
                // status flowControlSubject is set in the beforeQueueProcessor
                processFlowControl(extractFcSubject(msg), ErrorListener.FlowControlSource.HEARTBEAT);
            }
        }
    }

    private void processFlowControl(String fcSubject, ErrorListener.FlowControlSource source) {
        // we may get multiple fc/hb messages with the same reply
        // only need to post to that subject once
        if (fcSubject != null && !fcSubject.equals(lastFcSubject)) {
            conn.publishInternal(fcSubject, null, null, null);
            lastFcSubject = fcSubject; // set after publish in case the pub fails
            conn.executeCallback((c, el) -> el.flowControlProcessed(c, sub, fcSubject, source));
        }
    }
}
