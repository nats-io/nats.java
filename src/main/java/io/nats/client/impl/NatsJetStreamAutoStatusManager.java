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

import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.support.Status;

import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;

public class NatsJetStreamAutoStatusManager {
    private static final List<Integer> PULL_KNOWN_STATUS_CODES = Arrays.asList(404, 408);
    private static final int THRESHOLD = 3;

    private final NatsConnection conn;
    private NatsJetStreamSubscription sub;

    private final boolean syncMode;
    private final boolean queueMode;
    private final boolean pull;
    private final boolean gap;
    private final boolean hb;
    private final boolean fc;

    private final long idleHeartbeatSetting;
    private final long alarmPeriodSetting;

    private String lastFcSubject;
    private long lastStreamSeq;
    private long lastConsumerSeq;
    private long expectedConsumerSeq;

    private final AtomicLong lastMsgReceived;
    private final ErrorListener errorListener;
    private TimerWrapper timerWrapper;

    NatsJetStreamAutoStatusManager(NatsConnection conn, SubscribeOptions so,
                                   ConsumerConfiguration cc,
                                   boolean queueMode, boolean syncMode)
    {
        this.conn = conn;
        this.syncMode = syncMode;
        this.queueMode = queueMode;
        lastStreamSeq = -1;
        lastConsumerSeq = -1;
        expectedConsumerSeq = so.getExpectedConsumerSeq();
        lastMsgReceived = new AtomicLong();

        pull = so.isPull();

        if (queueMode) {
            gap = false;
            hb = false;
            fc = false;
            idleHeartbeatSetting = 0;
            alarmPeriodSetting = 0;
        }
        else {
            gap = so.detectGaps();
            idleHeartbeatSetting = cc.getIdleHeartbeat().toMillis();
            if (idleHeartbeatSetting == 0) {
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
            fc = hb && cc.getFlowControl(); // can't have fc w/o heartbeat
        }

        errorListener = conn.getOptions().getErrorListener() == null
            ? new DefaultErrorListener()
            : conn.getOptions().getErrorListener();
    }

    // chicken or egg situation here. The handler needs the sub in case of error
    // but the sub needs the handler in order to be created
    void setSub(NatsJetStreamSubscription sub) {
        this.sub = sub;
        if (hb) {
            conn.setBeforeQueueProcessor(this::beforeQueueProcessor);
            timerWrapper = new TimerWrapper();
        }
    }

    void shutdown() {
        if (timerWrapper != null) {
            timerWrapper.shutdown();
        }
    }

    class TimerWrapper {
        Timer timer;

        class TimerWrapperTimerTask extends TimerTask {
            @Override
            public void run() {
                long sinceLast = System.currentTimeMillis() - lastMsgReceived.get();
                if (sinceLast > alarmPeriodSetting) {
                    errorListener.heartbeatAlarm(conn, sub,
                        lastStreamSeq, lastConsumerSeq, expectedConsumerSeq);
                }
                restart();
            }
        }

        public TimerWrapper() {
            restart();
        }

        synchronized void restart() {
            cancel();
            if (sub.isActive()) {
                timer = new Timer();
                timer.schedule(new TimerWrapperTimerTask(), alarmPeriodSetting);
            }
        }

        synchronized public void shutdown() {
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
    boolean isPull() { return pull; }
    boolean isGap() { return gap; }
    boolean isFc() { return fc; }
    boolean isHb() { return hb; }

    long getIdleHeartbeatSetting() { return idleHeartbeatSetting; }
    long getAlarmPeriodSetting() { return alarmPeriodSetting; }

    String getLastFcSubject() { return lastFcSubject; }
    public long getLastStreamSequence() { return lastStreamSeq; }
    public long getLastConsumerSequence() { return lastConsumerSeq; }
    long getExpectedConsumerSequence() { return expectedConsumerSeq; }
    long getLastMsgReceived() { return lastMsgReceived.get(); }

    boolean manage(Message msg) {
        if (pull ? checkStatusForPullMode(msg) : checkStatusForPushMode(msg)) {
            return true;
        }
        if (gap) {
            detectGaps(msg);
        }
        return false;
    }

    private void detectGaps(Message msg) {
        long receivedConsumerSeq = msg.metaData().consumerSequence();
        // expectedConsumerSeq <= 0 they didn't tell me where to start so assume whatever it is is correct
        if (expectedConsumerSeq > 0) {
            if (expectedConsumerSeq != receivedConsumerSeq) {
                if (syncMode) {
                    throw new JetStreamGapException(sub, expectedConsumerSeq, receivedConsumerSeq);
                }
                errorListener.messageGapDetected(conn, sub,
                    lastStreamSeq, lastConsumerSeq,
                    expectedConsumerSeq, receivedConsumerSeq);
            }
        }
        lastStreamSeq = msg.metaData().streamSequence();
        lastConsumerSeq = receivedConsumerSeq;
        expectedConsumerSeq = receivedConsumerSeq + 1;
    }

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

    private String extractFcSubject(Message msg) {
        return msg.getHeaders() == null ? null : msg.getHeaders().getFirst(CONSUMER_STALLED_HDR);
    }

    private boolean checkStatusForPushMode(Message msg) {
        // this checks fc, hb and unknown
        // only process fc and hb if those flags are set
        // otherwise they are simply known statuses
        if (msg.isStatusMessage()) {
            Status status = msg.getStatus();
            if (status.isFlowControl()) {
                if (fc) {
                    _processFlowControl(msg.getReplyTo());
                }
                return true;
            }

            if (status.isHeartbeat()) {
                if (fc) {
                    // status flowControlSubject is set in the beforeQueueProcessor
                    _processFlowControl(extractFcSubject(msg));
                }
                return true;
            }

            // this status is unknown to us, how we let the user know
            // depends on whether they are sync or async
            if (syncMode) {
                throw new JetStreamStatusException(sub, status);
            }

            errorListener.unhandledStatus(conn, sub, status);
            return true;
        }
        return false;
    }

    private void _processFlowControl(String fcSubject) {
        // we may get multiple fc/hb messages with the same reply
        // only need to post to that subject once
        if (fcSubject != null && !fcSubject.equals(lastFcSubject)) {
            conn.publishInternal(fcSubject, null, null, null, false);
            lastFcSubject = fcSubject; // set after publish in case the pub fails
        }
    }

    private boolean checkStatusForPullMode(Message msg) {
        if (msg.isStatusMessage()) {
            Status status = msg.getStatus();
            if ( !PULL_KNOWN_STATUS_CODES.contains(status.getCode()) ) {
                // pull is always sync
                throw new JetStreamStatusException(sub, status);
            }
            return true;
        }
        return false;
    }
}
