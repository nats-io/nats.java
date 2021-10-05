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
import java.util.function.Function;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;

public class NatsJetStreamAutoStatusManager {
    private static final List<Integer> PULL_KNOWN_STATUS_CODES = Arrays.asList(404, 408);
    private static final int THRESHOLD = 3;

    protected NatsConnection conn;
    protected NatsJetStreamSubscription sub;

    private Function<Message, Boolean> preProcessImpl;

    private final boolean syncMode;
    private final boolean queueMode;
    private final boolean pull;
    private final boolean gap;
    private final boolean hb;
    private final boolean fc;

    private final long idleHeartbeatSetting;
    private final long alarmPeriodSetting;

    private String lastFcSubject;
    private long lastStreamSequence;
    private long lastConsumerSequence;
    private long expectedConsumerSequence;
    private final AtomicLong lastMessageReceivedTime;
    private ErrorListener el;
    private TimerWrapper timerWrapper;

    NatsJetStreamAutoStatusManager(NatsConnection conn, SubscribeOptions so,
                                   ConsumerConfiguration cc,
                                   boolean queueMode, boolean syncMode)
    {
        this.conn = conn;
        this.syncMode = syncMode;
        this.queueMode = queueMode;
        lastStreamSequence = -1;
        lastConsumerSequence = -1;
        expectedConsumerSequence = so.getExpectedConsumerSeq();
        lastMessageReceivedTime = new AtomicLong();

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

        // construct a function based on the things that need to be checked
        // alternatively we could have just gone through these checks every single time
        // but also constructing ahead of time helper determine if this is a no-op
        // which can help optimize whether the preprocessor needs to be called at all
        if (pull) {
            if (gap) { // +pull +gap
                preProcessImpl = msg -> {
                    if (checkStatusForPull(msg)) {
                        return true;
                    }
                    detectGaps(msg);
                    return false;
                };
            }
            else { // +pull -gap
                preProcessImpl = this::checkStatusForPull;
            }
        }
        else if (idleHeartbeatSetting > 0) {
            if (fc) {
                if (gap) { // +push +hb +fc +gap
                    preProcessImpl = msg -> {
                        trackReceivedTime();
                        if (checkStatusForPush(msg)) {
                            return true;
                        }
                        detectGaps(msg);
                        return false;
                    };
                }
                else { // +push +hb +fc -gap
                    preProcessImpl = msg -> {
                        trackReceivedTime();
                        return checkStatusForPush(msg);
                    };
                }
            }
            else if (gap) { // +push +hb -fc +gap
                preProcessImpl = msg -> {
                    trackReceivedTime();
                    if (checkStatusForPush(msg)) {
                        return true;
                    }
                    detectGaps(msg);
                    return false;
                };
            }
            else { // +push +hb -fc -gap
                preProcessImpl = msg -> {
                    trackReceivedTime();
                    return checkStatusForPush(msg);
                };
            }
        }
        else if (gap) { // +push -hb -fc +gap
            preProcessImpl = msg -> {
                if (checkStatusForPush(msg)) {
                    return true;
                }
                detectGaps(msg);
                return false;
            };
        }
        else { // +push -hb -fc -gap
            preProcessImpl = this::checkStatusForPush;
        }

        if (conn != null) {
            Options options = conn.getOptions();
            if (options != null) {
                el = options.getErrorListener();;
            }
        }

        if (hb) {
            timerWrapper = new TimerWrapper();
        }
    }

    public void shutdown() {
        if (timerWrapper != null) {
            timerWrapper.shutdown();
        }
    }

    class OutOfTime extends TimerTask {
        @Override
        public void run() {
            long sinceLast = System.currentTimeMillis() - lastMessageReceivedTime.get();
            if (sinceLast > alarmPeriodSetting) {
                el.heartbeatAlarm(conn, sub,
                    lastStreamSequence, lastConsumerSequence, expectedConsumerSequence);
            }
            timerWrapper.restart();
        }
    }

    class TimerWrapper {
        boolean active;
        Timer timer;

        public TimerWrapper() {
            if (el == null) {
                active = false;
            }
            else {
                timer = new Timer();
                timer.schedule(new OutOfTime(),
                    alarmPeriodSetting + 1000); // first one, let's give everything time to warm up
                active = true;
            }
        }

        synchronized void restart() {
            cancel();
            if (sub.isActive()) {
                timer = new Timer();
                timer.schedule(new OutOfTime(), alarmPeriodSetting);
            }
        }

        synchronized public void shutdown() {
            cancel();
            active = false;
        }

        private void cancel() {
            if (timer != null) {
                timer.cancel();
                timer.purge();
                timer = null;
            }
        }
    }

    // chicken or egg situation here. The handler needs the sub in case of error
    // but the sub needs the handler in order to be created
    void setSub(NatsJetStreamSubscription sub) {
        this.sub = sub;
    }

    boolean isSyncMode() { return syncMode; }
    boolean isQueueMode() { return queueMode; }
    boolean isPull() { return pull; }
    boolean isGap() { return gap; }
    boolean isFc() { return fc; }
    boolean isHb() { return hb; }

    String getLastFcSubject() { return lastFcSubject; }

    public long getLastStreamSequence() { return lastStreamSequence; }
    public long getLastConsumerSequence() { return lastConsumerSequence; }
    long getExpectedConsumerSequence() { return expectedConsumerSequence; }
    long getLastMessageReceivedTime() { return lastMessageReceivedTime.get(); }
    long getIdleHeartbeatSetting() { return idleHeartbeatSetting; }
    long getAlarmPeriodSetting() { return alarmPeriodSetting; }

    boolean manage(Message msg) {
        return preProcessImpl.apply(msg);
    }

    private void trackReceivedTime() {
        lastMessageReceivedTime.set(System.currentTimeMillis());
    }

    private void detectGaps(Message msg) {
        if (msg.isJetStream()) {
            long receivedConsumerSeq = msg.metaData().consumerSequence();
            // expectedConsumerSeq <= 0 they didn't tell me where to start so assume whatever it is is correct
            if (expectedConsumerSequence > 0) {
                if (expectedConsumerSequence != receivedConsumerSeq) {
                    if (syncMode) {
                        throw new JetStreamGapException(sub, expectedConsumerSequence, receivedConsumerSeq);
                    }
                    if (el != null) {
                        el.messageGapDetected(conn, sub,
                            lastStreamSequence, lastConsumerSequence,
                            expectedConsumerSequence, receivedConsumerSeq);
                    }
                }
            }
            lastStreamSequence = msg.metaData().streamSequence();
            lastConsumerSequence = receivedConsumerSeq;
            expectedConsumerSequence = receivedConsumerSeq + 1;
        }
    }

    private boolean checkStatusForPush(Message msg) {
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
                if (hb) {
                    Headers h = msg.getHeaders();
                    if (h != null) {
                        _processFlowControl(h.getFirst(CONSUMER_STALLED_HDR));
                    }
                }
                return true;
            }

            // this status is unknown to us, how we let the user know
            // depends on whether they are sync or async
            if (syncMode) {
                throw new JetStreamStatusException(sub, status);
            }

            // Can't assume they have an error listener.
            if (el != null) {
                el.unhandledStatus(conn, sub, status);
            }
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

    private boolean checkStatusForPull(Message msg) {
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
