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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;

public class NatsJetStreamMessagePreProcessor {
    private static final List<Integer> PULL_STATUS_CODES = Arrays.asList(404, 408);
    private static final int THRESHOLD = 3;

    protected NatsConnection conn;
    protected NatsJetStreamSubscription sub;

    private Function<Message, Boolean> preImpl;

    private final boolean syncMode;
    private final boolean queueMode;
    private final boolean pull;
    private final boolean asm;
    private final boolean gap;
    private final boolean hb;
    private final boolean fc;
    private boolean noOp;

    private final long idleHeartbeatSetting;
    private final long alarmPeriodSetting;

    private String lastFcSubject;
    private long expectedConsumerSeq;
    private final AtomicLong lastMessageReceivedTime;

    NatsJetStreamMessagePreProcessor(NatsConnection conn, SubscribeOptions so,
                                     ConsumerConfiguration cc, NatsJetStreamSubscription sub,
                                     boolean queueMode, boolean syncMode) {
        this.conn = conn;
        this.sub = sub;
        this.syncMode = syncMode;
        this.queueMode = queueMode;
        this.expectedConsumerSeq = so.getExpectedConsumerSeq();
        lastMessageReceivedTime = new AtomicLong();

        pull = so.isPull();
        asm = so.autoStatusManagement();

        if (queueMode) {
            gap = false;
            hb = false;
            fc = false;
            idleHeartbeatSetting = 0;
            alarmPeriodSetting = 0;
        }
        else {
            gap = so.autoGapDetect();
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
            fc = hb && cc.getFlowControl();
        }


        if (asm) {
            if (pull) {
                if (gap) { // +asm +pull +gap
                    preImpl = msg -> {
                        if (pullCheckStatus(msg)) {
                            return true;
                        }
                        detectGaps(msg);
                        return false;
                    };
                }
                else { // +asm +pull -gap
                    preImpl = this::pullCheckStatus;
                }
            }
            else if (idleHeartbeatSetting > 0) {
                if (fc) {
                    if (gap) { // +asm +push +hb +fc +gap
                        preImpl = msg -> {
                            trackReceivedTime();
                            if (pushCheckStatus(msg)) {
                                return true;
                            }
                            detectGaps(msg);
                            return false;
                        };
                    }
                    else { // +asm +push +hb +fc -gap
                        preImpl = msg -> {
                            trackReceivedTime();
                            return pushCheckStatus(msg);
                        };
                    }
                }
                else if (gap) { // +asm +push +hb -fc +gap
                    preImpl = msg -> {
                        trackReceivedTime();
                        if (pushCheckStatus(msg)) {
                            return true;
                        }
                        detectGaps(msg);
                        return false;
                    };
                }
                else { // +asm +push +hb -fc -gap
                    preImpl = msg -> {
                        trackReceivedTime();
                        return pushCheckStatus(msg);
                    };
                }
            }
            else if (gap) { // +asm +push -hb -fc +gap
                preImpl = msg -> {
                    if (pushCheckStatus(msg)) {
                        return true;
                    }
                    detectGaps(msg);
                    return false;
                };
            }
            else { // +asm +push -hb -fc -gap
                preImpl = this::pushCheckStatus;
            }
        }
        else if (gap) { // -asm +push-or-pull +gap
            preImpl = msg -> {
                detectGaps(msg);
                return false;
            };
        }

        noOp = preImpl == null;
        if (noOp) {
            preImpl = msg -> false;
        }
    }

    public void setSub(NatsJetStreamSubscription sub) {
        this.sub = sub;
    }

    public boolean isSyncMode() {
        return syncMode;
    }

    public boolean isQueueMode() {
        return queueMode;
    }

    public boolean isPull() {
        return pull;
    }

    public boolean isAsm() {
        return asm;
    }

    public boolean isGap() {
        return gap;
    }

    public boolean isFc() {
        return fc;
    }

    public boolean isHb() {
        return hb;
    }

    public boolean isNoOp() {
        return noOp;
    }

    String getLastFcSubject() {
        return lastFcSubject;
    }

    long getExpectedConsumerSeq() {
        return expectedConsumerSeq;
    }

    long getLastMessageReceivedTime() {
        return lastMessageReceivedTime.get();
    }

    long getIdleHeartbeatSetting() {
        return idleHeartbeatSetting;
    }

    long getAlarmPeriodSetting() {
        return alarmPeriodSetting;
    }

    boolean preProcess(Message msg) {
        return preImpl.apply(msg);
    }

    private void trackReceivedTime() {
        lastMessageReceivedTime.set(System.currentTimeMillis());
    }

    private void detectGaps(Message msg) {
        if (msg.isJetStream()) {
            long receivedConsumerSeq = msg.metaData().consumerSequence();
            // expectedConsumerSeq <= 0 they didn't tell me where to start so assume whatever it is is correct
            if (expectedConsumerSeq > 0) {
                if (expectedConsumerSeq != receivedConsumerSeq) {
                    if (syncMode) {
                        throw new JetStreamGapException(sub, expectedConsumerSeq, receivedConsumerSeq);
                    }
                    ErrorListener el = conn.getOptions().getErrorListener();
                    if (el != null) {
                        el.messageGapDetected(conn, sub, expectedConsumerSeq, receivedConsumerSeq);
                    }
                }
            }
            expectedConsumerSeq = receivedConsumerSeq + 1;
        }
    }

    private boolean pushCheckStatus(Message msg) {
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

            if (syncMode) {
                throw new JetStreamStatusException(sub, status);
            }

            ErrorListener el = conn.getOptions().getErrorListener();
            if (el != null) {
                el.unhandledStatus(conn, sub, status);
            }
            return true;
        }
        return false;
    }

    private boolean pullCheckStatus(Message msg) {
        if (msg.isStatusMessage()) {
            Status status = msg.getStatus();
            if ( !PULL_STATUS_CODES.contains(status.getCode()) ) {
                throw new JetStreamStatusException(sub, status);
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
}
