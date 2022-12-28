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

import static io.nats.client.support.NatsJetStreamConstants.CONSUMER_STALLED_HDR;

class PushMessageManager extends MessageManager {

    protected static final List<Integer> PUSH_KNOWN_STATUS_CODES = Collections.singletonList(409);

    protected final NatsJetStream js;
    protected final String stream;
    protected final ConsumerConfiguration originalCc;

    protected final boolean queueMode;
    protected final boolean fc;

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

        if (queueMode) {
            fc = false;
        }
        else {
            initIdleHeartbeat(originalCc.getIdleHeartbeat(), so.getMessageAlarmTime());
            fc = hb && originalCc.isFlowControl(); // can't have fc w/o heartbeat
        }
    }

    boolean isQueueMode()       { return queueMode; }
    boolean isFc()              { return fc; }
    String getLastFcSubject()   { return lastFcSubject; }

    protected boolean pushSubManage(Message msg) {
        return false;
    }

    @Override
    protected boolean manage(Message msg) {
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

        return pushSubManage(msg) || super.manage(msg);
    }

    @Override
    protected String extractFcSubject(Message msg) {
        return msg.getHeaders() == null ? null : msg.getHeaders().getFirst(CONSUMER_STALLED_HDR);
    }

    private void _processFlowControl(String fcSubject, ErrorListener.FlowControlSource source) {
        // we may get multiple fc/hb messages with the same reply
        // only need to post to that subject once
        if (fcSubject != null && !fcSubject.equals(lastFcSubject)) {
            conn.publishInternal(fcSubject, null, null, null, false);
            lastFcSubject = fcSubject; // set after publish in case the pub fails
            conn.executeCallback((c, el) -> el.flowControlProcessed(c, sub, fcSubject, source));
        }
    }
}
