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

package io.nats.examples.chaosTestApp;

import io.nats.client.*;
import io.nats.client.api.ServerInfo;
import io.nats.client.support.Status;

public class OutputErrorListener implements ErrorListener {
    String id;
    java.util.function.Consumer<String> watcher;

    public OutputErrorListener(String id) {
        this(id, null);
    }

    public OutputErrorListener(String id, java.util.function.Consumer<String> watcher) {
        this.id = id;
        this.watcher = watcher;
    }

    private String supplyMessage(String label, Connection conn, Consumer consumer, Subscription sub, Object... pairs) {
        StringBuilder sb = new StringBuilder(label);
        if (conn != null) {
            ServerInfo si = conn.getServerInfo();
            if (si != null) {
                sb.append(", CONN: ").append(conn.getServerInfo().getClientId());
            }
        }
        if (consumer != null) {
            sb.append(", CON: ").append(consumer.hashCode());
        }
        if (sub != null) {
            sb.append(", SUB: ").append(sub.hashCode());
            if (sub instanceof JetStreamSubscription) {
                JetStreamSubscription jssub = (JetStreamSubscription)sub;
                sb.append(", CON: ").append(jssub.getConsumerName());
            }
        }
        for (int x = 0; x < pairs.length; x++) {
            sb.append(", ").append(pairs[x]).append(pairs[++x]);
        }
        if (watcher != null) {
            watcher.accept(sb.toString());
        }
        return sb.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void errorOccurred(final Connection conn, final String error) {
        Output.controlMessage(id, supplyMessage("SEVERE errorOccurred", conn, null, null, "Error: ", error));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionOccurred(final Connection conn, final Exception exp) {
        Output.controlMessage(id, supplyMessage("SEVERE exceptionOccurred", conn, null, null, "EX: ", exp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void slowConsumerDetected(final Connection conn, final Consumer consumer) {
        Output.controlMessage(id, supplyMessage("WARN slowConsumerDetected", conn, consumer, null));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageDiscarded(final Connection conn, final Message msg) {
        Output.controlMessage(id, supplyMessage("INFO messageDiscarded", conn, null, null, "Message: ", msg));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void heartbeatAlarm(final Connection conn, final JetStreamSubscription sub,
                               final long lastStreamSequence, final long lastConsumerSequence) {
        Output.controlMessage(id, supplyMessage("SEVERE HB Alarm", conn, null, sub, "lastStreamSeq: ", lastStreamSequence, "lastConsumerSeq: ", lastConsumerSequence));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhandledStatus(final Connection conn, final JetStreamSubscription sub, final Status status) {
        Output.controlMessage(id, supplyMessage("WARN unhandledStatus", conn, null, sub, "Status:", status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
//        Output.controlMessage(id, supplyMessage("WARN pullStatusWarning", conn, null, sub, "Status:", status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
        Output.controlMessage(id, supplyMessage("SEVERE pullStatusError", conn, null, sub, "Status:", status));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String id, FlowControlSource source) {
        Output.controlMessage(this.id, supplyMessage("INFO flowControlProcessed", conn, null, sub, "FlowControlSource:", source));
    }
}
