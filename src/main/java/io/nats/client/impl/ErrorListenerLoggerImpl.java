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
import io.nats.client.support.Status;

import java.util.logging.Logger;

public class ErrorListenerLoggerImpl implements ErrorListener {

    private final static Logger LOGGER = Logger.getLogger(ErrorListenerLoggerImpl.class.getName());

    private String supplyMessage(String label, Object... pairs) {
        StringBuilder sb = new StringBuilder(label).append('.');
        for (int x = 0; x < pairs.length; x++) {
            sb.append(pairs[x]).append(pairs[++x]);
        }
        return sb.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void errorOccurred(final Connection conn, final String error) {
        LOGGER.severe(() -> supplyMessage("errorOccurred", "Conn: ", conn, "Error: ", error));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionOccurred(final Connection conn, final Exception exp) {
        LOGGER.severe(() -> supplyMessage("exceptionOccurred", "Conn: ", conn, "Exception: ", exp));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void slowConsumerDetected(final Connection conn, final Consumer consumer) {
        LOGGER.warning(() -> supplyMessage("slowConsumerDetected", "Conn: ", conn, "Consumer: ", consumer));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageDiscarded(final Connection conn, final Message msg) {
        LOGGER.info(() -> supplyMessage("messageDiscarded", "Conn: ", conn, "Message: ", msg));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void heartbeatAlarm(final Connection conn, final JetStreamSubscription sub,
                               final long lastStreamSequence, final long lastConsumerSequence, final long expectedConsumerSequence) {
        LOGGER.severe(() -> supplyMessage("heartbeatAlarm", "Conn: ", conn, "Sub: ", sub, "lastStreamSequence: ", lastStreamSequence, "lastConsumerSequence: ", lastConsumerSequence, "expectedConsumerSequence: ", expectedConsumerSequence));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageGapDetected(final Connection conn, final JetStreamSubscription sub, final long lastStreamSequence, final long lastConsumerSequence, final long expectedConsumerSequence, final long receivedConsumerSequence) {
        LOGGER.warning(() -> supplyMessage("messageGapDetected", "Conn: ", conn, "Sub: ", sub, "lastStreamSequence: ", lastStreamSequence, "lastConsumerSequence: ", lastConsumerSequence, "expectedConsumerSequence: ", expectedConsumerSequence, "receivedConsumerSequence: ", receivedConsumerSequence));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhandledStatus(final Connection conn, final JetStreamSubscription sub, final Status status) {
        LOGGER.warning(() -> supplyMessage("unhandledStatus", "Conn: ", conn, "Sub: ", sub, "Status:", status));
    }
}
