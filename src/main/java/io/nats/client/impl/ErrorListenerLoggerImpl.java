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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorListenerLoggerImpl implements ErrorListener {

    private final static Logger LOG = LoggerFactory.getLogger(ErrorListenerLoggerImpl.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public void errorOccurred(final Connection conn, final String error) {
        LOG.error("errorOccurred for connection [{}] with error: [{}]", conn, error);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exceptionOccurred(final Connection conn, final Exception exp) {
        LOG.error("exceptionOccurred for connection [{}], Exception: ", conn, exp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void slowConsumerDetected(final Connection conn, final Consumer consumer) {
        LOG.warn("slowConsumerDetected for connection [{}] and consumer [{}]", conn, consumer != null ? consumer.hashCode() : "null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void messageDiscarded(final Connection conn, final Message msg) {
        LOG.info("messageDiscarded for connection [{}] and message [{}]", conn, msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void heartbeatAlarm(final Connection conn, final JetStreamSubscription sub,
                               final long lastStreamSequence, final long lastConsumerSequence) {
        LOG.error("heartbeatAlarm for connection [{}] and subscription [{}], lastStreamSequence: [{}], lastConsumerSequence: [{}]",
                conn, sub, lastStreamSequence, lastConsumerSequence);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unhandledStatus(final Connection conn, final JetStreamSubscription sub, final Status status) {
        LOG.warn("unhandledStatus for connection [{}] and subscription [{}], Status: [{}]", conn, sub, status);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullStatusWarning(final Connection conn, final JetStreamSubscription sub, final Status status) {
        LOG.warn("pullStatusWarning for connection [{}] and subscription [{}], Status: [{}]", conn, sub, status);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void pullStatusError(final Connection conn, final JetStreamSubscription sub, final Status status) {
        LOG.error("pullStatusError for connection [{}] and subscription [{}], Status: [{}]", conn, sub, status);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flowControlProcessed(final Connection conn, final JetStreamSubscription sub, final String id, final FlowControlSource source) {
       LOG.info("flowControlProcessed for connection [{}] and subscription [{}], ID [{}], FlowControlSource: [{}]", conn, sub, id, source);
    }
}
