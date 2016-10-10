/*******************************************************************************
 * Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 * materials are made available under the terms of the MIT License (MIT) which accompanies this
 * distribution, and is available at http://opensource.org/licenses/MIT
 *******************************************************************************/
/**
 * 
 */

package io.nats.client;

import java.io.IOException;
import java.util.concurrent.locks.Condition;

/*
 * This is the implementation of the AsyncSubscription interface.
 *
 */
class AsyncSubscriptionImpl extends SubscriptionImpl implements AsyncSubscription {

    MessageHandler msgHandler;
    Condition pCond;

    protected AsyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue,
            MessageHandler cb) {
        this(nc, subj, queue, cb, DEFAULT_MAX_PENDING_MSGS, DEFAULT_MAX_PENDING_BYTES);
    }

    protected AsyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue, MessageHandler cb,
            int maxMsgs, int maxBytes) {
        super(nc, subj, queue, maxMsgs, maxBytes);
        this.msgHandler = cb;
        pCond = mu.newCondition();
    }

    @Override
    public void start() {
        /* Deprecated */
    }

    @Override
    public void setMessageHandler(MessageHandler cb) {
        this.msgHandler = cb;
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public void unsubscribe() throws IOException {
        super.unsubscribe();
    }
}
