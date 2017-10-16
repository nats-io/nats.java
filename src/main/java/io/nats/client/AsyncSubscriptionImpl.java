/*
 *  Copyright (c) 2015-2016 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

/*
 * This is the implementation of the AsyncSubscription interface.
 *
 */
class AsyncSubscriptionImpl extends SubscriptionImpl implements AsyncSubscription {

    MessageHandler    msgHandler;
    MsgDeliveryWorker dlvWorker;

    AsyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue,
                          MessageHandler cb) {
        this(nc, subj, queue, cb, false);
    }

    AsyncSubscriptionImpl(ConnectionImpl nc, String subj, String queue,
                          MessageHandler cb, boolean useMsgDlvPool) {
        super(nc, subj, queue, DEFAULT_MAX_PENDING_MSGS, DEFAULT_MAX_PENDING_BYTES, useMsgDlvPool);
        this.msgHandler = cb;
    }

    @Override
    @Deprecated
    public void start() {
        /* Deprecated */
    }

    @Override
    public void setMessageHandler(MessageHandler cb) {
        this.dlvWorkerLock();
        this.msgHandler = cb;
        this.dlvWorkerUnlock();
    }

    @Override
    public MessageHandler getMessageHandler() {
        this.dlvWorkerLock();
        MessageHandler mh = msgHandler;
        this.dlvWorkerUnlock();
        return mh;
    }

    @Override
    public long getDelivered() {
        this.dlvWorkerLock();
        try {
            return super.getDelivered();
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    public int getPendingMsgs() {
        this.dlvWorkerLock();
        try {
            return super.getPendingMsgs();
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    public int getPendingBytes() {
        this.dlvWorkerLock();
        try {
            return super.getPendingBytes();
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    public int getPendingMsgsMax() {
        this.dlvWorkerLock();
        try {
            return super.getPendingMsgsMax();
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    public long getPendingBytesMax() {
        this.dlvWorkerLock();
        try {
            return super.getPendingBytesMax();
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    public void setPendingLimits(int msgs, int bytes) {
        this.dlvWorkerLock();
        try {
            super.setPendingLimits(msgs, bytes);
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    public int getPendingMsgsLimit() {
        this.dlvWorkerLock();
        try {
            return super.getPendingMsgsLimit();
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    public void clearMaxPending() {
        this.dlvWorkerLock();
        try {
            super.clearMaxPending();
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    public int getDropped() {
        this.dlvWorkerLock();
        try {
            return super.getDropped();
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    void setMax(long max) {
        this.dlvWorkerLock();
        try {
            super.setMax(max);
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    @Override
    void close(boolean connClosed) {
        this.dlvWorkerLock();
        try {
            super.close(connClosed);
        } finally {
            this.dlvWorkerUnlock();
        }
    }

    private void dlvWorkerLock() {
        if (this.dlvWorker != null) {
            this.dlvWorker.lock();
        }
    }

    private void dlvWorkerUnlock() {
        if (this.dlvWorker != null) {
            this.dlvWorker.unlock();
        }
    }

    MsgDeliveryWorker getDeliveryWorker() {
        return this.dlvWorker;
    }

    void setDeliveryWorker(MsgDeliveryWorker worker) {
        this.dlvWorker = worker;
    }
}
