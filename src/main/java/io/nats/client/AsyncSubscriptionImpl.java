// Copyright 2015-2018 The NATS Authors
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
