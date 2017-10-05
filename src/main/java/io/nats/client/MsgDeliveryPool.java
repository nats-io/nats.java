/*
 *  Copyright (c) 2017 Apcera Inc. All rights reserved. This program and the accompanying
 *  materials are made available under the terms of the MIT License (MIT) which accompanies this
 *  distribution, and is available at http://opensource.org/licenses/MIT
 */

package io.nats.client;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.*;


class MsgDeliveryPool {
    private List<MsgDeliveryWorker> workers = null;
    private int                     idx;
    private boolean                 shutdown;

    // Expand the pool of workers.
    // This call has no effect for a size lower to 1 or if given size
    // is lower or equal to current pool size.
    synchronized void setSize(int size) {
        if (size <= 0) {
            return;
        }
        if (this.workers == null) {
            this.workers = new ArrayList<MsgDeliveryWorker>(size);
        }
        // We support only expansion of the pool at this point
        if (size > this.workers.size()) {
            final int added = size-this.workers.size();
            for (int i=0; i<added; i++) {
                MsgDeliveryWorker w = new MsgDeliveryWorker();
                w.start();
                this.workers.add(w);
            }
        }
    }

    synchronized int getSize() {
        return this.workers.size();
    }

    synchronized void assignDeliveryWorker(AsyncSubscriptionImpl sub) {
        int idx = this.idx;
        if (++this.idx >= this.workers.size()) {
            this.idx = 0;
        }
        final MsgDeliveryWorker worker = this.workers.get(idx);
        sub.setDeliveryWorker(worker);
    }

    synchronized void shutdown() {
        if (this.shutdown) {
            return;
        }
        this.shutdown = true;
        for (int i=0; i<this.workers.size(); i++) {
            final MsgDeliveryWorker w = this.workers.get(i);
            w.shutdown();
        }
        this.workers.clear();
    }
}

class MsgDeliveryWorker extends Thread {
    private final Lock          mu       = new ReentrantLock();
    private final List<Message> msgs     = new LinkedList<Message>();
    private final Condition     cond     = mu.newCondition();
    private boolean             inWait   = false;
    private boolean             shutdown = false;

    MsgDeliveryWorker() {
        this.setName("jnats-msg-delivery-worker-thread");
    }

    // Add a message to the list and signal the worker thread.
    // Lock is assumed held on entry.
    void postMsg(Message msg) {
        this.msgs.add(msg);
        if (this.inWait) {
            this.cond.signal();
        }
    }

    @Override
    public void run() {
        Message msg = null;
        AsyncSubscriptionImpl sub = null;
        ConnectionImpl nc = null;
        MessageHandler mcb = null;
        long max = 0;
        long delivered = 0;

        this.mu.lock();
        while (true) {
            while (this.msgs.isEmpty() && !this.shutdown) {
                this.inWait = true;
                try { this.cond.await(); } catch (InterruptedException e) {}
                this.inWait = false;
            }
            // Exit only when all messages have been dispatched
            if (this.msgs.isEmpty() && this.shutdown) {
                break;
            }
            // Remove first message from list.
            msg = this.msgs.remove(0);

            // Get subscription reference from message
            sub = (AsyncSubscriptionImpl) msg.getSubscription();

            // Capture these under lock
            nc = (ConnectionImpl) sub.getConnection();
            mcb = sub.getMessageHandler();
            max = sub.max;

            sub.pMsgs--;
            sub.pBytes -= (msg.getData() == null ? 0 : msg.getData().length);

            // If sub is closed, simply go back at beginning of loop.
            if (sub.closed) {
                continue;
            }

            delivered = ++(sub.delivered);
            this.mu.unlock();

            if ((max == 0) || (delivered <= max)) {
                try {
                    mcb.onMessage(msg);
                } catch (Throwable t) {
                    // Ignore any exception thrown in the user callback.
                }
            }

            // Don't do 'else' because we need to remove when we have hit
            // the max (after the callback returns).
            if ((max > 0) && (delivered >= max))
            {
                // If we have hit the max for delivered msgs, remove sub.
                nc.mu.lock();
                try {
                    nc.removeSub(sub);
                } finally {
                    nc.mu.unlock();
                }
            }

            this.mu.lock();
        }
        this.mu.unlock();
    }

    void shutdown() {
        this.mu.lock();
        if (this.shutdown) {
            this.mu.unlock();
            return;
        }
        this.shutdown = true;
        if (this.inWait) {
            this.cond.signal();
        }
        this.mu.unlock();

        if (Thread.currentThread() != this) {
            try { this.join(); } catch (InterruptedException e) {}
        }
    }

    void lock() {
        this.mu.lock();
    }

    void unlock() {
        this.mu.unlock();
    }
}