/*
 * The MIT License
 *
 * Copyright 2017 Apcera, Inc..
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package io.nats.client;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous queue for delivering messages to async subscribers. Uses atomic
 * operations on ConcurrentHashMap in place of global locks where possible.
 *
 * @author Tim Boudreau
 */
final class AsyncDeliveryQueue {

    private final ConcurrentMap<Long, Messages> messagesForSubscriber;
    private final Map<Long, Thread> threadAffinity;

    AsyncDeliveryQueue() {
        this(10000, 0.75F, 16);
    }

    AsyncDeliveryQueue(int initialSize, float loadFactor, int concurrency) {
        messagesForSubscriber = new ConcurrentHashMap<>(initialSize, loadFactor, concurrency);
        threadAffinity = new ConcurrentHashMap<>(initialSize, loadFactor, concurrency);
    }

    /**
     * Add a message mapped to a subcriber id
     *
     * @param subscriberId The subscriber id
     * @param message The message
     */
    void add(long subscriberId, Message message) {
        addIfNotNull(messagesForSubscriber.putIfAbsent(subscriberId, new Messages().add(message)), message);
        signalOneThread();
    }

    /**
     * Pre-create a messages queue for a subscriber id, ensuring that contains()
     * tests for that id will return true.
     *
     * @param subscriberId The subscriber ide
     * @return True if the subscriber id was not already present
     */
    boolean touch(long subscriberId) {
        return messagesForSubscriber.putIfAbsent(subscriberId, new Messages()) == null;
    }

    /**
     * Remove a subscriber id
     *
     * @param subscriberId The id
     */
    void remove(long subscriberId) {
        messagesForSubscriber.remove(subscriberId);
        threadAffinity.remove(subscriberId);
    }

    /**
     * Remove a susbcriber id, retrieving any remaining messages
     *
     * @param subscriberId The id
     * @return A list of messages
     */
    List<Message> removeAndCollect(long subscriberId) {
        Messages m = messagesForSubscriber.remove(subscriberId);
        threadAffinity.remove(subscriberId);
        return m.drain(new long[1]);
    }

    /**
     * Test if the subscriber id is present.
     *
     * @param subscriberId The subscriber id
     * @return true if it is present
     */
    boolean contains(long subscriberId) {
        return messagesForSubscriber.containsKey(subscriberId);
    }

    private void addIfNotNull(Messages msgs, Message message) {
        if (msgs != null) {
            msgs.add(message);
        }
    }

    public int get(List<? super MessageBatch> into, long timeout, TimeUnit unit) throws InterruptedException {
        // If messages immediately available, return them without waiting
        if (drain(into) > 0) {
            return into.size();
        }
        // wait for signal
        synchronized (this) {
            wait(unit.toMillis(timeout));
        }
        // And drain
        return drain(into);
    }

    public int get(List<? super MessageBatch> into) throws InterruptedException {
        // If messages immediately available, return them without waiting
        if (drain(into) > 0) {
            return into.size();
        }
        // wait for signal
        while (into.isEmpty()) {
            // It *is* possible to have messages waiting which we will never
            // be signalled for, so this needs to be a slow busywait.  That
            // should only happen under heavy contention.
            get(into, 250, TimeUnit.MILLISECONDS);
        }
        return into.size();
    }

    public void clear() {
        synchronized (this) {
            notifyAll();
        }
        // Give a chance at message collection
        Thread.yield();
        messagesForSubscriber.clear();
        threadAffinity.clear();
    }

    private int drain(List<? super MessageBatch> batch) {
        int result = 0;
        boolean resignal = false;
        Thread currentThread = Thread.currentThread();
        for (Map.Entry<Long, Messages> e : messagesForSubscriber.entrySet()) {
            // Ensure that only this thread can read messages for this key, until
            // the batch for that key is closed.
            Thread threadForKey = threadAffinity.putIfAbsent(e.getKey(), currentThread);
            if (threadForKey != null && threadForKey != currentThread) {
                // Some other thread is currently dispatching messages for
                // this subscriber, so we won't take any
                // But note it and signal another thread (would be nicer to have a
                // per-long condition so we could signal the exact thread, but you can't have
                // atomic operations across two maps without a lock)
                resignal |= e.getValue().count > 0;
                continue;
            }
            // Atomically swap in a new Messages, *if* the messages object returned by
            // e.getValue() is still the one actually in the map - another thread may
            // have been in this method concurrently and claimed it, in which case we
            // skip this key - another thread already has its messages
            if (messagesForSubscriber.replace(e.getKey(), e.getValue(), new Messages())) {
                long[] totalBytes = new long[1];
                List<Message> msgs = e.getValue().drain(totalBytes);
                if (!msgs.isEmpty()) {
                    result += msgs.size();
                    batch.add(new MessageBatchImpl(e.getKey(), msgs, currentThread, totalBytes[0]));
                } else {
                    threadAffinity.remove(e.getKey());
                }
            }
        }
        // Note that resignal is best-effort - we cannot guarantee that another
        // thread did not add something not reflected in messagesForSubscriber.entrySet
        // while we were iterating it.  Hence the busywait rather than full wait
        // in wait()
        if (resignal) {
            signalOneThread();
        }
        return result;
    }

    private void signalOneThread() {
        synchronized (this) {
            notify();
        }
    }

    int estimatedSize() {
        int result = 0;
        for (Messages m : messagesForSubscriber.values()) {
            result += m.count;
        }
        return result;
    }

    Set<Long> lockedKeys() {
        return threadAffinity.keySet();
    }

    /**
     * Wrapper for a list of messages, mapped to subscriber ID. It is *critical*
     * to call close() on every batch returned. The call is idempotent.
     */
    interface MessageBatch extends Iterable<Message>, AutoCloseable {

        /**
         * The subscriber id this set of messages targets.
         *
         * @return The id
         */
        long subscriberId();

        /**
         * The number of messages retrievd (may be 0).
         *
         * @return The size
         */
        int size();

        /**
         * The total number of bytes in the data arrays of all messages in this
         * batch.
         *
         * @return The total number of bytes
         */
        long totalBytes();

        /**
         * Close this message batch, allowing other threads to enter and
         * retrieve messages for this subscriber id. If not called, messages may
         * be queued indefinitely..
         */
        @Override
        void close();

        List<Message> toList();
    }

    private final class MessageBatchImpl implements MessageBatch {

        private final List<Message> messages;
        private final long subscriberId;
        private final Thread creatingThread;
        private volatile boolean closed;
        private final long totalBytes;

        MessageBatchImpl(long subscriberId, List<Message> messages, Thread creatingThread, long totalBytes) {
            this.messages = messages;
            this.subscriberId = subscriberId;
            this.creatingThread = creatingThread;
            this.totalBytes = totalBytes;
        }

        @Override
        public long subscriberId() {
            return subscriberId;
        }

        @Override
        public Iterator<Message> iterator() {
            return messages.iterator();
        }

        @Override
        public int size() {
            return messages.size();
        }

        @Override
        public void close() {
            boolean wasClosed = closed;
            closed = true;
            // Ensure we don't remove a the wrong thread in the case close() is
            // not called from the same thread we have here
            if (!wasClosed && threadAffinity.get(subscriberId) == creatingThread) {
                threadAffinity.remove(subscriberId);
            }
        }

        @Override
        public long totalBytes() {
            return totalBytes;
        }

        @Override
        public List<Message> toList() {
            return messages;
        }

        public String toString() {
            return "MessageBatch for " + subscriberId() + " with " + messages.size() + " messages, total bytes " + totalBytes();
        }
    }

    static final class Messages {

        // A basic linked list structure, where the head is found by
        // iterating backwards
        MessageEntry tail;
        // Safe for atomic reads without a lock
        private volatile int count;

        // Using plain synchronization here - a java.util.concurrent lock
        // doesn't buy us anything;  and we create these frequently and throw
        // them away, to have a new value if needed for atomic operations,
        // so the cheaper construction is, the better, and you already
        // get a synchronization monitor with every Java object
        public Messages add(Message message) {
            synchronized (this) {
                count++;
                // Replace the tail
                if (tail == null) {
                    tail = new MessageEntry(null, message);
                } else {
                    tail = new MessageEntry(tail, message);
                }
            }
            return this;
        }

        public List<Message> drain(long[] totalBytes) {
            assert totalBytes != null && totalBytes.length == 1;
            if (count == 0) {
                return Collections.emptyList();
            }
            // Do the minimal amount under the lock
            MessageEntry oldTail;
            synchronized (this) {
                oldTail = tail;
                count = 0;
                tail = null;
            }
            // Populate the list iterating backwards from the tail
            if (oldTail != null) {
                if (oldTail.prev == null) {
                    if (oldTail.message.getData() != null) {
                        totalBytes[0] = oldTail.message.getData().length;
                    }
                    return Collections.singletonList(oldTail.message);
                }
                List<Message> all = new LinkedList<>();
                oldTail.drainTo(all, totalBytes);
                return all;
            }
            return Collections.emptyList();
        }

        static final class MessageEntry {

            private MessageEntry prev;
            private Message message;

            public MessageEntry(MessageEntry prev, Message message) {
                this.prev = prev;
                this.message = message;
            }

            void drainTo(List<? super Message> messages, long[] totalBytes) {
                MessageEntry e = this;
                while (e != null) {
                    messages.add(0, e.message);
                    if (e.message.getData() != null) {
                        totalBytes[0] += e.message.getData().length;
                    }
                    e = e.prev;
                }
            }
        }
    }
}
