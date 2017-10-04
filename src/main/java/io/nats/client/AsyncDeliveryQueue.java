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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.UnaryOperator;

/**
 * Asynchronous queue for delivering messages to async subscribers.
 * Uses AtomicReferenceArrays rather than traditional locks to manage
 * concurrency.  Internally, manages a list of such arrays - since
 * subscriber ids increase monotonically, despite the fact that they are
 * long-indexed, the common case is a fairly small number, the array
 * offset can be the index mapping.
 *
 * @author Tim Boudreau
 */
final class AsyncDeliveryQueue {

    private final int arraySize;
    private final List<AtomicReferenceArray<Messages>> arrays = new CopyOnWriteArrayList<>();
    private final List<AtomicLongArray> threads = new CopyOnWriteArrayList<>();

    AsyncDeliveryQueue() {
        this(64);
    }

    AsyncDeliveryQueue(int arraySize) {
        if (arraySize <= 0) {
            throw new IllegalStateException("Invalid array size " + arraySize);
        }
        this.arraySize = arraySize;
    }

    /**
     * Add a message mapped to a subcriber id
     *
     * @param subscriberId The subscriber id
     * @param message The message
     */
    void add(long subscriberId, Message msg) {
        int index = (int) (subscriberId / arraySize);
        int offset = (int) (subscriberId % arraySize);
        if (index >= arrays.size()) {
            // Intentional double-checked locking here - we need
            // an atomic size check + possible add in one shot
            synchronized (arrays) {
                if (index >= arrays.size()) {
                    Messages nue = new Messages(msg);
                    // Create a new array to contain messages for subscriberIds
                    // starting at arrays.size() * arraySize
                    AtomicReferenceArray<Messages> arr = new AtomicReferenceArray<>(arraySize);
                    // Create a new long array to hold the thread id of any thread
                    // that has exclusive claim on a particular subscriber id because
                    // it is delivering messages for it (this state is cleared in
                    // MessageBatch.close()).
                    threads.add(new AtomicLongArray(arraySize));
                    // Pre-populate it - the array must always contain a queue
                    // ready to receive messages - the queues are lightweight
                    for (int i = 0; i < arraySize; i++) {
                        if (i == offset) {
                            arr.set(i, nue);
                        } else {
                            arr.set(i, new Messages());
                        }
                    }
                    arrays.add(arr);
                    return;
                }
            }
        }
        AtomicReferenceArray<Messages> array = arrays.get(index);
        array.get(offset).add(msg);
        signalOneThread();
    }

    /**
     * Remove a subscriber id.  Clears any pending mesesages
     * (an entry remains).
     *
     * @param subscriberId The id
     */
    void remove(long subscriberId) {
        int index = (int) (subscriberId / arraySize);
        int offset = (int) (subscriberId % arraySize);
        if (index < arrays.size()) {
            arrays.get(index).set(offset, new Messages());
        }
        threads.get(index).set(offset, 0L);
    }

    /**
     * Remove a susbcriber id, retrieving any remaining messages.
     * Clears any pending mesesages (an entry remains).
     *
     * @param subscriberId The id
     * @return A list of messages
     */
    List<Message> removeAndCollect(long subscriberId) {
        int index = (int) (subscriberId / arraySize);
        int offset = (int) (subscriberId % arraySize);
        threads.get(index).set(offset, 0L);
        if (index < arrays.size()) {
            Messages old = arrays.get(index).getAndSet(offset, new Messages());
            if (old != null) {
                return old.drain(new long[1]);
            }
        }
        return Collections.emptyList();
    }

    /**
     * Test if the subscriber id is present.
     *
     * @param subscriberId The subscriber id
     * @return true if it is present
     */
    boolean containsMessagesFor(long subscriberId) {
        int index = (int) (subscriberId / arraySize);
        int offset = (int) (subscriberId % arraySize);
        if (index < arrays.size()) {
            Messages msgs = arrays.get(index).get(offset);
            return msgs != null && !msgs.isEmpty();
        }
        return false;
    }

    /**
     * Fetch messages, blocking for the specified timeout while waiting
     * for them.
     *
     * @param into A list to put batches of messages into
     * @param timeout The timeout
     * @param unit The timeout time unit
     * @return The total number of <i>Messages</i> retrieved in all
     * MessageBatches added to the list
     * @throws InterruptedException If the thread pool is shut down
     */
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

    /**
     * Fetch messages, blocking until messages are available.
     *
     * @param into A list to put batches of messages into
     * @param timeout The timeout
     * @param unit The timeout time unit
     * @return The total number of <i>Messages</i> retrieved in all
     * MessageBatches added to the list
     * @throws InterruptedException If the thread pool is shut down
     */
    public int get(List<? super MessageBatch> into) throws InterruptedException {
        // If messages immediately available, return them without waiting
        drain(into);
        // wait for signal
        while (into.isEmpty()) {
            synchronized(this) {
                wait();
            }
            drain(into);
        }
        return into.size();
    }

    public void clear() {
        synchronized (this) {
            notifyAll();
        }
        // Give a chance at message collection
        Thread.yield();
        arrays.clear();
        threads.clear();
    }

    private int drain(List<? super MessageBatch> batch) {
        int result = 0;
        boolean resignal = false;
        Thread currentThread = Thread.currentThread();
        int as = arrays.size();
        final long[] totalBytes = new long[1];
        for (int i = 0; i < as; i++) {
            AtomicReferenceArray<Messages> arr = arrays.get(i);
            for (int j = 0; j < arraySize; j++) {
                long subscriberId = (long) (i * arraySize) + j;
                boolean alreadyClaimed = !threads.get(i).compareAndSet(j, 0, currentThread.getId());
                // Ensure that only this thread can read messages for this key, until
                // the batch for that key is closed.
                if (alreadyClaimed) {
                    // Some other thread is currently dispatching messages for
                    // this subscriber, so we won't take any
                    // But note it and signal another thread (would be nicer to have a
                    // per-long condition so we could signal the exact thread, but you can't have
                    // atomic operations across two maps without a lock)
                    Messages m = arr.get(j);
                    resignal |= m != null && !m.isEmpty();
                    continue;
                }
                Messages m = arr.get(j);
                totalBytes[0] = 0L;
                List<Message> msgs = m == null ? Collections.<Message>emptyList() : m.drain(totalBytes);
                if (!msgs.isEmpty()) {
                    result += msgs.size();
                    batch.add(new MessageBatchImpl(subscriberId, msgs, currentThread, totalBytes[0]));
                } else {
                    threads.get(i).compareAndSet(j, currentThread.getId(), 0);
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

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < arrays.size(); i++) {
            AtomicReferenceArray<Messages> arr = arrays.get(i);
            for (int j = 0; j < arraySize; j++) {
                Messages m = arr.get(i);
                if (!m.isEmpty()) {
                    int sid = (i * arraySize) + j;
                    sb.append(sid).append(":").append(m);
                }
            }
        }
        return sb.toString();
    }

    private void signalOneThread() {
        synchronized (this) {
            notify();
        }
    }

    int estimatedSize() {
        int result = 0;
        for (AtomicReferenceArray<Messages> a : arrays) {
            for (int i = 0; i < arraySize; i++) {
                Messages m = a.get(i);
                if (m != null) {
                    result += a.get(i).size();
                }
            }
        }
        return result;
    }

    Set<Long> lockedKeys() {
        Set<Long> result = new HashSet<>();
        for (int i = 0; i < threads.size(); i++) {
            AtomicLongArray a = threads.get(i);
            for (int j = 0; j < arraySize; j++) {
                long val = a.get(j);
                if (val != 0L) {
                    result.add((long) (i * arraySize) + j);
                }
            }
        }
        return result;
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
            int index = (int) subscriberId / arraySize;
            int offset = (int) subscriberId % arraySize;
            if (!wasClosed) {
                threads.get(index).compareAndSet(offset, creatingThread.getId(), 0);
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
        AtomicReference<MessageEntry> tail;

        Messages(Message message) {
            tail = new AtomicReference<>(new MessageEntry(null, message));
        }

        Messages() {
            tail = new AtomicReference<>();
        }

        // Using plain synchronization here - a java.util.concurrent lock
        // doesn't buy us anything;  and we create these frequently and throw
        // them away, to have a new value if needed for atomic operations,
        // so the cheaper construction is, the better, and you already
        // get a synchronization monitor with every Java object
        public Messages add(final Message message) {
            tail.getAndUpdate(new Applier(message));
            return this;
        }

        // This is a JDK-8ism - if we're stuck on JDK 7,
        // just synchronize in add and drain instead, but
        // this gives us an atomic replacement of the tail
        // without the overhead of a lock
        static class Applier implements UnaryOperator<MessageEntry> {

            private final Message message;

            public Applier(Message message) {
                this.message = message;
            }

            @Override
            public MessageEntry apply(MessageEntry t) {
                if (t == null) {
                    return new MessageEntry(null, message);
                } else {
                    return new MessageEntry(t, message);
                }
            }
        }

        public List<Message> drain(long[] totalBytes) {
            assert totalBytes != null && totalBytes.length == 1;
            // Do the minimal amount under the lock
            MessageEntry oldTail = tail.getAndSet(null);
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

        public boolean isEmpty() {
            return tail.get() == null;
        }

        public int size() {
            MessageEntry tail = this.tail.get();
            int result = 0;
            while (tail != null) {
                result++;
                tail = tail.prev;
            }
            return result;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder("Messages: ");
            MessageEntry tail = this.tail.get();
            while (tail != null) {
                if (sb.length() > 0) {
                    sb.append(", ");
                }
                sb.append(tail.message);
                tail = tail.prev;
            }
            return sb.toString();
        }

        /**
         * A single linked list entry in the queue
         */
        static final class MessageEntry {

            private MessageEntry prev;
            private Message message;

            public MessageEntry(MessageEntry prev, Message message) {
                this.prev = prev;
                this.message = message;
            }

            void drainTo(List<? super Message> messages, long[] totalBytes) {
                // Iterate backwards populating the list of messages,
                // and also collect the total byte count
                MessageEntry e = this;
                while (e != null) {
                    messages.add(0, e.message);
                    byte[] data = e.message.getData();
                    if (data != null) {
                        totalBytes[0] += data.length;
                    }
                    e = e.prev;
                }
            }
        }
    }
}
