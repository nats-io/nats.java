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

package io.nats.client.impl;

import io.nats.client.*;
import io.nats.client.support.Status;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class TestHandler implements ErrorListener, ConnectionListener {
    private final AtomicInteger count = new AtomicInteger();

    private final HashMap<Events,AtomicInteger> eventCounts = new HashMap<>();
    private final HashMap<String,AtomicInteger> errorCounts = new HashMap<>();
    private final ReentrantLock lock = new ReentrantLock();

    private final AtomicInteger exceptionCount = new AtomicInteger();

    private CompletableFuture<Boolean> statusChanged;
    private CompletableFuture<Boolean> slowSubscriber;
    private CompletableFuture<Boolean> errorWaitFuture;
    private Events eventToWaitFor;
    private String errorToWaitFor;

    private Connection connection;
    private final ArrayList<Consumer> slowConsumers = new ArrayList<>();
    private final ArrayList<Message> discardedMessages = new ArrayList<>();
    private final ArrayList<UnhandledStatusEvent> unhandledStatuses = new ArrayList<>();

    private final boolean printExceptions;
    private final boolean verbose;

    public TestHandler() {
        this(false, false);
    }

    public TestHandler(boolean printExceptions, boolean verbose) {
        this.printExceptions = printExceptions;
        this.verbose = verbose;
    }

    public void reset() {
        count.set(0);
        eventCounts.clear();
        errorCounts.clear();
        exceptionCount.set(0);
        statusChanged = null;
        slowSubscriber = null;
        errorWaitFuture = null;
        eventToWaitFor = null;
        errorToWaitFor = null;
        slowConsumers.clear();
        discardedMessages.clear();
        unhandledStatuses.clear();
    }

    public void prepForStatusChange(Events waitFor) {
        lock.lock();
        try {
            statusChanged = new CompletableFuture<>();
            eventToWaitFor = waitFor;
            if (verbose) {
                report("prepForStatusChange",  waitFor);
            }
        } finally {
            lock.unlock();
        }
    }

    private boolean waitForFuture(CompletableFuture<Boolean> future, long timeout, TimeUnit units) {
        try {
            return future.get(timeout, units);
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            if (printExceptions) {
                e.printStackTrace();
            }
            return false;
        }
    }

    public boolean waitForStatusChange(long timeout, TimeUnit units) {
        return waitForFuture(statusChanged, timeout, units);
    }

    public void exceptionOccurred(Connection conn, Exception exp) {
        connection = conn;
        count.incrementAndGet();
        exceptionCount.incrementAndGet();

        if (exp != null) {
            if (verbose) {
                report("exceptionOccurred",  exp);
            }
            else if (printExceptions) {
                exp.printStackTrace();
            }
        }
    }

    public void prepForError(String waitFor) {
        lock.lock();
        try {
            errorWaitFuture = new CompletableFuture<>();
            errorToWaitFor = waitFor;
            if (verbose) {
                report("prepForError",  waitFor);
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean waitForError(long timeout, TimeUnit units) {
        return waitForFuture(errorWaitFuture, timeout, units);
    }

    public void errorOccurred(Connection conn, String type) {
        connection = conn;
        count.incrementAndGet();

        lock.lock();
        try {
            AtomicInteger counter = errorCounts.get(type);
            if (counter == null) {
                counter = new AtomicInteger();
                errorCounts.put(type, counter);
            }
            counter.incrementAndGet();
            if (errorWaitFuture != null && type.equals(errorToWaitFor)) {
                errorWaitFuture.complete(Boolean.TRUE);
            }
            if (verbose) {
                report("errorOccurred",  type);
            }
        } finally {
            lock.unlock();
        }
    }

    public void messageDiscarded(Connection conn, Message msg) {
        connection = conn;
        count.incrementAndGet();

        lock.lock();
        try {
            discardedMessages.add(msg);
            if (verbose) {
                report("messageDiscarded",  msg);
            }
        } finally {
            lock.unlock();
        }
    }

    public void connectionEvent(Connection conn, Events type) {
        connection = conn;
        count.incrementAndGet();

        lock.lock();
        try {
            AtomicInteger counter = eventCounts.get(type);
            if (counter == null) {
                counter = new AtomicInteger();
                eventCounts.put(type, counter);
            }
            counter.incrementAndGet();
            if (statusChanged != null && type == eventToWaitFor) {
                statusChanged.complete(Boolean.TRUE);
            }
            if (verbose) {
                report("connectionEvent",  type);
            }
        } finally {
            lock.unlock();
        }
    }

    public Future<Boolean> waitForSlow() {
        slowSubscriber = new CompletableFuture<>();
        return slowSubscriber;
    }

    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        count.incrementAndGet();

        lock.lock();
        try {
            slowConsumers.add(consumer);
            if (slowSubscriber != null) {
                slowSubscriber.complete(true);
            }
            if (verbose) {
                String msg;
                if (consumer instanceof NatsSubscription) {
                    NatsSubscription nats = (NatsSubscription)consumer;
                    msg = "Subscription " + nats.getSID() + " for " + nats.getSubject();
                }
                else if (consumer instanceof NatsDispatcher) {
                    NatsDispatcher nats = (NatsDispatcher)consumer;
                    msg = "Dispatcher " + nats.getId();
                }
                else {
                    msg = consumer.toString();
                }
                report("slowConsumerDetected",  msg);
            }
        } finally {
            lock.unlock();
        }
    }

    private void report(String func, Object message) {
        System.out.println("" + System.currentTimeMillis() + " [TestHelper." + func + "] " + message);
    }

    public List<Consumer> getSlowConsumers() {
        return slowConsumers;
    }

    public List<Message> getDiscardedMessages() {
        return discardedMessages;
    }

    public int getCount() {
        return count.get();
    }

    public int getExceptionCount() {
        return exceptionCount.get();
    }

    public int getEventCount(Events type) {
        int retVal = 0;
        lock.lock();
        try {
            AtomicInteger counter = eventCounts.get(type);
            if (counter != null) {
                retVal = counter.get();
            }
        } finally {
            lock.unlock();
        }
        return retVal;
    }

    public int getErrorCount(String type) {
        int retVal = 0;
        lock.lock();
        try {
            AtomicInteger counter = errorCounts.get(type);
            if (counter != null) {
                retVal = counter.get();
            }
        } finally {
            lock.unlock();
        }
        return retVal;
    }

    public void dumpErrorCountsToStdOut() {
        lock.lock();
        try {
            System.out.println("#### Test Handler Error Counts ####");
            for (String key : errorCounts.keySet()) {
                int count = errorCounts.get(key).get();
                System.out.println(key+": "+count);
            }
        } finally {
            lock.unlock();
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public static class UnhandledStatusEvent {
        String sid;
        Status status;

        public UnhandledStatusEvent(JetStreamSubscription sub, Status status) {
            this.sid = ((NatsJetStreamSubscription)sub).getSID();
            this.status = status;
        }
    }

    @Override
    public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
        unhandledStatuses.add(new UnhandledStatusEvent(sub, status));
    }

    public ArrayList<UnhandledStatusEvent> getUnhandledStatuses() {
        return unhandledStatuses;
    }
}