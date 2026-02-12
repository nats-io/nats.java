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
import io.nats.client.support.DateTimeUtils;
import io.nats.client.support.Status;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;

@SuppressWarnings("CallToPrintStackTrace")
public class ListenerForTesting implements ErrorListener, ConnectionListener {
    private final ReentrantLock prepLock = new ReentrantLock();

    private final AtomicInteger count = new AtomicInteger();
    private final AtomicInteger exceptionCount = new AtomicInteger();
    private final HashMap<Events,AtomicInteger> eventCounts = new HashMap<>();
    private final HashMap<String,AtomicInteger> errorCounts = new HashMap<>();
    private final AtomicBoolean exitOnDisconnect = new AtomicBoolean(false);
    private final AtomicBoolean exitOnHeartbeatError = new AtomicBoolean(false);

    private Connection lastEventConnection;

    private CompletableFuture<Boolean> statusChanged;
    private CompletableFuture<Boolean> slowSubscriber;
    private CompletableFuture<Boolean> errorWaitFuture;
    private CompletableFuture<HeartbeatAlarmEvent> heartbeatAlarmEventWaitFuture;
    private CompletableFuture<StatusEvent> pullStatusWarningWaitFuture;
    private CompletableFuture<StatusEvent> pullStatusErrorWaitFuture;
    private Events eventToWaitFor;
    private String errorToWaitFor;

    private final List<Events> connectionEvents = new ArrayList<>();
    private final List<String> errors = new ArrayList<>();
    private final List<Exception> exceptions = new ArrayList<>();
    private final List<Consumer> slowConsumers = new ArrayList<>();
    private final List<Message> discardedMessages = new ArrayList<>();
    private final List<StatusEvent> unhandledStatuses = new ArrayList<>();
    private final List<StatusEvent> pullStatusWarnings = new ArrayList<>();
    private final List<StatusEvent> pullStatusErrors = new ArrayList<>();
    private final List<HeartbeatAlarmEvent> heartbeatAlarms = new ArrayList<>();
    private final List<FlowControlProcessedEvent> flowControlProcessedEvents = new ArrayList<>();
    public final AtomicInteger socketWriteTimeoutCount = new AtomicInteger();

    private final boolean printExceptions;
    private final boolean verbose;

    public ListenerForTesting() {
        this(false, false);
    }

    public ListenerForTesting(boolean printExceptions, boolean verbose) {
        this.printExceptions = printExceptions;
        this.verbose = verbose;
    }

    public void reset() {
        count.set(0);
        exceptionCount.set(0);
        eventCounts.clear();
        errorCounts.clear();
        exitOnDisconnect.set(false);
        exitOnHeartbeatError.set(false);
        lastEventConnection = null;
        statusChanged = null;
        slowSubscriber = null;
        errorWaitFuture = null;
        heartbeatAlarmEventWaitFuture = null;
        pullStatusWarningWaitFuture = null;
        pullStatusErrorWaitFuture = null;
        eventToWaitFor = null;
        errorToWaitFor = null;
        connectionEvents.clear();
        errors.clear();
        exceptions.clear();
        slowConsumers.clear();
        discardedMessages.clear();
        unhandledStatuses.clear();
        pullStatusWarnings.clear();
        pullStatusErrors.clear();
        heartbeatAlarms.clear();
        flowControlProcessedEvents.clear();
        socketWriteTimeoutCount.set(0);
    }

    private boolean waitForBooleanFuture(CompletableFuture<Boolean> future, long timeout, TimeUnit units) {
        try {
            return future.get(timeout, units);
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            maybePrintException("waitForBooleanFuture", e);
            return false;
        }
    }

    private <T> T waitForFuture(CompletableFuture<T> future, long waitInMillis) {
        try {
            return future.get(waitInMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | ExecutionException | InterruptedException e) {
            maybePrintException("waitForFuture", e);
            return null;
        }
    }

    private void maybePrintException(String label, Exception e) {
        if (printExceptions) {
            System.err.print("LFT " + label + ": ");
            e.printStackTrace();
        }
    }

    public void setExitOnDisconnect() {
        exitOnDisconnect.set(true);
    }

    public void setExitOnHeartbeatError() {
        exitOnHeartbeatError.set(true);
    }

    public void clearExitOnDisconnect() {
        exitOnDisconnect.set(false);
    }

    public void clearExitOnHeartbeatError() {
        exitOnHeartbeatError.set(false);
    }

    public void prepForStatusChange(Events waitFor) {
        prepLock.lock();
        try {
            statusChanged = new CompletableFuture<>();
            eventToWaitFor = waitFor;
            if (verbose) {
                report("prepForStatusChange",  waitFor);
            }
        } finally {
            prepLock.unlock();
        }
    }

    public boolean waitForStatusChange(long timeout, TimeUnit units) {
        return waitForBooleanFuture(statusChanged, timeout, units);
    }

    public void exceptionOccurred(Connection conn, Exception exp) {
        lastEventConnection = conn;
        exceptions.add(exp);
        count.incrementAndGet();
        exceptionCount.incrementAndGet();

        if (exp != null) {
            if (verbose) {
                report("exceptionOccurred",  "conn:" + conn.hashCode() + ", " + exp);
            }
            maybePrintException("exceptionOccurred", exp);
        }
    }

    public <T> boolean _eventually(long timeout, Supplier<List<T>> listSupplier, Predicate<T> predicate) {
        long start = System.currentTimeMillis();
        int i = 0;
        do {
            List<T> list = listSupplier.get();
            int size = list.size();
            for (; i < size; i++) {
                if (predicate.test(list.get(i))) {
                    return true;
                }
            }
        }
        while (System.currentTimeMillis() - start <= timeout);
        return false;
    }

    public void prepForError(String waitFor) {
        prepLock.lock();
        try {
            errorWaitFuture = new CompletableFuture<>();
            errorToWaitFor = waitFor;
            if (verbose) {
                report("prepForError",  waitFor);
            }
        } finally {
            prepLock.unlock();
        }
    }

    public boolean errorsEventually(String contains, long timeout) {
        return _eventually(timeout, () -> copy(errors), (s) -> s.contains(contains));
    }

    public void errorOccurred(Connection conn, String errorText) {
        lastEventConnection = conn;
        add(errors, errorText);
        count.incrementAndGet();

        prepLock.lock();
        try {
            AtomicInteger counter = errorCounts.computeIfAbsent(errorText, k -> new AtomicInteger());
            counter.incrementAndGet();
            if (errorWaitFuture != null && errorText.contains(errorToWaitFor)) {
                errorWaitFuture.complete(Boolean.TRUE);
            }
            if (verbose) {
                report("errorOccurred",  errorText);
            }
        } finally {
            prepLock.unlock();
        }
    }

    public void messageDiscarded(Connection conn, Message msg) {
        lastEventConnection = conn;
        count.incrementAndGet();

        prepLock.lock();
        try {
            discardedMessages.add(msg);
            if (verbose) {
                report("messageDiscarded",  msg);
            }
        } finally {
            prepLock.unlock();
        }
    }

    @Override
    public void connectionEvent(Connection conn, Events type) {
        connectionEvent(conn, type, null, null);
    }

    @Override
    public void connectionEvent(Connection conn, Events type, Long time, String uriDetails) {
        lastEventConnection = conn;
        connectionEvents.add(type);
        count.incrementAndGet();

        if (exitOnDisconnect.get() && type == Events.DISCONNECTED) {
            System.exit(-1);
        }

        prepLock.lock();
        try {
            AtomicInteger counter = eventCounts.computeIfAbsent(type, k -> new AtomicInteger());
            counter.incrementAndGet();
            if (statusChanged != null && type == eventToWaitFor) {
                statusChanged.complete(Boolean.TRUE);
            }
            if (verbose) {
                report("connectionEvent",  type);
            }
        } finally {
            prepLock.unlock();
        }
    }

    public Future<Boolean> waitForSlow() {
        slowSubscriber = new CompletableFuture<>();
        return slowSubscriber;
    }

    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        count.incrementAndGet();

        prepLock.lock();
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
            prepLock.unlock();
        }
    }

    public static final DateTimeFormatter SIMPLE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static String simpleTime() {
        return SIMPLE_TIME_FORMATTER.format(DateTimeUtils.gmtNow());
    }

    private void report(String func, Object message) {
        System.out.println("[" + simpleTime() + " ListenerForTesting." + func + "] " + message);
    }

    private final ReentrantLock listLock = new ReentrantLock();
    private <T> List<T> copy(List<T> list) {
        listLock.lock();
        try {
            return new ArrayList<>(list);
        }
        finally {
            listLock.unlock();
        }
    }

    private <T> void add(List<T> list, T t) {
        listLock.lock();
        try {
            list.add(t);
        }
        finally {
            listLock.unlock();
        }
    }

    public List<Events> getConnectionEvents() {
        return connectionEvents;
    }

    public List<String> getErrors() {
        return errors;
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    public List<Consumer> getSlowConsumers() {
        return slowConsumers;
    }

    public List<Message> getDiscardedMessages() {
        return discardedMessages;
    }

    public List<StatusEvent> getUnhandledStatuses() {
        return unhandledStatuses;
    }

    public List<StatusEvent> getPullStatusWarnings() {
        return pullStatusWarnings;
    }

    public List<StatusEvent> getPullStatusErrors() {
        return pullStatusErrors;
    }

    public List<HeartbeatAlarmEvent> getHeartbeatAlarms() {
        return heartbeatAlarms;
    }

    public List<FlowControlProcessedEvent> getFlowControlProcessedEvents() {
        return flowControlProcessedEvents;
    }

    public int getSocketWriteTimeoutCount() {
        return socketWriteTimeoutCount.get();
    }

    public int getCount() {
        return count.get();
    }

    public int getExceptionCount() {
        return exceptionCount.get();
    }

    public int getEventCount(Events type) {
        int retVal = 0;
        prepLock.lock();
        try {
            AtomicInteger counter = eventCounts.get(type);
            if (counter != null) {
                retVal = counter.get();
            }
        } finally {
            prepLock.unlock();
        }
        return retVal;
    }

    public int getErrorCount(String type) {
        int retVal = 0;
        prepLock.lock();
        try {
            AtomicInteger counter = errorCounts.get(type);
            if (counter != null) {
                retVal = counter.get();
            }
        } finally {
            prepLock.unlock();
        }
        return retVal;
    }

    public void dumpErrorCountsToStdOut() {
        prepLock.lock();
        try {
            System.out.println("#### Test Handler Error Counts ####");
            for (String key : errorCounts.keySet()) {
                int count = errorCounts.get(key).get();
                System.out.println(key+": "+count);
            }
        } finally {
            prepLock.unlock();
        }
    }

    public Connection getLastEventConnection() {
        return lastEventConnection;
    }

    @Override
    public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
        unhandledStatuses.add(new StatusEvent(sub, status));
    }

    public void prepForPullStatusWarning() {
        prepLock.lock();
        try {
            pullStatusWarningWaitFuture = new CompletableFuture<>();
        }
        finally {
            prepLock.unlock();
        }
    }

    public StatusEvent waitForPullStatusWarning(long waitInMillis) {
        return waitForFuture(pullStatusWarningWaitFuture, waitInMillis);
    }

    public boolean pullStatusWarningEventually(String contains, long timeout) {
        return _eventually(timeout, () -> copy(pullStatusWarnings),
            (se) -> se.status.getMessage().contains(contains));
    }

    @Override
    public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
        prepLock.lock();
        try {
            StatusEvent event = new StatusEvent(sub, status);
            if (verbose) {
                report("pullStatusWarning",  event);
            }
            add(pullStatusWarnings, event);
            if (pullStatusWarningWaitFuture != null) {
                pullStatusWarningWaitFuture.complete(event);
            }
        } finally {
            prepLock.unlock();
        }
    }

    public void prepForPullStatusError() {
        prepLock.lock();
        try {
            pullStatusErrorWaitFuture = new CompletableFuture<>();
        }
        finally {
            prepLock.unlock();
        }
    }

    public StatusEvent waitForPullStatusError(long waitInMillis) {
        return waitForFuture(pullStatusErrorWaitFuture, waitInMillis);
    }

    public boolean pullStatusErrorOrWait(String contains, long timeout) {
        return _eventually(timeout, () -> copy(pullStatusErrors),
            (se) -> se.status.getMessage().contains(contains));
    }

    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
        prepLock.lock();
        try {
            StatusEvent event = new StatusEvent(sub, status);
            if (verbose) {
                report("pullStatusError",  event);
            }
            add(pullStatusErrors, event);
            if (pullStatusErrorWaitFuture != null) {
                pullStatusErrorWaitFuture.complete(event);
            }
        } finally {
            prepLock.unlock();
        }
    }

    public void prepForHeartbeatAlarm() {
        prepLock.lock();
        try {
            heartbeatAlarmEventWaitFuture = new CompletableFuture<>();
        }
        finally {
            prepLock.unlock();
        }
    }

    public HeartbeatAlarmEvent waitForHeartbeatAlarm(long waitInMillis) {
        return waitForFuture(heartbeatAlarmEventWaitFuture, waitInMillis);
    }

    @Override
    public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
        prepLock.lock();
        try {
            if (exitOnHeartbeatError.get()) {
                System.exit(-2);
            }
            HeartbeatAlarmEvent event = new HeartbeatAlarmEvent(sub, lastStreamSequence, lastConsumerSequence);
            if (verbose) {
                report("heartbeatAlarm",  event);
            }
            heartbeatAlarms.add(event);
            if (heartbeatAlarmEventWaitFuture != null) {
                heartbeatAlarmEventWaitFuture.complete(event);
            }
        } finally {
            prepLock.unlock();
        }
    }

    @Override
    public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
        flowControlProcessedEvents.add(new FlowControlProcessedEvent(sub, subject, source));
    }

    @Override
    public void socketWriteTimeout(Connection conn) {
        socketWriteTimeoutCount.incrementAndGet();
    }

    public static class StatusEvent {
        String sid;
        Status status;

        public StatusEvent(JetStreamSubscription sub, Status status) {
            this.sid = extractSid(sub);
            this.status = status;
        }

        @Override
        public String toString() {
            return "StatusEvent{" +
                "sid='" + sid + '\'' +
                ", status=" + status +
                '}';
        }
    }

    public static class HeartbeatAlarmEvent {
        String sid;
        long lastStreamSequence;
        long lastConsumerSequence;

        public HeartbeatAlarmEvent(JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
            this.sid = extractSid(sub);
            this.lastStreamSequence = lastStreamSequence;
            this.lastConsumerSequence = lastConsumerSequence;
        }

        @Override
        public String toString() {
            return "HeartbeatAlarmEvent{" +
                "sid='" + sid + '\'' +
                ", lastStreamSequence=" + lastStreamSequence +
                ", lastConsumerSequence=" + lastConsumerSequence +
                '}';
        }
    }

    public static class FlowControlProcessedEvent {
        String sid;
        String subject;
        FlowControlSource source;

        public FlowControlProcessedEvent(JetStreamSubscription sub, String subject, FlowControlSource source) {
            this.sid = extractSid(sub);
            this.subject = subject;
            this.source = source;
        }

        @Override
        public String toString() {
            return "FlowControlEvent{" +
                "sid='" + sid + '\'' +
                ", subject='" + subject + '\'' +
                ", source=" + source +
                '}';
        }
    }

    private static String extractSid(JetStreamSubscription sub) {
        return ((NatsJetStreamSubscription)sub).getSID();
    }
}
