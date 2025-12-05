// Copyright 2025 The NATS Authors
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

package io.nats.client.support;

import io.nats.client.*;
import org.junit.jupiter.api.Assertions;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

@SuppressWarnings({"CallToPrintStackTrace", "RedundantMethodOverride"})
public class Listener implements ErrorListener, ConnectionListener {
    public static final int SHORT_VALIDATE_TIMEOUT     =  2000;
    public static final int DEFAULT_VALIDATE_TIMEOUT   =  5000;
    public static final int MEDIUM_VALIDATE_TIMEOUT    =  7500;
    public static final int LONG_VALIDATE_TIMEOUT      = 12500;
    public static final int VERY_LONG_VALIDATE_TIMEOUT = 20000;

    private final boolean printExceptions;
    private final boolean verbose;

    private final List<ListenerFuture> futures;
    private final List<Message> discardedMessages;
    private Connection lastConnectionEventConnection;
    private int exceptionCount;
    private int heartbeatAlarmCount;
    private int flowControlCount;
    private int pullStatusWarningsCount;
    private int socketWriteTimeoutCount;

    public Listener() {
        this(false, false);
    }

    public Listener(boolean verbose) {
        this(false, verbose);
    }

    public Listener(boolean printExceptions, boolean verbose) {
        this.printExceptions = printExceptions;
        this.verbose = verbose;
        futures = new ArrayList<>();
        discardedMessages = new ArrayList<>();
    }

    public void reset() {
        for (ListenerFuture f : futures) {
            f.cancel(true);
        }
        futures.clear();
        discardedMessages.clear();
        lastConnectionEventConnection = null;
        exceptionCount = 0;
        heartbeatAlarmCount = 0;
        flowControlCount = 0;
        pullStatusWarningsCount = 0;
        socketWriteTimeoutCount = 0;
    }

    // ----------------------------------------------------------------------------------------------------
    // Valdiate
    // ----------------------------------------------------------------------------------------------------
    public void validate() {
        _validate(futures.get(0));
    }

    public void validateAll() {
        List<ListenerFuture> copy = new ArrayList<>(futures);
        for (ListenerFuture future : copy) {
            _validate(future);
        }
    }

    private void _validate(ListenerFuture f) {
        try {
            f.get(f.validateTimeout, TimeUnit.MILLISECONDS);
            // future was completed, it and all the rest can be cancelled and removed from tracking
            futures.remove(f);
        }
        catch (TimeoutException | ExecutionException | InterruptedException e) {
            futures.remove(f); // removed from tracking
            f.cancel(true);
            Assertions.fail("'Validate Received' Failed " + f.getDetails(), e);
        }
    }

    public void validateForAny() {
        List<ListenerFuture> futuresToTry = new ArrayList<>(futures);
        int len = futuresToTry.size();
        int lastIx = len - 1;
        for (int ix = 0; ix < len; ix++) {
            ListenerFuture f = futuresToTry.get(ix);
            try {
                f.get(f.validateTimeout, TimeUnit.MILLISECONDS);
                // future was completed, it and all the rest can be cancelled and removed from tracking
                while (ix < len) {
                    f = futuresToTry.get(ix++);
                    futures.remove(f);
                    f.cancel(true);
                }
                return;
            }
            catch (TimeoutException | ExecutionException | InterruptedException e) {
                futures.remove(f); // removed from tracking
                f.cancel(true);
                if (ix == lastIx) {
                    Assertions.fail("'Validate Received' Failed " + f.getDetails(), e);
                }
            }
        }
    }

    public void validateNotReceived() {
        ListenerFuture f = futures.get(0);
        futures.remove(f); // removed from tracking
        try {
            f.get(f.validateTimeout, TimeUnit.MILLISECONDS);
            Assertions.fail("'Validate Not Received' Failed " + f.getDetails());
        }
        catch (TimeoutException ignore) {
            // this is what is supposed to happen!
        }
        catch (InterruptedException e) {
            f.cancel(true);
        }
        catch (ExecutionException e) {
            Assertions.fail("'Validate Not Received' Failed " + f.getDetails(), e);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Queue
    // ----------------------------------------------------------------------------------------------------
    private void queue(String label, ListenerFuture f) {
        if (verbose) {
            report("Queue For " + label, f.getDetails());
        }
        futures.add(f);
    }

    public void queueConnectionEvent(Events type) {
        queue("Event", new ListenerFuture(type, DEFAULT_VALIDATE_TIMEOUT));
    }

    public void queueConnectionEvent(Events type, int validateTimeout) {
        queue("Event", new ListenerFuture(type, validateTimeout));
    }

    public void queueException(Class<?> exceptionClass) {
        queue("Exception", new ListenerFuture(exceptionClass, DEFAULT_VALIDATE_TIMEOUT));
    }

    public void queueException(Class<?> exceptionClass, int validateTimeout) {
        queue("Exception", new ListenerFuture(exceptionClass, validateTimeout));
    }

    public void queueException(Class<?> exceptionClass, String contains) {
        queue("Exception", new ListenerFuture(exceptionClass, contains, DEFAULT_VALIDATE_TIMEOUT));
    }

    public void queueException(Class<?> exceptionClass, String contains, int validateTimeout) {
        queue("Exception", new ListenerFuture(exceptionClass, contains, validateTimeout));
    }

    public void queueError(String errorText) {
        queue("Error", new ListenerFuture(errorText, DEFAULT_VALIDATE_TIMEOUT));
    }

    public void queueError(String errorText, int validateTimeout) {
        queue("Error", new ListenerFuture(errorText, validateTimeout));
    }

    public void queueStatus(ListenerStatusType type, int statusCode) {
        queue("Status", new ListenerFuture(type, statusCode, DEFAULT_VALIDATE_TIMEOUT));
    }

    public void queueStatus(ListenerStatusType type, int statusCode, int validateTimeout) {
        queue("Status", new ListenerFuture(type, statusCode, validateTimeout));
    }

    public void queueFlowControl(String fcSubject, FlowControlSource fcSource) {
        queue("FlowControl", new ListenerFuture(fcSubject, fcSource, DEFAULT_VALIDATE_TIMEOUT));
    }

    public void queueFlowControl(String fcSubject, FlowControlSource fcSource, int validateTimeout) {
        queue("FlowControl", new ListenerFuture(fcSubject, fcSource, validateTimeout));
    }

    public void queueHeartbeat() {
        queue("Heartbeat", new ListenerFuture(true, false, DEFAULT_VALIDATE_TIMEOUT));
    }

    public void queueHeartbeat(int validateTimeout) {
        queue("Heartbeat", new ListenerFuture(true, false, validateTimeout));
    }

    public void queueSocketWriteTimeout() {
        queue("SocketWriteTimeout", new ListenerFuture(false, true, DEFAULT_VALIDATE_TIMEOUT));
    }

    public void queueSocketWriteTimeout(int validateTimeout) {
        queue("SocketWriteTimeout", new ListenerFuture(false, true, validateTimeout));
    }

    // ----------------------------------------------------------------------------------------------------
    // Getters
    // ----------------------------------------------------------------------------------------------------
    public List<Message> getDiscardedMessages() {
        return discardedMessages;
    }

    public Connection getLastConnectionEventConnection() {
        return lastConnectionEventConnection;
    }

    public int getExceptionCount() {
        return exceptionCount;
    }

    public int getHeartbeatAlarmCount() {
        return heartbeatAlarmCount;
    }

    public int getFlowControlCount() {
        return flowControlCount;
    }

    public int getPullStatusWarningsCount() {
        return pullStatusWarningsCount;
    }

    public int getSocketWriteTimeoutCount() { return socketWriteTimeoutCount; }

    // ----------------------------------------------------------------------------------------------------
    // Connection Listener
    // ----------------------------------------------------------------------------------------------------
    @Override
    public void connectionEvent(Connection conn, Events type) {
        connectionEvent(conn, type, 0L, null);
    }

    @Override
    public void connectionEvent(Connection conn, Events type, Long time, String uriDetails) {
        if (verbose) {
            report("connectionEvent", type);
        }
        lastConnectionEventConnection = conn;
        tryToComplete(futures, f -> type.equals(f.eventType));
    }

    // ----------------------------------------------------------------------------------------------------
    // Error Listener
    // ----------------------------------------------------------------------------------------------------
    @Override
    public void errorOccurred(Connection conn, String error) {
        if (verbose) {
            report("errorOccurred", error);
        }
        tryToComplete(futures, f -> error.equals(f.error));
     }

    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        exceptionCount++;
        if (printExceptions) {
            System.err.print("exceptionOccurred:");
            exp.printStackTrace();
        }
        else if (verbose) {
            report("exceptionOccurred", exp.getClass() + " --> " + exp.getMessage());
        }
        tryToComplete(futures, f -> {
            Throwable t = exp;
            while (t != null) {
                if (t.getClass().equals(f.exceptionClass)) {
                    if (f.exContains == null || exp.getMessage().contains(f.exContains)) {
                        f.complete(null);
                        f.receivedException = exp;
                        return true;
                    }
                }
                t = t.getCause();
            }
            return false;
        });
    }

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        // see SlowConsumerTests.SlowConsumerListener
    }

    @Override
    public void messageDiscarded(Connection conn, Message msg) {
        discardedMessages.add(msg);
    }

    @Override
    public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
        if (verbose) {
            report("Heartbeat Alarm", lastStreamSequence + " " + lastConsumerSequence);
        }
        heartbeatAlarmCount++;
        tryToComplete(futures, f -> f.forHeartbeat);
    }

    private void statusReceived(ListenerStatusType type, Status status) {
        if (verbose) {
            report("Status Received " + type.name(), status);
        }
        tryToComplete(futures, f -> type.equals(f.lbfStatusType) && f.statusCode == status.getCode());
    }

    @Override
    public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
        statusReceived(ListenerStatusType.Unhandled, status);
    }

    @Override
    public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
        statusReceived(ListenerStatusType.PullWarning, status);
        pullStatusWarningsCount++;
    }

    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
        statusReceived(ListenerStatusType.PullError, status);
    }

    @Override
    public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
        if (verbose) {
            report("flowControlProcessed", subject + " " + source);
        }
        flowControlCount++;
        tryToComplete(futures, f -> subject.equals(f.fcSubject) && f.fcSource == source);
    }

    @Override
    public void socketWriteTimeout(Connection conn) {
        if (verbose) {
            report("Socket Write Timeout");
        }
        socketWriteTimeoutCount++;
        tryToComplete(futures, f -> f.forSocketWriteTimeout);
    }

    // ----------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------
    private void tryToComplete(List<ListenerFuture> futures, Predicate<ListenerFuture> predicate) {
        for (ListenerFuture f : futures) {
            if (predicate.test(f)) {
                f.complete(null);
                return;
            }
        }
    }
    public static final DateTimeFormatter SIMPLE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static String simpleTime() {
        return SIMPLE_TIME_FORMATTER.format(DateTimeUtils.gmtNow());
    }

    @SuppressWarnings("SameParameterValue")
    private void report(String func) {
        System.out.println("[" + simpleTime() + " " + func + "]");
    }

    private void report(String func, Object message) {
        System.out.println("[" + simpleTime() + " " + func + "] " + message);
    }
}
