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
import org.junit.jupiter.api.Assertions;

import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

@SuppressWarnings({"CallToPrintStackTrace", "RedundantMethodOverride"})
public class ListenerByFutures implements ErrorListener, ConnectionListener {
    private final ReentrantLock lock = new ReentrantLock();

    private final Map<Events, CompletableFuture<Void>> eventsFutures = new HashMap<>();
    private final Map<String, CompletableFuture<Void>> textFutures = new HashMap<>();
    private CompletableFuture<Void> statusFuture;
    private StatusType preppedStatusType;
    private int preppedStatusCode;

    private final boolean printExceptions;
    private final boolean verbose;

    public ListenerByFutures() {
        this(false, false);
    }

    public ListenerByFutures(boolean verbose) {
        this(false, verbose);
    }

    public ListenerByFutures(boolean printExceptions, boolean verbose) {
        this.printExceptions = printExceptions;
        this.verbose = verbose;
    }

    public void reset() {
        for (CompletableFuture<Void> f : eventsFutures.values()) {
            f.cancel(true);
        }
        for (CompletableFuture<Void> f : textFutures.values()) {
            f.cancel(true);
        }
        eventsFutures.clear();
        textFutures.clear();
    }

    public void validate(CompletableFuture<Void> future, long waitInMillis, String failMessage) {
        try {
            future.get(waitInMillis, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException | ExecutionException | InterruptedException e) {
            Assertions.fail("Failed to get '" + failMessage + "' message.", e);
        }
    }

    public void validate(CompletableFuture<Void> future, long waitInMillis, Events type) {
        try {
            future.get(waitInMillis, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException | ExecutionException | InterruptedException e) {
            Assertions.fail("Failed to get " + type.name() + " event.", e);
        }
    }

    public void validateStatus(CompletableFuture<Void> future, long waitInMillis) {
        try {
            future.get(waitInMillis, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException | ExecutionException | InterruptedException e) {
            Assertions.fail("Failed to get correct status.", e);
        }
    }

    public enum StatusType {
        Unhandled, PullWarning, PullError
    }

    public static class StatusException extends RuntimeException {
        public final StatusType receivedType;
        public final Status receivedStatus;
        public final StatusType preppedStatusType;
        public final int preppedStatusCode;

        public StatusException(StatusType receivedType, Status receivedStatus, StatusType preppedStatusType, int preppedStatusCode) {
            this.receivedType = receivedType;
            this.receivedStatus = receivedStatus;
            this.preppedStatusType = preppedStatusType;
            this.preppedStatusCode = preppedStatusCode;
        }

        @Override
        public String toString() {
            return "StatusException{" +
                "receivedType=" + receivedType +
                ", receivedStatusCode=" + receivedStatus.getCode() +
                ", preppedStatusType=" + preppedStatusType +
                ", preppedStatusCode=" + preppedStatusCode +
                "} " + super.toString();
        }
    }

    private <T> CompletableFuture<Void> prepFor(String label, Map<T, CompletableFuture<Void>> map, T key) {
        lock.lock();
        try {
            if (verbose) {
                report(label, key.toString());
            }
            return map.computeIfAbsent(key, k -> new CompletableFuture<>());
        } finally {
            lock.unlock();
        }
    }

    public CompletableFuture<Void> prepForEvent(Events type) {
        return prepFor("prepForEvent", eventsFutures, type);
    }

    public CompletableFuture<Void> prepForError(String errorText) {
        return prepFor("prepForError", textFutures, errorText);
    }

    public CompletableFuture<Void> prepForStatus(StatusType type, int statusCode) {
        lock.lock();
        try {
            if (verbose) {
                report("prepForStatus", type + " " + statusCode);
            }
            preppedStatusType = type;
            preppedStatusCode = statusCode;
            statusFuture = new CompletableFuture<>();
            return statusFuture;
        } finally {
            lock.unlock();
        }
    }

    private <T> void complete(String label, Map<T, CompletableFuture<Void>> map, T key) {
        lock.lock();
        try {
            if (verbose) {
                report(label, key.toString());
            }
            CompletableFuture<Void> f = map.get(key);
            if (f != null) {
                f.complete(null);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void connectionEvent(Connection conn, Events type, Long time, String uriDetails) {
        complete("connectionEvent", eventsFutures, type);
    }

    @Override
    public void errorOccurred(Connection conn, String error) {
        complete("errorOccurred", textFutures, error);
    }

    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        if (printExceptions) {
            System.err.print("exceptionOccurred:");
            exp.printStackTrace();
        }
    }

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
    }

    @Override
    public void messageDiscarded(Connection conn, Message msg) {
    }

    @Override
    public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
    }

    private void statusReceived(StatusType receivedType, Status received) {
        if (statusFuture != null) {
            if (preppedStatusType.equals(receivedType) && preppedStatusCode == received.getCode()) {
                statusFuture.complete(null);
            }
            else {
                statusFuture.completeExceptionally(new StatusException(receivedType, received, preppedStatusType, preppedStatusCode));
            }
        }
    }

    @Override
    public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
        statusReceived(StatusType.Unhandled, status);
    }

    @Override
    public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
        statusReceived(StatusType.PullWarning, status);
    }

    @Override
    public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
        statusReceived(StatusType.PullError, status);
    }

    @Override
    public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
    }

    @Override
    public void socketWriteTimeout(Connection conn) {
    }

    public static final DateTimeFormatter SIMPLE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static String simpleTime() {
        return SIMPLE_TIME_FORMATTER.format(DateTimeUtils.gmtNow());
    }

    private void report(String func, Object message) {
        System.out.println("[" + simpleTime() + " " + func + "] " + message);
    }

    @Override
    public void connectionEvent(Connection conn, Events type) {
        // deprecated
    }
}
