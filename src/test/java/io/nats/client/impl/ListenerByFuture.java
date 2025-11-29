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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

@SuppressWarnings({"CallToPrintStackTrace", "RedundantMethodOverride"})
public class ListenerByFuture implements ErrorListener, ConnectionListener {
    private static final int VALIDATE_TIMEOUT = 5000;

    private final boolean printExceptions;
    private final boolean verbose;

    private List<ListenerFuture> eventFutures;
    private List<ListenerFuture> errorFutures;
    private List<ListenerFuture> exceptionFutures;
    private List<ListenerFuture> statusFutures;
    private List<ListenerFuture> fcFutures;

    public enum StatusType {
        Unhandled, PullWarning, PullError
    }
    
    public ListenerByFuture() {
        this(false, false);
    }

    public ListenerByFuture(boolean verbose) {
        this(false, verbose);
    }

    public ListenerByFuture(boolean printExceptions, boolean verbose) {
        this.printExceptions = printExceptions;
        this.verbose = verbose;
        eventFutures = new ArrayList<>();
        errorFutures = new ArrayList<>();
        exceptionFutures = new ArrayList<>();
        statusFutures = new ArrayList<>();
        fcFutures = new ArrayList<>();
    }

    public void reset() {
        eventFutures.clear();
        errorFutures.clear();
        exceptionFutures.clear();
        statusFutures.clear();
        fcFutures.clear();
    }

    public ListenerFuture prepForEvent(Events type) {
        if (verbose) {
            report("Future For Event", type);
        }
        ListenerFuture f = new ListenerFuture(type);
        eventFutures.add(f);
        return f;
    }

    public ListenerFuture prepForException(Class<?> exceptionClass) {
        if (verbose) {
            report("Future For Exception", exceptionClass);
        }
        ListenerFuture f = new ListenerFuture(exceptionClass);
        exceptionFutures.add(f);
        return f;
    }

    public ListenerFuture prepForError(String errorText) {
        if (verbose) {
            report("Future For Event", errorText);
        }
        ListenerFuture f = new ListenerFuture(errorText);
        errorFutures.add(f);
        return f;
    }

    public ListenerFuture prepForStatus(StatusType type, int statusCode) {
        if (verbose) {
            report("Future For Status", type + " " + statusCode);
        }
        ListenerFuture f = new ListenerFuture(type, statusCode);
        statusFutures.add(f);
        return f;
    }

    public ListenerFuture prepForFlowControl(String fcSubject, FlowControlSource fcSource) {
        if (verbose) {
            report("Future For FlowControl", fcSubject + " " + fcSource);
        }
        ListenerFuture f = new ListenerFuture(fcSubject, fcSource);
        fcFutures.add(f);
        return f;
    }

    // ----------------------------------------------------------------------------------------------------
    // Prep
    // ----------------------------------------------------------------------------------------------------
    public static class ListenerFuture extends CompletableFuture<Void> {
        private Events eventType;
        private String error;
        private Class<?> exceptionClass;
        private StatusType statusType;
        private int statusCode = -1;
        private String fcSubject;
        private FlowControlSource fcSource;

        public ListenerFuture(Events type) {
            this.eventType = type;
        }

        public ListenerFuture(Class<?> exceptionClass) {
            this.exceptionClass = exceptionClass;
        }

        public ListenerFuture(String errorText) {
            error = errorText;
        }

        public ListenerFuture(StatusType type, int statusCode) {
            statusType = type;
            this.statusCode = statusCode;
        }

        public ListenerFuture(String fcSubject, FlowControlSource fcSource) {
            this.fcSubject = fcSubject;
            this.fcSource = fcSource;
        }

        public void validate() {
            try {
                get(VALIDATE_TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException | ExecutionException | InterruptedException e) {
                Assertions.fail("Validate Failed " + getDetails(), e);
            }
        }

        public void validate(ListenerFuture second) {
            try {
                get(VALIDATE_TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (TimeoutException | ExecutionException | InterruptedException e) {
                second.validate();
            }
        }

        public List<String> getDetails() {
            List<String> details = new ArrayList<>();
            if (eventType != null) { details.add(eventType.toString()); }
            if (error != null) { details.add(error); }
            if (exceptionClass != null) { details.add(exceptionClass.toString()); }
            if (statusType != null) { details.add(statusType.toString()); }
            if (statusCode != -1) { details.add(Integer.toString(statusCode)); }
            if (fcSubject != null) { details.add(fcSubject); }
            if (fcSource != null) { details.add(fcSource.toString()); }
            return details;
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Connection Listener
    // ----------------------------------------------------------------------------------------------------
    @Override
    public void connectionEvent(Connection conn, Events type) {
        // deprecated
    }

    private void tryToComplete(List<ListenerFuture> futures, Predicate<ListenerFuture> predicate) {
        for (int i = 0; i < futures.size(); i++) {
            ListenerFuture f = futures.get(i);
            if (predicate.test(f)) {
                f.complete(null);
                futures.remove(i);
                return;
            }
        }
    }

    @Override
    public void connectionEvent(Connection conn, Events type, Long time, String uriDetails) {
        if (verbose) {
            report("connectionEvent", type);
        }
        tryToComplete(eventFutures, f -> f.eventType.equals(type));
    }

    // ----------------------------------------------------------------------------------------------------
    // Error Listener
    // ----------------------------------------------------------------------------------------------------
    @Override
    public void errorOccurred(Connection conn, String error) {
        if (verbose) {
            report("errorOccurred", error);
        }
        tryToComplete(errorFutures, f -> f.error.equals(error));
     }

    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        if (printExceptions) {
            System.err.print("exceptionOccurred:");
            exp.printStackTrace();
        }
        else if (verbose) {
            report("exceptionOccurred", exp.getClass() + " --> " + exp.getMessage());
        }
        tryToComplete(exceptionFutures, f -> {
            Throwable t = exp;
            while (t != null) {
                if (f.exceptionClass.equals(t.getClass())) {
                    f.complete(null);
                    return true;
                }
                t = t.getCause();
            }
            return false;
        });
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

    private void statusReceived(StatusType type, Status status) {
        if (verbose) {
            report("statusReceived", status);
        }
        tryToComplete(statusFutures, f -> f.statusType.equals(type) && f.statusCode == status.getCode());
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
        if (verbose) {
            report("flowControlProcessed", subject + " " + source);
        }
        tryToComplete(fcFutures, f -> f.fcSubject.equals(subject) && f.fcSource == source);
    }

    @Override
    public void socketWriteTimeout(Connection conn) {
    }

    // ----------------------------------------------------------------------------------------------------
    // Helpers
    // ----------------------------------------------------------------------------------------------------
    public static final DateTimeFormatter SIMPLE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    public static String simpleTime() {
        return SIMPLE_TIME_FORMATTER.format(DateTimeUtils.gmtNow());
    }

    private void report(String func, Object message) {
        System.out.println("[" + simpleTime() + " " + func + "] " + message);
    }
}
