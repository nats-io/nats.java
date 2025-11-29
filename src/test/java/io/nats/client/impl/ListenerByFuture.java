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
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings({"CallToPrintStackTrace", "RedundantMethodOverride"})
public class ListenerByFuture implements ErrorListener, ConnectionListener {
    private CompletableFuture<Void> eventFuture;
    private Events eventFutureType;

    private CompletableFuture<Void> errorFuture;
    private String errorFutureText;

    private CompletableFuture<Void> statusFuture;
    private StatusType preppedStatusType;
    private int preppedStatusCode;

    private final boolean printExceptions;
    private final boolean verbose;

    private CompletableFuture<Void> fcFuture;
    private String preppedFcSubject;
    private FlowControlSource preppedFcSource;

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
    }

    public void reset() {
        if (eventFuture != null) {
            eventFuture.cancel(true);
            eventFuture = null;
            eventFutureType = null;
        }

        if (errorFuture != null) {
            errorFuture.cancel(true);
            errorFuture = null;
            errorFutureText = null;
        }

        if (statusFuture != null) {
            statusFuture.cancel(true);
            statusFuture = null;
            preppedStatusType = null;
            preppedStatusCode = -1;
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Validate
    // ----------------------------------------------------------------------------------------------------
    public void validate(CompletableFuture<Void> future, long waitInMillis, Object... details) {
        try {
            future.get(waitInMillis, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException | ExecutionException | InterruptedException e) {
            Assertions.fail("Listener Validate Failed " + Arrays.asList(details), e);
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Prep
    // ----------------------------------------------------------------------------------------------------
    public CompletableFuture<Void> prepForEvent(Events type) {
        if (verbose) {
            report("prepForEvent", type);
        }
        if (eventFuture != null) {
            eventFuture.cancel(true);
        }
        eventFuture = new CompletableFuture<>();
        eventFutureType = type;
        return eventFuture;
    }

    public CompletableFuture<Void> prepForError(String errorText) {
        if (verbose) {
            report("prepForEvent", errorText);
        }
        if (errorFuture != null) {
            errorFuture.cancel(true);
        }
        errorFuture = new CompletableFuture<>();
        errorFutureText = errorText;
        return errorFuture;
    }

    public CompletableFuture<Void> prepForStatus(StatusType type, int statusCode) {
        if (verbose) {
            report("prepForStatus", type + " " + statusCode);
        }
        if (statusFuture != null) {
            statusFuture.cancel(true);
        }
        statusFuture = new CompletableFuture<>();
        preppedStatusType = type;
        preppedStatusCode = statusCode;
        return statusFuture;
    }

    public CompletableFuture<Void> prepForFlowControl(String fcSubject, FlowControlSource fcSource) {
        if (verbose) {
            report("prepForFlowControl", fcSubject + " " + fcSource);
        }
        if (fcFuture != null) {
            fcFuture.cancel(true);
        }
        fcFuture = new CompletableFuture<>();
        preppedFcSubject = fcSubject;
        preppedFcSource = fcSource;
        return fcFuture;
    }

    // ----------------------------------------------------------------------------------------------------
    // Connection Listener
    // ----------------------------------------------------------------------------------------------------
    @Override
    public void connectionEvent(Connection conn, Events type) {
        // deprecated
    }

    @Override
    public void connectionEvent(Connection conn, Events type, Long time, String uriDetails) {
        if (verbose) {
            report("connectionEvent", type);
        }
        if (eventFuture != null) {
            if (eventFutureType.equals(type)) {
                eventFuture.complete(null);
            }
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // Error Listener
    // ----------------------------------------------------------------------------------------------------
    @Override
    public void errorOccurred(Connection conn, String error) {
        if (verbose) {
            report("errorOccurred", error);
        }
        if (errorFuture != null) {
            if (errorFutureText.equals(error)) {
                errorFuture.complete(null);
            }
        }
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
        if (verbose) {
            report("statusReceived", received);
        }
        if (statusFuture != null) {
            if (preppedStatusType.equals(receivedType) && preppedStatusCode == received.getCode()) {
                statusFuture.complete(null);
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
        if (verbose) {
            report("flowControlProcessed", subject + " " + source);
        }
        if (fcFuture != null) {
            if (preppedFcSubject.equals(subject) && preppedFcSource == source) {
                fcFuture.complete(null);
            }
        }
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
