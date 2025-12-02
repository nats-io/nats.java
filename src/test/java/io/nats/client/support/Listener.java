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
    private static final int VALIDATE_TIMEOUT = 5000;

    private final boolean printExceptions;
    private final boolean verbose;

    private final List<ListenerFuture> futures;
    private int exceptionCount;

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
    }

    public void reset() {
        futures.clear();
    }

    public void validateReceived(ListenerFuture f) {
        try {
            f.get(VALIDATE_TIMEOUT, TimeUnit.MILLISECONDS);
            // future was completed, it and all the rest can be cancelled and removed from tracking
            futures.remove(f);
        }
        catch (TimeoutException | ExecutionException | InterruptedException e) {
            futures.remove(f); // removed from tracking
            f.cancel(true);
        }
    }

    public void validateAnyReceived(ListenerFuture... futuresToTry) {
        int len = futuresToTry.length;
        int lastIx = len - 1;
        for (int ix = 0; ix < len; ix++) {
            ListenerFuture f = futuresToTry[ix];
            try {
                f.get(VALIDATE_TIMEOUT, TimeUnit.MILLISECONDS);
                // future was completed, it and all the rest can be cancelled and removed from tracking
                while (ix < len) {
                    f = futuresToTry[ix++];
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

    public void validateNotReceived(ListenerFuture f) {
        futures.remove(f); // removed from tracking
        try {
            f.get(VALIDATE_TIMEOUT, TimeUnit.MILLISECONDS);
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

    private ListenerFuture prepFor(String label, ListenerFuture f) {
        if (verbose) {
            report("Future For " + label, f.getDetails());
        }
        futures.add(f);
        return f;
    }

    public ListenerFuture prepForConnectionEvent(Events type) {
        return prepFor("Event", new ListenerFuture(type));
    }

    public ListenerFuture prepForException(Class<?> exceptionClass) {
        return prepFor("Exception", new ListenerFuture(exceptionClass));
    }

    public ListenerFuture prepForError(String errorText) {
        return prepFor("Error", new ListenerFuture(errorText));
    }

    public ListenerFuture prepForStatus(ListenerStatusType type, int statusCode) {
        return prepFor("Status", new ListenerFuture(type, statusCode));
    }

    public ListenerFuture prepForFlowControl(String fcSubject, FlowControlSource fcSource) {
        return prepFor("FlowControl", new ListenerFuture(fcSubject, fcSource));
    }

    public int getExceptionCount() {
        return exceptionCount;
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
                    f.complete(null);
                    f.receivedException = exp;
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
        if (verbose) {
            report("Heartbeat Alarm", lastStreamSequence + " " + lastConsumerSequence);
        }
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
        tryToComplete(futures, f -> subject.equals(f.fcSubject) && f.fcSource == source);
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
