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
import io.nats.client.ConnectionListener.Events;
import io.nats.client.support.Status;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public class ErrorListenerTests {

    @Test
    public void testLastError() throws Exception {
        NatsConnection nc = null;
        ListenerForTesting listener = new ListenerForTesting();
        String[] customArgs = {"--user", "stephen", "--pass", "password"};

        try (NatsTestServer ts = new NatsTestServer();
             NatsTestServer ts2 = new NatsTestServer(customArgs, false); //ts2 requires auth
             NatsTestServer ts3 = new NatsTestServer()) {
            Options options = new Options.Builder()
                .server(ts.getURI())
                .server(ts2.getURI())
                .server(ts3.getURI())
                .noRandomize()
                .connectionListener(listener)
                .errorListener(listener)
                .maxReconnects(-1)
                .build();
            nc = (NatsConnection) Nats.connect(options);
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            assertEquals(ts.getURI(), nc.getConnectedUrl());
            listener.prepForStatusChange(Events.DISCONNECTED);

            ts.close();

            try {
                nc.flush(Duration.ofSeconds(1));
            }
            catch (Exception exp) {
                // this usually fails
            }

            listener.waitForStatusChange(5, TimeUnit.SECONDS);

            listener.prepForStatusChange(Events.RECONNECTED);
            listener.waitForStatusChange(5, TimeUnit.SECONDS);

            assertTrue(listener.errorsEventually("Authorization Violation", 2000));

            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            assertEquals(ts3.getURI(), nc.getConnectedUrl());
        } finally {
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testClearLastError() throws Exception {
        NatsConnection nc = null;
        ListenerForTesting listener = new ListenerForTesting();
        String[] customArgs = {"--user", "stephen", "--pass", "password"};

        try (NatsTestServer ts = new NatsTestServer();
             NatsTestServer ts2 = new NatsTestServer(customArgs, false); //ts2 requires auth
             NatsTestServer ts3 = new NatsTestServer()) {
            Options options = new Options.Builder()
                .server(ts.getURI())
                .server(ts2.getURI())
                .server(ts3.getURI())
                .noRandomize()
                .connectionListener(listener)
                .errorListener(listener)
                .maxReconnects(-1)
                .build();
            nc = (NatsConnection) Nats.connect(options);
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            assertEquals(ts.getURI(), nc.getConnectedUrl());
            listener.prepForStatusChange(Events.DISCONNECTED);

            ts.close();

            try {
                nc.flush(Duration.ofSeconds(1));
            }
            catch (Exception exp) {
                // this usually fails
            }

            listener.waitForStatusChange(5, TimeUnit.SECONDS);

            listener.prepForStatusChange(Events.RECONNECTED);
            listener.waitForStatusChange(5, TimeUnit.SECONDS);

            assertTrue(listener.errorsEventually("Authorization Violation", 2000));

            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
            assertEquals(ts3.getURI(), nc.getConnectedUrl());

            nc.clearLastError();
            assertNull(nc.getLastError());
        }
        finally {
            standardCloseConnection(nc);
        }
    }

    @Test
    public void testErrorOnNoAuth() throws Exception {
        String[] customArgs = {"--user", "stephen", "--pass", "password"};
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            sleep(1000); // give the server time to get ready, otherwise sometimes this test flaps
            // See config file for user/pass
            // no or wrong u/p in the options is an error
            Options options = new Options.Builder().
                    server(ts.getURI())
                    .maxReconnects(0)
                    .errorListener(listener)
                    .build();
            try {
                Nats.connect(options);
                fail();
            }
            catch (Exception ignore) {}
            assertTrue(listener.errorsEventually("Authorization Violation", 10000));
        }
    }

    @Test
    public void testExceptionOnBadDispatcher() throws Exception {
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    maxReconnects(0).
                    errorListener(listener).
                    build();
            Connection nc = Nats.connect(options);
            try {
                Dispatcher d = nc.createDispatcher((msg) -> {
                    throw new ArithmeticException();
                });
                d.subscribe("subject");
                Future<Message> incoming = nc.request("subject", null);

                Message msg;

                try {
                    msg = incoming.get(200, TimeUnit.MILLISECONDS);
                } catch (TimeoutException te) {
                    msg = null;
                }

                assertNull(msg);
                assertEquals(1, listener.getCount());
            } finally {
                standardCloseConnection(nc);
            }
        }
    }

    @Test
    public void testExceptionInErrorHandler() throws Exception {
        String[] customArgs = {"--user", "stephen", "--pass", "password"};
        BadHandler listener = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder()
                    .server(ts.getURI())
                    .maxReconnects(0)
                    .errorListener(listener)
                    // skip this so we get an error userInfo("stephen", "password").
                    .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testExceptionInSlowConsumerHandler() throws Exception {
        BadHandler listener = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer(false);
             NatsConnection nc = (NatsConnection) Nats.connect(new Options.Builder().
                     server(ts.getURI()).
                     errorListener(listener).
                     build())) {

            Subscription sub = nc.subscribe("subject");
            sub.setPendingLimits(1, -1);

            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.publish("subject", null);
            nc.publish("subject", null);

            nc.flush(Duration.ofMillis(5000));

            assertEquals(3, sub.getDroppedCount());

            nc.close(); // should force the exception listener through

            assertTrue(nc.getStatisticsCollector().getExceptions() > 0);
        }
    }

    @Test
    public void testExceptionInExceptionHandler() throws Exception {
        BadHandler listener = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    maxReconnects(0).
                    errorListener(listener).
                    build();
            Connection nc = Nats.connect(options);
            try {
                Dispatcher d = nc.createDispatcher((msg) -> {
                    throw new ArithmeticException();
                });
                d.subscribe("subject");
                Future<Message> incoming = nc.request("subject", null);

                Message msg;

                try {
                    msg = incoming.get(200, TimeUnit.MILLISECONDS);
                } catch (TimeoutException te) {
                    msg = null;
                }

                assertNull(msg);
                assertEquals(((NatsConnection) nc).getStatisticsCollector().getExceptions(), 2); // 1 for the dispatcher, 1 for the handlers
            } finally {
                standardCloseConnection(nc);
            }
        }
    }

    @Test
    public void testDiscardedMessageFastProducer() throws Exception {
        int maxMessages = 10;
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    maxMessagesInOutgoingQueue(maxMessages).
                    discardMessagesWhenOutgoingQueueFull().
                    errorListener(listener).
                    pingInterval(Duration.ofSeconds(100)). // make this long so we don't ping during test
                    build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            try {
                nc.flush(Duration.ofSeconds(2));

                nc.getWriter().stop().get(2, TimeUnit.SECONDS);
                for (int i = 0; i < maxMessages + 1; i++) {
                    nc.publish("subject" + i, ("message" + i).getBytes());
                }
                nc.getWriter().start(nc.getDataPortFuture());

                nc.flush(Duration.ofSeconds(2));
            } finally {
                standardCloseConnection(nc);
            }
        }

        List<Message> discardedMessages = listener.getDiscardedMessages();
        assertFalse(discardedMessages.isEmpty(), "expected discardedMessages > 0, got " + discardedMessages.size());
        int offset = maxMessages + 1 - discardedMessages.size();
        assertEquals("subject" + offset, discardedMessages.get(0).getSubject());
        assertEquals("message" + offset, new String(discardedMessages.get(0).getData()));
    }

    @Test
    public void testDiscardedMessageServerClosed() throws Exception {
        int maxMessages = 10;
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().
                    server(ts.getURI()).
                    maxMessagesInOutgoingQueue(maxMessages).
                    discardMessagesWhenOutgoingQueueFull().
                    pingInterval(Duration.ofSeconds(100)). // make this long so we don't ping during test
                    connectionListener(listener).
                    errorListener(listener).
                    build();
            Connection nc = standardConnection(options);

            try {
                nc.flush(Duration.ofSeconds(1)); // Get the sub to the server

                listener.prepForStatusChange(Events.DISCONNECTED);
                ts.close();
                listener.waitForStatusChange(2, TimeUnit.SECONDS); // make sure the connection is down

                for (int i = 0; i < maxMessages + 1; i++) {
                    nc.publish("subject" + i, ("message" + i).getBytes());
                }
            } finally {
                standardCloseConnection(nc);
            }
        }

        List<Message> discardedMessages = listener.getDiscardedMessages();
        assertFalse(discardedMessages.isEmpty(), "At least one message discarded");
        assertTrue(discardedMessages.get(0).getSubject().startsWith("subject"), "Message subject");
        assertTrue(new String(discardedMessages.get(0).getData()).startsWith("message"), "Message data");
    }

    @Test
    public void testCoverage() {
        // this exercises default interface implementation
        _cover(new ErrorListener() {});

        // exercises the default implementation
        _cover(new ErrorListenerLoggerImpl());

        // exercises the console implementation
        _cover(new ErrorListenerConsoleImpl());

        // exercises a little more than the defaults
        AtomicBoolean errorOccurredFlag = new AtomicBoolean();
        AtomicBoolean exceptionOccurredFlag = new AtomicBoolean();
        AtomicBoolean slowConsumerDetectedFlag = new AtomicBoolean();
        AtomicBoolean messageDiscardedFlag = new AtomicBoolean();
        AtomicBoolean heartbeatAlarmFlag = new AtomicBoolean();
        AtomicBoolean unhandledStatusFlag = new AtomicBoolean();
        AtomicBoolean pullStatusWarningFlag = new AtomicBoolean();
        AtomicBoolean pullStatusErrorFlag = new AtomicBoolean();
        AtomicBoolean flowControlProcessedFlag = new AtomicBoolean();

        _cover(new ErrorListener() {
            @Override
            public void errorOccurred(Connection conn, String error) {
                errorOccurredFlag.set(true);
            }

            @Override
            public void exceptionOccurred(Connection conn, Exception exp) {
                exceptionOccurredFlag.set(true);
            }

            @Override
            public void slowConsumerDetected(Connection conn, Consumer consumer) {
                slowConsumerDetectedFlag.set(true);
            }

            @Override
            public void messageDiscarded(Connection conn, Message msg) {
                messageDiscardedFlag.set(true);
            }

            @Override
            public void heartbeatAlarm(Connection conn, JetStreamSubscription sub, long lastStreamSequence, long lastConsumerSequence) {
                heartbeatAlarmFlag.set(true);
            }

            @Override
            public void unhandledStatus(Connection conn, JetStreamSubscription sub, Status status) {
                unhandledStatusFlag.set(true);
            }

            @Override
            public void pullStatusWarning(Connection conn, JetStreamSubscription sub, Status status) {
                pullStatusWarningFlag.set(true);
            }

            @Override
            public void pullStatusError(Connection conn, JetStreamSubscription sub, Status status) {
                pullStatusErrorFlag.set(true);
            }

            @Override
            public void flowControlProcessed(Connection conn, JetStreamSubscription sub, String subject, FlowControlSource source) {
                flowControlProcessedFlag.set(true);
            }
        });

        assertTrue(errorOccurredFlag.get());
        assertTrue(exceptionOccurredFlag.get());
        assertTrue(slowConsumerDetectedFlag.get());
        assertTrue(messageDiscardedFlag.get());
        assertTrue(heartbeatAlarmFlag.get());
        assertTrue(unhandledStatusFlag.get());
        assertTrue(pullStatusWarningFlag.get());
        assertTrue(pullStatusErrorFlag.get());
        assertTrue(flowControlProcessedFlag.get());
    }

    private void _cover(ErrorListener el) {
        el.errorOccurred(null, null);
        el.exceptionOccurred(null, null);
        el.slowConsumerDetected(null, null);
        el.messageDiscarded(null, null);
        el.heartbeatAlarm(null, null, -1, -1);
        el.unhandledStatus(null, null, null);
        el.pullStatusWarning(null, null, null);
        el.pullStatusError(null, null, null);
        el.flowControlProcessed(null, null, null, null);
    }
}
