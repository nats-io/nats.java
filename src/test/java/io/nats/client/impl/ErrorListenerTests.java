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
import io.nats.client.support.Listener;
import io.nats.client.support.Status;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.nats.client.support.Listener.LONG_VALIDATE_TIMEOUT;
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static org.junit.jupiter.api.Assertions.*;

public class ErrorListenerTests extends TestBase {

    @Test
    public void testLastError_ClearError_AuthViolation() throws Exception {
        NatsConnection nc;
        Listener listener = new Listener();
        String[] customArgs = {"--user", "stephen", "--pass", "password"};

        try (NatsTestServer ts = new NatsTestServer();
             NatsTestServer ts2 = new NatsTestServer(customArgs); //ts2 requires auth
             NatsTestServer ts3 = new NatsTestServer()) {
            Options options = optionsBuilder(ts.getServerUri(), ts2.getServerUri(), ts3.getServerUri())
                .noRandomize()
                .connectionListener(listener)
                .errorListener(listener)
                .maxReconnects(-1)
                .build();
            nc = (NatsConnection) Nats.connect(options);
            assertConnected(nc);
            assertEquals(ts.getServerUri(), nc.getConnectedUrl());
            listener.queueConnectionEvent(Events.DISCONNECTED);
            listener.queueConnectionEvent(Events.RECONNECTED);
            listener.queueError("Authorization Violation");

            ts.close();

            try {
                nc.flush(Duration.ofSeconds(1));
            }
            catch (Exception exp) {
                // this usually fails
            }

            listener.validateAll();

            confirmConnected(nc); // wait for reconnect
            assertEquals(ts3.getServerUri(), nc.getConnectedUrl());

            nc.clearLastError();
            assertNull(nc.getLastError());
        }
    }

    @Test
    public void testExceptionOnBadDispatcher() throws Exception {
        Listener listener = new Listener();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                .maxReconnects(0)
                .errorListener(listener)
                .build();
            try (Connection nc = Nats.connect(options)) {
                Dispatcher d = nc.createDispatcher(msg -> {
                    throw new ArithmeticException();
                });
                String subject = random();
                d.subscribe(subject);
                Future<Message> incoming = nc.request(subject, null);

                try {
                    incoming.get(200, TimeUnit.MILLISECONDS);
                    fail();
                }
                catch (TimeoutException te) {
                    // expected
                }
                assertEquals(1, listener.getExceptionCount());
            }
        }
    }

    @Test
    public void testExceptionInErrorHandler() throws Exception {
        String[] customArgs = {"--user", "stephen", "--pass", "password"};
        BadHandler listener = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer(customArgs)) {
            // See config file for user/pass
            // don't put u/p in options
            Options options = optionsBuilder(ts)
                .maxReconnects(0)
                .errorListener(listener)
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testExceptionInSlowConsumerHandler() throws Exception {
        BadHandler listener = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer();
             Connection nc = Nats.connect(optionsBuilder(ts).errorListener(listener).build())) {

            String subject = random();
            Subscription sub = nc.subscribe(subject);
            sub.setPendingLimits(1, -1);

            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.publish(subject, null);
            nc.publish(subject, null);

            nc.flush(Duration.ofMillis(5000));

            assertEquals(3, sub.getDroppedCount());

            nc.close(); // should force the exception listener through

            assertTrue(nc.getStatistics().getExceptions() > 0);
        }
    }

    @Test
    public void testExceptionInExceptionHandler() throws Exception {
        BadHandler listener = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts).maxReconnects(0).errorListener(listener).build();
            Connection nc = Nats.connect(options);
            try {
                Dispatcher d = nc.createDispatcher(msg -> {
                    throw new ArithmeticException();
                });
                String subject = random();
                d.subscribe(subject);
                Future<Message> incoming = nc.request(subject, null);

                Message msg;

                try {
                    msg = incoming.get(200, TimeUnit.MILLISECONDS);
                } catch (TimeoutException te) {
                    msg = null;
                }

                assertNull(msg);
                assertEquals(2, nc.getStatistics().getExceptions()); // 1 for the dispatcher, 1 for the handlers
            } finally {
                closeAndConfirm(nc);
            }
        }
    }

    @Test
    public void testDiscardedMessageFastProducer() throws Exception {
        String subject = random();
        int maxMessages = 10;
        Listener listener = new Listener();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                .maxMessagesInOutgoingQueue(maxMessages)
                .discardMessagesWhenOutgoingQueueFull()
                .errorListener(listener)
                .pingInterval(Duration.ofSeconds(100)) // make this long so we don't ping during test
                .build();
            NatsConnection nc = (NatsConnection) Nats.connect(options);

            try {
                nc.flush(Duration.ofSeconds(2));
                nc.getWriter().stop().get(2, TimeUnit.SECONDS);
                for (int i = 0; i < maxMessages + 1; i++) {
                    nc.publish(subject + i, ("message" + i).getBytes());
                }
                nc.getWriter().start(nc.getDataPortFuture());

                nc.flush(Duration.ofSeconds(2));
            } finally {
                closeAndConfirm(nc);
            }
        }

        List<Message> discardedMessages = listener.getDiscardedMessages();
        assertFalse(discardedMessages.isEmpty(), "expected discardedMessages > 0, got " + discardedMessages.size());
        int offset = maxMessages + 1 - discardedMessages.size();
        assertEquals(subject + offset, discardedMessages.get(0).getSubject());
        assertEquals("message" + offset, new String(discardedMessages.get(0).getData()));
    }

    @Test
    public void testDiscardedMessageServerClosed() throws Exception {
        String subject = random();
        int maxMessages = 10;
        Listener listener = new Listener();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = optionsBuilder(ts)
                .maxMessagesInOutgoingQueue(maxMessages)
                .discardMessagesWhenOutgoingQueueFull()
                .connectionListener(listener)
                .errorListener(listener)
                .pingInterval(Duration.ofSeconds(100)) // make this long so we don't ping during test
                .build();
            listener.queueConnectionEvent(Events.CONNECTED, LONG_VALIDATE_TIMEOUT);
            listener.queueConnectionEvent(Events.DISCONNECTED, LONG_VALIDATE_TIMEOUT);
            try (Connection nc = managedConnect(options)) {
                nc.flush(Duration.ofSeconds(1));
                listener.validate();
                ts.close();
                listener.validate();

                for (int i = 0; i < maxMessages + 1; i++) {
                    nc.publish(subject + i, ("message" + i).getBytes());
                }
            }
        }

        List<Message> discardedMessages = listener.getDiscardedMessages();
        assertFalse(discardedMessages.isEmpty(), "At least one message discarded");
        assertTrue(discardedMessages.get(0).getSubject().startsWith(subject), "Message subject");
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

        _cover(new ReaderListenerConsoleImpl());
        _cover(new ReadListener() {
            @Override
            public void protocol(String op, String text) {
            }

            @Override
            public void message(String op, Message message) {
            }
        });
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
        el.socketWriteTimeout(null);
    }

    private void _cover(ReadListener rl) {
        rl.protocol("OP", null);
        rl.message("OP", new NatsMessage("subject", "replyTo", null));
        rl.message("OP", new NatsMessage("subject", "replyTo", "body".getBytes()));
        rl.message("OP", new NatsJetStreamMessage(null));
    }
}
