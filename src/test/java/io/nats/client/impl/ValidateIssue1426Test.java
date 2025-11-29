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

package io.nats.client.impl;

import io.nats.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/* Test to validate #1426 */
public class ValidateIssue1426Test {

    private static final int NUMBER_OF_SUBS = 1000;
    private static final int NUM_DISPATCHERS = 5;

    @Test
    public void test1426() throws Exception {
        int port = NatsTestServer.nextPort();
        try (ServerContext ctx = new ServerContext(port)) {
            ctx.startServer();

            AtomicBoolean violated = new AtomicBoolean(false);
            ErrorListener errorListener = new ErrorListener() {
                @Override
                public void errorOccurred(Connection conn, String error) {
                    if (error.contains("Authorization Violation")) {
                        violated.set(true);
                    }
                }
            };

            Options options = optionsBuilder(port)
                .token(new char[]{'1', '2', '3', '4'})
                .maxMessagesInOutgoingQueue(NUMBER_OF_SUBS )
                .reconnectBufferSize(NUMBER_OF_SUBS * 100)
                .connectionTimeout(Duration.ofSeconds(1))
                .reconnectWait(Duration.ofSeconds(2))
                .errorListener(errorListener)
                .build();

            try (Connection nc = Nats.connect(options)) {

                NatsDispatcher[] dispatchers = new NatsDispatcher[NUM_DISPATCHERS];
                for (int i = 0; i < NUM_DISPATCHERS; i++) {
                    dispatchers[i] = (NatsDispatcher) nc.createDispatcher();
                }

                int dispatcherIndex = 0;
                CountDownLatch processedMessages = new CountDownLatch(NUMBER_OF_SUBS);
                for (int i = 0; i < NUMBER_OF_SUBS; i++) {
                    String subject = makeSubject(i);
                    if (dispatcherIndex == NUM_DISPATCHERS) {
                        dispatcherIndex = 0;
                    }
                    dispatchers[dispatcherIndex++].subscribe(subject, m -> processedMessages.countDown());
                }

                // wait for the server to restart so we can go to the next step
                ctx.restartLatch.await();

                // wait until we are connected after the server restart
                while (nc.getStatus() != Connection.Status.CONNECTED) {
                    //noinspection BusyWait
                    Thread.sleep(50);
                }
                assertFalse(violated.get());

                // publish a message to every sub
                for (int i = 0; i < NUMBER_OF_SUBS; i++) {
                    String subject = makeSubject(i);
                    nc.publish(subject, null);
                }

                assertTrue(processedMessages.await(60, TimeUnit.SECONDS));
            }
        }
    }

    private static String makeSubject(int i) {
        return "test_" + i;
    }

    private static class ServerContext implements AutoCloseable {
        public final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        public final AtomicReference<NatsTestServer> server = new AtomicReference<>();
        public final CountDownLatch restartLatch = new CountDownLatch(1);
        final int port;

        public ServerContext(int port) {
            this.port = port;
            scheduler.schedule(this::restartServer, 10, TimeUnit.SECONDS);
        }

        public void startServer() throws IOException {
            server.set(new NatsTestServer(new String[]{"--auth", "1234"}, port));
        }

        private void restartServer() {
            try {
                server.get().shutdown();
                Thread.sleep(250);
                startServer();
                restartLatch.countDown();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() throws Exception {
            scheduler.shutdown();
            server.get().shutdown();
        }
    }
}
