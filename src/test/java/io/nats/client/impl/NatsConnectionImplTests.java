// Copyright 20125 The NATS Authors
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

import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public class NatsConnectionImplTests {

    @Test
    public void testConnectionClosedProperly() throws Exception {
        try (NatsTestServer server = new NatsTestServer(true)) {
            Options options = standardOptions(server.getNatsLocalhostUri());
            verifyInternalExecutors(options);

            // using options copied from options to demonstrate the executors
            // came from the internal factory and were not reused
            options = new Options.Builder(options).build();
            verifyInternalExecutors(options);

            ExecutorService es = Executors.newFixedThreadPool(3);
            ScheduledExecutorService ses = Executors.newScheduledThreadPool(3);
            ExecutorService callbackEs = Executors.newSingleThreadExecutor();
            ExecutorService connectEs = Executors.newSingleThreadExecutor();
            assertFalse(es.isShutdown());
            assertFalse(ses.isShutdown());

            options = standardOptionsBuilder(server.getNatsLocalhostUri())
                .executor(es)
                .scheduledExecutor(ses)
                .callbackExecutor(callbackEs)
                .connectExecutor(connectEs)
                .build();
            verifyExternalExecutors(options, es, ses, callbackEs, connectEs);

            // also shows the executors where not shutdown
            verifyExternalExecutors(options, es, ses, callbackEs, connectEs);

            ThreadFactory callbackThreadFactory = r -> new Thread(r, "callback");
            ThreadFactory connectThreadFactory = r -> new Thread(r, "connect");
            options = standardOptionsBuilder(server.getNatsLocalhostUri())
                .executor(es)
                .scheduledExecutor(ses)
                .callbackThreadFactory(callbackThreadFactory)
                .connectThreadFactory(connectThreadFactory)
                .build();
            verifyExternalExecutors(options, es, ses, null, null);

            es.shutdownNow();
            ses.shutdownNow();
            callbackEs.shutdownNow();
            connectEs.shutdownNow();
            assertTrue(es.isShutdown());
            assertTrue(ses.isShutdown());
            assertTrue(callbackEs.isShutdown());
            assertTrue(connectEs.isShutdown());
        }
    }

    private static void verifyInternalExecutors(Options options) throws InterruptedException, IOException {
        try (NatsConnection nc = (NatsConnection)standardConnection(options)) {
            ExecutorService es = options.getExecutor();
            ScheduledExecutorService ses = options.getScheduledExecutor();
            ExecutorService callbackEs = options.getCallbackExecutor();
            ExecutorService connectEs = options.getConnectExecutor();

            assertTrue(options.executorIsInternal());
            assertTrue(options.scheduledExecutorIsInternal());
            assertTrue(options.callbackExecutorIsInternal());
            assertTrue(options.connectExecutorIsInternal());

            assertFalse(nc.executorIsClosed());
            assertFalse(nc.scheduledExecutorIsClosed());
            assertFalse(nc.callbackExecutorIsClosed());
            assertFalse(nc.connectExecutorIsClosed());

            assertFalse(es.isShutdown());
            assertFalse(ses.isShutdown());
            assertFalse(callbackEs.isShutdown());
            assertFalse(connectEs.isShutdown());

            nc.subscribe("*");
            Thread.sleep(1000);
            nc.close();

            assertTrue(nc.callbackExecutorIsClosed());
            assertTrue(nc.connectExecutorIsClosed());
            assertTrue(nc.executorIsClosed());
            assertTrue(nc.scheduledExecutorIsClosed());

            assertTrue(es.isShutdown());
            assertTrue(ses.isShutdown());
            assertTrue(callbackEs.isShutdown());
            assertTrue(connectEs.isShutdown());
        }
    }

    private static void verifyExternalExecutors(Options options,
                                                ExecutorService userEs, ScheduledExecutorService userSes,
                                                ExecutorService userCallbackEs, ExecutorService userConnectEs
    ) throws InterruptedException, IOException {
        try (NatsConnection nc = (NatsConnection)standardConnection(options)) {
            ExecutorService es = options.getExecutor();
            ScheduledExecutorService ses = options.getScheduledExecutor();
            ExecutorService callbackEs = options.getCallbackExecutor();
            ExecutorService connectEs = options.getConnectExecutor();

            assertEquals(es, userEs);
            assertEquals(ses, userSes);
            if (userCallbackEs != null) {
                assertEquals(callbackEs, userCallbackEs);
            }
            if (userConnectEs != null) {
                assertEquals(connectEs, userConnectEs);
            }

            assertFalse(options.executorIsInternal());
            assertFalse(options.scheduledExecutorIsInternal());
            assertFalse(options.callbackExecutorIsInternal());
            assertFalse(options.connectExecutorIsInternal());

            assertFalse(nc.executorIsClosed());
            assertFalse(nc.scheduledExecutorIsClosed());
            assertFalse(nc.callbackExecutorIsClosed());
            assertFalse(nc.connectExecutorIsClosed());

            assertFalse(es.isShutdown());
            assertFalse(ses.isShutdown());
            assertFalse(callbackEs.isShutdown());
            assertFalse(connectEs.isShutdown());

            assertFalse(userEs.isShutdown());
            assertFalse(userSes.isShutdown());
            if (userCallbackEs != null) {
                assertFalse(userCallbackEs.isShutdown());
            }
            if (userConnectEs != null) {
                assertFalse(userConnectEs.isShutdown());
            }

            nc.subscribe("*");
            Thread.sleep(1000);
            nc.close();

            assertTrue(nc.executorIsClosed());
            assertTrue(nc.scheduledExecutorIsClosed());
            assertTrue(nc.callbackExecutorIsClosed());
            assertTrue(nc.connectExecutorIsClosed());

            assertFalse(es.isShutdown());
            assertFalse(ses.isShutdown());
            assertFalse(callbackEs.isShutdown());
            assertFalse(connectEs.isShutdown());

            assertFalse(userEs.isShutdown());
            assertFalse(userSes.isShutdown());
            if (userCallbackEs != null) {
                assertFalse(userCallbackEs.isShutdown());
            }
            if (userConnectEs != null) {
                assertFalse(userConnectEs.isShutdown());
            }
        }
    }
}
