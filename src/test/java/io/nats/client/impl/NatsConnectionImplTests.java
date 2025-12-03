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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NatsConnectionImplTests {

    @Test
    public void testConnectionClosedProperly() throws Exception {
        try (NatsTestServer server = new NatsTestServer()) {
            Options options = standardOptions(server.getNatsLocalhostUri());
            verifyInternalExecutors(options, (NatsConnection)standardConnection(options));

            // using the same options to demonstrate the executors came
            // from the internal factory and were not reused
            verifyInternalExecutors(options, (NatsConnection)standardConnection(options));

            // using options copied from options to demonstrate the executors
            // came from the internal factory and were not reused
            options = new Options.Builder(options).build();
            verifyInternalExecutors(options, (NatsConnection)standardConnection(options));

            ExecutorService es = Executors.newFixedThreadPool(3);
            ScheduledExecutorService ses = Executors.newScheduledThreadPool(3);
            assertFalse(es.isShutdown());
            assertFalse(ses.isShutdown());

            options = standardOptionsBuilder(server.getNatsLocalhostUri())
                .executor(es)
                .scheduledExecutor(ses)
                .build();
            verifyExternalExecutors(options, (NatsConnection)standardConnection(options));

            // same options just because
            verifyExternalExecutors(options, (NatsConnection)standardConnection(options));

            es.shutdown();
            ses.shutdown();
            assertTrue(es.isShutdown());
            assertTrue(ses.isShutdown());
        }
    }

    private static void verifyInternalExecutors(Options options, NatsConnection nc) throws InterruptedException {
        assertTrue(options.executorIsInternal());
        assertTrue(options.scheduledExecutorIsInternal());

        assertFalse(nc.callbackRunnerClosed());
        assertFalse(nc.connectExecutorClosed());

        assertFalse(nc.executorIsClosed());
        assertFalse(nc.scheduledExecutorIsClosed());

        nc.subscribe("*");
        Thread.sleep(1000);
        nc.close();

        assertTrue(nc.callbackRunnerClosed());
        assertTrue(nc.connectExecutorClosed());

        assertTrue(nc.executorIsClosed());
        assertTrue(nc.scheduledExecutorIsClosed());
    }

    private static void verifyExternalExecutors(Options options, NatsConnection nc) throws InterruptedException {
        assertFalse(options.executorIsInternal());
        assertFalse(options.scheduledExecutorIsInternal());

        assertFalse(nc.callbackRunnerClosed());
        assertFalse(nc.connectExecutorClosed());

        assertFalse(nc.executorIsClosed());
        assertFalse(nc.scheduledExecutorIsClosed());

        nc.subscribe("*");
        Thread.sleep(1000);
        nc.close();

        assertTrue(nc.callbackRunnerClosed());
        assertTrue(nc.connectExecutorClosed());

        assertTrue(nc.executorIsClosed());
        assertTrue(nc.scheduledExecutorIsClosed());
    }
}
