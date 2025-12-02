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

import static io.nats.client.utils.ConnectionUtils.standardConnect;
import static io.nats.client.utils.OptionsUtils.NOOP_EL;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NatsConnectionImplTests {

    @Test
    public void testConnectionClosedProperly() throws Exception {
        try (NatsTestServer ts = new NatsTestServer()) {

            Options options = Options.builder().server(ts.getServerUri()).errorListener(NOOP_EL).build();

            verifyInternalExecutors((NatsConnection) standardConnect(options));

            // using the same options to demonstrate the executors came
            // from the internal factory and were not reused
            verifyInternalExecutors((NatsConnection) standardConnect(options));

            // using options copied from options to demonstrate the executors
            // came from the internal factory and were not reused
            options = new Options.Builder(options).build();
            verifyInternalExecutors((NatsConnection) standardConnect(options));

            // the test options builder has all its own executors so none are "internal"
            options = optionsBuilder(ts).build();
            verifyExternalExecutors((NatsConnection) standardConnect(options));

            // same options just because
            verifyExternalExecutors((NatsConnection) standardConnect(options));
        }
    }

    private static void verifyInternalExecutors(NatsConnection nc) throws InterruptedException {
        Options options = nc.getOptions();
        assertTrue(options.executorIsInternal());
        assertTrue(options.scheduledExecutorIsInternal());

        assertFalse(nc.callbackRunnerIsShutdown());
        assertFalse(nc.connectExecutorIsShutdown());

        assertFalse(nc.executorIsShutdown());
        assertFalse(nc.scheduledExecutorIsShutdown());

        nc.close();

        assertTrue(nc.callbackRunnerIsShutdown());
        assertTrue(nc.connectExecutorIsShutdown());

        assertTrue(nc.executorIsShutdown());
        assertTrue(nc.scheduledExecutorIsShutdown());
    }

    private static void verifyExternalExecutors(NatsConnection nc) throws InterruptedException {
        Options options = nc.getOptions();
        assertFalse(options.executorIsInternal());
        assertFalse(options.scheduledExecutorIsInternal());

        assertFalse(nc.callbackRunnerIsShutdown());
        assertFalse(nc.connectExecutorIsShutdown());

        assertFalse(nc.executorIsShutdown());
        assertFalse(nc.scheduledExecutorIsShutdown());

        nc.close();

        assertFalse(nc.executorIsShutdown());
        assertFalse(nc.scheduledExecutorIsShutdown());
        assertFalse(nc.callbackRunnerIsShutdown());
        assertFalse(nc.connectExecutorIsShutdown());
    }
}
