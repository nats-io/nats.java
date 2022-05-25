// Copyright 2022 The NATS Authors
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
import nats.io.NatsRunnerUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdditionalConnectTests {

    // THESE TESTS ARE HERE BECAUSE I NEED SOMETHING PACKAGE SCOPED

    @Test
    public void testConnectionWithServerUriManagement() throws IOException, InterruptedException {
        int cport1 = NatsRunnerUtils.nextPort();
        int cport2 = NatsRunnerUtils.nextPort();
        int cport3 = NatsRunnerUtils.nextPort();
        File storageDir1 = null;
        File storageDir2 = null;
        File storageDir3 = null;

        String[] customArgs1 = makeClusterArgs(1, cport1, cport2, cport3, storageDir1);
        String[] customArgs2 = makeClusterArgs(2, cport2, cport1, cport3, storageDir2);
        String[] customArgs3 = makeClusterArgs(3, cport3, cport1, cport2, storageDir3);

        try (
            NatsTestServer ts = new NatsTestServer(0, false, false, null, null, customArgs1);
            NatsTestServer ts2 = new NatsTestServer(0, false, false, null, null, customArgs2);
            NatsTestServer ts3 = new NatsTestServer(0, false, false, null, null, customArgs3)
        ) {
            sleep(1000); // just make sure the cluster is all read

            Options options1 = new Options.Builder().server(ts.getURI()).build();
            TestNatsConnection conn1 = new TestNatsConnection(options1);
            conn1.connect(false);

            Options options2 = new Options.Builder().server(ts.getURI()).ignoreDiscoveredServers().build();
            TestNatsConnection conn2 = new TestNatsConnection(options2);
            conn2.connect(false);

            Options options3 = new Options.Builder().server(ts.getURI()).noRandomize().build();
            TestNatsConnection conn3 = new TestNatsConnection(options3);
            conn3.connect(false);

            standardConnectionWait(conn1);
            standardConnectionWait(conn2);
            standardConnectionWait(conn3);

            final List<String> optionsServers1 = new ArrayList<>();
            conn1.addOptionsServers(optionsServers1);
            final List<String> discoveredServers1 = new ArrayList<>();
            conn1.addDiscoveredServers(discoveredServers1, null);

            final List<String> optionsServers2 = new ArrayList<>();
            conn2.addOptionsServers(optionsServers2);
            final List<String> discoveredServers2 = new ArrayList<>();
            conn2.addDiscoveredServers(discoveredServers2, null);

            final List<String> optionsServers3 = new ArrayList<>();
            conn3.addOptionsServers(optionsServers3);
            final List<String> discoveredServers3 = new ArrayList<>();
            conn3.addDiscoveredServers(discoveredServers3, null);

            // option 1 is randomized so just check that both options and discovered are there
            assertEquals(ts.getURI(), conn1.getConnectedUrl());
            assertEquals(optionsServers1.size() + discoveredServers1.size(), conn1.getServersToTry().size());
            List<String> toTry = conn1.getServersToTry();
            for (String url : optionsServers1) {
                assertTrue(toTry.contains(url));
            }
            for (String url : discoveredServers1) {
                assertTrue(toTry.contains(url));
            }
            String ds = discoveredServers1.toString();
            assertTrue(ds.contains("" + ts.getPort()));
            assertTrue(ds.contains("" + ts2.getPort()));
            assertTrue(ds.contains("" + ts3.getPort()));

            // option 2 is ignore discovered to to try must only be the options
            assertEquals(ts.getURI(), conn2.getConnectedUrl());
            assertEquals(1, conn2.getServersToTry().size());
            assertEquals(ts.getURI(), conn2.getServersToTry().get(0));
            ds = discoveredServers2.toString();
            assertTrue(ds.contains("" + ts.getPort()));
            assertTrue(ds.contains("" + ts2.getPort()));
            assertTrue(ds.contains("" + ts3.getPort()));

            // option 3 is no randomize so the order in to try is maintained
            assertEquals(ts.getURI(), conn3.getConnectedUrl());
            List<String> expected = new ArrayList<>();
            expected.addAll(optionsServers3);
            expected.addAll(discoveredServers1);
            toTry = conn3.getServersToTry();
            assertEquals(expected.size(), toTry.size());
            for (int x = 0; x < toTry.size(); x++) {
                assertEquals(expected.get(x), toTry.get(x));
            }
            ds = discoveredServers3.toString();
            assertTrue(ds.contains("" + ts.getPort()));
            assertTrue(ds.contains("" + ts2.getPort()));
            assertTrue(ds.contains("" + ts3.getPort()));

            standardCloseConnection(conn1);
            standardCloseConnection(conn2);
            standardCloseConnection(conn3);
        }
    }

    static class TestNatsConnection extends NatsConnection {
        public TestNatsConnection(Options options) {
            super(options);
        }

        @Override
        public List<String> getServersToTry() {
            return super.getServersToTry();
        }

        @Override
        public void addOptionsServers(List<String> servers) {
            super.addOptionsServers(servers);
        }

        @Override
        public void addDiscoveredServers(List<String> servers, List<String> rawServers) {
            super.addDiscoveredServers(servers, rawServers);
        }
    }

    private String[] makeClusterArgs(int id, int cport, int cportR1, int cportR2, File storageDir) {
        String[] args = new String[storageDir == null ? 8 : 11];
        args[0] = "--name";
        args[1] = "tc" + id;
        args[2] = "--cluster_name";
        args[3] = "testcluster";
        args[4] = "--cluster";
        args[5] = "\"nats://localhost:" + cport + "\"";
        args[6] = "--routes";
        args[7] = "\"nats://localhost:" + cportR1 + ",nats://localhost:" + cportR2 + "\"";
        if (storageDir != null) {
            args[8] = "-js";
            args[9] = "-sd";
            args[10] = "\"" + storageDir + "\"";
        }
        return args;
    }
}