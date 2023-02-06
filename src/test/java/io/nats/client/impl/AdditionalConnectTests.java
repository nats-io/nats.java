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

import io.nats.client.Options;
import nats.io.NatsRunnerUtils;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdditionalConnectTests {

    // THESE TESTS ARE HERE BECAUSE I NEED SOMETHING PACKAGE SCOPED
    private List<String> getOptionsServers(Options options) {
        List<String> servers = new ArrayList<>();
        for (URI uri : options.getServers()) {
            String srv = uri.toString();
            if (!servers.contains(srv)) {
                servers.add(srv);
            }
        }
        return servers;
    }

    @Test
    public void testConnectionWithServerUriManagement() throws Exception {
        int cport1 = NatsRunnerUtils.nextPort();
        int cport2 = NatsRunnerUtils.nextPort();
        int cport3 = NatsRunnerUtils.nextPort();
        String[] insert1 = makeClusterInsert(1, cport1, cport2, cport3);
        String[] insert2 = makeClusterInsert(2, cport2, cport1, cport3);
        String[] insert3 = makeClusterInsert(3, cport3, cport1, cport2);

        try (
            NatsServerRunner srv1 = NatsServerRunner.builder().configInserts(insert1).build();
            NatsServerRunner srv2 = NatsServerRunner.builder().configInserts(insert2).build();
            NatsServerRunner srv3 = NatsServerRunner.builder().configInserts(insert3).build()
        ) {
            sleep(1000); // just make sure the cluster is all ready

            Options options1 = new Options.Builder().server(srv1.getURI()).build();
            TestNatsConnection conn1 = new TestNatsConnection(options1);
            conn1.connect(false);

            Options options2 = new Options.Builder().server(srv1.getURI()).ignoreDiscoveredServers().build();
            TestNatsConnection conn2 = new TestNatsConnection(options2);
            conn2.connect(false);

            Options options3 = new Options.Builder().server(srv1.getURI()).noRandomize().build();
            TestNatsConnection conn3 = new TestNatsConnection(options3);
            conn3.connect(false);

            standardConnectionWait(conn1);
            standardConnectionWait(conn2);
            standardConnectionWait(conn3);

            final List<String> optionsServers1 = getOptionsServers(conn1.getOptions());
            final List<String> discoveredServers1 = new ArrayList<>();
            conn1.addDiscoveredServers(discoveredServers1);

            final List<String> discoveredServers2 = new ArrayList<>();
            conn2.addDiscoveredServers(discoveredServers2);

            final List<String> optionsServers3 = getOptionsServers(conn3.getOptions());
            final List<String> discoveredServers3 = new ArrayList<>();
            conn3.addDiscoveredServers(discoveredServers3);

            // option 1 is randomized so just check that both options and discovered are there
            assertEquals(srv1.getURI(), conn1.getConnectedUrl());
            assertEquals(optionsServers1.size() + discoveredServers1.size(), conn1.getServersToTry().size());
            List<String> toTry = conn1.getServersToTry();
            for (String url : optionsServers1) {
                assertTrue(toTry.contains(url));
            }
            for (String url : discoveredServers1) {
                assertTrue(toTry.contains(url));
            }
            String ds = discoveredServers1.toString();
            assertTrue(ds.contains("" + srv1.getPort()));
            assertTrue(ds.contains("" + srv2.getPort()));
            assertTrue(ds.contains("" + srv3.getPort()));

            // option 2 is to ignore discovered, must only be the options
            assertEquals(srv1.getURI(), conn2.getConnectedUrl());
            assertEquals(1, conn2.getServersToTry().size());
            assertEquals(srv1.getURI(), conn2.getServersToTry().get(0));
            ds = discoveredServers2.toString();
            assertTrue(ds.contains("" + srv1.getPort()));
            assertTrue(ds.contains("" + srv2.getPort()));
            assertTrue(ds.contains("" + srv3.getPort()));

            // option 3 is no randomize so the order in to try is maintained
            assertEquals(srv1.getURI(), conn3.getConnectedUrl());
            List<String> expected = new ArrayList<>();
            expected.addAll(optionsServers3);
            expected.addAll(discoveredServers1);
            toTry = conn3.getServersToTry();
            assertEquals(expected.size(), toTry.size());
            for (int x = 0; x < toTry.size(); x++) {
                assertEquals(expected.get(x), toTry.get(x));
            }
            ds = discoveredServers3.toString();
            assertTrue(ds.contains("" + srv1.getPort()));
            assertTrue(ds.contains("" + srv2.getPort()));
            assertTrue(ds.contains("" + srv3.getPort()));

            standardCloseConnection(conn1);
            standardCloseConnection(conn2);
            standardCloseConnection(conn3);
        }
    }

    public static class TestNatsConnection extends NatsConnection {
        public TestNatsConnection(Options options) {
            super(options);
        }

        @Override
        public List<String> getServersToTry() {
            return super.getServersToTry();
        }

        @Override
        public void addDiscoveredServers(List<String> servers) {
            super.addDiscoveredServers(servers);
        }
    }

    private String[] makeClusterInsert(int id, int listen, int route1, int route2) {
        String[] args = new String[12];
        args[0] = "";
        args[1] = "server_name=srv" + id;
        args[2] = "";
        args[3] = "cluster {";
        args[4] = "  name: testcluster";
        args[5] = "  listen: 127.0.0.1:" + listen;
        args[6] = "  routes: [";
        args[7] = "    nats-route://127.0.0.1:" + route1;
        args[8] = "    nats-route://127.0.0.1:" + route2;
        args[9] = "  ]";
        args[10] = "}";
        args[11] = "";
        return args;
    }
}
