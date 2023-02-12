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

import io.nats.client.NatsServerListProvider;
import io.nats.client.Options;
import io.nats.client.support.NatsUri;
import io.nats.client.utils.TestBase;
import nats.io.NatsRunnerUtils;
import nats.io.NatsServerRunner;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AdditionalConnectTests extends TestBase {

    // THIS CLASS IS PACKAGED HERE BECAUSE MockNatsConnection only works package scoped

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
            MockNatsConnection conn1 = new MockNatsConnection(options1);
            conn1.connect(false);

            Options options2 = new Options.Builder().server(srv1.getURI()).ignoreDiscoveredServers().build();
            MockNatsConnection conn2 = new MockNatsConnection(options2);
            conn2.connect(false);

            Options options3 = new Options.Builder().server(srv1.getURI()).noRandomize().build();
            MockNatsConnection conn3 = new MockNatsConnection(options3);
            conn3.connect(false);

            standardConnectionWait(conn1);
            standardConnectionWait(conn2);
            standardConnectionWait(conn3);

            final List<NatsUri> optionsServers1 = conn1.getOptions().getNatsServerUris();
            final List<NatsUri> discoveredServers1 = new ArrayList<>();
            for (String s : conn1.getServerInfo().getConnectURLs()) {
                try {
                    discoveredServers1.add(new NatsUri(s));
                }
                catch (URISyntaxException ignore) {}
            }

            final List<NatsUri> discoveredServers2 = new ArrayList<>();
            for (String s : conn2.getServerInfo().getConnectURLs()) {
                try {
                    discoveredServers2.add(new NatsUri(s));
                }
                catch (URISyntaxException ignore) {}
            }

            final List<NatsUri> optionsServers3 = conn3.getOptions().getNatsServerUris();
            final List<NatsUri> discoveredServers3 = new ArrayList<>();
            for (String s : conn3.getServerInfo().getConnectURLs()) {
                try {
                    discoveredServers3.add(new NatsUri(s));
                }
                catch (URISyntaxException ignore) {}
            }

            // option 1 is randomized so just check that both options and discovered are there
            assertEquals(srv1.getURI(), conn1.getConnectedUrl());
            assertEquals(optionsServers1.size() + discoveredServers1.size(), conn1.getServersToTry().size());
            List<NatsUri> toTry = conn1.getServersToTry();
            for (NatsUri uri : optionsServers1) {
                assertTrue(toTry.contains(uri));
            }
            for (NatsUri uri : discoveredServers1) {
                assertTrue(toTry.contains(uri));
            }
            String ds = discoveredServers1.toString();
            assertTrue(ds.contains("" + srv1.getPort()));
            assertTrue(ds.contains("" + srv2.getPort()));
            assertTrue(ds.contains("" + srv3.getPort()));

            // option 2 is to ignore discovered, must only be the options
            assertEquals(srv1.getURI(), conn2.getConnectedUrl());
            assertEquals(1, conn2.getServersToTry().size());
            assertEquals(srv1.getURI(), conn2.getServersToTry().get(0).toString());
            ds = discoveredServers2.toString();
            assertTrue(ds.contains("" + srv1.getPort()));
            assertTrue(ds.contains("" + srv2.getPort()));
            assertTrue(ds.contains("" + srv3.getPort()));

            // option 3 is no randomize so the order in to try is maintained
            // note even on no randomize, the lastConnectedServer gets moved to the end
            assertEquals(srv1.getURI(), conn3.getConnectedUrl());
            List<NatsUri> expected = new ArrayList<>();
            expected.addAll(discoveredServers1);
            expected.addAll(optionsServers3); // done in this order b/c the 1 server in optionsServers3 is last connected
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

    @Test
    public void testServerListProvider() throws URISyntaxException {
        String[] optionsNatsUris = new String[]{"nats://one", "nats://two"};
        List<String> serverInfoConnectUrls = Arrays.asList("nats://one:4222", "nats://two:4222", "nats://three:4222", "bad://");

        Options o = new Options.Builder().servers(optionsNatsUris).build();
        NatsUri lastConnectedServer = new NatsUri("nats://one");

        NatsServerListProvider nslp = new NatsServerListProvider(o);
        nslp.acceptDiscoveredUrls(serverInfoConnectUrls);
        List<NatsUri> list = nslp.getServerList(null);
        validateNslp(list, null, false, "nats://one", "nats://two", "nats://three");

        nslp = new NatsServerListProvider(o);
        nslp.acceptDiscoveredUrls(serverInfoConnectUrls);
        list = nslp.getServerList(lastConnectedServer);
        validateNslp(list, lastConnectedServer, false, "nats://one", "nats://two", "nats://three");

        o = new Options.Builder().noRandomize().servers(optionsNatsUris).build();
        nslp = new NatsServerListProvider(o);
        nslp.acceptDiscoveredUrls(serverInfoConnectUrls);
        list = nslp.getServerList(null);
        validateNslp(list, null, true, "nats://one", "nats://two", "nats://three");

        nslp = new NatsServerListProvider(o);
        nslp.acceptDiscoveredUrls(serverInfoConnectUrls);
        list = nslp.getServerList(lastConnectedServer);
        validateNslp(list, lastConnectedServer, true, "nats://one", "nats://two", "nats://three");

        o = new Options.Builder().ignoreDiscoveredServers().servers(optionsNatsUris).build();
        nslp = new NatsServerListProvider(o);
        nslp.acceptDiscoveredUrls(serverInfoConnectUrls);
        list = nslp.getServerList(null);
        validateNslp(list, null, false, "nats://one", "nats://two");

        nslp = new NatsServerListProvider(o);
        nslp.acceptDiscoveredUrls(serverInfoConnectUrls);
        list = nslp.getServerList(lastConnectedServer);
        validateNslp(list, lastConnectedServer, false, "nats://one", "nats://two");

        o = new Options.Builder().server("connect.ngs.global").build();
        nslp = new NatsServerListProvider(o);
        list = nslp.getServerList(null);
        assertEquals(1, list.size());
        assertEquals(new NatsUri("connect.ngs.global"), list.get(0));

        o = new Options.Builder().resolveHostnames().server("connect.ngs.global").build();
        nslp = new NatsServerListProvider(o);
        list = nslp.getServerList(null);
        assertTrue(list.size() > 1);
        for (NatsUri nuri : list) {
            assertTrue(nuri.hostIsIpAddress());
        }
    }

    private static void validateNslp(List<NatsUri> list, NatsUri last, boolean notRandom, String... expectedUrls) throws URISyntaxException {
        int expectedSize = expectedUrls.length;
        assertEquals(expectedSize, list.size());
        for (int i = 0; i < expectedUrls.length; i++) {
            String url = expectedUrls[i];
            NatsUri nuri = new NatsUri(url);
            assertTrue(list.contains(nuri));
            if (notRandom && last == null) {
                assertEquals(nuri, list.get(i));
            }
        }
        if (last != null) {
            assertEquals(last, list.get(list.size() - 1));
        }
    }

    public static class MockNatsConnection extends NatsConnection {
        public MockNatsConnection(Options options) {
            super(options);
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
