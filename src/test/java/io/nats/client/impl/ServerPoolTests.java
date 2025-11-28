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
import io.nats.client.support.NatsUri;
import io.nats.client.utils.OptionsUtils;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.*;

import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static org.junit.jupiter.api.Assertions.*;

public class ServerPoolTests extends TestBase {

    public static final String BOOT_ONE = "nats://b1";
    public static final String BOOT_TWO = "nats://b2";
    public static final String BOOT_ONE_SECURE = "tls://b1";
    public static final String DISC_ONE = "nats://d1";
    public static final String DISC_TWO = "nats://d2";
    public static final String DISC_THREE = "nats://d3";
    public static final String HOST_THAT_CAN_BE_RESOLVED = "connect.ngs.global";
    public static final String[] bootstrap = new String[]{BOOT_ONE, BOOT_TWO};
    public static final String[] combined = new String[]{BOOT_ONE, BOOT_TWO, DISC_ONE, DISC_TWO, DISC_THREE};
    public static final List<String> discoveredServers = Arrays.asList(BOOT_TWO, DISC_ONE, DISC_TWO, DISC_THREE);

    @Test
    public void testPoolOptions() throws URISyntaxException {
        NatsUri lastConnectedServer = new NatsUri(BOOT_ONE);

        // testing that the expected show up in the pool
        Options o = OptionsUtils.optionsBuilder(bootstrap).build();
        NatsServerPool nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, false, combined);

        // ... and last connected is moved to the end
        nsp = newNatsServerPool(o, lastConnectedServer, discoveredServers);
        validateNslp(nsp, lastConnectedServer, false, combined);

        // testing that noRandomize maintains order
        o = OptionsUtils.optionsBuilder(bootstrap).noRandomize().build();
        nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, true, combined);

        // ... and still properly moves last connected server to end of list
        nsp = newNatsServerPool(o, lastConnectedServer, discoveredServers);
        validateNslp(nsp, lastConnectedServer, true, combined);

        // testing that ignoreDiscoveredServers ignores discovered servers
        o = OptionsUtils.optionsBuilder(bootstrap).ignoreDiscoveredServers().build();
        nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, false, BOOT_ONE, BOOT_TWO);

        // testing that duplicates don't get added
        String[] secureAndNotSecure = new String[]{BOOT_ONE, BOOT_ONE_SECURE};
        String[] secureBootstrap = new String[]{BOOT_ONE_SECURE};
        o = OptionsUtils.optionsBuilder(secureAndNotSecure).build();
        nsp = newNatsServerPool(o, null, null);
        validateNslp(nsp, null, false, secureBootstrap);

        secureAndNotSecure = new String[]{BOOT_ONE_SECURE, BOOT_ONE};
        o = OptionsUtils.optionsBuilder(secureAndNotSecure).build();
        nsp = newNatsServerPool(o, null, null);
        validateNslp(nsp, null, false, secureBootstrap);
    }

    @Test
    public void testMaxReconnects() throws URISyntaxException {
        NatsUri failed = new NatsUri(BOOT_ONE);

        // testing that servers that fail max times and is removed
        Options o = OptionsUtils.optionsBuilder(BOOT_ONE).maxReconnects(3).build();
        NatsServerPool nsp = newNatsServerPool(o, null, null);
        for (int x = 0; x < 4; x++) {
            nsp.nextServer();
            validateNslp(nsp, null, false, BOOT_ONE);
            nsp.connectFailed(failed);
        }
        assertNull(nsp.nextServer());

        // and that it's put back
        nsp.acceptDiscoveredUrls(Collections.singletonList(BOOT_ONE));
        validateNslp(nsp, null, false, BOOT_ONE);

        // testing that servers that fail max times and is removed
        o = OptionsUtils.optionsBuilder(BOOT_ONE).maxReconnects(0).build();
        nsp = newNatsServerPool(o, null, null);
        nsp.nextServer();
        validateNslp(nsp, null, false, BOOT_ONE);
        nsp.connectFailed(failed);
        assertNull(nsp.nextServer());

        // and that it's put back
        nsp.acceptDiscoveredUrls(Collections.singletonList(BOOT_ONE));
        validateNslp(nsp, null, false, BOOT_ONE);
    }

    @Test
    public void testPruning() throws URISyntaxException {
        // making sure that pruning happens. get baseline
        Options o = OptionsUtils.optionsBuilder(bootstrap).maxReconnects(0).build();
        NatsServerPool nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, false, combined);

        // new discovered list is missing d3
        nsp.acceptDiscoveredUrls(Arrays.asList(DISC_ONE, DISC_TWO));
        validateNslp(nsp, null, false, BOOT_ONE, BOOT_TWO, DISC_ONE, DISC_TWO);

        // new discovered list is missing the last connected
        nsp.connectSucceeded(new NatsUri(DISC_TWO));
        nsp.acceptDiscoveredUrls(Collections.singletonList(DISC_ONE));
        validateNslp(nsp, null, false, BOOT_ONE, BOOT_TWO, DISC_ONE, DISC_TWO);
    }

    @Test
    public void testResolvingHostname() throws URISyntaxException {
        // resolving host name is false
        NatsUri ngs = new NatsUri(HOST_THAT_CAN_BE_RESOLVED);
        Options o = optionsBuilder().noResolveHostnames().build();
        NatsServerPool nsp = newNatsServerPool(o, null, null);
        List<String> resolved = nsp.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED);
        assertNull(resolved);

        // resolving host name is true
        o = optionsBuilder().build();
        nsp = newNatsServerPool(o, null, null);
        resolved = nsp.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED);
        assertNotNull(resolved);
        assertTrue(resolved.size() > 1);
        for (String ip : resolved) {
            NatsUri nuri = ngs.reHost(ip);
            assertTrue(nuri.hostIsIpAddress());
        }
    }

    private static NatsServerPool newNatsServerPool(Options o, NatsUri last, List<String> discoveredServers) {
        NatsServerPool nsp = new NatsServerPool();
        nsp.initialize(o);
        if (last != null) {
            NatsUri next = nsp.nextServer();
            while (!last.equals(next)) {
                next = nsp.nextServer();
            }
            assertEquals(last, next);
            nsp.connectSucceeded(last);
        }
        if (discoveredServers != null) {
            nsp.acceptDiscoveredUrls(discoveredServers);
        }
        return nsp;
    }

    private static List<NatsUri> convertToNuri(Collection<String> urls) {
        final List<NatsUri> nuris = new ArrayList<>();
        for (String s : urls) {
            try {
                nuris.add(new NatsUri(s));
            } catch (URISyntaxException ignore) {
            }
        }
        return nuris;
    }

    private static void validateNslp(NatsServerPool nsp, NatsUri last, boolean notRandom, String... expectedUrls) throws URISyntaxException {
        List<NatsUri> supplied = convertToNuri(nsp.getServerList());
        int expectedSize = expectedUrls.length;
        assertEquals(expectedSize, supplied.size());
        for (int i = 0; i < expectedUrls.length; i++) {
            NatsUri expected = new NatsUri(expectedUrls[i]);
            assertTrue(supplied.contains(expected));
            if (notRandom && last == null) {
                assertEquals(expected, supplied.get(i));
            }
        }
        if (last != null) {
            assertEquals(last, supplied.get(supplied.size() - 1));
        }
    }

    @Test
    public void testServerPoolEntry() throws URISyntaxException {
        NatsUri nuri = new NatsUri(BOOT_ONE);
        ServerPoolEntry entry = new ServerPoolEntry(nuri, true);
        assertEquals(nuri, entry.nuri);
        assertTrue(entry.isGossiped);
        assertTrue(entry.toString().contains(nuri.toString()));
        assertTrue(entry.toString().contains("true/0"));

        entry = new ServerPoolEntry(nuri, false);
        entry.failedAttempts = 1;
        assertEquals(nuri, entry.nuri);
        assertFalse(entry.isGossiped);
        assertTrue(entry.toString().contains(nuri.toString()));
        assertTrue(entry.toString().contains("false/1"));
    }
}
