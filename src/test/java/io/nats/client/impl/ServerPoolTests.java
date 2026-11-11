// Copyright 2022-2026 The NATS Authors
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
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ServerPoolTests extends TestBase {

    public static final String BOOT_ONE = "nats://b1";
    public static final String BOOT_TWO = "nats://b2";
    public static final String BOOT_ONE_SECURE = "tls://b1";
    public static final String DISC_ONE = "nats://d1";
    public static final String DISC_TWO = "nats://d2";
    public static final String DISC_THREE = "nats://d3";
    public static final String HOST_THAT_CAN_BE_RESOLVED_TO_MULTIPLE_IPS = "connect.ngs.global";
    public static final String HOST_THAT_CAN_BE_RESOLVED_TO_ONE_IP = "demo.nats.io";
    public static final String HOST_THAT_CANT_BE_RESOLVED = "not.real.host";
    public static final String[] bootstrap = new String[]{BOOT_ONE, BOOT_TWO};
    public static final String[] combined = new String[]{BOOT_ONE, BOOT_TWO, DISC_ONE, DISC_TWO, DISC_THREE};
    // connect_urls entries are host:port, as nats-server writes them
    public static final String ENTRY_BOOT_ONE = "b1:4222";
    public static final String ENTRY_BOOT_TWO = "b2:4222";
    public static final String ENTRY_DISC_ONE = "d1:4222";
    public static final String ENTRY_DISC_TWO = "d2:4222";
    public static final String ENTRY_DISC_THREE = "d3:4222";
    public static final List<String> discoveredServers = Arrays.asList(ENTRY_BOOT_TWO, ENTRY_DISC_ONE, ENTRY_DISC_TWO, ENTRY_DISC_THREE);

    @Test
    public void testPoolOptions() throws URISyntaxException {
        NatsUri lastConnectedServer = new NatsUri(BOOT_ONE);

        // testing that the expected show up in the pool
        Options o = new Options.Builder().servers(bootstrap).build();
        NatsServerPool nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, false, combined);

        // ... and last connected is moved to the end
        nsp = newNatsServerPool(o, lastConnectedServer, discoveredServers);
        validateNslp(nsp, lastConnectedServer, false, combined);

        // testing that noRandomize maintains order
        o = new Options.Builder().noRandomize().servers(bootstrap).build();
        nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, true, combined);

        // ... and still properly moves last connected server to end of list
        nsp = newNatsServerPool(o, lastConnectedServer, discoveredServers);
        validateNslp(nsp, lastConnectedServer, true, combined);

        // testing that ignoreDiscoveredServers ignores discovered servers
        o = new Options.Builder().ignoreDiscoveredServers().servers(bootstrap).build();
        nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, false, BOOT_ONE, BOOT_TWO);

        // testing that duplicates don't get added
        String[] secureAndNotSecure = new String[]{BOOT_ONE, BOOT_ONE_SECURE};
        String[] secureBootstrap = new String[]{BOOT_ONE_SECURE};
        o = new Options.Builder().servers(secureAndNotSecure).build();
        nsp = newNatsServerPool(o, null, null);
        validateNslp(nsp, null, false, secureBootstrap);

        secureAndNotSecure = new String[]{BOOT_ONE_SECURE, BOOT_ONE};
        o = new Options.Builder().servers(secureAndNotSecure).build();
        nsp = newNatsServerPool(o, null, null);
        validateNslp(nsp, null, false, secureBootstrap);
    }

    @Test
    public void testMaxReconnects() throws URISyntaxException {
        NatsUri failed = new NatsUri(BOOT_ONE);

        // testing that servers that fail max times and is removed
        Options o = new Options.Builder().server(BOOT_ONE).maxReconnects(3).build();
        NatsServerPool nsp = newNatsServerPool(o, null, null);
        for (int x = 0; x < 4; x++) {
            nsp.nextServer();
            validateNslp(nsp, null, false, BOOT_ONE);
            nsp.connectFailed(failed);
        }
        assertNull(nsp.nextServer());

        // and that it's put back
        nsp.acceptDiscoveredUrls(Collections.singletonList(ENTRY_BOOT_ONE));
        validateNslp(nsp, null, false, BOOT_ONE);

        // testing that servers that fail max times and is removed
        o = new Options.Builder().server(BOOT_ONE).maxReconnects(0).build();
        nsp = newNatsServerPool(o, null, null);
        nsp.nextServer();
        validateNslp(nsp, null, false, BOOT_ONE);
        nsp.connectFailed(failed);
        assertNull(nsp.nextServer());

        // and that it's put back
        nsp.acceptDiscoveredUrls(Collections.singletonList(ENTRY_BOOT_ONE));
        validateNslp(nsp, null, false, BOOT_ONE);
    }

    @Test
    public void testPruning() throws URISyntaxException {
        // making sure that pruning happens. get baseline
        Options o = new Options.Builder().servers(bootstrap).maxReconnects(0).build();
        NatsServerPool nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, false, combined);

        // new discovered list is missing d3
        nsp.acceptDiscoveredUrls(Arrays.asList(ENTRY_DISC_ONE, ENTRY_DISC_TWO));
        validateNslp(nsp, null, false, BOOT_ONE, BOOT_TWO, DISC_ONE, DISC_TWO);

        // new discovered list is missing the last connected
        nsp.connectSucceeded(new NatsUri(DISC_TWO));
        nsp.acceptDiscoveredUrls(Collections.singletonList(ENTRY_DISC_ONE));
        validateNslp(nsp, null, false, BOOT_ONE, BOOT_TWO, DISC_ONE, DISC_TWO);
    }

    @Test
    public void testServerRejected() throws URISyntaxException {
        // a rejected server leaves the pool and does not come back when it is discovered again
        Options o = new Options.Builder().noRandomize().servers(bootstrap).build();
        NatsServerPool nsp = newNatsServerPool(o, null, discoveredServers);
        validateNslp(nsp, null, true, combined);

        nsp.serverRejected(new NatsUri(DISC_ONE));
        validateNslp(nsp, null, true, BOOT_ONE, BOOT_TWO, DISC_TWO, DISC_THREE);

        // discovered again: not taken back, and not counted as an unknown server
        assertFalse(nsp.acceptDiscoveredUrls(discoveredServers));
        validateNslp(nsp, null, true, BOOT_ONE, BOOT_TWO, DISC_TWO, DISC_THREE);

        // a server the pool does not hold is remembered all the same; the empty result prunes the gossiped servers as usual
        nsp.serverRejected(new NatsUri("nats://d4"));
        assertFalse(nsp.acceptDiscoveredUrls(Collections.singletonList("d4:4222")));
        validateNslp(nsp, null, true, BOOT_ONE, BOOT_TWO);

        // the memory is the exact uri: the scheme is what made the server unusable, and a discovered server takes
        // the scheme of the server that reported it. So the same host and port reported over another transport is
        // still accepted, while reported over the rejected transport it is not
        nsp.serverRejected(new NatsUri("ws://d9"));
        assertTrue(nsp.acceptDiscoveredUrls(Collections.singletonList("d9:4222"), new NatsUri(BOOT_ONE)));
        validateNslp(nsp, null, true, BOOT_ONE, BOOT_TWO, "nats://d9");
        assertFalse(nsp.acceptDiscoveredUrls(Collections.singletonList("d9:4222"), new NatsUri("ws://w1")));
        validateNslp(nsp, null, true, BOOT_ONE, BOOT_TWO);
    }

    @Test
    public void testDiscoveredServersTakeTheSchemeOfTheOrigin() throws URISyntaxException {
        // ADR-40, connecting to a discovered server: it is dialed with the transport and the TLS of the server that
        // reported it. The pool gives it the origin's scheme; the entry itself never names one.
        // The ws:// origin is not configured, since the options refuse ws:// next to a TLS scheme
        Options o = new Options.Builder().servers(new String[]{BOOT_ONE, "tls://s1", "wss://x1"}).noRandomize().build();
        NatsServerPool nsp = new NatsServerPool();
        nsp.initialize(o);
        nsp.acceptDiscoveredUrls(Collections.singletonList("d1:4222"), new NatsUri(BOOT_ONE));
        assertEquals("nats", entry(nsp, "d1").nuri.getScheme());
        nsp.acceptDiscoveredUrls(Arrays.asList("d1:4222", "d2:4222"), new NatsUri("tls://s1"));
        assertEquals("tls", entry(nsp, "d2").nuri.getScheme());
        nsp.acceptDiscoveredUrls(Arrays.asList("d1:4222", "d2:4222", "d3:4222"), new NatsUri("ws://w1"));
        assertEquals("ws", entry(nsp, "d3").nuri.getScheme());
        assertTrue(entry(nsp, "d3").nuri.isWebsocket());
        nsp.acceptDiscoveredUrls(Arrays.asList("d1:4222", "d2:4222", "d3:4222", "d4:4222"), new NatsUri("wss://x1"));
        assertEquals("wss", entry(nsp, "d4").nuri.getScheme());
        assertTrue(entry(nsp, "d4").nuri.isSecure());

        // a second hop: a server reported by a discovered server takes that server's scheme, so the transport is kept
        nsp.acceptDiscoveredUrls(Arrays.asList("d1:4222", "d2:4222", "d3:4222", "d4:4222", "d5:4222"), entry(nsp, "d2").nuri);
        assertEquals("tls", entry(nsp, "d5").nuri.getScheme());

        // the first report decides: a known discovered server keeps its scheme when reported again over another transport
        nsp.acceptDiscoveredUrls(Arrays.asList("d1:4222", "d2:4222", "d3:4222", "d4:4222", "d5:4222"), new NatsUri("ws://w1"));
        assertEquals("nats", entry(nsp, "d1").nuri.getScheme());
        assertEquals("tls", entry(nsp, "d2").nuri.getScheme());

        // a configured server reported by another server does not change
        nsp.acceptDiscoveredUrls(Arrays.asList("b1:4222", "s1:4222", "x1:4222"), new NatsUri("ws://w1"));
        assertEquals("nats", entry(nsp, "b1").nuri.getScheme());
        assertEquals("tls", entry(nsp, "s1").nuri.getScheme());
        assertEquals("wss", entry(nsp, "x1").nuri.getScheme());

        // without an origin, the pool's default scheme: the first configured scheme that is not nats, else nats
        nsp.acceptDiscoveredUrls(Collections.singletonList("d6:4222"));
        assertEquals("tls", entry(nsp, "d6").nuri.getScheme());
        nsp = newNatsServerPool(new Options.Builder().servers(bootstrap).build(), null, Collections.singletonList("d7:4222"));
        assertEquals("nats", entry(nsp, "d7").nuri.getScheme());
    }

    @Test
    public void testDiscoveredEntriesAreHostAndPortOnly() throws URISyntaxException {
        // ADR-40, the form of a connect_urls entry: the pool applies the rule of NatsUri.discoveredHostPort itself.
        // An entry with a scheme, one that does not parse as host:port, and one with a port out of range are left
        // out, and the other entries are used; see OptionsTests.testNatsUriDiscoveredHostPort for the forms
        Options o = new Options.Builder().noRandomize().servers(bootstrap).build();
        NatsServerPool nsp = newNatsServerPool(o, null, null);
        List<String> entries = Arrays.asList("nats://d1:4222", "ws://d2:4222", "d3", "d4:0", "d5:65536", "d6/x:4222", "u:p@d7:4222", "d8:4222");
        assertTrue(nsp.acceptDiscoveredUrls(entries, new NatsUri(BOOT_ONE)));
        validateNslp(nsp, null, true, BOOT_ONE, BOOT_TWO, "nats://d7", "nats://d8");

        // a list of which no entry is usable is an empty list: it prunes the discovered servers, as an empty list does
        assertFalse(nsp.acceptDiscoveredUrls(Arrays.asList("nats://d7:4222", "tls://d8:4222"), new NatsUri(BOOT_ONE)));
        validateNslp(nsp, null, true, BOOT_ONE, BOOT_TWO);
    }

    @Test
    public void testDiscoveredServersInheritTheOriginsUserInfo() throws URISyntaxException {
        String bootWithCreds = "nats://derek:password@b1";
        Options o = new Options.Builder().servers(new String[]{bootWithCreds, BOOT_TWO}).noRandomize().build();
        NatsServerPool nsp = new NatsServerPool();
        nsp.initialize(o);
        NatsUri b1 = entry(nsp, "b1").nuri;
        NatsUri b2 = entry(nsp, "b2").nuri;

        // learned through the configured server with credentials: the discovered servers inherit them
        nsp.acceptDiscoveredUrls(Arrays.asList(ENTRY_DISC_ONE, ENTRY_DISC_TWO), b1);
        assertEquals("derek:password", entry(nsp, "d1").nuri.getInheritedUserInfo());
        assertEquals("derek:password", entry(nsp, "d2").nuri.getInheritedUserInfo());
        // the configured server is unchanged, and no server list string carries inherited credentials
        assertNull(entry(nsp, "b1").nuri.getInheritedUserInfo());
        for (String s : nsp.getServerList()) {
            assertTrue(s.equals(bootWithCreds + ":4222") || !s.contains("derek"), s);
        }

        // learned through a discovered server: what that server inherited is passed on
        nsp.acceptDiscoveredUrls(Arrays.asList(ENTRY_DISC_ONE, ENTRY_DISC_TWO, ENTRY_DISC_THREE), entry(nsp, "d1").nuri);
        assertEquals("derek:password", entry(nsp, "d3").nuri.getInheritedUserInfo());

        // the first report decides: a later report through a server without credentials changes nothing
        nsp.acceptDiscoveredUrls(Arrays.asList(ENTRY_DISC_ONE, ENTRY_DISC_TWO, ENTRY_DISC_THREE, "d4:4222"), b2);
        assertEquals("derek:password", entry(nsp, "d1").nuri.getInheritedUserInfo());
        assertNull(entry(nsp, "d4").nuri.getInheritedUserInfo());

        // user info the entry carries is dropped: the server is sent the credentials used for the server that reported it
        nsp.acceptDiscoveredUrls(Arrays.asList(ENTRY_DISC_ONE, ENTRY_DISC_TWO, ENTRY_DISC_THREE, "d4:4222", "own:pw@d5:4222"), b1);
        assertNull(entry(nsp, "d5").nuri.getUserInfo());
        assertEquals("derek:password", entry(nsp, "d5").nuri.getInheritedUserInfo());
        assertEquals("derek:password", entry(nsp, "d5").nuri.getConnectUserInfo());
        assertEquals("nats://d5:4222", entry(nsp, "d5").nuri.toString());

        // no origin, nothing inherited, and the entry's own user info is still dropped
        nsp.acceptDiscoveredUrls(Arrays.asList(ENTRY_DISC_ONE, ENTRY_DISC_TWO, ENTRY_DISC_THREE, "d4:4222", "own:pw@d5:4222", "own:pw@d6:4222"), null);
        assertNull(entry(nsp, "d6").nuri.getInheritedUserInfo());
        assertNull(entry(nsp, "d6").nuri.getConnectUserInfo());
    }

    private static ServerPoolEntry entry(NatsServerPool nsp, String host) {
        for (ServerPoolEntry e : nsp.entryList) {
            if (e.nuri.getHost().equals(host)) {
                return e;
            }
        }
        fail("no entry for " + host);
        return null;
    }

    private static NatsServerPool newNatsServerPool(Options o, NatsUri last, List<String> discoveredServers) {
        NatsServerPool nsp = new NatsServerPool();
        nsp.initialize(o);
        if (last != null) {
            NatsUri next = nsp.nextServer();
            while (next != null && !next.equals(last)) {
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

    @Test
    public void testNatsHostResolver() throws URISyntaxException {
        List<String> resolved = NatsHostResolver.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED_TO_MULTIPLE_IPS, true, false);
        assertNotNull(resolved);
        assertEquals(1, resolved.size());

        // this is coverage since I can't guarantee that any the host will be resolved to an IPV6
        resolved = NatsHostResolver.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED_TO_MULTIPLE_IPS, true, true);
        assertNotNull(resolved);
        assertEquals(1, resolved.size());

        resolved = NatsHostResolver.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED_TO_MULTIPLE_IPS, false, false);
        assertNotNull(resolved);
        assertTrue(resolved.size() > 1);

        // this is coverage since I can't guarantee that any the host will be resolved to an IPV6
        resolved = NatsHostResolver.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED_TO_MULTIPLE_IPS, false, true);
        assertNotNull(resolved);
        assertTrue(resolved.size() > 1);

        NatsUri ngs = new NatsUri(HOST_THAT_CAN_BE_RESOLVED_TO_MULTIPLE_IPS);
        for (String ip : resolved) {
            NatsUri nuri = ngs.reHost(ip);
            assertTrue(nuri.hostIsIpAddress());
        }

        resolved = NatsHostResolver.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED_TO_ONE_IP, true, false);
        assertNotNull(resolved);
        assertEquals(1, resolved.size());

        resolved = NatsHostResolver.resolveHostToIps(HOST_THAT_CAN_BE_RESOLVED_TO_ONE_IP, false, false);
        assertNotNull(resolved);
        assertEquals(1, resolved.size());

        resolved = NatsHostResolver.resolveHostToIps(HOST_THAT_CANT_BE_RESOLVED, false, false);
        assertNull(resolved);
    }
}
