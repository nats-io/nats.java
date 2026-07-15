// Copyright 2026 The NATS Authors
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

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.utils.TestBase;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proves that the maxReconnect budget is honored per logical server, independent of how many
 * IP addresses that server's hostname resolves to.
 *
 * <p>Before the fix, {@code connectFailed} was invoked once per resolved IP address, so a hostname
 * resolving to N addresses exhausted the maxReconnect budget roughly N times faster than a
 * single-address server. With maxReconnects = M and a hostname resolving to N addresses, the client
 * should make exactly N * (M + 1) connection attempts before giving up (the "+1" is the initial
 * connect round, which shares the same budget). The bug made it give up after far fewer.
 */
public class MaxReconnectResolvedIpsTest extends TestBase {

    private static final AtomicInteger CONNECT_ATTEMPTS = new AtomicInteger(0);
    private static final String CONNECT_ATTEMPTS_LOCK = "MaxReconnectResolvedIpsTest.CONNECT_ATTEMPTS";

    // A DataPort that never connects and simply counts how many connect attempts were made.
    // Must be public: Options.buildDataPort() instantiates it reflectively from another package.
    public static class CountingFailDataPort implements DataPort {
        @Override
        @SuppressWarnings("ClassEscapesDefinedScope")
        public void connect(@NonNull String serverURI, @NonNull NatsConnection conn, long timeoutNanos) throws IOException {
            CONNECT_ATTEMPTS.incrementAndGet();
            throw new IOException("test data port refuses to connect");
        }

        @Override public void upgradeToSecure() {}
        @Override public int read(byte[] dst, int off, int len) { return -1; }
        @Override public void write(byte[] src, int toWrite) {}
        @Override public void shutdownInput() {}
        @Override public void close() {}
        @Override public void flush() {}
    }

    // A server pool that resolves the single configured hostname to a fixed set of IP addresses.
    public static class MultiIpServerPool extends NatsServerPool {
        private final List<String> ips;
        MultiIpServerPool(List<String> ips) {
            this.ips = ips;
        }
        @Override
        public List<String> resolveHostToIps(@NonNull String host, boolean maxOneResult, boolean includeIPV6) {
            return ips;
        }
    }

    @Test
    @ResourceLock(CONNECT_ATTEMPTS_LOCK)
    public void testMaxReconnectNotDividedByResolvedIpCount() throws Exception {
        int maxReconnects = 4;
        List<String> resolvedIps = Arrays.asList("192.0.2.1", "192.0.2.2", "192.0.2.3"); // TEST-NET-1, never routable
        int numIps = resolvedIps.size();

        CONNECT_ATTEMPTS.set(0);

        Options options = new Options.Builder()
            .server("nats://fakehost.invalid:4222") // a hostname (not an IP) so resolveHost is exercised
            .serverPool(new MultiIpServerPool(resolvedIps))
            .dataPortType(CountingFailDataPort.class.getName())
            .maxReconnects(maxReconnects)
            .reconnectWait(Duration.ofMillis(1))
            .reconnectJitter(Duration.ofMillis(1))
            .connectionTimeout(Duration.ofMillis(200))
            .noRandomize()
            .build();

        // reconnectOnConnect makes the initial connect share the same reconnect budget, so the
        // whole budget is exhausted synchronously before this call returns a closed connection.
        try (Connection conn = Nats.connectReconnectOnConnect(options)) {
            assertEquals(Connection.Status.CLOSED, conn.getStatus());

            // Initial round + maxReconnects rounds, each trying every resolved IP once.
            int expected = numIps * (maxReconnects + 1);
            assertEquals(expected, CONNECT_ATTEMPTS.get(),
                    "maxReconnect budget must be per-server, not per-resolved-IP");
        }
    }

    @Test
    @ResourceLock(CONNECT_ATTEMPTS_LOCK)
    public void testSingleIpBaselineUnchanged() throws Exception {
        // Same scenario with a single resolved IP: the attempt count must match the multi-IP case
        // scaled by IP count, confirming the budget itself is unchanged (numIps * (maxReconnects + 1)).
        int maxReconnects = 4;
        List<String> resolvedIps = Collections.singletonList("192.0.2.1");

        CONNECT_ATTEMPTS.set(0);

        final Options options = new Options.Builder()
            .server("nats://fakehost.invalid:4222")
            .serverPool(new MultiIpServerPool(resolvedIps))
            .dataPortType(CountingFailDataPort.class.getName())
            .maxReconnects(maxReconnects)
            .reconnectWait(Duration.ofMillis(1))
            .reconnectJitter(Duration.ofMillis(1))
            .connectionTimeout(Duration.ofMillis(200))
            .noRandomize()
            .build();

        try (Connection conn = Nats.connectReconnectOnConnect(options)) {
            assertEquals(Connection.Status.CLOSED, conn.getStatus());
            assertEquals(maxReconnects + 1, CONNECT_ATTEMPTS.get());
        }
    }
}
