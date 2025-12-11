// Copyright 2015-2025 The NATS Authors
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

import io.nats.client.NatsSystemClock;
import io.nats.client.NatsSystemClockProvider;
import io.nats.client.Options;
import io.nats.client.support.NatsInetAddress;
import io.nats.client.support.NatsInetAddressProvider;
import io.nats.client.support.NatsUri;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class NatsProvidersAndImplementationsTests {
    // MOST OF THIS IS JUST FOR COVERAGE

    @Test
    public void testNatsInetAddress() throws UnknownHostException {
        validateNatsInetAddress("synadia.io");
        validateNatsInetAddress("synadia.com");

        NatsInetAddress.setProvider(null);
        validateNatsInetAddress("synadia.io");
        validateNatsInetAddress("synadia.com");

        NatsInetAddress.setProvider(new NatsInetAddressProvider() {});
        validateNatsInetAddress("synadia.io");
        validateNatsInetAddress("synadia.com");
    }

    private static void validateNatsInetAddress(String host) throws UnknownHostException {
        // This is just for coverage of NatsInetAddress and NatsInetAddressProvider
        InetAddress iaByName = NatsInetAddress.getByName(host);
        InetAddress iaByAddress = NatsInetAddress.getByAddress(iaByName.getHostName(), iaByName.getAddress());
        assertEquals(iaByName, iaByAddress);

        iaByAddress = NatsInetAddress.getByAddress(iaByName.getAddress());
        assertEquals(iaByName, iaByAddress);

        InetAddress[] allByName = NatsInetAddress.getAllByName(host);
        for (InetAddress ia : allByName) {
            iaByAddress = NatsInetAddress.getByAddress(ia.getHostName(), ia.getAddress());
            assertEquals(ia, iaByAddress);
        }

        NatsInetAddress.getLoopbackAddress();
        NatsInetAddress.getLocalHost();
    }

    @Test
    public void testNatsSystemClock() throws InterruptedException {
        validateNatsSystemClock();

        NatsSystemClock.setProvider(new NatsSystemClockProvider() {
        });
        validateNatsSystemClock();

        NatsSystemClock.setProvider(null);
        validateNatsSystemClock();
    }

    private static void validateNatsSystemClock() throws InterruptedException {
        // This is just for coverage of NatsSystemClock and NatsSystemClockProvider
        long now1Ms = NatsSystemClock.currentTimeMillis();
        long now1Nanos = NatsSystemClock.nanoTime();
        Thread.sleep(10);

        long now2Ms = NatsSystemClock.currentTimeMillis();
        long now2Nanos = NatsSystemClock.nanoTime();

        assertTrue(now1Ms < now2Ms);
        assertTrue(now1Nanos < now2Nanos);
    }

    @SuppressWarnings("DataFlowIssue") // calling methods with null is fine for the test
    @Test
    public void testDataPortInterfaceCoverage() throws IOException, URISyntaxException {
        // this is coverage for connect, afterConstruct and forceClose
        InterfaceCoverageDataPort cdp = new InterfaceCoverageDataPort();
        cdp.connect((NatsConnection) null, new NatsUri(Options.DEFAULT_URL), 0);
        cdp.afterConstruct(null);
        cdp.forceClose();

        assertTrue(cdp.connectCallsDefaultConnect.get());
        assertTrue(cdp.afterConstruct.get());
        assertTrue(cdp.forceCloseCallsClose.get());
    }

    static class InterfaceCoverageDataPort implements DataPort {
        AtomicBoolean connectCallsDefaultConnect = new AtomicBoolean();
        AtomicBoolean afterConstruct = new AtomicBoolean();
        AtomicBoolean forceCloseCallsClose = new AtomicBoolean();

        @Override
        public void connect(@NonNull String serverURI, @NonNull NatsConnection conn, long timeoutNanos) throws IOException {
            connectCallsDefaultConnect.set(true);
        }

        @Override
        public void afterConstruct(@NonNull Options options) {
            afterConstruct.set(true);
            DataPort.super.afterConstruct(options);
        }

        @Override
        public void upgradeToSecure() throws IOException {}

        @Override
        public int read(byte[] dst, int off, int len) throws IOException {
            return 0;
        }

        @Override
        public void write(byte[] src, int toWrite) throws IOException {
        }

        @Override
        public void shutdownInput() throws IOException {}

        @Override
        public void close() throws IOException {
            forceCloseCallsClose.set(true);
        }

        @Override
        public void flush() throws IOException {
        }
    }

    @Test
    public void testSocketDataPort() throws IOException, URISyntaxException {
        // this is coverage for connect, afterConstruct and forceClose
        InterfaceCoverageDataPort cdp = new InterfaceCoverageDataPort();
        //noinspection DataFlowIssue
        cdp.connect((NatsConnection) null, new NatsUri(Options.DEFAULT_URL), 0);
        //noinspection DataFlowIssue
        cdp.afterConstruct(null);
        cdp.forceClose();

        assertTrue(cdp.connectCallsDefaultConnect.get());
        assertTrue(cdp.afterConstruct.get());
        assertTrue(cdp.forceCloseCallsClose.get());
    }
}
