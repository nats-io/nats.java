// Copyright 2015-2018 The NATS Authors
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test for proxy hostname resolution bug fix.
 *
 * When a proxy is configured with domain name whitelisting, the client should NOT
 * resolve the hostname to an IP address before connecting. Instead, it should pass
 * the hostname as-is to the proxy so the proxy can enforce domain whitelisting.
 */
public class SocketDataPortProxyHostnameTest extends TestBase {

    /**
     * Mock proxy that tracks whether it received the CONNECT request with
     * a domain name or an IP address.
     */
    static class WhitelistingProxyServer implements Runnable {
        private final ServerSocket serverSocket;
        private volatile String receivedHost;
        private final CountDownLatch hostReceivedLatch = new CountDownLatch(1);
        private final ExecutorService executor;

        WhitelistingProxyServer(ExecutorService executor) throws IOException {
            this.executor = executor;
            // Bind to 127.0.0.1 explicitly to avoid IPv4/IPv6 mismatch
            this.serverSocket = new ServerSocket(0, 10, InetAddress.getByName("127.0.0.1"));
        }

        public int getPort() {
            return serverSocket.getLocalPort();
        }

        public String awaitReceivedHost(long timeout, TimeUnit unit) throws InterruptedException {
            hostReceivedLatch.await(timeout, unit);
            return receivedHost;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    Socket clientSocket = serverSocket.accept();
                    executor.submit(() -> handleClientConnection(clientSocket));
                }
            } catch (IOException e) {
                // Expected when shutting down
            }
        }

        private void handleClientConnection(Socket clientSocket) {
            try (Socket client = clientSocket;
                 InputStream in = client.getInputStream();
                 OutputStream out = client.getOutputStream()) {

                // Read CONNECT request line
                String connectRequest = readLine(in);

                if (connectRequest != null && connectRequest.startsWith("CONNECT ")) {
                    // Parse: CONNECT host:port HTTP/1.x
                    String[] parts = connectRequest.split("\\s+");
                    if (parts.length >= 2) {
                        String hostPort = parts[1];
                        String[] hostPortParts = hostPort.split(":");
                        if (hostPortParts.length >= 1) {
                            receivedHost = hostPortParts[0];
                            hostReceivedLatch.countDown();
                        }
                    }

                    // Read and discard headers until empty line
                    String line;
                    while ((line = readLine(in)) != null && !line.isEmpty()) {
                        // consume headers
                    }

                    // Send 200 OK response
                    out.write("HTTP/1.1 200 Connection Established\r\n".getBytes());
                    out.write("Content-Length: 0\r\n".getBytes());
                    out.write("\r\n".getBytes());
                    out.flush();

                    // Keep the connection open for a bit so the client can use it
                    Thread.sleep(1000);
                }
            } catch (IOException | InterruptedException e) {
                // Connection closed or error
            }
        }

        private String readLine(InputStream in) throws IOException {
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = in.read()) != -1) {
                if (ch == '\r') {
                    continue;
                }
                if (ch == '\n') {
                    return sb.toString();
                }
                sb.append((char) ch);
            }
            return sb.length() > 0 ? sb.toString() : null;
        }

        public void shutdown() throws IOException {
            serverSocket.close();
        }
    }

    /**
     * Test that when a proxy is configured and a domain name (non-IP) is used,
     * the SocketDataPort preserves the hostname instead of resolving it to IP.
     * This allows proxies with domain whitelisting to work correctly.
     */
    @Test
    public void testProxyReceivesDomainNameWithEnableInetAddressCreateUnresolved() throws Exception {
        testProxyHostnameResolution(
            true,  // enableNoResolveHostnames
            "nats://localhost:4222",
            false  // expectIpAddress
        );
    }

    /**
     * Test that WITHOUT isEnableInetAddressCreateUnresolved(), the proxy receives an IP address instead
     * of the domain name. This demonstrates the bug that was fixed.
     *
     * When isEnableInetAddressCreateUnresolved() is NOT set and a proxy is configured, the hostname
     * gets resolved to an IP address before being sent to the proxy. This breaks
     * proxies with domain name whitelisting.
     */
    @Test
    public void testProxyReceivesIpAddressWithoutEnableInetAddressCreateUnresolved() throws Exception {
        testProxyHostnameResolution(
            false, // disableNoResolveHostnames
            "nats://localhost:4222",
            true   // expectIpAddress
        );
    }

    /**
     * Helper method to test proxy hostname resolution behavior.
     *
     * @param useEnableInetAddressCreateUnresolved Whether to enable isEnableInetAddressCreateUnresolved() option
     * @param targetUri The URI to connect to
     * @param expectIpAddress True if expecting proxy to receive an IP, false for hostname
     */
    private void testProxyHostnameResolution(boolean useEnableInetAddressCreateUnresolved, String targetUri,
                                            boolean expectIpAddress)
            throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        WhitelistingProxyServer proxyServer = new WhitelistingProxyServer(executor);

        try {
            executor.submit(proxyServer);

            Options.Builder optionsBuilder = new Options.Builder()
                .proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("127.0.0.1", proxyServer.getPort())))
                .noReconnect();

            if (useEnableInetAddressCreateUnresolved) {
                optionsBuilder.enableInetAddressCreateUnresolved();
            }

            Options options = optionsBuilder.build();
            MockNatsConnection mockConnection = new MockNatsConnection(options);
            SocketDataPort dataPort = new SocketDataPort();
            NatsUri nuri = new NatsUri(targetUri);

            try {
                dataPort.connect(mockConnection, nuri, 5_000_000_000L);
            } catch (Exception e) {
                // Expected - connection will fail since there's no real server
            }

            String receivedHost = proxyServer.awaitReceivedHost(5, TimeUnit.SECONDS);
            assertNotNull(receivedHost, "Proxy never received a CONNECT request");
            if (expectIpAddress) {
                assertTrue(
                    isIpAddress(receivedHost),
                    "Expected IP address but proxy received: " + receivedHost
                );
            } else {
                assertFalse(
                    isIpAddress(receivedHost),
                    "Expected hostname but proxy received IP: " + receivedHost
                );
            }
        } finally {
            safeShutdown(proxyServer, executor);
        }
    }

    /**
     * Safely shutdown the proxy server and executor.
     */
    private void safeShutdown(WhitelistingProxyServer proxyServer, ExecutorService executor) {
        try {
            proxyServer.shutdown();
        } catch (IOException e) {
            // ignore
        }
        executor.shutdown();
    }

    /**
     * Check if a string is an IP address (IPv4 or IPv6)
     */
    private boolean isIpAddress(String host) {
        if (host == null) {
            return false;
        }
        try {
            InetAddress.getByName(host);
            // If getByName doesn't throw and we only got back an IP, it's likely an IP
            // This is a simple heuristic check
            return host.matches("^\\d+\\.\\d+\\.\\d+\\.\\d+$") || host.startsWith("[");
        } catch (UnknownHostException e) {
            return false;
        }
    }
}
