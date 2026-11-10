// Copyright 2015-2026 The NATS Authors
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

import io.nats.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

public class InfoHandlerTests {

    // A server that writes the payload to its first client and then closes, the way a full nats-server
    // answers with INFO followed by -ERR 'maximum connections exceeded'.
    static ServerSocket serverThatWritesThenCloses(String payload) throws IOException {
        ServerSocket ss = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        Thread t = new Thread(() -> {
            try (Socket s = ss.accept()) {
                s.getOutputStream().write(payload.getBytes(StandardCharsets.UTF_8));
                s.getOutputStream().flush();
                Thread.sleep(500); // let the client read before the close
            }
            catch (Exception ignore) {
                // the test asserts on the client side
            }
        });
        t.setDaemon(true);
        t.start();
        return ss;
    }

    @Test
    public void testDataAfterInfoOnAPlaintextConnectionIsProtocol() throws Exception {
        // On a plaintext connection, bytes that arrive together with the INFO line are protocol and are parsed
        // by the reader. A full server's -ERR reaches the error listener instead of failing the read of INFO.
        String payload = "INFO {\"server_id\":\"test\",\"version\":\"2.11.0\"}\r\n-ERR 'maximum connections exceeded'\r\n";
        try (ServerSocket ss = serverThatWritesThenCloses(payload)) {
            ListenerForTesting listener = new ListenerForTesting();
            Options options = new Options.Builder()
                .server("nats://localhost:" + ss.getLocalPort())
                .maxReconnects(0)
                .connectionTimeout(Duration.ofMillis(500))
                .errorListener(listener)
                .build();
            assertThrows(IOException.class, () -> Nats.connect(options));
            assertTrue(listener.getErrors().stream().anyMatch(e -> e.contains("maximum connections exceeded")),
                "errors: " + listener.getErrors() + " exceptions: " + listener.getExceptions());
            assertTrue(listener.getExceptions().stream().noneMatch(e -> String.valueOf(e.getMessage()).contains("Read past initial info")),
                listener.getExceptions().toString());
        }
    }

    @Test
    public void testQuoteUntrusted() {
        // first line only, quoted
        assertEquals("\"-ERR 'maximum connections exceeded'\"",
            NatsConnection.quoteUntrusted("-ERR 'maximum connections exceeded'\r\nPING\r\n".getBytes(StandardCharsets.UTF_8)));
        // quotes, backslashes, tabs and every other control character are escaped, so a terminal escape sequence is inert
        assertEquals("\"a\\u001b[31mb\\tc\\\"d\\\\e\\u0007\"",
            NatsConnection.quoteUntrusted("a\u001b[31mb\tc\"d\\e\u0007".getBytes(StandardCharsets.UTF_8)));
        // cut at 256 bytes
        byte[] big = new byte[300];
        Arrays.fill(big, (byte) 'x');
        String quoted = NatsConnection.quoteUntrusted(big);
        assertTrue(quoted.endsWith("\" (cut at 256 bytes)"), quoted);
        assertEquals(256 + 2 + " (cut at 256 bytes)".length(), quoted.length());
        // empty
        assertEquals("\"\"", NatsConnection.quoteUntrusted(new byte[0]));
    }

    @Test
    public void testInitialInfo() throws IOException, InterruptedException {
        String customInfo = "{\"server_id\":\"myid\", \"version\":\"9.9.99\"}";

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo)) {
            Connection nc = Nats.connect(ts.getNatsLocalhostUri());
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                assertEquals("myid", nc.getServerInfo().getServerId(), "got custom info");
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }



    @Test
    public void testUnsolicitedInfo() throws IOException, InterruptedException, ExecutionException {
        String customInfo = "{\"server_id\":\"myid\", \"version\":\"9.9.99\"}";
        CompletableFuture<Boolean> gotPong = new CompletableFuture<>();
        CompletableFuture<Boolean> sendInfo = new CompletableFuture<>();

        NatsServerProtocolMock.Customizer infoCustomizer = (ts, r, w) -> {

            // Wait for client to be ready.
            try {
                sendInfo.get();
            } catch (Exception e) {
                // return, we will fail the test
                gotPong.cancel(true);
                return;
            }

            System.out.println("*** Mock Server @" + ts.getPort() + " sending INFO ...");
            w.write("INFO {\"server_id\":\"replacement\", \"version\":\"9.9.99\"}\r\n");
            w.flush();

            System.out.println("*** Mock Server @" + ts.getPort() + " sending PING ...");
            w.write("PING\r\n");
            w.flush();

            String pong = "";

            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for PONG ...");
            try {
                pong = r.readLine();
            } catch (Exception e) {
                gotPong.cancel(true);
                return;
            }

            if (pong != null && pong.startsWith("PONG")) {
                System.out.println("*** Mock Server @" + ts.getPort() + " got PONG ...");
                gotPong.complete(Boolean.TRUE);
            } else {
                System.out.println("*** Mock Server @" + ts.getPort() + " got something else... " + pong);
                gotPong.complete(Boolean.FALSE);
            }
        };

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(infoCustomizer, customInfo)) {
            Connection nc = Nats.connect(ts.getNatsLocalhostUri());
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                assertEquals("myid", nc.getServerInfo().getServerId(), "got custom info");
                sendInfo.complete(Boolean.TRUE);

                assertTrue(gotPong.get().booleanValue(), "Got pong."); // Server round tripped so we should have new info
                assertEquals("replacement", nc.getServerInfo().getServerId(), "got replacement info");
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }
    }



    @Test
    public void testLDM() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        String customInfo = "{\"server_id\":\"myid\", \"version\":\"9.9.99\", \"ldm\":true}";
        CompletableFuture<Boolean> gotPong = new CompletableFuture<>();
        CompletableFuture<Boolean> sendInfo = new CompletableFuture<>();
        CompletableFuture<ConnectionListener.Events> connectLDM = new CompletableFuture<>();

        NatsServerProtocolMock.Customizer infoCustomizer = (ts, r, w) -> {

            // Wait for client to be ready.
            try {
                sendInfo.get();
            } catch (Exception e) {
                // return, we will fail the test
                gotPong.cancel(true);
                return;
            }

            System.out.println("*** Mock Server @" + ts.getPort() + " sending INFO ...");
            w.write("INFO {\"server_id\":\"replacement\"}\r\n");
            w.flush();

            System.out.println("*** Mock Server @" + ts.getPort() + " sending PING ...");
            w.write("PING\r\n");
            w.flush();

            String pong = "";

            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for PONG ...");
            try {
                pong = r.readLine();
            } catch (Exception e) {
                gotPong.cancel(true);
                return;
            }

            if (pong != null && pong.startsWith("PONG")) {
                System.out.println("*** Mock Server @" + ts.getPort() + " got PONG ...");
                gotPong.complete(Boolean.TRUE);
            } else {
                System.out.println("*** Mock Server @" + ts.getPort() + " got something else... " + pong);
                gotPong.complete(Boolean.FALSE);
            }
        };

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(infoCustomizer, customInfo)) {

            Options options = new Options.Builder().server(ts.getNatsLocalhostUri()).connectionListener(new ConnectionListener() {
                @Override
                public void connectionEvent(Connection conn, Events type) {
                    if (type.equals(Events.LAME_DUCK)) connectLDM.complete(type);
                }
            }).build();

            Connection nc = Nats.connect(options);
            try {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");
                assertEquals("myid", nc.getServerInfo().getServerId(), "got custom info");
                sendInfo.complete(Boolean.TRUE);

                assertTrue(gotPong.get(), "Got pong."); // Server round tripped so we should have new info
                assertEquals("replacement", nc.getServerInfo().getServerId(), "got replacement info");
            } finally {
                nc.close();
                assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");
            }
        }

        ConnectionListener.Events event = connectLDM.get(5, TimeUnit.SECONDS);
        assertEquals(event, ConnectionListener.Events.LAME_DUCK);
        System.out.println(event);
    }
}