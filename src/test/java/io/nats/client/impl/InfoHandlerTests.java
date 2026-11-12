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
import io.nats.client.support.UntrustedText;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
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
                .connectionTimeout(Duration.ofSeconds(5))
                .errorListener(listener)
                .build();
            // ADR-40, malformed input: a -ERR after which the server closes the connection fails the attempt with
            // the server's error, at once, not when the attempt's budget runs out
            long start = System.currentTimeMillis();
            IOException e = assertThrows(IOException.class, () -> Nats.connect(options));
            long elapsed = System.currentTimeMillis() - start;
            assertTrue(elapsed < 3000, "the attempt should end at the close, not at the 5 s budget: " + elapsed + " ms");
            boolean serverText = false;
            for (Throwable t = e; t != null; t = t.getCause()) {
                serverText |= String.valueOf(t.getMessage()).contains("maximum connections exceeded");
            }
            assertTrue(serverText, "expected the server's error in the cause chain of " + e);
            assertTrue(listener.getErrors().stream().anyMatch(err -> err.contains("maximum connections exceeded")),
                "errors: " + listener.getErrors() + " exceptions: " + listener.getExceptions());
            assertTrue(listener.getExceptions().stream().noneMatch(ex -> String.valueOf(ex.getMessage()).contains("Read past initial info")),
                listener.getExceptions().toString());
        }
    }

    @Test
    public void testInfoThatDoesNotParseIsSkippedOnAnEstablishedConnection() throws Exception {
        // ADR-40, malformed input: on an established connection an INFO that does not parse is skipped and reported,
        // and the connection stays up; the server's PING that follows is still answered. During an attempt such an
        // INFO fails the attempt
        String customInfo = "{\"server_id\":\"myid\", \"version\":\"9.9.99\"}";
        CompletableFuture<Boolean> gotPong = new CompletableFuture<>();
        CompletableFuture<Boolean> sendInfo = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer customizer = (ts, r, w) -> {
            try {
                sendInfo.get();
            }
            catch (Exception e) {
                gotPong.cancel(true);
                return;
            }
            w.write("INFO {\"server_id\":\"replacement\", \"version\": not json}\r\n");
            w.write("PING\r\n");
            w.flush();
            try {
                String pong = r.readLine();
                gotPong.complete(pong != null && pong.startsWith("PONG"));
            }
            catch (Exception e) {
                gotPong.cancel(true);
            }
        };
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(customizer, customInfo)) {
            Options options = new Options.Builder().server(ts.getNatsLocalhostUri()).errorListener(listener).build();
            try (Connection nc = Nats.connect(options)) {
                assertEquals(Connection.Status.CONNECTED, nc.getStatus());
                sendInfo.complete(Boolean.TRUE);
                assertTrue(gotPong.get(5, TimeUnit.SECONDS), "the PING after the bad INFO was answered");
                assertEquals(Connection.Status.CONNECTED, nc.getStatus());
                assertEquals("myid", nc.getServerInfo().getServerId(), "the bad INFO did not replace the server info");
                assertTrue(listener._eventually(2000, listener::getExceptions, ex -> String.valueOf(ex.getMessage()).contains("does not parse and was skipped")), listener.getExceptions().toString());
                Exception reported = listener.getExceptions().stream().filter(ex -> String.valueOf(ex.getMessage()).contains("does not parse")).findFirst().get();
                assertTrue(reported.getMessage().contains("skipped: \"{"), reported.getMessage()); // quoted, as untrusted text
            }
        }
        // during the attempt the same INFO fails the attempt
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, "{\"server_id\":\"myid\", \"version\": not json}")) {
            Options options = new Options.Builder().server(ts.getNatsLocalhostUri()).maxReconnects(0).connectionTimeout(Duration.ofSeconds(2)).build();
            assertThrows(IOException.class, () -> Nats.connect(options));
        }
    }

    @Test
    public void testHeaderBlockThatDoesNotParseFromAPublisherThroughARealServerDropsOnlyThatMessage() throws Exception {
        // nats-server relays a header block as the publisher wrote it: HPUB with a block that is not "NATS/1.0..."
        // reaches the subscriber unchanged (measured against nats-server main, 2.16.0-dev, 2026-11-11). Before this
        // change that ended the subscriber's reader without a word. Now the message is dropped and reported, the
        // connection stays up, and the next message is delivered
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsTestServer ts = new NatsTestServer(false);
             Connection nc = Nats.connect(new Options.Builder().server(ts.getURI()).errorListener(listener).build())) {
            Subscription sub = nc.subscribe("hp");
            nc.flush(Duration.ofSeconds(2));
            int port = new URI(ts.getURI()).getPort();
            try (Socket publisher = new Socket("localhost", port)) {
                publisher.setSoTimeout(3000);
                InputStream in = publisher.getInputStream();
                OutputStream out = publisher.getOutputStream();
                byte[] buf = new byte[4096];
                assertTrue(in.read(buf) > 0); // INFO
                out.write(("CONNECT {\"headers\":true,\"verbose\":false}\r\n"
                    + "HPUB hp 4 4\r\nXXXX\r\n"                   // a header block that does not parse
                    + "HPUB hp 12 14\r\nNATS/1.0\r\n\r\nok\r\n"  // a good one
                    + "PING\r\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
                StringBuilder answered = new StringBuilder();
                long end = System.currentTimeMillis() + 3000;
                while (System.currentTimeMillis() < end && answered.indexOf("PONG") < 0 && answered.indexOf("-ERR") < 0) {
                    int n = in.read(buf);
                    if (n < 0) {
                        break;
                    }
                    answered.append(new String(buf, 0, n, StandardCharsets.UTF_8));
                }
                assertTrue(answered.indexOf("PONG") >= 0, "the server took both publishes: " + answered);
            }
            Message m = sub.nextMessage(Duration.ofSeconds(2));
            assertNotNull(m, "the good message after the dropped one");
            assertEquals("ok", new String(m.getData(), StandardCharsets.UTF_8));
            assertNull(sub.nextMessage(Duration.ofMillis(200)));
            assertEquals(Connection.Status.CONNECTED, nc.getStatus());
            assertTrue(listener._eventually(2000, listener::getExceptions, ex -> String.valueOf(ex.getMessage()).contains("header block does not parse")), listener.getExceptions().toString());
        }
    }

    @Test
    public void testHeaderBlockThatDoesNotParseDropsOnlyThatMessage() throws Exception {
        // ADR-40, malformed input: a header block that does not parse drops only the message that carries it; the
        // connection stays up and the next message is delivered. The drop is reported
        CompletableFuture<Boolean> sent = new CompletableFuture<>();
        NatsServerProtocolMock.Customizer customizer = (ts, r, w) -> {
            try {
                String line;
                while ((line = r.readLine()) != null && !line.startsWith("SUB ")) {
                    // wait for the subscription
                }
                if (line == null) {
                    sent.complete(false);
                    return;
                }
                String sid = line.split(" ")[2];
                // a header block with the wrong version, then a good message on the same subscription
                w.write("HMSG s " + sid + " 12 16\r\nNATS/0.0\r\n\r\nXXXX\r\n");
                w.write("MSG s " + sid + " 2\r\nok\r\n");
                w.flush();
                sent.complete(true);
            }
            catch (Exception e) {
                sent.complete(false);
            }
        };
        ListenerForTesting listener = new ListenerForTesting();
        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(customizer, "{\"server_id\":\"myid\", \"version\":\"9.9.99\", \"headers\":true}")) {
            Options options = new Options.Builder().server(ts.getNatsLocalhostUri()).errorListener(listener).build();
            try (Connection nc = Nats.connect(options)) {
                Subscription sub = nc.subscribe("s");
                assertTrue(sent.get(5, TimeUnit.SECONDS), "the mock sent the messages");
                Message m = sub.nextMessage(Duration.ofSeconds(2));
                assertNotNull(m, "the good message after the dropped one");
                assertEquals("ok", new String(m.getData(), StandardCharsets.UTF_8));
                assertNull(sub.nextMessage(Duration.ofMillis(200)));
                assertEquals(Connection.Status.CONNECTED, nc.getStatus());
                assertTrue(listener._eventually(2000, listener::getExceptions, ex -> String.valueOf(ex.getMessage()).contains("header block does not parse")), listener.getExceptions().toString());
            }
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
        // the public form in support quotes the same way; a token keeps a line break escaped instead of ending there
        assertEquals("\"a\"", UntrustedText.quoteFirstLine("a\r\nb".getBytes(StandardCharsets.UTF_8)));
        assertEquals("\"a\\u000d\\u000ab\"", UntrustedText.quote("a\r\nb"));
        assertEquals(NatsConnection.quoteUntrusted(big), UntrustedText.quoteFirstLine(big));
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