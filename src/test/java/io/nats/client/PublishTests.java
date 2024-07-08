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

package io.nats.client;

import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import org.junit.jupiter.api.Test;

import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.NatsConstants.*;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public class PublishTests {
    @Test
    public void throwsIfClosedOnPublish() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.close();
                nc.publish("subject", "replyto", null);
                fail();
            }
        });
    }

    @Test
    public void throwsIfClosedOnFlush() {
        assertThrows(TimeoutException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.close();
                nc.flush(null);
                fail();
            }
        });
    }

    @Test
    public void testThrowsWithoutSubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.publish(null, null);
                fail();
            }
        });
    }

    @Test
    public void testThrowsIfTooBig() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/max_payload.conf", false, false))
        {
            Connection nc = Nats.connect(ts.getURI());
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

            byte[] body = new byte[1001];
            assertThrows(IllegalArgumentException.class, () -> nc.publish("subject", null, null, body));
            nc.close();

            AtomicBoolean mpv = new AtomicBoolean(false);
            AtomicBoolean se = new AtomicBoolean(false);
            ErrorListener el = new ErrorListener() {
                @Override
                public void errorOccurred(Connection conn, String error) {
                    mpv.set(error.contains("Maximum Payload Violation"));
                }

                @Override
                public void exceptionOccurred(Connection conn, Exception exp) {
                    se.set(exp instanceof SocketException);
                }
            };
            Options options = Options.builder()
                .server(ts.getURI())
                .clientSideLimitChecks(false)
                .errorListener(el)
                .build();
            Connection nc2 = Nats.connect(options);
            assertSame(Connection.Status.CONNECTED, nc2.getStatus(), "Connected Status");
            nc2.publish("subject", null, null, body);

            sleep(100);
            assertTrue(mpv.get());
            assertTrue(se.get());
        }
    }

    @Test
    public void testThrowsIfheadersNotSupported() {
        assertThrows(IllegalArgumentException.class, () -> {
            String customInfo = "{\"server_id\":\"test\", \"version\":\"9.9.99\"}";

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo);
                 Connection nc = Nats.connect(ts.getURI())) {
                assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

                nc.publish(NatsMessage.builder()
                        .subject("testThrowsIfheadersNotSupported")
                        .headers(new Headers().add("key", "value"))
                        .build());
                fail();
            }
        });
    }

    @Test
    public void testEmptyPublish() throws Exception {
        runSimplePublishTest("testsubemptybody", null, null, "");
    }

    @Test
    public void testEmptyByDefaultPublish() throws Exception {
        runSimplePublishTest("testsubemptybody", null, null, null);
    }

    @Test
    public void testNoReplyPublish() throws Exception {
        runSimplePublishTest("testsub", null, null, "This is the message.");
    }

    @Test
    public void testReplyToInPublish() throws Exception {
        runSimplePublishTest("testsubforreply", "replyTo", null, "This is the message to reply to.");
        runSimplePublishTest("testsubforreply", "replyTo", new Headers().add("key", "value"), "This is the message to reply to.");
    }

    private void runSimplePublishTest(String subject, String replyTo, Headers headers, String bodyString)
            throws Exception {
        CompletableFuture<Boolean> gotPub = new CompletableFuture<>();
        AtomicReference<String> hdrProto  = new AtomicReference<>("");
        AtomicReference<String> body  = new AtomicReference<>("");
        AtomicReference<String> protocol  = new AtomicReference<>("");

        boolean hPub = headers != null && !headers.isEmpty();
        String proto = hPub ? OP_HPUB : OP_PUB;
        int hdrlen = hPub ? headers.serializedLength() : 0;

        NatsServerProtocolMock.Customizer receiveMessageCustomizer = (ts, r,w) -> {
            String pubLine;
            StringBuilder headerLine;
            String bodyLine;
            
            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for " + proto + " ...");
            try {
                pubLine = r.readLine();
                if (hPub) {
                    // the version \r\n, each header \r\n, then separator \r\n
                    headerLine = new StringBuilder(r.readLine()).append("\r\n");
                    while (headerLine.length() < hdrlen) {
                        headerLine.append(r.readLine()).append("\r\n");
                    }
                }
                else {
                    headerLine = new StringBuilder();
                }
                bodyLine = r.readLine(); // Ignores encoding, but ok for test
            } catch(Exception e) {
                gotPub.cancel(true);
                return;
            }

            if (pubLine.startsWith(proto)) {
                System.out.println("*** Mock Server @" + ts.getPort() + " got " + proto + " ...");
                protocol.set(pubLine);
                hdrProto.set(headerLine.toString());
                body.set(bodyLine);
                gotPub.complete(Boolean.TRUE);
            }
        };

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(receiveMessageCustomizer);
             Connection nc = standardConnection(ts.getURI())) {

            byte[] bodyBytes;
            if (bodyString == null || bodyString.isEmpty()) {
                bodyBytes = EMPTY_BODY;
                bodyString = "";
            }
            else {
                bodyBytes = bodyString.getBytes(StandardCharsets.UTF_8);
            }

            nc.publish(NatsMessage.builder().subject(subject).replyTo(replyTo).headers(headers).data(bodyBytes).build());

            assertTrue(gotPub.get(), "Got " + proto + "."); //wait for receipt to close up
            standardCloseConnection(nc);

            if (proto.equals(OP_PUB)) {
                String expectedProtocol;
                if (replyTo == null) {
                    expectedProtocol = proto + " " + subject + " " + bodyBytes.length;
                } else {
                    expectedProtocol = proto + " " + subject + " " + replyTo + " " + bodyBytes.length;
                }
                assertEquals(expectedProtocol, protocol.get(), "Protocol matches");
                assertEquals(bodyString, body.get(), "Body matches");
            }
            else {
                String expectedProtocol;
                int hdrLen = headers.serializedLength();
                int totLen = hdrLen + bodyBytes.length;
                if (replyTo == null) {
                    expectedProtocol = proto + " " + subject + " " + hdrLen + " " + totLen;
                } else {
                    expectedProtocol = proto + " " + subject + " " + replyTo + " " + hdrLen + " " + totLen;
                }
                assertEquals(expectedProtocol, protocol.get(), "Protocol matches");
                assertEquals(bodyString, body.get(), "Body matches");
                assertEquals(new String(headers.getSerialized()), hdrProto.get());
            }
        }
    }

    @Test
    public void testMaxPayload() throws Exception {
        runInServer(standardOptionsBuilder().noReconnect(), nc -> {
            int maxPayload = (int)nc.getServerInfo().getMaxPayload();
            nc.publish("mptest", new byte[maxPayload-1]);
            nc.publish("mptest", new byte[maxPayload]);
        });

        try {
            runInServer(standardOptionsBuilder().noReconnect().clientSideLimitChecks(false), nc -> {
                int maxPayload = (int)nc.getServerInfo().getMaxPayload();
                for (int x = 1; x < 1000; x++) {
                    nc.publish("mptest", new byte[maxPayload + x]);
                }
            });
            fail("Expecting IllegalStateException");
        }
        catch (IllegalStateException ignore) {}

        try {
            runInServer(standardOptionsBuilder().noReconnect(), nc -> {
                int maxPayload = (int)nc.getServerInfo().getMaxPayload();
                for (int x = 1; x < 1000; x++) {
                    nc.publish("mptest", new byte[maxPayload + x]);
                }
            });
            fail("Expecting IllegalArgumentException");
        }
        catch (IllegalArgumentException ignore) {}
    }

    @Test
    public void testUtf8Subjects() throws Exception {
        String subject = dataAsLines("utf8-test-strings.txt").get(0);
        String jsSubject = variant() + "-" + subject; // just to have a different;

        AtomicReference<String> coreReceivedSubjectNotSupported = new AtomicReference<>();
        AtomicReference<String> coreReceivedSubjectWhenSupported = new AtomicReference<>();
        AtomicReference<String> jsReceivedSubjectNotSupported = new AtomicReference<>();
        AtomicReference<String> jsReceivedSubjectWhenSupported = new AtomicReference<>();
        CountDownLatch coreReceivedLatchNotSupported = new CountDownLatch(1);
        CountDownLatch coreReceivedLatchWhenSupported = new CountDownLatch(1);
        CountDownLatch jsReceivedLatchNotSupported = new CountDownLatch(1);
        CountDownLatch jsReceivedLatchWhenSupported = new CountDownLatch(1);

        try (NatsTestServer ts = new NatsTestServer(false, true);
             Connection ncNotSupported = standardConnection(standardOptionsBuilder(ts.getURI()).build());
             Connection ncSupported = standardConnection(standardOptionsBuilder(ts.getURI()).supportUTF8Subjects().build()))
        {
            try {
                ncNotSupported.jetStreamManagement().addStream(
                    StreamConfiguration.builder()
                        .name(stream())
                        .subjects(jsSubject)
                        .build());
                JetStream jsNotSupported = ncNotSupported.jetStream();
                JetStream jsSupported = ncNotSupported.jetStream();

                Dispatcher dNotSupported = ncNotSupported.createDispatcher();
                Dispatcher dSupported = ncSupported.createDispatcher();

                dNotSupported.subscribe(subject, m -> {
                    coreReceivedSubjectNotSupported.set(m.getSubject());
                    coreReceivedLatchNotSupported.countDown();
                });

                dSupported.subscribe(subject, m -> {
                    coreReceivedSubjectWhenSupported.set(m.getSubject());
                    coreReceivedLatchWhenSupported.countDown();
                });

                jsNotSupported.subscribe(jsSubject, dNotSupported, m -> {
                    jsReceivedSubjectNotSupported.set(m.getSubject());
                    jsReceivedLatchNotSupported.countDown();
                }, false);

                jsSupported.subscribe(jsSubject, dSupported, m -> {
                    jsReceivedSubjectWhenSupported.set(m.getSubject());
                    jsReceivedLatchWhenSupported.countDown();
                }, false);

                ncNotSupported.publish(subject, null); // demonstrates that publishing always does utf8
                jsSupported.publish(jsSubject, null);

                assertTrue(coreReceivedLatchNotSupported.await(1, TimeUnit.SECONDS));
                assertTrue(coreReceivedLatchWhenSupported.await(1, TimeUnit.SECONDS));
                assertTrue(jsReceivedLatchNotSupported.await(1, TimeUnit.SECONDS));
                assertTrue(jsReceivedLatchWhenSupported.await(1, TimeUnit.SECONDS));

                assertNotEquals(subject, coreReceivedSubjectNotSupported.get());
                assertEquals(subject, coreReceivedSubjectWhenSupported.get());
                assertNotEquals(jsSubject, jsReceivedSubjectNotSupported.get());
                assertEquals(jsSubject, jsReceivedSubjectWhenSupported.get());

            }
            finally {
                cleanupJs(ncSupported);
            }
        }
    }
}
