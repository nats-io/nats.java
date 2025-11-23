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

import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.NatsConstants.*;
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static org.junit.jupiter.api.Assertions.*;

public class PublishTests extends TestBase {
    @Test
    public void throwsIfClosed() throws Exception {
        runInSharedOwnNc(nc -> {
            nc.close();
            // can't publish after close
            assertThrows(IllegalStateException.class, () -> nc.publish("subject", "replyto", null));

            // flush after close always times out
            assertThrows(TimeoutException.class, () -> nc.flush(null));
        });
    }

    @Test
    public void testThrowsWithoutSubject() throws Exception {
        runInShared(nc -> {
            //noinspection DataFlowIssue
            assertThrows(IllegalArgumentException.class, () -> nc.publish(null, null));
        });
    }

    @Test
    public void testThrowsIfTooBig() throws Exception {
        try (NatsTestServer ts = NatsTestServer.configFileServer("max_payload.conf")) {
            Connection nc = standardConnectionWait(ts.getLocalhostUri());

            byte[] body = new byte[1024]; // 1024 is > than max_payload.conf max_payload: 1000
            assertThrows(IllegalArgumentException.class, () -> nc.publish(random(), null, null, body));
            nc.close();
            Thread.sleep(1000);

            CountDownLatch mpvLatch = new CountDownLatch(1);
            CountDownLatch seLatch = new CountDownLatch(1);
            ErrorListener el = new ErrorListener() {
                @Override
                public void errorOccurred(Connection conn, String error) {
                    if (error.contains("Maximum Payload Violation")) {
                        mpvLatch.countDown();
                    }
                }

                @Override
                public void exceptionOccurred(Connection conn, Exception exp) {
                    if (exp instanceof SocketException) {
                        seLatch.countDown();
                    }
                }
            };
            Options options = optionsBuilder(ts)
                .clientSideLimitChecks(false)
                .errorListener(el)
                .build();
            Connection nc2 = longConnectionWait(options);
            nc2.publish(random(), null, null, body);

            if (!mpvLatch.await(3, TimeUnit.SECONDS)) {
                fail();
            }
            if (!seLatch.await(3, TimeUnit.SECONDS)) {
                fail();
            }
        }
    }

    @Test
    public void testThrowsIfHeadersNotSupported() {
        assertThrows(IllegalArgumentException.class, () -> {
            String customInfo = "{\"server_id\":\"test\", \"version\":\"9.9.99\"}";

            try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(null, customInfo);
                 Connection nc = Nats.connect(mockTs.getMockUri()))
            {
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

        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(receiveMessageCustomizer);
             Connection nc = standardConnectionWait(mockTs.getMockUri())) {

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
        runInSharedOwnNc(standardOptionsBuilder().noReconnect(), nc -> {
            int maxPayload = (int)nc.getServerInfo().getMaxPayload();
            nc.publish("mptest", new byte[maxPayload-1]);
            nc.publish("mptest", new byte[maxPayload]);
        });

        try {
            runInSharedOwnNc(standardOptionsBuilder().noReconnect().clientSideLimitChecks(false), nc -> {
                int maxPayload = (int)nc.getServerInfo().getMaxPayload();
                for (int x = 1; x < 1000; x++) {
                    nc.publish("mptest", new byte[maxPayload + x]);
                }
            });
            fail("Expecting IllegalStateException");
        }
        catch (IllegalStateException ignore) {}

        try {
            runInSharedOwnNc(standardOptionsBuilder().noReconnect(), nc -> {
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
        String jsSubject = random() + "-" + subject; // just to have a different;

        AtomicReference<String> coreReceivedSubjectNotSupported = new AtomicReference<>();
        AtomicReference<String> coreReceivedSubjectWhenSupported = new AtomicReference<>();
        AtomicReference<String> jsReceivedSubjectNotSupported = new AtomicReference<>();
        AtomicReference<String> jsReceivedSubjectWhenSupported = new AtomicReference<>();
        CountDownLatch coreReceivedLatchNotSupported = new CountDownLatch(1);
        CountDownLatch coreReceivedLatchWhenSupported = new CountDownLatch(1);
        CountDownLatch jsReceivedLatchNotSupported = new CountDownLatch(1);
        CountDownLatch jsReceivedLatchWhenSupported = new CountDownLatch(1);

        Options.Builder ncNotSupportedOptionsBuilder = optionsBuilder().noReconnect().clientSideLimitChecks(false);
        runInSharedOwnNc(ncNotSupportedOptionsBuilder, ncNotSupported -> {
            Options ncSupportedOptions = optionsBuilder(ncNotSupported.getConnectedUrl()).supportUTF8Subjects().build();
            try (Connection ncSupported = standardConnectionWait(ncSupportedOptions)) {
                ncNotSupported.jetStreamManagement().addStream(
                    StreamConfiguration.builder()
                        .name(random())
                        .storageType(StorageType.Memory)
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
        });
    }
}
