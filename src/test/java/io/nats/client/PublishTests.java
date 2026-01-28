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

import io.nats.client.ConnectionListener.Events;
import io.nats.client.impl.Headers;
import io.nats.client.impl.JetStreamTestingContext;
import io.nats.client.impl.NatsMessage;
import io.nats.client.support.Listener;
import io.nats.client.utils.TestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.support.NatsConstants.*;
import static io.nats.client.utils.ConnectionUtils.*;
import static io.nats.client.utils.OptionsUtils.options;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static org.junit.jupiter.api.Assertions.*;

public class PublishTests extends TestBase {
    @Test
    public void throwsIfClosed() throws Exception {
        runInSharedOwnNc(nc -> {
            nc.close();
            // can't publish after close
            assertThrows(IllegalStateException.class, () -> nc.publish(random(), random(), null));

            // flush after close always times out
            assertThrows(TimeoutException.class, () -> nc.flush(null));

            // a normal api call after close
            assertThrows(IOException.class, nc::RTT);
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
        byte[] body = new byte[1024]; // 1024 is > than max_payload.conf max_payload: 1000
        runInConfiguredServer("max_payload.conf", ts -> {
            try (Connection nc = managedConnect(options(ts))) {
                assertThrows(IllegalArgumentException.class, () -> nc.publish(random(), null, null, body));
            }

            Listener listener = new Listener();
            Options options = optionsBuilder(ts)
                .clientSideLimitChecks(false)
                .errorListener(listener)
                .build();
            try (Connection nc = managedConnect(options)) {
                listener.queueError("Maximum Payload Violation");
                listener.queueException(SocketException.class);
                nc.publish(random(), null, null, body);
                listener.validateForAny(); // sometimes the exception comes in before the error and the error never comes, so validate for either.
            }
        });
    }

    @Test
    public void testThrowsIfTooBig() throws Exception {
        try (NatsTestServer ts = new NatsTestServer("src/test/resources/max_payload.conf", false, false))
        {
            Connection nc = Nats.connect(ts.getURI());
            assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");

            byte[] body1001 = new byte[1001];
            assertThrows(IllegalArgumentException.class, () -> nc.publish("subject", null, null, body1001));

            byte[] body977 = new byte[977];
            Headers h = new Headers();
            h.put("abcd", "12345"); // NATS/1.0\r\nabcd:12345\r\n\r\n 24 characters 971 + 24 = 1001
            assertThrows(IllegalArgumentException.class, () -> nc.publish("subject", null, h, body977));

            nc.close();

            AtomicBoolean mpv = new AtomicBoolean(false);
            AtomicBoolean se = new AtomicBoolean(false);
            ErrorListener el = new ErrorListener() {
                @Override
                public void errorOccurred(Connection conn, String error) {
                    mpv.set(error.contains("Maximum Payload Violation"));
                }

            Listener listener = new Listener();
            Options options = optionsBuilder(ts)
                .clientSideLimitChecks(false)
                .errorListener(listener)
                .build();
            Connection nc2 = Nats.connect(options);
            assertSame(Connection.Status.CONNECTED, nc2.getStatus(), "Connected Status");
            nc2.publish("subject", null, null, body1001);

            sleep(250);
            assertTrue(mpv.get());
            assertTrue(se.get());
        }
    }

    @Test
    public void testThrowsIfHeadersNotSupported() throws Exception {
        String customInfo = "{\"server_id\":\"test\", \"version\":\"9.9.99\"}";
        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(null, customInfo)) {
            try (Connection nc = standardConnect(mockTs)) {
                assertThrows(IllegalArgumentException.class,
                    () -> nc.publish(NatsMessage.builder()
                        .subject("testThrowsIfheadersNotSupported")
                        .headers(new Headers().add("key", "value"))
                        .build()));
            }
        }
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
            
            // System.out.println("*** Mock Server @" + ts.getPort() + " waiting for " + proto + " ...");
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
                // System.out.println("*** Mock Server @" + ts.getPort() + " got " + proto + " ...");
                protocol.set(pubLine);
                hdrProto.set(headerLine.toString());
                body.set(bodyLine);
                gotPub.complete(Boolean.TRUE);
            }
        };

        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(receiveMessageCustomizer)) {
            try (Connection nc = standardConnect(mockTs)) {
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
                closeAndConfirm(nc);

                if (proto.equals(OP_PUB)) {
                    String expectedProtocol;
                    if (replyTo == null) {
                        expectedProtocol = proto + " " + subject + " " + bodyBytes.length;
                    }
                    else {
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
                    }
                    else {
                        expectedProtocol = proto + " " + subject + " " + replyTo + " " + hdrLen + " " + totLen;
                    }
                    assertEquals(expectedProtocol, protocol.get(), "Protocol matches");
                    assertEquals(bodyString, body.get(), "Body matches");
                    assertEquals(new String(headers.getSerialized()), hdrProto.get());
                }
            }
        }
    }

    @Test
    public void testMaxPayload() throws Exception {
        runInSharedOwnNc(optionsBuilder().noReconnect(), nc -> {
            int maxPayload = (int)nc.getServerInfo().getMaxPayload();
            nc.publish(random(), new byte[maxPayload - 1]);
            assertThrows(IllegalArgumentException.class, () -> nc.publish(random(), new byte[maxPayload + 1]));
        });
    }

    @Test
    public void testMaxPayloadNoClientSideLimitChecks() throws Exception {
        Listener listener = new Listener();
        Options.Builder builder = optionsBuilder()
            .noReconnect()
            .clientSideLimitChecks(false)
            .errorListener(listener)
            .connectionListener(listener);

        runInSharedOwnNc(builder, nc -> {
            listener.queueError("Maximum Payload Violation");
            listener.queueConnectionEvent(Events.DISCONNECTED);
            int maxPayload = (int)nc.getServerInfo().getMaxPayload();
            nc.publish(random(), new byte[maxPayload + 1]);
            listener.validateAll();
        });
    }

    @Test
    public void testUtf8Subjects() throws Exception {
        String utfSubject = dataAsLines("utf8-test-strings.txt").get(0);
        String jsSubject = random() + "-" + utfSubject; // just to have a different;

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
            Options ncSupportedOptions = optionsBuilder(ncNotSupported).supportUTF8Subjects().build();
            try (Connection ncSupported = managedConnect(ncSupportedOptions)) {
                try (JetStreamTestingContext ctxNotSupported = new JetStreamTestingContext(ncNotSupported, 0)) {
                    ctxNotSupported.createOrReplaceStream(jsSubject);
                    JetStream jsNotSupported = ncNotSupported.jetStream();
                    JetStream jsSupported = ncNotSupported.jetStream();

                    Dispatcher dNotSupported = ncNotSupported.createDispatcher();
                    Dispatcher dSupported = ncSupported.createDispatcher();

                    dNotSupported.subscribe(utfSubject, m -> {
                        coreReceivedSubjectNotSupported.set(m.getSubject());
                        coreReceivedLatchNotSupported.countDown();
                    });

                    dSupported.subscribe(utfSubject, m -> {
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

                    ncNotSupported.publish(utfSubject, null); // demonstrates that publishing always does utf8
                    jsSupported.publish(jsSubject, null);

                    assertTrue(coreReceivedLatchNotSupported.await(1, TimeUnit.SECONDS));
                    assertTrue(coreReceivedLatchWhenSupported.await(1, TimeUnit.SECONDS));
                    assertTrue(jsReceivedLatchNotSupported.await(1, TimeUnit.SECONDS));
                    assertTrue(jsReceivedLatchWhenSupported.await(1, TimeUnit.SECONDS));

                    assertNotEquals(utfSubject, coreReceivedSubjectNotSupported.get());
                    assertEquals(utfSubject, coreReceivedSubjectWhenSupported.get());
                    assertNotEquals(jsSubject, jsReceivedSubjectNotSupported.get());
                    assertEquals(jsSubject, jsReceivedSubjectWhenSupported.get());
                }
            }
        });
    }
}
