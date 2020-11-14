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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class PublishTests {
    @Test
    public void throwsIfClosedOnPublish() {
        assertThrows(IllegalStateException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.close();
                nc.publish("subject", "replyto", null);
                assertFalse(true);
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
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowsWithoutSubject() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.publish(null, null);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowsWithoutReplyTo() {
        assertThrows(IllegalArgumentException.class, () -> {
            try (NatsTestServer ts = new NatsTestServer(false);
                        Connection nc = Nats.connect(ts.getURI())) {
                nc.publish("subject", "", null);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testThrowsIfTooBig() {
        assertThrows(IllegalArgumentException.class, () -> {
            String customInfo = "{\"server_id\":\"myid\",\"max_payload\": 1000}";

            try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo);
                        Connection nc = Nats.connect(ts.getURI())) {
                assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

                byte[] body = new byte[1001];
                nc.publish("subject", null, null, body);
                assertFalse(true);
            }
        });
    }

    @Test
    public void testEmptyPublish() throws IOException, InterruptedException,ExecutionException {
        runSimplePublishTest("testsubemptybody", null, "");
    }

    @Test
    public void testEmptyByDefaultPublish() throws IOException, InterruptedException,ExecutionException {
        runSimplePublishTest("testsubemptybody", null, null);
    }

    @Test
    public void testNoReplyPublish() throws IOException, InterruptedException,ExecutionException {
        runSimplePublishTest("testsub", null, "This is the message.");
    }

    @Test
    public void testReplyToInPublish() throws IOException, InterruptedException,ExecutionException {
        runSimplePublishTest("testsubforreply", "replyTo", "This is the message to reply to.");
    }

    public void runSimplePublishTest(String subject, String replyTo, String bodyString) throws IOException, InterruptedException,ExecutionException {
        CompletableFuture<Boolean> gotPub = new CompletableFuture<>();
        AtomicReference<String> body  = new AtomicReference<>("");
        AtomicReference<String> protocol  = new AtomicReference<>("");

        NatsServerProtocolMock.Customizer receiveMessageCustomizer = (ts, r,w) -> {
            String pubLine = "";
            String bodyLine = "";
            
            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for PUB ...");
            try {
                pubLine = r.readLine();
                bodyLine = r.readLine(); // Ignores encoding, but ok for test
            } catch(Exception e) {
                gotPub.cancel(true);
                return;
            }

            if (pubLine.startsWith("PUB")) {
                System.out.println("*** Mock Server @" + ts.getPort() + " got PUB ...");
                protocol.set(pubLine);
                body.set(bodyLine);
                gotPub.complete(Boolean.TRUE);
            }
        };

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(receiveMessageCustomizer);
                    Connection  nc = Nats.connect(ts.getURI())) {
            byte[] bodyBytes = (bodyString != null) ? bodyString.getBytes(StandardCharsets.UTF_8) : null;

            assertTrue(Connection.Status.CONNECTED == nc.getStatus(), "Connected Status");

            nc.publish(subject, replyTo, bodyBytes);

            // This is used for the default test
            if (bodyString == null) {
                bodyBytes = new byte[0];
                bodyString = "";
            }

            assertTrue(gotPub.get().booleanValue(), "Got pub."); //wait for receipt to close up
            nc.close();
            assertTrue(Connection.Status.CLOSED == nc.getStatus(), "Closed Status");

            String expectedProtocol = null;
            if (replyTo == null) {
                expectedProtocol = "PUB "+subject+" "+bodyBytes.length;
            } else {
                expectedProtocol = "PUB "+subject+" "+replyTo+" "+bodyBytes.length;
            }
            assertEquals(expectedProtocol, protocol.get(), "Protocol matches");

            assertEquals(bodyString, body.get(), "Body matches");
        }
    }
}