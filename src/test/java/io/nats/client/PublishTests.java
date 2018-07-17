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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

public class PublishTests {
    @Test(expected = IllegalStateException.class)
    public void throwsIfClosedOnPublish() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.close();
            nc.publish("subject", "replyto", null);
            assertFalse(true);
        }
    }

    @Test(expected = TimeoutException.class)
    public void throwsIfClosedOnFlush() throws IOException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.close();
            nc.flush(null);
            assertFalse(true);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsWithoutSubject() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.publish(null, null);
            assertFalse(true);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsWithoutReplyTo() throws IOException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false);
                    Connection nc = Nats.connect(ts.getURI())) {
            nc.publish("subject", "", null);
            assertFalse(true);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThrowsIfTooBig() throws IOException, InterruptedException {
        String customInfo = "{\"server_id\":\"myid\",\"max_payload\": 1000}";

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo);
                    Connection nc = Nats.connect(ts.getURI())) {
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            byte[] body = new byte[1001];
            nc.publish("subject", null, body);
            assertFalse(true);
        }
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

            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());

            nc.publish(subject, replyTo, bodyBytes);

            // This is used for the default test
            if (bodyString == null) {
                bodyBytes = new byte[0];
                bodyString = "";
            }

            assertTrue("Got pub.", gotPub.get().booleanValue()); //wait for receipt to close up
            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());

            String expectedProtocol = null;
            if (replyTo == null) {
                expectedProtocol = "PUB "+subject+" "+bodyBytes.length;
            } else {
                expectedProtocol = "PUB "+subject+" "+replyTo+" "+bodyBytes.length;
            }
            assertEquals("Protocol matches", expectedProtocol, protocol.get());

            assertEquals("Body matches", bodyString, body.get());
        }
    }
}