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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.Nats;

public class InfoHandlerTests {
    @Test
    public void testInitialInfo() throws IOException, InterruptedException {
        String customInfo = "{\"server_id\":\"myid\"}";

        try (NatsServerProtocolMock ts = new NatsServerProtocolMock(null, customInfo)) {
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals("got custom info", "myid", ((NatsConnection) nc).getInfo().getServerId());
                assertEquals(customInfo, ((NatsConnection) nc).getInfo().getRawJson());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testUnsolicitedInfo() throws IOException, InterruptedException, ExecutionException {
        String customInfo = "{\"server_id\":\"myid\"}";
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
            Connection nc = Nats.connect(ts.getURI());
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                assertEquals("got custom info", "myid", ((NatsConnection) nc).getInfo().getServerId());
                sendInfo.complete(Boolean.TRUE);

                assertTrue("Got pong.", gotPong.get().booleanValue()); // Server round tripped so we should have new info
                assertEquals("got replacement info", "replacement", ((NatsConnection) nc).getInfo().getServerId());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }
}