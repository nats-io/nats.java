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

import io.nats.client.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.nats.client.utils.ConnectionUtils.assertClosed;
import static io.nats.client.utils.ConnectionUtils.assertConnected;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InfoHandlerTests {
    @Test
    public void testInitialInfo() throws IOException, InterruptedException {
        String customInfo = "{\"server_id\":\"myid\", \"version\":\"9.9.99\"}";

        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(null, customInfo)) {
            Connection nc = Nats.connect(mockTs.getServerUri());
            try {
                assertConnected(nc);
                assertEquals("myid", nc.getServerInfo().getServerId(), "got custom info");
            } finally {
                nc.close();
                assertClosed(nc);
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

            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for PONG ...");
            String pong;
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

        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(infoCustomizer, customInfo)) {
            Connection nc = Nats.connect(mockTs.getServerUri());
            try {
                assertConnected(nc);
                assertEquals("myid", nc.getServerInfo().getServerId(), "got custom info");
                sendInfo.complete(Boolean.TRUE);

                assertTrue(gotPong.get(), "Got pong."); // Server round tripped so we should have new info
                assertEquals("replacement", nc.getServerInfo().getServerId(), "got replacement info");
            } finally {
                nc.close();
                assertClosed(nc);
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

            System.out.println("*** Mock Server @" + ts.getPort() + " waiting for PONG ...");
            String pong;
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

        try (NatsServerProtocolMock mockTs = new NatsServerProtocolMock(infoCustomizer, customInfo)) {

            ConnectionListener cl = (conn, type) -> {
                if (type.equals(ConnectionListener.Events.LAME_DUCK)) connectLDM.complete(type);
            };

            Options options = optionsBuilder(mockTs).connectionListener(cl).build();

            Connection nc = Nats.connect(options);
            try {
                assertConnected(nc);
                assertEquals("myid", nc.getServerInfo().getServerId(), "got custom info");
                sendInfo.complete(Boolean.TRUE);

                assertTrue(gotPong.get(), "Got pong."); // Server round tripped so we should have new info
                assertEquals("replacement", nc.getServerInfo().getServerId(), "got replacement info");
            } finally {
                nc.close();
                assertClosed(nc);
            }
        }

        ConnectionListener.Events event = connectLDM.get(5, TimeUnit.SECONDS);
        assertEquals(ConnectionListener.Events.LAME_DUCK, event);
        System.out.println(event);
    }
}