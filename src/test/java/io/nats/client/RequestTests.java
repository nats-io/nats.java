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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;

public class RequestTests {
    @Test
    public void testSimpleRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Connection nc = Nats.connect(ts.getURI());
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            Future<Message> incoming = nc.request("subject", null);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertEquals(0, nc.getStatistics().getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') < msg.getSubject().lastIndexOf('.'));

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testRequireCleanupOnTimeout() throws IOException, ExecutionException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofHours(1)).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            Future<Message> incoming = nc.request("subject", null);
            Message msg = null;
            
            try {
                incoming.get(100, TimeUnit.MILLISECONDS);
                assertFalse(true);
            } catch(TimeoutException e) {
                assertTrue(true);
            }

            assertNull(msg);
            assertEquals(1, nc.getStatistics().getOutstandingRequests());

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testRequireCleanupOnCancel() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofHours(1)).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            Future<Message> incoming = nc.request("subject", null);
            incoming.cancel(true);

            assertEquals(1, nc.getStatistics().getOutstandingRequests());

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testCleanupTimerWorks() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            long cleanupInterval = 200;
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofMillis(cleanupInterval)).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            Future<Message> incoming = nc.request("subject", null);
            incoming.cancel(true);
            incoming = nc.request("subject", null);
            incoming.cancel(true);
            incoming = nc.request("subject", null);
            incoming.cancel(true);

            Thread.sleep(2 * cleanupInterval);
            assertEquals(0, nc.getStatistics().getOutstandingRequests());

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

    @Test
    public void testRequestsVsCleanup() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            long cleanupInterval = 50;
            int msgCount = 100;
            Options options = new Options.Builder().server(ts.getURI()).requestCleanupInterval(Duration.ofMillis(cleanupInterval)).build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            long start = System.nanoTime();
            long end = start;

            while ((end-start) <= 2 * cleanupInterval * 1_000_000) {
                for (int i=0;i<msgCount;i++) {
                    Future<Message> incoming = nc.request("subject", null);
                    Message msg = incoming.get(500, TimeUnit.MILLISECONDS);
                    assertNotNull(msg);
                    assertEquals(0, msg.getData().length);
                }
                end = System.nanoTime();
            }

            assertTrue((end-start) > 2 * cleanupInterval * 1_000_000);
            assertEquals(0, nc.getStatistics().getOutstandingRequests());

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }

        @Test
        public void testDelayInPickingUpFuture() throws IOException, ExecutionException, TimeoutException, InterruptedException {
            try (NatsTestServer ts = new NatsTestServer(false)) {
                int msgCount = 100;
                ArrayList<Future<Message>> messages = new ArrayList<>();
                Connection nc = Nats.connect(ts.getURI());
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
                
                Dispatcher d = nc.createDispatcher((msg) -> {
                    nc.publish(msg.getReplyTo(), new byte[1]);
                });
                d.subscribe("subject");
    
                for (int i=0;i<msgCount;i++) {
                    Future<Message> incoming = nc.request("subject", null);
                    messages.add(incoming);
                }

                for (Future<Message> f : messages) {
                    Message msg = f.get(500, TimeUnit.MILLISECONDS);
                    assertNotNull(msg);
                    assertEquals(1, msg.getData().length);
                }
    
                assertEquals(0, nc.getStatistics().getOutstandingRequests());
    
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
    }

    @Test
    public void testOldStyleRequest() throws IOException, ExecutionException, TimeoutException, InterruptedException {
        try (NatsTestServer ts = new NatsTestServer(false)) {
            Options options = new Options.Builder().server(ts.getURI()).oldRequestStyle().build();
            Connection nc = Nats.connect(options);
            assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            
            Dispatcher d = nc.createDispatcher((msg) -> {
                nc.publish(msg.getReplyTo(), null);
            });
            d.subscribe("subject");

            Future<Message> incoming = nc.request("subject", null);
            Message msg = incoming.get(500, TimeUnit.MILLISECONDS);

            assertEquals(0, nc.getStatistics().getOutstandingRequests());
            assertNotNull(msg);
            assertEquals(0, msg.getData().length);
            assertTrue(msg.getSubject().indexOf('.') == msg.getSubject().lastIndexOf('.'));

            nc.close();
            assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
        }
    }
}