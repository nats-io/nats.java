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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import io.nats.client.BadHandler;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.TestHandler;

public class ErrorListenerTests {
    @Test
    public void testErrorOnNoAuth() throws Exception {
        String[] customArgs = {"--user","stephen","--pass","password"};
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        errorListener(handler).
                        // skip this so we get an error userInfo("stephen", "password").
                        build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
                assertTrue(handler.getCount() > 0);
                assertEquals(1, handler.getErrorCount("Authorization Violation"));
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testExceptionOnBadDispatcher() throws Exception {
        TestHandler handler = new TestHandler();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        errorListener(handler).
                        build();
            Connection nc = Nats.connect(options);
            try {
                Dispatcher d = nc.createDispatcher((msg) -> {
                    throw new ArithmeticException();
                });
                d.subscribe("subject");
                Future<Message> incoming = nc.request("subject", null);
    
                Message msg = null;

                try {
                    msg = incoming.get(200, TimeUnit.MILLISECONDS);
                } catch (TimeoutException te) {
                    msg = null;
                }

                assertNull(msg);
                assertEquals(1, handler.getCount());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testExceptionInErrorHandler() throws Exception {
        String[] customArgs = {"--user","stephen","--pass","password"};
        BadHandler handler = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        errorListener(handler).
                        // skip this so we get an error userInfo("stephen", "password").
                        build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.DISCONNECTED == nc.getStatus());
                assertTrue(((NatsConnection)nc).getNatsStatistics().getExceptions()>0);
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testExceptionInExceptionHandler() throws Exception {
        BadHandler handler = new BadHandler();
        try (NatsTestServer ts = new NatsTestServer()) {
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        errorListener(handler).
                        build();
            Connection nc = Nats.connect(options);
            try {
                Dispatcher d = nc.createDispatcher((msg) -> {
                    throw new ArithmeticException();
                });
                d.subscribe("subject");
                Future<Message> incoming = nc.request("subject", null);
    
                Message msg = null;

                try {
                    msg = incoming.get(200, TimeUnit.MILLISECONDS);
                } catch (TimeoutException te) {
                    msg = null;
                }

                assertNull(msg);
                assertTrue(((NatsConnection)nc).getNatsStatistics().getExceptions() == 2); // 1 for the dispatcher, 1 for the handlers
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }
}