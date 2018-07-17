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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

public class AuthTests {
    @Test
    public void testUserPass() throws Exception {
        String[] customArgs = {"--user","stephen","--pass","password"};
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        userInfo("stephen", "password").
                        build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testUserBCryptPass() throws Exception {
        /*
        go run mkpasswd.go -p
        password: password
        bcrypt hash: $2a$11$1oJy/wZYNTxr9jNwMNwS3eUGhBpHT3On8CL9o7ey89mpgo88VG6ba
        */
        String[] customArgs = {"--user","ginger","--pass","$2a$11$1oJy/wZYNTxr9jNwMNwS3eUGhBpHT3On8CL9o7ey89mpgo88VG6ba"};
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        userInfo("ginger", "password").
                        build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testUserPassInURL() throws Exception {
        String[] customArgs = {"--user","stephen","--pass","password"};
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server("nats://stephen:password@localhost:"+ts.getPort()).
                        maxReconnects(0).
                        build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testToken() throws Exception {
        String[] customArgs = {"--auth","derek"};
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        token("derek").
                        build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test
    public void testTokenInURL() throws Exception {
        String[] customArgs = {"--auth","alberto"};
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server("nats://alberto@localhost:"+ts.getPort()).
                        maxReconnects(0).
                        build();
            Connection nc = Nats.connect(options);
            try {
                assertTrue("Connected Status", Connection.Status.CONNECTED == nc.getStatus());
            } finally {
                nc.close();
                assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
            }
        }
    }

    @Test(expected=IOException.class)
    public void testBadUserBadPass() throws Exception {
        Connection nc = null;
        String[] customArgs = {"--user","stephen","--pass","password"};
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        userInfo("sam", "notthepassword").
                        build();
            try {
                nc = Nats.connect(options);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testMissingUserPass() throws Exception {
        Connection nc = null;
        String[] customArgs = {"--user","wally","--pass","password"};
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        build();
            try {
                nc = Nats.connect(options);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testBadToken() throws Exception {
        Connection nc = null;
        String[] customArgs = {"--auth","colin"};
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        token("notthetoken").
                        build();
            try {
                nc = Nats.connect(options);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }

    @Test(expected=IOException.class)
    public void testMissingToken() throws Exception {
        Connection nc = null;
        String[] customArgs = {"--auth","ivan"};
        try (NatsTestServer ts = new NatsTestServer(customArgs, false)) {
            // See config file for user/pass
            Options options = new Options.Builder().
                        server(ts.getURI()).
                        maxReconnects(0).
                        build();
            try {
                nc = Nats.connect(options);
            } finally {
                if (nc != null) {
                    nc.close();
                    assertTrue("Closed Status", Connection.Status.CLOSED == nc.getStatus());
                }
            }
        }
    }
}