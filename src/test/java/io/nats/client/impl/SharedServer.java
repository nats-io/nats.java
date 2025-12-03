// Copyright 2025 The NATS Authors
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

import io.nats.client.Connection;
import io.nats.client.NUID;
import io.nats.client.NatsTestServer;
import io.nats.client.Options;
import io.nats.client.utils.ConnectionUtils;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.NatsTestServer.configFileBuilder;
import static io.nats.client.utils.OptionsUtils.optionsBuilder;
import static io.nats.client.utils.ThreadUtils.sleep;
import static io.nats.client.utils.VersionUtils.initVersionServerInfo;

/**
 * This class is in the impl package instead of the support package
 * so it can access the package scope class NatsConnection
 */
public class SharedServer {

    private static final int NUM_REUSABLE_CONNECTIONS = 3;
    private static final Thread SHARED_SHUTDOWN_HOOK_THREAD;
    private static final Map<String, SharedServer> SHARED_BY_NAME;
    private static final Map<String, SharedServer> SHARED_BY_URL;
    private static final ReentrantLock STATIC_LOCK;

    static {
        STATIC_LOCK = new ReentrantLock();
        SHARED_BY_NAME = new HashMap<>();
        SHARED_BY_URL = new HashMap<>();
        SHARED_SHUTDOWN_HOOK_THREAD = new Thread("Reusables-Shutdown-Hook") {
            @Override
            public void run() {
                for (SharedServer rs : SHARED_BY_NAME.values()) {
                    rs.shutdown();
                }
                SHARED_BY_NAME.clear();
                SHARED_BY_URL.clear();
            }
        };
        Runtime.getRuntime().addShutdownHook(SHARED_SHUTDOWN_HOOK_THREAD);
    }

    private final ReentrantLock instanceLock;
    private final String reusableConnectionPrefix;
    private final Map<String, Connection> connectionMap;
    private final AtomicInteger currentReusableId;
    private NatsTestServer natsTestServer;

    public final String serverUrl;

    public static SharedServer getInstance(String name) throws IOException {
        return getInstance(name, null);
    }

    public static SharedServer getInstance(String name, String confFile) throws IOException {
        STATIC_LOCK.lock();
        try {
            SharedServer shared = SHARED_BY_NAME.get(name);
            if (shared == null) {
                shared = new SharedServer(name, confFile);
                SHARED_BY_NAME.put(name, shared);
                SHARED_BY_URL.put(shared.serverUrl, shared);
            }
            return shared;
        }
        finally {
            STATIC_LOCK.unlock();
        }
    }

    public static void shutdown(String... names) {
        for (String name : names) {
            SharedServer shared = SHARED_BY_NAME.get(name);
            if (shared != null) {
                SHARED_BY_NAME.remove(name);
                SHARED_BY_URL.remove(shared.serverUrl);
                shared.shutdown();
            }
        }
    }

    private SharedServer(@NonNull String name, @Nullable String confFile) throws IOException {
        instanceLock = new ReentrantLock();
        reusableConnectionPrefix = new NUID().next();
        connectionMap = new HashMap<>();
        currentReusableId = new AtomicInteger(-1);
        if (confFile == null) {
            natsTestServer = new NatsTestServer(
                NatsTestServer.builder()
                    .jetstream(true)
                    .customName(name)
            );
        }
        else {
            natsTestServer = new NatsTestServer(configFileBuilder(confFile)
                .customName(name));
        }
        serverUrl = natsTestServer.getServerUri();
    }

    public NatsTestServer getServer() {
        return natsTestServer;
    }

    public Connection getSharedConnection() {
        int id = currentReusableId.incrementAndGet();
        if (id >= NUM_REUSABLE_CONNECTIONS) {
            currentReusableId.set(0);
            id = 0;
        }
        return getSharedConnection(reusableConnectionPrefix + "-" + id);
    }

    public static Connection sharedConnectionForServer(NatsTestServer ts) {
        for (Map.Entry<String, SharedServer> entry : SHARED_BY_NAME.entrySet()) {
            SharedServer shared = entry.getValue();
            if (shared.natsTestServer == ts) {
                return shared.getSharedConnection();
            }
        }
        throw new RuntimeException("No shared matching server.");
    }

    public static Connection sharedConnectionForSameServer(Connection nc) {
        SharedServer shared = SHARED_BY_URL.get(nc.getConnectedUrl());
        if (shared == null) {
            throw new RuntimeException("No shared server for that connection.");
        }
        return shared.getSharedConnection();
    }

    public static Connection connectionForSameServer(Connection nc, Options.Builder builder) {
        SharedServer shared = SHARED_BY_URL.get(nc.getConnectedUrl());
        if (shared == null) {
            throw new RuntimeException("No shared server for that connection.");
        }
        return shared.newConnection(builder);
    }

    private void waitUntilStatus(Connection conn) {
        for (long x = 0; x < 100; x++) {
            sleep(100);
            if (conn.getStatus() == Connection.Status.CONNECTED) {
                return;
            }
        }
    }

    private Connection getSharedConnection(String name) {
        instanceLock.lock();
        try {
            Connection ncs = connectionMap.get(name);
            if (ncs == null) {
                ncs = newConnection(optionsBuilder());
                connectionMap.put(name, ncs);
                waitUntilStatus(ncs);
                initVersionServerInfo(ncs);
            }
            else if (ncs.getStatus() != Connection.Status.CONNECTED) {
                try { ncs.close(); } catch (Exception ignore) {}
                return getSharedConnection(name);
            }
            return ncs;
        }
        finally {
            instanceLock.unlock();
        }
    }

    public Connection newConnection(Options.Builder builder) {
        return ConnectionUtils.standardConnect(builder.server(serverUrl).build());
    }

    public void shutdown() {
        instanceLock.lock();
        try {
            for (Connection nc : connectionMap.values()) {
                try {
                    ((NatsConnection)nc).close(false, true);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (natsTestServer != null) {
                try {
                    natsTestServer.shutdown(false);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        finally {
            connectionMap.clear();
            natsTestServer = null;
            instanceLock.unlock();
        }
    }
}
