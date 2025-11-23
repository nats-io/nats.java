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

package io.nats.client.utils;

import io.nats.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import static io.nats.client.utils.OptionsUtils.options;
import static io.nats.client.utils.ThreadUtils.sleep;
import static io.nats.client.utils.VersionUtils.initVersionServerInfo;

public class ReusableServer {

    private static final int RETRY_DELAY_INCREMENT = 50;
    private static final int CONNECTION_RETRIES = 10;
    private static final long RETRY_DELAY = 100;
    private static final Thread REUSABLES_SHUTDOWN_HOOK_THREAD;
    private static final Map<String, ReusableServer> REUSABLES;
    private static final ReentrantLock STATIC_LOCK;

    static {
        STATIC_LOCK = new ReentrantLock();
        REUSABLES = new HashMap<>();
        REUSABLES_SHUTDOWN_HOOK_THREAD = new Thread("Reusables-Shutdown-Hook") {
            @Override
            public void run() {
                for (ReusableServer rs : REUSABLES.values()) {
                    rs.shutdown();
                }
                REUSABLES.clear();
            }
        };
        Runtime.getRuntime().addShutdownHook(REUSABLES_SHUTDOWN_HOOK_THREAD);
    }

    private final ReentrantLock instanceLock;
    private final String internalConnectionName;
    private final Map<String, Connection> connectionMap;
    private NatsTestServer natsTestServer;

    public final String serverUri;

    public static ReusableServer getInstance(String name) throws IOException {
        STATIC_LOCK.lock();
        try {
            ReusableServer rs = REUSABLES.get(name);
            if (rs == null) {
                rs = new ReusableServer();
                REUSABLES.put(name, rs);
            }
            return rs;
        }
        finally {
            STATIC_LOCK.unlock();
        }
    }

    public static void shutdownInstance(String name) {
        STATIC_LOCK.lock();
        try {
            ReusableServer rs = REUSABLES.remove(name);
            if (rs != null) {
                rs.shutdown();
            }
        }
        finally {
            STATIC_LOCK.unlock();
        }
    }

    private ReusableServer() throws IOException {
        instanceLock = new ReentrantLock();
        internalConnectionName = new NUID().next();
        connectionMap = new HashMap<>();

        natsTestServer = new NatsTestServer(
            NatsTestServer.builder()
                .jetstream(true)
                .customName("Reusable")
        );
        serverUri = natsTestServer.getLocalhostUri();
    }

    public Connection getReusableNc() {
        return getConnection(internalConnectionName);
    }

    private void waitUntilStatus(Connection conn) {
        for (long x = 0; x < 100; x++) {
            sleep(100);
            if (conn.getStatus() == Connection.Status.CONNECTED) {
                return;
            }
        }
    }

    private Connection getConnection(String name) {
        instanceLock.lock();
        try {
            Connection ncs = connectionMap.get(name);
            if (ncs == null) {
                long delay = RETRY_DELAY - RETRY_DELAY_INCREMENT;
                Options options = options(serverUri);
                for (int x = 0; ncs == null && x < CONNECTION_RETRIES; x++) {
                    if (x > 0) {
                        delay += RETRY_DELAY_INCREMENT;
                        sleep(delay);
                    }
                    try {
                        ncs = Nats.connect(options);
                    }
                    catch (IOException ignored) {}
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
                if (ncs != null) {
                    connectionMap.put(name, ncs);
                    waitUntilStatus(ncs);
                    initVersionServerInfo(ncs);
                }
            }
            else if (ncs.getStatus() != Connection.Status.CONNECTED) {
                try { ncs.close(); } catch (Exception ignore) {}
                return getConnection(name);
            }
            return ncs;
        }
        finally {
            instanceLock.unlock();
        }
    }

    public void shutdown() {
        instanceLock.lock();
        try {
            if (natsTestServer != null) {
                natsTestServer.shutdown(false);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            natsTestServer = null;
            instanceLock.unlock();
        }
    }
}
