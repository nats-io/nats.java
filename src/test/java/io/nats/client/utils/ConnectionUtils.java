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

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.impl.ListenerForTesting;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.assertSame;

public abstract class ConnectionUtils {

    public static final long STANDARD_CONNECTION_WAIT_MS = 5000;
    public static final long LONG_CONNECTION_WAIT_MS = 7500;
    public static final long VERY_LONG_CONNECTION_WAIT_MS = 10000;
    public static final long STANDARD_FLUSH_TIMEOUT_MS = 2000;
    public static final long MEDIUM_FLUSH_TIMEOUT_MS = 5000;
    public static final long LONG_TIMEOUT_MS = 15000;

    // ----------------------------------------------------------------------------------------------------
    // connect or wait for a connection
    // ----------------------------------------------------------------------------------------------------
    public static Connection connectionWait(Connection conn, long millis) {
        return waitUntilStatus(conn, millis, Connection.Status.CONNECTED);
    }

    public static Connection standardConnectionWait(Options options) throws IOException, InterruptedException {
        return connectionWait(Nats.connect(options), STANDARD_CONNECTION_WAIT_MS);
    }

    public static Connection standardConnectionWait(Connection conn) {
        return connectionWait(conn, STANDARD_CONNECTION_WAIT_MS);
    }

    public static Connection longConnectionWait(Options options) throws IOException, InterruptedException {
        return connectionWait(Nats.connect(options), LONG_CONNECTION_WAIT_MS);
    }

    public static Connection longConnectionWait(Connection conn) {
        return connectionWait(conn, LONG_CONNECTION_WAIT_MS);
    }

    public static Connection listenerConnectionWait(Options options, ListenerForTesting listener) throws IOException, InterruptedException {
        Connection conn = Nats.connect(options);
        listenerConnectionWait(conn, listener, LONG_CONNECTION_WAIT_MS);
        return conn;
    }

    public static void listenerConnectionWait(Connection conn, ListenerForTesting listener) {
        listenerConnectionWait(conn, listener, LONG_CONNECTION_WAIT_MS);
    }

    public static void listenerConnectionWait(Connection conn, ListenerForTesting listener, long millis) {
        listener.waitForStatusChange(millis, TimeUnit.MILLISECONDS);
        assertConnected(conn);
    }

    // ----------------------------------------------------------------------------------------------------
    // close
    // ----------------------------------------------------------------------------------------------------
    public static void standardCloseConnection(Connection conn) {
        closeConnection(conn, STANDARD_CONNECTION_WAIT_MS);
    }

    public static void closeConnection(Connection conn, long millis) {
        if (conn != null) {
            close(conn);
            waitUntilStatus(conn, millis, Connection.Status.CLOSED);
            assertClosed(conn);
        }
    }

    public static void close(Connection conn) {
        try { conn.close(); } catch (InterruptedException e) { /* ignored */ }
    }

    // ----------------------------------------------------------------------------------------------------
    // connection waiting
    // ----------------------------------------------------------------------------------------------------
    public static Connection waitUntilStatus(Connection conn, long millis, Connection.Status waitUntilStatus) {
        long times = (millis + 99) / 100;
        for (long x = 0; x < times; x++) {
            sleep(100);
            if (conn.getStatus() == waitUntilStatus) {
                return conn;
            }
        }

        throw new AssertionFailedError(expectingMessage(conn, waitUntilStatus));
    }

    // ----------------------------------------------------------------------------------------------------
    // assertions
    // ----------------------------------------------------------------------------------------------------
    public static void assertConnected(Connection conn) {
        assertSame(Connection.Status.CONNECTED, conn.getStatus(),
            () -> expectingMessage(conn, Connection.Status.CONNECTED));
    }

    public static void assertClosed(Connection conn) {
        assertSame(Connection.Status.CLOSED, conn.getStatus(),
            () -> expectingMessage(conn, Connection.Status.CLOSED));
    }

    public static void assertCanConnect(Options options) throws IOException, InterruptedException {
        standardCloseConnection( standardConnectionWait(options) );
    }

    private static String expectingMessage(Connection conn, Connection.Status expecting) {
        return "Failed expecting Connection Status " + expecting.name() + " but was " + conn.getStatus();
    }

    // ----------------------------------------------------------------------------------------------------
    // new connection better for java 21
    // ----------------------------------------------------------------------------------------------------
    private static final int RETRY_DELAY_INCREMENT = 50;
    private static final int CONNECTION_RETRIES = 10;
    private static final long RETRY_DELAY = 100;
    public static Connection newConnection(Options options) {
        IOException last = null;
        long delay = RETRY_DELAY - RETRY_DELAY_INCREMENT;
        for (int x = 1; x <= CONNECTION_RETRIES; x++) {
            if (x > 1) {
                delay += RETRY_DELAY_INCREMENT;
                sleep(delay);
            }
            try {
                return Nats.connect(options);
            }
            catch (IOException ioe) {
                last = ioe;
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Unable to open a new connection to reusable sever.", ie);
            }
        }
        throw new RuntimeException("Unable to open a new connection to reusable sever.", last);
    }
}
