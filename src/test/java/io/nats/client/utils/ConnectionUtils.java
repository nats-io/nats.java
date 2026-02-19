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
import io.nats.client.NatsServerProtocolMock;
import io.nats.client.Options;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;

import static io.nats.client.utils.OptionsUtils.options;
import static io.nats.client.utils.ThreadUtils.sleep;
import static org.junit.jupiter.api.Assertions.assertSame;

public abstract class ConnectionUtils {

    public static final int DEFAULT_WAIT   =  5000;
    public static final int MEDIUM_WAIT    =  8000;
    public static final int LONG_WAIT      = 12000;
    public static final int VERY_LONG_WAIT = 20000;

    public static final long STANDARD_FLUSH_TIMEOUT_MS = 2000;
    public static final long MEDIUM_FLUSH_TIMEOUT_MS = 5000;

    // ----------------------------------------------------------------------------------------------------
    // connectWithRetry
    // ----------------------------------------------------------------------------------------------------
    private static final int RETRY_DELAY_INCREMENT = 50;
    private static final int CONNECTION_RETRIES = 10;
    private static final long RETRY_DELAY = 100;

    public static Connection managedConnect(Options options) {
        try {
            return managedConnect(options, DEFAULT_WAIT);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to make a connection.", e);
        }
    }

    public static Connection managedConnect(Options options, long waitTime) throws IOException, InterruptedException {
        IOException last = null;
        long delay = RETRY_DELAY - RETRY_DELAY_INCREMENT;
        for (int x = 1; x <= CONNECTION_RETRIES; x++) {
            if (x > 1) {
                delay += RETRY_DELAY_INCREMENT;
                sleep(delay);
            }
            try {
                return waitUntilStatus(Nats.connect(options), waitTime, Connection.Status.CONNECTED);
            }
            catch (IOException ioe) {
                last = ioe;
            }
            catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw ie;
            }
        }
        throw last;
    }

    // ----------------------------------------------------------------------------------------------------
    // standardConnect
    // ----------------------------------------------------------------------------------------------------
    public static Connection standardConnect(NatsServerProtocolMock ts) throws IOException, InterruptedException {
        return confirmConnected(Nats.connect(options(ts)));
    }

    public static Connection standardConnect(Options options) throws IOException, InterruptedException {
        return confirmConnected(Nats.connect(options));
    }

    // ----------------------------------------------------------------------------------------------------
    // connect or wait for a connection
    // ----------------------------------------------------------------------------------------------------
    @SuppressWarnings("UnusedReturnValue")
    public static Connection confirmConnected(Connection conn) {
        return waitUntilStatus(conn, DEFAULT_WAIT, Connection.Status.CONNECTED);
    }

    public static Connection confirmConnected(Connection conn, long waitTime) {
        return waitUntilStatus(conn, waitTime, Connection.Status.CONNECTED);
    }

    // ----------------------------------------------------------------------------------------------------
    // connect or wait for a connection
    // ----------------------------------------------------------------------------------------------------
    public static void confirmConnectedThenClosed(Connection conn) {
        closeAndConfirm(confirmConnected(conn, DEFAULT_WAIT), DEFAULT_WAIT);
    }

    public static void confirmConnectedThenClosed(Connection conn, long waitTime) {
        closeAndConfirm(confirmConnected(conn, waitTime), DEFAULT_WAIT);
    }

    public static void confirmConnectedThenClosed(Connection conn, long waitTime, long closeTime) {
        closeAndConfirm(confirmConnected(conn, waitTime), closeTime);
    }

    // ----------------------------------------------------------------------------------------------------
    // close
    // ----------------------------------------------------------------------------------------------------
    public static void closeAndConfirm(Connection conn) {
        closeAndConfirm(conn, DEFAULT_WAIT);
    }

    public static void closeAndConfirm(Connection conn, long millis) {
        if (conn != null) {
            close(conn);
            waitUntilStatus(conn, millis, Connection.Status.CLOSED);
            assertClosed(conn);
        }
    }

    public static void close(Connection conn) {
        try {
            conn.close();
        }
        catch (InterruptedException e) { /* ignored */ }
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

    public static void assertCanConnect(NatsServerProtocolMock ts) {
        closeAndConfirm(managedConnect(options(ts)));
    }

    public static void assertCanConnect(Options options) {
        closeAndConfirm(managedConnect(options));
    }

    private static String expectingMessage(Connection conn, Connection.Status expecting) {
        return "Failed expecting Connection Status " + expecting.name() + " but was " + conn.getStatus();
    }
}
