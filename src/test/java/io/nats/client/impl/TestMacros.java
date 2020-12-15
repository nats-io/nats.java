// Copyright 2020 The NATS Authors
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
import io.nats.client.TestHandler;

import static io.nats.client.Connection.Status.CLOSED;
import static io.nats.client.Connection.Status.CONNECTED;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.fail;

public final class TestMacros {

    // ----------------------------------------------------------------------------------------------------
    // assertions
    // ----------------------------------------------------------------------------------------------------
    public static void assertConnected(Connection conn) {
        assertSame(CONNECTED, conn.getStatus(), "Connected Status");
    }

    public static void assertClosed(Connection conn) {
        assertSame(CLOSED, conn.getStatus(), "Closed Status");
    }

    // ----------------------------------------------------------------------------------------------------
    // utils
    // ----------------------------------------------------------------------------------------------------
    public static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        }
        catch (InterruptedException e) {
            fail(e); // will never happen, but if it does...
        }
    }

    // ----------------------------------------------------------------------------------------------------
    // macro utils
    // ----------------------------------------------------------------------------------------------------
    public static void waitThenAssertConnected(Connection conn, TestHandler handler) {
        waitThenAssertConnected(conn, handler, 5000);
    }

    public static void waitThenAssertConnected(Connection conn, TestHandler handler, long millis) {
        handler.waitForStatusChange(millis, MILLISECONDS);
        assertConnected(conn);
    }

    public static void closeThenAssertClosed(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
                waitALittleForStatus(conn, CLOSED);
                assertClosed(conn);
            } catch (InterruptedException e) {
                fail(e); // will never happen, but if it does...
            }
        }
    }
    private static void waitALittleForStatus(Connection conn, Connection.Status status) {
        waitALittleForStatus(conn, status, 1000);
    }

    private static void waitALittleForStatus(Connection conn, Connection.Status status, long millis) {
        int tries = (int)((millis+99) / 100);
        while (tries-- > 0 && conn.getStatus() != status) {
            sleep(100);
        }
    }
}
