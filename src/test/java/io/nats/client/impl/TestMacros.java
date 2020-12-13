package io.nats.client.impl;

import io.nats.client.Connection;
import io.nats.client.TestHandler;

import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertSame;

public final class TestMacros {

    public static void assertConnected(Connection nc) {
        assertSame(Connection.Status.CONNECTED, nc.getStatus(), "Connected Status");
    }

    public static void assertClosed(Connection nc) {
        assertSame(Connection.Status.CLOSED, nc.getStatus(), "Closed Status");
    }

    public static void allowTimeToConnect(Connection nc, TestHandler handler) {
        allowTimeToConnect(nc, handler, 5000);
    }

    public static void allowTimeToConnect(Connection nc, TestHandler handler, long millis) {
        handler.waitForStatusChange(millis, TimeUnit.MILLISECONDS);
        assertConnected(nc);
    }

    public static void allowTimeToConnect(Connection nc, long ms) throws InterruptedException {
        sleep(ms);
        assertConnected(nc);
    }

    public static void closeConnection(Connection nc) throws InterruptedException {
        if (nc != null) {
            nc.close();
            assertClosed(nc);
        }
    }
}
