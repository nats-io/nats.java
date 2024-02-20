package io.nats.client.impl;

import io.nats.client.Options;
import io.nats.client.support.LoggerTests;
import io.nats.client.support.StdOutLogger;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.*;

public class LoggerOptionsTest {

    @Test
    public void testOptionsWithConnection() {
        LoggerTests.TestLogger testLogger = new LoggerTests.TestLogger();
        Options options = Options.builder().natsLogger(testLogger).build();
        new NatsConnection(options);
        assertEquals(5, testLogger.getLogEvents().size());

        System.out.println("-------------------");

        options = Options.builder().build();
        assertInstanceOf(StdOutLogger.class, options.getNatsLogger());
        assertEquals(Level.OFF, options.getNatsLogger().getMinLevel());

        System.out.println("-------------------");

        options = Options.builder().traceConnection().build();
        assertInstanceOf(StdOutLogger.class, options.getNatsLogger());
        NatsConnection conn = new NatsConnection(options);
        assertInstanceOf(StdOutLogger.class, conn.getLOGGER());
        assertEquals(Level.INFO, conn.getLOGGER().getMinLevel());
    }

}
