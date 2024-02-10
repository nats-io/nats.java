package io.nats.client.impl;

import io.nats.client.Options;
import io.nats.client.support.LoggerTests;
import io.nats.client.support.NatsLoggerFacade;
import io.nats.client.support.NoOpLogger;
import io.nats.client.support.StdOutLogger;
import org.junit.jupiter.api.Test;

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
        assertInstanceOf(NoOpLogger.class, options.getNatsLogger());

        System.out.println("-------------------");

        options = Options.builder().traceConnection().build();
        assertInstanceOf(NoOpLogger.class, options.getNatsLogger());

        System.out.println("-------------------");

        new NatsConnection(options);
        assertInstanceOf(StdOutLogger.class, NatsLoggerFacade.getNatsLogger());
    }

}
