package io.nats.client.impl;

import org.junit.jupiter.api.Test;

import static io.nats.client.impl.NatsJetStream.isValidStreamName;
import static org.junit.jupiter.api.Assertions.*;

public class NatsJetStreamTests {

    @Test
    public void constructAccountLimitImpl() {
        NatsJetStream.AccountLimitImpl impl = new NatsJetStream.AccountLimitImpl(
                "{\"max_memory\": 42, \"max_storage\": 24, \"max_streams\": 73, \"max_consumers\": 37}");

        assertEquals(42, impl.getMaxMemory());
        assertEquals(24, impl.getMaxStorage());
        assertEquals(73, impl.getMaxStreams());
        assertEquals(37, impl.getMaxConsumers());

        impl = new NatsJetStream.AccountLimitImpl("{}");
        assertEquals(-1, impl.getMaxMemory());
        assertEquals(-1, impl.getMaxStorage());
        assertEquals(-1, impl.getMaxStreams());
        assertEquals(+1, impl.getMaxConsumers());
    }

    @Test
    public void constructAccountStatsImpl() {
        NatsJetStream.AccountStatsImpl impl = new NatsJetStream.AccountStatsImpl(
                "{\"memory\": 42, \"storage\": 24, \"streams\": 73, \"consumers\": 37}");

        assertEquals(42, impl.getMemory());
        assertEquals(24, impl.getStorage());
        assertEquals(73, impl.getStreams());
        assertEquals(37, impl.getConsumers());

        impl = new NatsJetStream.AccountStatsImpl("{}");
        assertEquals(-1, impl.getMemory());
        assertEquals(-1, impl.getStorage());
        assertEquals(-1, impl.getStreams());
        assertEquals(+1, impl.getConsumers());
    }


    @Test
    public void miscCoverage() {
        assertFalse(isValidStreamName(null));
        assertFalse(isValidStreamName("no.dot"));
        assertFalse(isValidStreamName("no*star"));
        assertFalse(isValidStreamName("no>gt"));
        assertTrue(isValidStreamName("ok"));
    }
}
