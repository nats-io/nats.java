package io.nats.client.api;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KeyResultTest {
    @Test
    void testNoArgsConstructor() {
        KeyResult actual = new KeyResult();

        assertNull(actual.getKey());
        assertNull(actual.getException());
    }

    @Test
    void testKeyArg() {
        KeyResult actual = new KeyResult("key");

        assertEquals("key", actual.getKey());
        assertNull(actual.getException());
    }

    @Test
    void testExceptionArg() {
        Exception exception = new Exception("message");
        KeyResult actual = new KeyResult(exception);

        assertNull(actual.getKey());
        assertSame(exception, actual.getException());
    }
}
