package io.nats.client.support;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import static io.nats.client.support.WithTimeout.withTimeout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WithTimeoutTests {
    public class CustomException extends Exception {
    }

    public class WrapException extends Exception {
        public WrapException(Throwable cause) {
            super(cause);
        }
    }

    @Test
    public void customExceptionTest() {
        // Any "normal" exception should NOT be wrapped:
        assertThrows(CustomException.class,
            () -> {
                withTimeout(() -> {
                    if (true) throw new CustomException();
                    return "";
                }, Duration.ofSeconds(2), e -> new WrapException(e));
            });
    }

    @Test
    public void interruptedExceptionTest() {
        // Any "normal" exception should NOT be wrapped:
        WrapException ex = assertThrows(WrapException.class,
            () -> {
                // Mark the current thread as interrupted:
                Thread.currentThread().interrupt();
                withTimeout(() -> {
                    if (true) throw new RuntimeException("unreachable");
                    return "";
                }, Duration.ofSeconds(2), e -> new WrapException(e));
            });
        // Clear the interrupt status:
        assertTrue(Thread.interrupted());
        assertEquals(InterruptedException.class, ex.getCause().getClass());
    }
}
