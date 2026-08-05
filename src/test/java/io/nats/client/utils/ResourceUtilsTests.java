package io.nats.client.utils;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;

import static io.nats.client.utils.ResourceUtils.*;
import static org.junit.jupiter.api.Assertions.*;

public class ResourceUtilsTests {

    private static final String MISSING = "ThisResourceDoesNotExist.json";

    @Test
    public void testMissingResourceIdentifiesTheFile() {
        assertMissing(assertThrows(RuntimeException.class, () -> dataAsString(MISSING)));
        assertMissing(assertThrows(RuntimeException.class, () -> dataAsLines(MISSING)));
        assertMissing(assertThrows(RuntimeException.class, () -> dataAsInputStream(MISSING)));
    }

    private void assertMissing(RuntimeException e) {
        Throwable cause = e.getCause();
        assertInstanceOf(FileNotFoundException.class, cause);
        assertTrue(cause.getMessage().contains(MISSING));
    }

    @Test
    public void testResourceStillLoads() {
        assertTrue(dataAsString("StreamConfiguration.json").contains("retention"));
        assertFalse(dataAsLines("StreamConfiguration.json").isEmpty());
        assertNotNull(dataAsInputStream("StreamConfiguration.json"));
    }
}
