package io.nats.client.impl;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ByteArrayBuilderTests {

    @Test
    public void byte_buffer_builder_works() {
        ByteArrayBuilder bbb = new ByteArrayBuilder(5);
        String expectedString = "";
        for (int x = 1; x < 1000; x++) {
            String more = "abcdefghij" + x;
            bbb.append(more);
            expectedString = expectedString + more;
            byte[] bytes = bbb.toByteArray();
            assertEquals(expectedString.length(), bytes.length);
            assertEquals(expectedString, new String(bytes, StandardCharsets.US_ASCII));
        }
    }
}
