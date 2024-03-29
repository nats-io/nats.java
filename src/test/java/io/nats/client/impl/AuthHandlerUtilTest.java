package io.nats.client.impl;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.CharBuffer;

public class AuthHandlerUtilTest {
    private static final String TEST_CRED_JWT_COMBINED = "-jwt\neyJ...BQ\n-nkey\nSUAG...7PHUE";
    private static final String TEST_CRED_JWT_ONLY = "eyJ...BQ";
    private static final String TEST_CRED_NKEY_ONLY = "SUAG...7PHUE";

    @Test
    public void testExtractJWTFromCombined() {
        CharBuffer data = CharBuffer.wrap(TEST_CRED_JWT_COMBINED);
        char[] jwt = AuthHandlerUtil.extract(data, 1);
        Assertions.assertArrayEquals(TEST_CRED_JWT_ONLY.toCharArray(), jwt, "JWT extracted matches expected value.");
    }

    @Test
    public void testExtractNKeyFromCombined() {
        CharBuffer data = CharBuffer.wrap(TEST_CRED_JWT_COMBINED);
        char[] nkey = AuthHandlerUtil.extract(data, 2);
        Assertions.assertArrayEquals(TEST_CRED_NKEY_ONLY.toCharArray(), nkey, "NKey extracted matches expected value.");
    }

    @Test
    public void testExtractWithSkippingLines() {
        String testData = "-skip\n-skip\n" + TEST_CRED_JWT_COMBINED;
        CharBuffer data = CharBuffer.wrap(testData);
        char[] result = AuthHandlerUtil.extract(data, 3); // Skip 2 lines then extract JWT
        Assertions.assertArrayEquals(TEST_CRED_JWT_ONLY.toCharArray(), result, "Extracted JWT should correctly skip headers.");
    }

    @Test
    public void testExtractJWTOnly() {
        CharBuffer data = CharBuffer.wrap(TEST_CRED_JWT_ONLY);
        char[] result = AuthHandlerUtil.extract(data, 0); // No headers before JWT
        Assertions.assertArrayEquals(TEST_CRED_JWT_ONLY.toCharArray(), result, "Extracted JWT matches expected value.");
    }

    @Test
    public void testExtractNKeyOnly() {
        CharBuffer data = CharBuffer.wrap(TEST_CRED_NKEY_ONLY);
        char[] result = AuthHandlerUtil.extract(data, 0); // No headers before NKey
        Assertions.assertArrayEquals(TEST_CRED_NKEY_ONLY.toCharArray(), result, "Extracted NKey matches expected value.");
    }
}
