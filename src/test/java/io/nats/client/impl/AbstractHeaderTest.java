package io.nats.client.impl;

import java.util.Collection;

import static org.junit.Assert.*;

public abstract class AbstractHeaderTest {

    protected static final String KEY1 = "key1";
    protected static final String KEY2 = "key2";
    protected static final String KEY3 = "key3";
    protected static final String VAL1 = "val1";
    protected static final String VAL2 = "val2";
    protected static final String VAL3 = "val3";
    protected static final String EMPTY = "";
    protected static final String ANY_VAL = "matters-it-does-not";

    interface IllegalArgumentHandler {
        void execute();
    }

    protected void assertIllegalArgument(IllegalArgumentHandler handler) {
        try {
            handler.execute();
            fail("IllegalArgumentException was expected to be thrown");
        } catch (IllegalArgumentException shouldBeThrown) {
        }
    }

    protected void assertContainsExactly(Header header, String... values) {
        assertNotNull(header.getValues());
        assertEquals(values.length, header.size());
        for (String v : values) {
            assertTrue(header.getValues().contains(v));
        }
    }

    protected void assertContainsExactly(Collection<String> collection, String... values) {
        assertNotNull(collection);
        assertEquals(collection.size(), values.length);
        for (String v : values) {
            assertTrue(collection.contains(v));
        }
    }

    protected Header testHeader() {
        return keyedTestHeader(KEY1, VAL1);
    }

    protected Header testHeader(String... values) {
        return keyedTestHeader(KEY1, values);
    }

    protected Header keyedTestHeader(String key) {
        return keyedTestHeader(key, VAL1);
    }

    protected Header keyedTestHeader(String key, String... values) {
        return new Header(key, values);
    }
}
