package io.nats.client.impl;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class HeadersTests {
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";
    private static final String KEY3 = "key3";
    private static final String VAL1 = "val1";
    private static final String VAL2 = "val2";
    private static final String VAL3 = "val3";
    private static final String EMPTY = "";
    private static final String ANY_VAL = "matters-it-does-not";

    @Test
    public void add_key_strings_works() {
        add(
                headers -> headers.add(KEY1, VAL1),
                headers -> headers.add(KEY1, VAL2),
                headers -> headers.add(KEY2, VAL3));
    }

    @Test
    public void add_key_collection_works() {
        add(
                headers -> headers.add(KEY1, Collections.singletonList(VAL1)),
                headers -> headers.add(KEY1, Collections.singletonList(VAL2)),
                headers -> headers.add(KEY2, Collections.singletonList(VAL3)));
    }

    private void add(
            Consumer<Headers> stepKey1Val1,
            Consumer<Headers> step2Key1Val2,
            Consumer<Headers> step3Key2Val3)
    {
        Headers headers = new Headers();

        stepKey1Val1.accept(headers);
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertContainsExactly(headers.values(KEY1), VAL1);

        step2Key1Val2.accept(headers);
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertContainsExactly(headers.values(KEY1), VAL1, VAL2);

        step3Key2Val3.accept(headers);
        assertEquals(2, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.containsKey(KEY2));
        assertContainsExactly(headers.values(KEY1), VAL1, VAL2);
        assertContainsExactly(headers.values(KEY2), VAL3);
    }

    @Test
    public void set_key_strings_works() {
        set(
                headers -> headers.put(KEY1, VAL1),
                headers -> headers.put(KEY1, VAL2),
                headers -> headers.put(KEY2, VAL3));
    }

    @Test
    public void set_key_collection_works() {
        set(
                headers -> headers.put(KEY1, Collections.singletonList(VAL1)),
                headers -> headers.put(KEY1, Collections.singletonList(VAL2)),
                headers -> headers.put(KEY2, Collections.singletonList(VAL3)));
    }

    private void set(
            Consumer<Headers> stepKey1Val1,
            Consumer<Headers> step2Key1Val2,
            Consumer<Headers> step3Key2Val3)
    {
        Headers headers = new Headers();
        assertTrue(headers.isEmpty());

        stepKey1Val1.accept(headers);
        assertEquals(1, headers.size());
        assertEquals(1, headers.keySet().size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.keySet().contains(KEY1));
        assertContainsExactly(headers.values(KEY1), VAL1);

        step2Key1Val2.accept(headers);
        assertEquals(1, headers.size());
        assertEquals(1, headers.keySet().size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.keySet().contains(KEY1));
        assertContainsExactly(headers.values(KEY1), VAL2);

        step3Key2Val3.accept(headers);
        assertEquals(2, headers.size());
        assertEquals(2, headers.keySet().size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.containsKey(KEY2));
        assertTrue(headers.keySet().contains(KEY1));
        assertTrue(headers.keySet().contains(KEY2));
        assertContainsExactly(headers.values(KEY1), VAL2);
        assertContainsExactly(headers.values(KEY2), VAL3);
    }

    @Test
    public void keyCannotBeNullOrEmpty() {
        Headers headers = new Headers();
        assertIllegalArgument(() -> headers.put(null, VAL1));
        assertIllegalArgument(() -> headers.put(null, VAL1, VAL2));
        assertIllegalArgument(() -> headers.put(null, Collections.singletonList(VAL1)));
        assertIllegalArgument(() -> headers.put(EMPTY, VAL1));
        assertIllegalArgument(() -> headers.put(EMPTY, VAL1, VAL2));
        assertIllegalArgument(() -> headers.put(EMPTY, Collections.singletonList(VAL1)));
        assertIllegalArgument(() -> headers.add(null, VAL1));
        assertIllegalArgument(() -> headers.add(null, VAL1, VAL2));
        assertIllegalArgument(() -> headers.add(null, Collections.singletonList(VAL1)));
        assertIllegalArgument(() -> headers.add(EMPTY, VAL1));
        assertIllegalArgument(() -> headers.add(EMPTY, VAL1, VAL2));
        assertIllegalArgument(() -> headers.add(EMPTY, Collections.singletonList(VAL1)));
    }

    @Test
    public void valuesCannotBeNullOrEmpty() {
        Headers headers = new Headers();

        assertIllegalArgument(() -> headers.put(KEY1, (String) null));
        assertIllegalArgument(() -> headers.put(KEY1, EMPTY));
        assertIllegalArgument(() -> headers.put(KEY1, ANY_VAL, EMPTY));
        assertIllegalArgument(() -> headers.put(KEY1, ANY_VAL, null));
        assertIllegalArgument(() -> headers.put(KEY1, (Collection<String>) null));
        assertIllegalArgument(() -> headers.put(KEY1, Arrays.asList(KEY1, EMPTY)));
        assertIllegalArgument(() -> headers.put(KEY1, Arrays.asList(KEY1, null)));

        assertIllegalArgument(() -> headers.add(KEY1, (String) null));
        assertIllegalArgument(() -> headers.add(KEY1, EMPTY));
        assertIllegalArgument(() -> headers.add(KEY1, ANY_VAL, EMPTY));
        assertIllegalArgument(() -> headers.add(KEY1, ANY_VAL, null));
        assertIllegalArgument(() -> headers.add(KEY1, (Collection<String>) null));
        assertIllegalArgument(() -> headers.add(KEY1, Arrays.asList(KEY1, EMPTY)));
        assertIllegalArgument(() -> headers.add(KEY1, Arrays.asList(KEY1, null)));
    }

    @Test
    public void removes_work() {
        Headers headers = testHeaders();
        assertTrue(headers.remove(KEY1));
        assertFalse(headers.remove(KEY1));
        assertContainsKeysExactly(headers, KEY2, KEY3);

        headers = testHeaders();
        assertTrue(headers.remove(KEY2, KEY3));
        assertFalse(headers.remove(KEY2, KEY3));
        assertContainsKeysExactly(headers, KEY1);

        headers = testHeaders();
        assertTrue(headers.remove(Collections.singletonList(KEY1)));
        assertFalse(headers.remove(Collections.singletonList(KEY1)));
        assertContainsKeysExactly(headers, KEY2, KEY3);

        headers = testHeaders();
        assertTrue(headers.remove(Arrays.asList(KEY2, KEY3)));
        assertFalse(headers.remove(Arrays.asList(KEY2, KEY3)));
        assertContainsKeysExactly(headers, KEY1);
    }

    @Test
    public void equalsHashcodeClearSizeEmpty_work() {
        assertEquals(testHeaders(), testHeaders());
        assertEquals(testHeaders().hashCode(), testHeaders().hashCode());

        Headers headers1 = new Headers();
        headers1.put(KEY1, VAL1);
        Headers headers2 = new Headers();
        headers2.put(KEY2, VAL2);
        assertNotEquals(headers1, headers2);
        assertEquals(headers1.hashCode(), headers2.hashCode());

        assertEquals(1, headers1.size());
        assertFalse(headers1.isEmpty());
        headers1.clear();
        assertEquals(0, headers1.size());
        assertTrue(headers1.isEmpty());
    }

    private Headers testHeaders() {
        Headers headers = new Headers();
        headers.put(KEY1, VAL1);
        headers.put(KEY2, VAL2);
        headers.put(KEY3, VAL3);
        return headers;
    }

    // assert macros
    interface IllegalArgumentHandler {
        void execute();
    }

    private void assertIllegalArgument(IllegalArgumentHandler handler) {
        try {
            handler.execute();
            fail("IllegalArgumentException was expected to be thrown");
        } catch (IllegalArgumentException ignored) {}
    }

    private void assertContainsExactly(Set<String> actual, String... expected) {
        assertNotNull(actual);
        assertEquals(actual.size(), expected.length);
        for (String v : expected) {
            assertTrue(actual.contains(v));
        }
    }

    private void assertContainsKeysExactly(Headers header, String... expected) {
        assertEquals(header.size(), expected.length);
        for (String key : expected) {
            assertTrue(header.containsKey(key));
        }
    }
}
