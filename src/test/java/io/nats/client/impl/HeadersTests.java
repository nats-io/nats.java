package io.nats.client.impl;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HeadersTests extends AbstractHeaderTest {

    @Test
    public void add_header_works() {
        add(
                headers -> headers.add(keyedTestHeader(KEY1, VAL1)),
                headers -> headers.add(keyedTestHeader(KEY1, VAL2)),
                headers -> headers.add(keyedTestHeader(KEY2, VAL3)));
    }

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
        assertEquals(1, headers.headers().size());
        assertTrue(headers.containsKey(KEY1));
        assertContainsExactly(headers.header(KEY1), VAL1);
        assertContainsExactly(headers.headerValues(KEY1), VAL1);

        step2Key1Val2.accept(headers);
        assertEquals(1, headers.size());
        assertEquals(1, headers.headers().size());
        assertTrue(headers.containsKey(KEY1));
        assertContainsExactly(headers.header(KEY1), VAL1, VAL2);
        assertContainsExactly(headers.headerValues(KEY1), VAL1, VAL2);

        step3Key2Val3.accept(headers);
        assertEquals(2, headers.size());
        assertEquals(2, headers.headers().size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.containsKey(KEY2));
        assertContainsExactly(headers.header(KEY1), VAL1, VAL2);
        assertContainsExactly(headers.headerValues(KEY1), VAL1, VAL2);
        assertContainsExactly(headers.header(KEY2), VAL3);
        assertContainsExactly(headers.headerValues(KEY2), VAL3);
    }

    @Test
    public void set_header_works() {
        set(
                headers -> headers.set(keyedTestHeader(KEY1, VAL1)),
                headers -> headers.set(keyedTestHeader(KEY1, VAL2)),
                headers -> headers.set(keyedTestHeader(KEY2, VAL3)));
    }

    @Test
    public void set_key_strings_works() {
        set(
                headers -> headers.set(KEY1, VAL1),
                headers -> headers.set(KEY1, VAL2),
                headers -> headers.set(KEY2, VAL3));
    }

    @Test
    public void set_key_collection_works() {
        set(
                headers -> headers.set(KEY1, Collections.singletonList(VAL1)),
                headers -> headers.set(KEY1, Collections.singletonList(VAL2)),
                headers -> headers.set(KEY2, Collections.singletonList(VAL3)));
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
        assertEquals(1, headers.keys().size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.keys().contains(KEY1));
        assertContainsExactly(headers.header(KEY1), VAL1);
        assertContainsExactly(headers.headerValues(KEY1), VAL1);

        step2Key1Val2.accept(headers);
        assertEquals(1, headers.size());
        assertEquals(1, headers.keys().size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.keys().contains(KEY1));
        assertContainsExactly(headers.header(KEY1), VAL2);
        assertContainsExactly(headers.headerValues(KEY1), VAL2);

        step3Key2Val3.accept(headers);
        assertEquals(2, headers.size());
        assertEquals(2, headers.keys().size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.containsKey(KEY2));
        assertTrue(headers.keys().contains(KEY1));
        assertTrue(headers.keys().contains(KEY2));
        assertContainsExactly(headers.header(KEY1), VAL2);
        assertContainsExactly(headers.headerValues(KEY1), VAL2);
        assertContainsExactly(headers.header(KEY2), VAL3);
        assertContainsExactly(headers.headerValues(KEY2), VAL3);
    }

    @Test
    public void remove_works() {
        Headers headers = forRemove_startHeaders();
        headers.remove(KEY1);
        assertEquals(2, headers.size());
        assertTrue(headers.containsKey(KEY2));
        assertTrue(headers.containsKey(KEY3));

        headers = forRemove_startHeaders();
        headers.remove(KEY1, KEY2);
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(KEY3));

        headers = forRemove_startHeaders();
        headers.remove(Collections.singleton(KEY1));
        assertEquals(2, headers.size());
        assertTrue(headers.containsKey(KEY2));
        assertTrue(headers.containsKey(KEY3));

        headers = forRemove_startHeaders();
        headers.remove(Arrays.asList(KEY1, KEY2));
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(KEY3));

        headers.clear();
        assertTrue(headers.isEmpty());
    }

    @Test
    public void stream_iterator_forEach_works() {
        Headers headers = forRemove_startHeaders();
        assertEquals(3, headers.stream().count());

        final AtomicInteger iterCount = new AtomicInteger(0);
        headers.iterator().forEachRemaining(h -> iterCount.incrementAndGet());
        assertEquals(3, iterCount.get());

        final AtomicInteger splCount = new AtomicInteger(0);
        headers.spliterator().forEachRemaining(h -> splCount.incrementAndGet());
        assertEquals(3, splCount.get());

        final AtomicInteger feCount = new AtomicInteger(0);
        headers.forEach(h -> feCount.incrementAndGet());
        assertEquals(3, feCount.get());
    }

    private Headers forRemove_startHeaders() {
        Headers headers = new Headers();
        assertTrue(headers.isEmpty());
        headers.set(KEY1, VAL1);
        headers.set(KEY2, VAL2);
        headers.set(KEY3, VAL3);
        forRemove_assert3Keys(headers);
        return headers;
    }

    private void forRemove_assert3Keys(Headers headers) {
        assertEquals(3, headers.size());
        assertTrue(headers.containsKey(KEY1));
        assertTrue(headers.containsKey(KEY2));
        assertTrue(headers.containsKey(KEY3));
    }
}
