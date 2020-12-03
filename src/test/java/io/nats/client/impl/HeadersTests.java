package io.nats.client.impl;

import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.support.Status;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public class HeadersTests {
    private static final String KEY1 = "key1";
    private static final String KEY2 = "key2";
    private static final String KEY3 = "key3";
    private static final String VAL1 = "val1";
    private static final String VAL2 = "val2";
    private static final String VAL3 = "val3";
    private static final String EMPTY = "";

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
        assertThrows(IllegalArgumentException.class, () -> headers.put(null, VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.put(null, VAL1, VAL2));
        assertThrows(IllegalArgumentException.class, () -> headers.put(null, Collections.singletonList(VAL1)));
        assertThrows(IllegalArgumentException.class, () -> headers.put(EMPTY, VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.put(EMPTY, VAL1, VAL2));
        assertThrows(IllegalArgumentException.class, () -> headers.put(EMPTY, Collections.singletonList(VAL1)));
        assertThrows(IllegalArgumentException.class, () -> headers.add(null, VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.add(null, VAL1, VAL2));
        assertThrows(IllegalArgumentException.class, () -> headers.add(null, Collections.singletonList(VAL1)));
        assertThrows(IllegalArgumentException.class, () -> headers.add(EMPTY, VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.add(EMPTY, VAL1, VAL2));
        assertThrows(IllegalArgumentException.class, () -> headers.add(EMPTY, Collections.singletonList(VAL1)));
    }

    @Test
    public void valuesThatAreEmptyButAreAllowed() {
        Headers headers = new Headers();
        assertEquals(0, headers.size());

        headers.add(KEY1, "");
        assertEquals(1, headers.values(KEY1).size());

        headers.put(KEY1, "");
        assertEquals(1, headers.values(KEY1).size());

        headers = new Headers();
        headers.add(KEY1, VAL1, "", VAL2);
        assertEquals(3, headers.values(KEY1).size());

        headers.put(KEY1, VAL1, "", VAL2);
        assertEquals(3, headers.values(KEY1).size());
    }

    @Test
    public void valuesThatAreNullButAreIgnored() {
        Headers headers = new Headers();
        assertEquals(0, headers.size());

        headers.add(KEY1, VAL1, null, VAL2);
        assertEquals(2, headers.values(KEY1).size());

        headers.put(KEY1, VAL1, null, VAL2);
        assertEquals(2, headers.values(KEY1).size());

        headers.clear();
        assertEquals(0, headers.size());

        headers.add(KEY1);
        assertEquals(0, headers.size());

        headers.put(KEY1);
        assertEquals(0, headers.size());

        headers.add(KEY1, (String)null);
        assertEquals(0, headers.size());

        headers.put(KEY1, (String)null);
        assertEquals(0, headers.size());

        headers.add(KEY1, (Collection<String>)null);
        assertEquals(0, headers.size());

        headers.put(KEY1, (Collection<String> )null);
        assertEquals(0, headers.size());
    }

    @Test
    public void keyCharactersMustBePrintableExceptForColon() {
        Headers headers = new Headers();
        // ctrl characters, space and colon are not allowed
        for (char c = 0; c < 33; c++) {
            final String key = "key" + c;
            assertThrows(IllegalArgumentException.class, () -> headers.put(key, VAL1));
        }
        assertThrows(IllegalArgumentException.class, () -> headers.put("key:", VAL1));
        assertThrows(IllegalArgumentException.class, () -> headers.put("key" + (char)127, VAL1));

        // all other characters are good
        for (char c = 33; c < ':'; c++) {
            headers.put("key" + c, VAL1);
        }

        for (char c = ':' + 1; c < 127; c++) {
            headers.put("key" + c, VAL1);
        }
    }

    @Test
    public void valueCharactersMustBePrintableOrTab() {
        Headers headers = new Headers();
        // ctrl characters, except for tab not allowed
        for (char c = 0; c < 9; c++) {
            final String val = "val" + c;
            assertThrows(IllegalArgumentException.class, () -> headers.put(KEY1, val));
        }
        for (char c = 10; c < 32; c++) {
            final String val = "val" + c;
            assertThrows(IllegalArgumentException.class, () -> headers.put(KEY1, val));
        }
        assertThrows(IllegalArgumentException.class, () -> headers.put(KEY1, "val" + (char)127));

        // printable and tab are allowed
        for (char c = 32; c < 127; c++) {
            headers.put(KEY1, "" + c);
        }

        headers.put(KEY1, "val" + (char)9);
    }

    @Test
    public void removes_work() {
        Headers headers = testHeaders();
        headers.remove(KEY1);
        assertContainsKeysExactly(headers, KEY2, KEY3);

        headers = testHeaders();
        headers.remove(KEY2, KEY3);
        assertContainsKeysExactly(headers, KEY1);

        headers = testHeaders();
        headers.remove(Collections.singletonList(KEY1));
        assertContainsKeysExactly(headers, KEY2, KEY3);

        headers = testHeaders();
        headers.remove(Arrays.asList(KEY2, KEY3));
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

        assertEquals(1, headers1.size());
        assertFalse(headers1.isEmpty());
        headers1.clear();
        assertEquals(0, headers1.size());
        assertTrue(headers1.isEmpty());
    }

    @Test
    public void serialize_deserialize() {
        Headers headers1 = new Headers();
        headers1.add(KEY1, VAL1);
        headers1.add(KEY1, VAL3);
        headers1.add(KEY2, VAL2);
        headers1.add(KEY3, EMPTY);

        byte[] serialized = headers1.getSerialized();
        assertEquals(serialized.length, headers1.serializedLength());

        IncomingHeadersProcessor incomingHeadersProcessor = new IncomingHeadersProcessor(serialized);
        Headers headers2 = incomingHeadersProcessor.getHeaders();
        assertNotNull(headers2);

        assertEquals(headers1.size(), headers2.size());
        assertTrue(headers2.containsKey(KEY1));
        assertTrue(headers2.containsKey(KEY2));
        assertEquals(2, headers2.values(KEY1).size());
        assertEquals(1, headers2.values(KEY2).size());
        assertEquals(1, headers2.values(KEY3).size());
        assertTrue(headers2.values(KEY1).contains(VAL1));
        assertTrue(headers2.values(KEY1).contains(VAL3));
        assertTrue(headers2.values(KEY2).contains(VAL2));
        assertTrue(headers2.values(KEY3).contains(EMPTY));
    }

    @Test
    public void constructHeadersWithInvalidBytes() {
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor((byte[]) null));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/0.0".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 \r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0X\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 \r\n\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\n\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 503\r".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 503\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 FiveOhThree\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\n\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\n\r\n\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\nk1:v1".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\nk1:v1\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\nk1:v1\r\r\n".getBytes()));
    }

    @Test
    public void constructHeadersWithValidBytes() {
        assertValidHeader("NATS/1.0\r\nk1:v1\r\n\r\n", "k1", "v1");
        assertValidHeader("NATS/1.0\r\nk1: v1\r\n\r\n", "k1", "v1");
        assertValidHeader("NATS/1.0\r\nk1:\r\n\r\n", "k1", EMPTY);
        assertValidHeader("NATS/1.0\r\nk1: \r\n\r\n", "k1", EMPTY);
    }

    private void assertValidHeader(String test, String key, String val) {
        IncomingHeadersProcessor incomingHeadersProcessor = new IncomingHeadersProcessor(test.getBytes());
        Headers headers = incomingHeadersProcessor.getHeaders();
        assertNotNull(headers);
        assertEquals(1, headers.size());
        assertTrue(headers.containsKey(key));
        assertEquals(1, headers.values(key).size());
        assertEquals(val, headers.values(key).get(0));
    }

    @Test
    public void constructStatusWithValidBytes() {
        assertValidStatus("NATS/1.0 503\r\n", 503, null);
        assertValidStatus("NATS/1.0 503 No Responders\r\n", 503, "No Responders");
        assertValidStatus("NATS/1.0   503   No Responders\r\n", 503, "No Responders");
    }

    private void assertValidStatus(String test, int code, String msg) {
        IncomingHeadersProcessor incomingHeadersProcessor = new IncomingHeadersProcessor(test.getBytes());
        Status status = incomingHeadersProcessor.getStatus();
        assertNotNull(status);
        assertEquals(code, status.getCode());
        if (msg != null) {
            assertEquals(msg, status.getMessage());
        }
    }

    class IteratorTestHelper {
        String forEachString = "";
        String entrySetString = "";
        String manualString = "";
    }

    @Test
    public void iteratorsTest() {
        Headers headers = testHeaders();
        IteratorTestHelper helper = new IteratorTestHelper();

        for (String key : headers.keySet()) {
            helper.manualString += key;
            headers.values(key).forEach(v -> helper.manualString += v);
        }

        headers.forEach((key, values) -> {
            helper.forEachString += key;
            values.forEach(v -> helper.forEachString += v);
        });

        headers.entrySet().forEach(entry -> {
            helper.entrySetString += entry.getKey();
            entry.getValue().forEach(v -> helper.entrySetString += v);
        });

        assertEquals(helper.manualString, helper.forEachString);
        assertEquals(helper.manualString, helper.entrySetString);
    }

    private Headers testHeaders() {
        Headers headers = new Headers();
        headers.put(KEY1, VAL1);
        headers.put(KEY2, VAL2);
        headers.put(KEY3, VAL3);
        return headers;
    }

    private void assertContainsExactly(List<String> actual, String... expected) {
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
