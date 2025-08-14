package io.nats.client.impl;

import io.nats.client.support.IncomingHeadersProcessor;
import io.nats.client.support.Status;
import io.nats.client.support.Token;
import io.nats.client.support.TokenType;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public class HeadersTests {
    private static final String KEY1 = "Key1";
    private static final String KEY2 = "Key2";
    private static final String KEY3 = "Key3";
    private static final String KEY1_ALT = "KEY1";
    private static final String KEY1_OTHER = "kEy1";
    private static final String KEY2_OTHER = "kEy2";
    private static final String VAL1 = "val1";
    private static final String VAL2 = "val2";
    private static final String VAL3 = "val3";
    private static final String VAL4 = "val4";
    private static final String VAL5 = "val5";
    private static final String VAL6 = "val6";
    private static final String EMPTY = "";

    @Test
    public void add_key_strings_works() {
        add(
            headers -> headers.add(KEY1, VAL1),
            headers -> headers.add(KEY1, VAL2),
            headers -> headers.add(KEY2, VAL3),
            headers -> { headers.add(KEY1_ALT, VAL4); headers.add(KEY1_ALT, VAL5); }
        );
    }

    @Test
    public void add_key_collection_works() {
        add(
            headers -> headers.add(KEY1, Collections.singletonList(VAL1)),
            headers -> headers.add(KEY1, Collections.singletonList(VAL2)),
            headers -> headers.add(KEY2, Collections.singletonList(VAL3)),
            headers -> headers.add(KEY1_ALT, Arrays.asList(VAL4, VAL5))
        );
    }

    private void add(
        Consumer<Headers> stepKey1Val1,
        Consumer<Headers> step2Key1Val2,
        Consumer<Headers> step3Key2Val3,
        Consumer<Headers> step4Key1AVal4Val5
    ) {
        Headers headers = new Headers();

        stepKey1Val1.accept(headers);
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER));
        assertNotContainsKeysIgnoreCase(headers, Arrays.asList(KEY2, KEY2_OTHER, KEY3));
        assertContainsKeys(headers, 1, Collections.singletonList(KEY1));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER), Collections.singletonList(VAL1));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1), Collections.singletonList(VAL1));
        validateDirtyAndLength(headers);

        step2Key1Val2.accept(headers);
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER));
        assertNotContainsKeysIgnoreCase(headers, Arrays.asList(KEY2, KEY2_OTHER, KEY3));
        assertContainsKeys(headers, 1, Collections.singletonList(KEY1));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER), Arrays.asList(VAL1, VAL2));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1), Arrays.asList(VAL1, VAL2));
        validateDirtyAndLength(headers);

        step3Key2Val3.accept(headers);
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER, KEY2, KEY2_OTHER));
        assertNotContainsKeysIgnoreCase(headers, Collections.singletonList(KEY3));
        assertContainsKeys(headers, 2, Arrays.asList(KEY1, KEY2));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY2, KEY2_OTHER), Collections.singletonList(VAL3));
        assertKeyContainsValues(headers, Collections.singletonList(KEY2), Collections.singletonList(VAL3));
        validateDirtyAndLength(headers);

        step4Key1AVal4Val5.accept(headers);
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER, KEY2, KEY2_OTHER));
        assertContainsKeys(headers, 3, Arrays.asList(KEY1, KEY2, KEY1_ALT));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER), Arrays.asList(VAL1, VAL2, VAL4, VAL5));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1), Arrays.asList(VAL1, VAL2));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1_ALT), Arrays.asList(VAL4, VAL5));
        validateDirtyAndLength(headers);
    }

    @Test
    public void put_key_strings_works() {
        put(
            headers -> headers.put(KEY1, VAL1),
            headers -> headers.put(KEY1, VAL2),
            headers -> headers.put(KEY2, VAL3),
            headers -> headers.put(KEY1_ALT, VAL4),
            headers -> headers.put(KEY1_OTHER, VAL5)
        );
    }

    @Test
    public void put_key_collection_works() {
        put(
            headers -> headers.put(KEY1, Collections.singletonList(VAL1)),
            headers -> headers.put(KEY1, Collections.singletonList(VAL2)),
            headers -> headers.put(KEY2, Collections.singletonList(VAL3)),
            headers -> headers.put(KEY1_ALT, Collections.singletonList(VAL4)),
            headers -> headers.put(KEY1_OTHER, Collections.singletonList(VAL5))
        );
    }

    private void put(
        Consumer<Headers> step1PutKey1Val1,
        Consumer<Headers> step2PutKey1Val2,
        Consumer<Headers> step3PutKey2Val3,
        Consumer<Headers> step4PutKey1AVal4,
        Consumer<Headers> step6PutKey1HVal5)
    {
        Headers headers = new Headers();
        assertTrue(headers.isEmpty());

        step1PutKey1Val1.accept(headers);
        assertContainsKeys(headers, 1, Collections.singletonList(KEY1));
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER));
        assertNotContainsKeysIgnoreCase(headers, Arrays.asList(KEY2, KEY2_OTHER, KEY3));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1), Collections.singletonList(VAL1));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER), Collections.singletonList(VAL1));
        validateDirtyAndLength(headers);

        step2PutKey1Val2.accept(headers);
        assertContainsKeys(headers, 1, Collections.singletonList(KEY1));
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER));
        assertNotContainsKeysIgnoreCase(headers, Arrays.asList(KEY2, KEY2_OTHER, KEY3));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1), Collections.singletonList(VAL2));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER), Collections.singletonList(VAL2));
        validateDirtyAndLength(headers);

        step3PutKey2Val3.accept(headers);
        assertContainsKeys(headers, 2, Arrays.asList(KEY1, KEY2));
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER, KEY2, KEY2_OTHER));
        assertNotContainsKeysIgnoreCase(headers, Collections.singletonList(KEY3));
        assertKeyContainsValues(headers, Collections.singletonList(KEY2), Collections.singletonList(VAL3));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY2, KEY2_OTHER), Collections.singletonList(VAL3));
        validateDirtyAndLength(headers);

        step4PutKey1AVal4.accept(headers);
        assertContainsKeys(headers, 3, Arrays.asList(KEY1, KEY1_ALT, KEY2));
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER, KEY2, KEY2_OTHER));
        assertNotContainsKeysIgnoreCase(headers, Collections.singletonList(KEY3));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1_ALT), Collections.singletonList(VAL4));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER), Arrays.asList(VAL2, VAL4));
        validateDirtyAndLength(headers);

        step6PutKey1HVal5.accept(headers);
        assertContainsKeys(headers, 4, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER, KEY2));
        assertNotContainsKeysIgnoreCase(headers, Collections.singletonList(KEY3));
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER, KEY2, KEY2_OTHER));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1_OTHER), Collections.singletonList(VAL5));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER), Arrays.asList(VAL2, VAL4, VAL5));
    }

    private void assertKeyIgnoreCaseContainsValues(Headers headers, List<String> keys, List<String> values) {
        for (String k : keys) {
            List<String> hVals = headers.getIgnoreCase(k);
            assertNotNull(hVals);
            assertEquals(values.size(), hVals.size());
            for (String v : values) {
                assertTrue(hVals.contains(v));
            }
        }
    }

    private void assertKeyContainsValues(Headers headers, List<String> keys, List<String> values) {
        for (String k : keys) {
            List<String> hVals = headers.get(k);
            assertNotNull(hVals);
            assertEquals(values.size(), hVals.size());
            for (String v : values) {
                assertTrue(hVals.contains(v));
            }
        }
    }

    private void assertContainsKeysIgnoreCase(Headers headers, List<String> keys) {
        Set<String> keySet = headers.keySetIgnoreCase();
        assertNotNull(keySet);
        for (String k : keys) {
            assertTrue(keySet.contains(k.toLowerCase()));
            assertTrue(headers.containsKeyIgnoreCase(k));
        }
    }

    private void assertNotContainsKeysIgnoreCase(Headers headers, List<String> keys) {
        Set<String> keySet = headers.keySetIgnoreCase();
        for (String k : keys) {
            assertFalse(keySet.contains(k.toLowerCase()));
            assertFalse(headers.containsKeyIgnoreCase(k));
            assertNull(headers.getIgnoreCase(k));
        }
    }

    private void assertContainsKeys(Headers headers, int countUniqueKeys, List<String> keys) {
        Set<String> keySet = headers.keySet();
        assertNotNull(keySet);
        assertEquals(countUniqueKeys, headers.size());
        for (String k : keys) {
            assertTrue(keySet.contains(k));
            assertTrue(headers.containsKey(k));
        }
    }

    @Test
    public void testReadOnly() {
        Headers notRO = new Headers();
        notRO.put(KEY1, VAL1);
        assertFalse(notRO.isReadOnly());
        Headers headers1 = new Headers(notRO, true);
        assertTrue(headers1.isReadOnly());
        assertThrows(UnsupportedOperationException.class, () -> headers1.put(KEY1, VAL2));
        assertThrows(UnsupportedOperationException.class, () -> headers1.put(KEY1, VAL2));
        assertThrows(UnsupportedOperationException.class, () -> headers1.put(KEY1, VAL1, VAL2));
        assertThrows(UnsupportedOperationException.class, () -> headers1.put(KEY1, Arrays.asList(VAL1, VAL2)));
        assertThrows(UnsupportedOperationException.class, () -> headers1.remove(KEY1));
        assertThrows(UnsupportedOperationException.class, () -> headers1.remove(KEY1,KEY2));
        assertThrows(UnsupportedOperationException.class, () -> headers1.remove(Arrays.asList(KEY1,KEY2)));
        assertThrows(UnsupportedOperationException.class, () -> headers1.add(KEY1, VAL2));
        assertThrows(UnsupportedOperationException.class, () -> headers1.add(KEY1, Arrays.asList(VAL1, VAL2)));
        assertThrows(UnsupportedOperationException.class, headers1::clear);
        assertEquals(VAL1, headers1.getFirst(KEY1));
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
        validateDirtyAndLength(headers);

        headers.add(KEY1, "");
        List<String> values = headers.get(KEY1);
        assertNotNull(values);
        assertEquals(1, values.size());
        validateDirtyAndLength(headers);

        headers.put(KEY1, "");
        values = headers.get(KEY1);
        assertNotNull(values);
        assertEquals(1, values.size());
        validateDirtyAndLength(headers);

        headers = new Headers();
        headers.add(KEY1, VAL1, "", VAL2);
        values = headers.get(KEY1);
        assertNotNull(values);
        assertEquals(3, values.size());
        validateDirtyAndLength(headers);

        headers.put(KEY1, VAL1, "", VAL2);
        values = headers.get(KEY1);
        assertNotNull(values);
        assertEquals(3, values.size());
        validateDirtyAndLength(headers);
    }

    @Test
    public void valuesThatAreNullButAreIgnored() {
        Headers headers = new Headers();
        assertEquals(0, headers.size());
        validateDirtyAndLength(headers);

        headers.add(KEY1, VAL1, null, VAL2);
        List<String> values = headers.get(KEY1);
        assertNotNull(values);
        assertEquals(2, values.size());
        validateDirtyAndLength(headers);

        headers.put(KEY1, VAL1, null, VAL2);
        values = headers.get(KEY1);
        assertNotNull(values);
        assertEquals(2, values.size());
        validateDirtyAndLength(headers);

        headers.clear();
        assertEquals(0, headers.size());
        validateDirtyAndLength(headers);

        headers.add(KEY1);
        assertEquals(0, headers.size());
        validateNotDirtyAndLength(headers);

        headers.put(KEY1);
        assertEquals(0, headers.size());
        validateNotDirtyAndLength(headers);

        headers.add(KEY1, (String)null);
        assertEquals(0, headers.size());
        validateNotDirtyAndLength(headers);

        headers.put(KEY1, (String)null);
        assertEquals(0, headers.size());
        validateNotDirtyAndLength(headers);

        headers.add(KEY1, (Collection<String>)null);
        assertEquals(0, headers.size());
        validateNotDirtyAndLength(headers);

        headers.put(KEY1, (Collection<String> )null);
        assertEquals(0, headers.size());
        validateNotDirtyAndLength(headers);
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
    public void valueCharactersMustBeUSAsciiExceptForCRLF() {
        Headers headers = new Headers();
        for (char c = 0; c < 256; c++) {
            final String val = "val" + c;
            if (c == 10 || c == 13 || c > 127) {
                assertThrows(IllegalArgumentException.class, () -> headers.put(KEY1, val));
            }
            else {
                assertDoesNotThrow(() -> headers.put(KEY1, val));
            }
        }
    }

    @Test
    public void remove_string_work() {
        remove(
            headers -> headers.remove(KEY1),
            headers -> headers.remove(KEY1_ALT),
            headers -> headers.remove(KEY1_OTHER),
            headers -> headers.remove(KEY2, KEY3)
        );
    }

    @Test
    public void remove_collection_work() {
        remove(
            headers -> headers.remove(Collections.singletonList(KEY1)),
            headers -> headers.remove(Collections.singletonList(KEY1_ALT)),
            headers -> headers.remove(Collections.singletonList(KEY1_OTHER)),
            headers -> headers.remove(Arrays.asList(KEY2, KEY3))
        );
    }

    @Test
    public void testGetFirstGetLast() {
        Headers headers = new Headers();
        assertNull(headers.getFirst(KEY1));
        assertNull(headers.getLast(KEY1));
        headers.add(KEY1, VAL1);
        assertEquals(VAL1, headers.getFirst(KEY1));
        assertEquals(VAL1, headers.getLast(KEY1));
        headers.add(KEY1, VAL2);
        assertEquals(VAL1, headers.getFirst(KEY1));
        assertEquals(VAL2, headers.getLast(KEY1));
        headers.put(KEY1, VAL3);
        assertEquals(VAL3, headers.getFirst(KEY1));
        assertEquals(VAL3, headers.getLast(KEY1));
    }

    private void remove(
        Consumer<Headers> step1RemoveKey1,
        Consumer<Headers> step2RemoveKey1A,
        Consumer<Headers> step3RemoveKey1H,
        Consumer<Headers> step4RemoveKey2Key3)
    {
        Headers headers = testHeaders();

        step1RemoveKey1.accept(headers);
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY2, KEY2_OTHER, KEY3));
        assertNotContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER));
        assertContainsKeys(headers, 2, Arrays.asList(KEY2, KEY3));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY2, KEY2_OTHER), Collections.singletonList(VAL2));
        assertKeyIgnoreCaseContainsValues(headers, Collections.singletonList(KEY3), Collections.singletonList(VAL3));
        assertKeyContainsValues(headers, Collections.singletonList(KEY2), Collections.singletonList(VAL2));
        assertKeyContainsValues(headers, Collections.singletonList(KEY3), Collections.singletonList(VAL3));
        validateDirtyAndLength(headers);

        headers = testHeaders();
        step2RemoveKey1A.accept(headers);
        assertContainsKeys(headers, 3, Arrays.asList(KEY1, KEY2, KEY3));

        headers = testHeaders();
        step3RemoveKey1H.accept(headers);
        assertContainsKeys(headers, 3, Arrays.asList(KEY1, KEY2, KEY3));

        headers = testHeaders();
        step4RemoveKey2Key3.accept(headers);
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER));
        assertNotContainsKeysIgnoreCase(headers, Arrays.asList(KEY2, KEY2_OTHER, KEY3));
        assertContainsKeys(headers, 1, Collections.singletonList(KEY1));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER), Collections.singletonList(VAL1));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1), Collections.singletonList(VAL1));
        validateDirtyAndLength(headers);
    }

    private byte[] validateDirtyAndLength(Headers headers) {
        assertTrue(headers.isDirty());
        byte[] serialized = headers.getSerialized();
        assertFalse(headers.isDirty());
        assertEquals(serialized.length, headers.serializedLength());
        return serialized;
    }

    private void validateNotDirtyAndLength(Headers headers) {
        assertFalse(headers.isDirty());
        byte[] serialized = headers.getSerialized();
        assertFalse(headers.isDirty());
        assertEquals(serialized.length, headers.serializedLength());
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
        byte[] serialized = validateDirtyAndLength(headers1);

        IncomingHeadersProcessor incomingHeadersProcessor = new IncomingHeadersProcessor(serialized);
        Headers headers2 = incomingHeadersProcessor.getHeaders();
        assertNotNull(headers2);
        validateDirtyAndLength(headers2);

        assertEquals(headers1.size(), headers2.size());
        assertTrue(headers2.containsKey(KEY1));
        assertTrue(headers2.containsKey(KEY2));
        List<String> values21 = headers2.get(KEY1);
        List<String> values22 = headers2.get(KEY2);
        List<String> values23 = headers2.get(KEY3);
        assertNotNull(values21);
        assertNotNull(values22);
        assertNotNull(values23);
        assertEquals(2, values21.size());
        assertEquals(1, values22.size());
        assertEquals(1, values23.size());
        assertTrue(values21.contains(VAL1));
        assertTrue(values21.contains(VAL3));
        assertTrue(values22.contains(VAL2));
        assertTrue(values23.contains(EMPTY));
    }

    @Test
    public void constructHeadersWithInvalidBytes() {
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor(null));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/0.0".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 \r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0X\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 \r\n\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 503\r".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 503\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0 FiveOhThree\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\nk1:v1".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\nk1:v1\r\n".getBytes()));
        assertThrows(IllegalArgumentException.class, () -> new IncomingHeadersProcessor("NATS/1.0\r\nk1:v1\r\r\n".getBytes()));
    }

    @Test
    public void constructHeadersWithValidBytes() {
        assertValidHeader("NATS/1.0\r\nk1:v1\r\n\r\n", "k1", "v1");
        assertValidHeader("NATS/1.0\r\nks1: v1\r\n\r\n", "ks1", "v1");
        assertValidHeader("NATS/1.0\r\nk1:\r\n\r\n", "k1", EMPTY);
        assertValidHeader("NATS/1.0\r\nks1: \r\n\r\n", "ks1", EMPTY);
        assertValidHeader("NATS/1.0\r\ncolons::::\r\n\r\n", "colons", ":::");
        assertValidHeader("NATS/1.0\r\n\r\n\r\n", null, null);
    }

    @Test
    public void constructStatusWithValidBytes() {
        assertValidStatus("NATS/1.0 503\r\n", 503, "No Responders Available For Request"); // status made message
        assertValidStatus("NATS/1.0 404\r\n", 404, "Server Status Message: 404");         // status made message
        assertValidStatus("NATS/1.0 503 No Responders\r\n", 503, "No Responders");         // from data
        assertValidStatus("NATS/1.0   503   No Responders\r\n", 503, "No Responders");
    }

    @Test
    public void verifyStatusBooleans() {
        Status status = new Status(Status.FLOW_OR_HEARTBEAT_STATUS_CODE, Status.FLOW_CONTROL_TEXT);
        assertTrue(status.isFlowControl());
        assertFalse(status.isHeartbeat());
        assertFalse(status.isNoResponders());

        status = new Status(Status.FLOW_OR_HEARTBEAT_STATUS_CODE, Status.HEARTBEAT_TEXT);
        assertFalse(status.isFlowControl());
        assertTrue(status.isHeartbeat());
        assertFalse(status.isNoResponders());

        status = new Status(Status.NO_RESPONDERS_CODE, Status.NO_RESPONDERS_TEXT);
        assertFalse(status.isFlowControl());
        assertFalse(status.isHeartbeat());
        assertTrue(status.isNoResponders());

        // path coverage
        status = new Status(Status.NO_RESPONDERS_CODE, "not no responders text");
        assertFalse(status.isNoResponders());
    }

    @Test
    public void constructHasStatusAndHeaders() {
        IncomingHeadersProcessor ihp = assertValidStatus("NATS/1.0 503\r\nfoo:bar\r\n\r\n", 503, "No Responders Available For Request"); // status made message
        assertValidHeader(ihp, "foo", "bar");
        ihp = assertValidStatus("NATS/1.0 503 No Responders\r\nfoo:bar\r\n\r\n", 503, "No Responders");         // from data
        assertValidHeader(ihp, "foo", "bar");
    }

    private void assertValidHeader(String test, String key, String val) {
        IncomingHeadersProcessor ihp = new IncomingHeadersProcessor(test.getBytes());
        assertValidHeader(ihp, key, val);
    }

    private void assertValidHeader(IncomingHeadersProcessor ihp, String key, String val) {
        Headers headers = ihp.getHeaders();
        if (key == null) {
            assertNull(headers);
        }
        else {
            assertNotNull(headers);
            assertEquals(1, headers.size());
            assertTrue(headers.containsKey(key));
            List<String> values = headers.get(key);
            assertNotNull(values);
            assertEquals(1, values.size());
            assertEquals(val, values.get(0));
        }
    }

    private IncomingHeadersProcessor assertValidStatus(String test, int code, String msg) {
        IncomingHeadersProcessor ihp = new IncomingHeadersProcessor(test.getBytes());
        assertValidStatus(ihp, code, msg);
        return ihp;
    }

    private void assertValidStatus(IncomingHeadersProcessor ihp, int code, String msg) {
        Status status = ihp.getStatus();
        assertNotNull(status);
        assertEquals(code, status.getCode());
        if (msg != null) {
            assertEquals(msg, status.getMessage());
        }
        IncomingMessageFactory imf = new IncomingMessageFactory("sid", "sub", "rt", 0, false);
        imf.setHeaders(ihp);
        assertTrue(imf.getMessage().isStatusMessage());
    }

    static class IteratorTestHelper {
        int manualCount = 0;
        int forEachCount = 0;
        int entrySetCount = 0;
        StringBuilder manualCompareString = new StringBuilder();
        StringBuilder forEachCompareString = new StringBuilder();
        StringBuilder entrySetCompareString = new StringBuilder();
    }

    @Test
    public void iteratorsTest() {
        Headers headers = testHeaders();
        headers.add(KEY1_ALT, VAL6);

        IteratorTestHelper helper = new IteratorTestHelper();
        for (String key : headers.keySet()) {
            helper.manualCount++;
            helper.manualCompareString.append(key);
            List<String> values = headers.get(key);
            assertNotNull(values);
            values.forEach(v -> helper.manualCompareString.append(v));
        }
        assertEquals(4, helper.manualCount);

        headers.forEach((key, values) -> {
            helper.forEachCount++;
            helper.forEachCompareString.append(key);
            values.forEach(v -> helper.forEachCompareString.append(v));
        });
        assertEquals(4, helper.forEachCount);

        headers.entrySet().forEach(entry -> {
            helper.entrySetCount++;
            helper.entrySetCompareString.append(entry.getKey());
            entry.getValue().forEach(v -> helper.entrySetCompareString.append(v));
        });
        assertEquals(4, helper.entrySetCount);

        assertEquals(helper.manualCompareString.toString(), helper.forEachCompareString.toString());
        assertEquals(helper.manualCompareString.toString(), helper.entrySetCompareString.toString());

        assertEquals(3, headers.keySetIgnoreCase().size());
    }

    private Headers testHeaders() {
        Headers headers = new Headers();
        validateDirtyAndLength(headers);
        headers.put(KEY1, VAL1);
        validateDirtyAndLength(headers);
        headers.put(KEY2, VAL2);
        validateDirtyAndLength(headers);
        headers.put(KEY3, VAL3);
        validateDirtyAndLength(headers);
        assertContainsKeysIgnoreCase(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER, KEY2, KEY2_OTHER, KEY3));
        assertContainsKeys(headers, 3, Arrays.asList(KEY1, KEY2, KEY3));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY1, KEY1_ALT, KEY1_OTHER), Collections.singletonList(VAL1));
        assertKeyIgnoreCaseContainsValues(headers, Arrays.asList(KEY2, KEY2_OTHER), Collections.singletonList(VAL2));
        assertKeyIgnoreCaseContainsValues(headers, Collections.singletonList(KEY3), Collections.singletonList(VAL3));
        assertKeyContainsValues(headers, Collections.singletonList(KEY1), Collections.singletonList(VAL1));
        assertKeyContainsValues(headers, Collections.singletonList(KEY2), Collections.singletonList(VAL2));
        assertKeyContainsValues(headers, Collections.singletonList(KEY3), Collections.singletonList(VAL3));
        return headers;
    }

    private void assertContainsExactly(Collection<String> actual, String... expected) {
        assertNotNull(actual);
        assertEquals(actual.size(), expected.length);
        for (String v : expected) {
            assertTrue(actual.contains(v));
        }
    }

    @Test
    public void nullPathways() {
        Headers h = new Headers();
        assertTrue(h.isEmpty());
        assertNull(h.get(KEY1));

        h = new Headers(h);
        assertTrue(h.isEmpty());

        h = new Headers(null);
        assertTrue(h.isEmpty());

        h.add(KEY1, (String[])null);
        assertTrue(h.isEmpty());

        h.put(KEY1, (Collection<String>)null);
        assertTrue(h.isEmpty());

        h.put(KEY1, (String[])null);
        assertTrue(h.isEmpty());
    }

    @Test
    public void equalsHash() {
        Headers h1 = new Headers();
        Headers h2 = new Headers();
        //noinspection MisorderedAssertEqualsArguments
        assertNotEquals(h1, null);
        //noinspection EqualsWithItself
        assertEquals(h1, h1);
        assertEquals(h1, h2);
        assertEquals(h1.hashCode(), h1.hashCode());
        assertEquals(h1.hashCode(), h2.hashCode());

        h1.add(KEY1, VAL1);
        h2.add(KEY1, VAL1);
        assertEquals(h1, h2);
        assertEquals(h1.hashCode(), h2.hashCode());

        h1.add(KEY2, VAL2);
        assertNotEquals(h1, h2);
        assertNotEquals(h1.hashCode(), h2.hashCode());

        //noinspection MisorderedAssertEqualsArguments
        assertNotEquals(h1, new Object());
    }

    @Test
    public void constructorWithHeaders() {
        Headers h = new Headers();
        h.add(KEY1, VAL1);
        h.add(KEY2, VAL2, VAL3);
        validateDirtyAndLength(h);

        Headers h2 = new Headers(h);
        assertEquals(2, h2.size());
        assertTrue(h2.containsKey(KEY1));
        assertTrue(h2.containsKey(KEY2));
        List<String> values1 = h2.get(KEY1);
        List<String> values2 = h2.get(KEY2);
        assertNotNull(values1);
        assertNotNull(values2);
        assertEquals(1, values1.size());
        assertEquals(2, values2.size());
        assertTrue(values1.contains(VAL1));
        assertTrue(values2.contains(VAL2));
        assertTrue(values2.contains(VAL3));
        validateDirtyAndLength(h2);
    }

    @Test
    public void testToken() {
        byte[] serialized1 = "notspaceorcrlf".getBytes(StandardCharsets.US_ASCII);
        assertThrows(IllegalArgumentException.class,
            () -> new Token(serialized1, serialized1.length, 0, TokenType.WORD));
        assertThrows(IllegalArgumentException.class,
            () -> new Token(serialized1, serialized1.length, 0, TokenType.KEY));
        assertThrows(IllegalArgumentException.class,
            () -> new Token(serialized1, serialized1.length, 0, TokenType.SPACE));
        assertThrows(IllegalArgumentException.class,
            () -> new Token(serialized1, serialized1.length, 0, TokenType.CRLF));
        byte[] serialized2 = "\r".getBytes(StandardCharsets.US_ASCII);
        assertThrows(IllegalArgumentException.class,
            () -> new Token(serialized2, serialized2.length, 0, TokenType.CRLF));
        byte[] serialized3 = "\rnotlf".getBytes(StandardCharsets.US_ASCII);
        assertThrows(IllegalArgumentException.class,
            () -> new Token(serialized3, serialized3.length, 0, TokenType.CRLF));
        Token t = new Token("k1:v1\r\n\r\n".getBytes(StandardCharsets.US_ASCII), 9, 0, TokenType.KEY);
        t.mustBe(TokenType.KEY);
        assertThrows(IllegalArgumentException.class, () -> t.mustBe(TokenType.CRLF));
    }

    @Test
    public void testTokenSamePoint() {
        byte[] serialized1 = " \r\n".getBytes(StandardCharsets.US_ASCII);
        Token t1 = new Token(serialized1, serialized1.length, 0, TokenType.SPACE);
        // equals
        Token t1Same = new Token(serialized1, serialized1.length, 0, TokenType.SPACE);
        assertTrue(t1.samePoint(t1Same));

        // same start, same end, different type
        byte[] notSame = "x\r\n".getBytes(StandardCharsets.US_ASCII);
        Token tNotSame = new Token(notSame, notSame.length, 0, TokenType.TEXT);
        assertFalse(t1.samePoint(tNotSame));

        // same start, different end, same type
        notSame = "  \r\n".getBytes(StandardCharsets.US_ASCII);
        tNotSame = new Token(notSame, notSame.length, 0, TokenType.SPACE);
        assertFalse(t1.samePoint(tNotSame));

        // different start
        notSame = "x  \r\n".getBytes(StandardCharsets.US_ASCII);
        tNotSame = new Token(notSame, notSame.length, 1, TokenType.SPACE);
        assertFalse(t1.samePoint(tNotSame));
    }

    @Test
    public void testToString() {
        assertNotNull(new Status(1, "msg").toString()); // COVERAGE

        Headers h = new Headers();
        assertEquals("", h.toString());

        h.add("NotAdded");
        h.add("Empty", "");
        h.add("Has1", "h1-1");
        h.add("Has2", "h2-1", "h2-2");

        assertFalse(h.toString().contains("NotAdded"));
        assertTrue(h.toString().contains("Empty:;"));
        assertTrue(h.toString().contains("Has1:h1-1;"));
        assertTrue(h.toString().contains("Has2:h2-1;"));
        assertTrue(h.toString().contains("Has2:h2-2;"));
    }

    @Test
    public void put_map_works() {
        Map<String, List<String>> map = new HashMap<>();
        map.put(KEY1, Collections.singletonList(VAL1));
        map.put(KEY2, Arrays.asList(VAL2, VAL3));
        Headers h = new Headers();
        h.put(map);

        assertEquals(2, h.size());
        assertTrue(h.containsKey(KEY1));
        assertTrue(h.containsKey(KEY2));
        List<String> l1 = h.get(KEY1);
        List<String> l2 = h.get(KEY2);
        assertNotNull(l1);
        assertNotNull(l2);
        assertEquals(1, l1.size());
        assertEquals(2, l2.size());
        assertTrue(l1.contains(VAL1));
        assertEquals(VAL1, h.getFirst(KEY1));
        assertTrue(l2.contains(VAL2));
        assertTrue(l2.contains(VAL3));
        assertEquals(VAL2, h.getFirst(KEY2));
    }

    @Test
    void testForEach() {
        Headers h = new Headers();
        h.put("test", "a","b","c");
        h.forEach((k, v) -> {
            assertEquals("test", k);
            assertContainsExactly(v, "a", "b", "c");
            assertThrows(UnsupportedOperationException.class, ()->v.add("z"));
        });
    }

    @Test
    void testCheckValue() {
        Headers h = new Headers();
        h.put("test1", "\u0000 \f\b\t");

        assertThrows(IllegalArgumentException.class, ()->h.put("test", "×"));
        assertThrows(IllegalArgumentException.class, ()->h.put("test", "\r"));
        assertThrows(IllegalArgumentException.class, ()->h.put("test", "\n"));

        assertEquals(1, h.size());
        List<String> l1 = h.get("test1");
        assertNotNull(l1);
        assertEquals(1, l1.size());
        assertEquals("\u0000 \f\b\t", h.getFirst("test1"));
    }

//    @Test
//    void benchmark_serializeToArray() {
//        Headers h = new Headers().put("test", "aaa", "bBb", "ZZZZZZZZ")
//            .put("ALongLongLongLongLongLongLongKey", "VeryLongLongLongLongLongLongLongLongLong:Value!");
//        assertEquals(
//            "ALongLongLongLongLongLongLongKey:VeryLongLongLongLongLongLongLongLongLong:Value!; test:aaa; test:bBb; test:ZZZZZZZZ;",
//            h.toString());
//
//        byte[] dst = new byte[1000];
//        for (int i = 0; i < 10_000; i++) {// warm-up
//            assertEquals(129, h.serializeToArray(0, dst));
//        }
//
//        long t = System.nanoTime();
//        int max = 100_000_000;
//        for (int i = 0; i < max; i++) {
//            h.serializeToArray(0, dst);
//        }
//        t = System.nanoTime() - t;
//        System.out.println("Time: " + t / 1000 / 1000.0 +"ms, Op/sec: "+(max*1_000_000_000L/t));
//    }
}
