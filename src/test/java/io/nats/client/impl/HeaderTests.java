package io.nats.client.impl;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.*;

public class HeaderTests {

    private static final String KEY = "key";
    private static final String VAL1 = "val1";
    private static final String VAL2 = "val2";
    private static final String VAL3 = "val3";
    private static final String EMPTY = "";
    private static final String ANY_VAL = "matters-it-does-not";

    interface IllegalArgumentHandler {
        void execute();
    }

    private void assertIllegalArgument(IllegalArgumentHandler handler) {
        try {
            handler.execute();
            fail("IllegalArgumentException was expected to be thrown");
        } catch (IllegalArgumentException shouldBeThrown) {
        }
    }

    @Test
    public void header_keyCannotBeNullOrEmpty() {
        // constructor
        assertIllegalArgument(() -> new Header(null, VAL1));
        assertIllegalArgument(() -> new Header(null, VAL1, VAL2));
        assertIllegalArgument(() -> new Header(null, Collections.singletonList(VAL1)));
        assertIllegalArgument(() -> new Header(EMPTY, VAL1));
        assertIllegalArgument(() -> new Header(EMPTY, VAL1, VAL2));
        assertIllegalArgument(() -> new Header(EMPTY, Collections.singletonList(VAL1)));
    }

    @Test
    public void header_valuesCannotBeNullOrEmpty() {
        // constructor
        assertIllegalArgument(() -> new Header(KEY, (String) null));
        assertIllegalArgument(() -> new Header(KEY, EMPTY));
        assertIllegalArgument(() -> new Header(KEY, ANY_VAL, EMPTY));
        assertIllegalArgument(() -> new Header(KEY, ANY_VAL, null));
        assertIllegalArgument(() -> new Header(KEY, (Collection<String>) null));
        assertIllegalArgument(() -> new Header(KEY, Arrays.asList(KEY, EMPTY)));
        assertIllegalArgument(() -> new Header(KEY, Arrays.asList(KEY, null)));

        // regular setters
        assertIllegalArgument(() -> starterHeader().setValues(EMPTY));
        assertIllegalArgument(() -> starterHeader().setValues((String) null));
        assertIllegalArgument(() -> starterHeader().setValues(ANY_VAL, EMPTY));
        assertIllegalArgument(() -> starterHeader().setValues(ANY_VAL, null));
        assertIllegalArgument(() -> starterHeader().setValues((Collection<String>) null));
        assertIllegalArgument(() -> starterHeader().setValues(Arrays.asList(KEY, EMPTY)));
        assertIllegalArgument(() -> starterHeader().setValues(Arrays.asList(KEY, null)));

        // fluent setters
        assertIllegalArgument(() -> starterHeader().values(EMPTY));
        assertIllegalArgument(() -> starterHeader().values((String) null));
        assertIllegalArgument(() -> starterHeader().values(ANY_VAL, EMPTY));
        assertIllegalArgument(() -> starterHeader().values(ANY_VAL, null));
        assertIllegalArgument(() -> starterHeader().values((Collection<String>) null));
        assertIllegalArgument(() -> starterHeader().values(Arrays.asList(KEY, EMPTY)));
        assertIllegalArgument(() -> starterHeader().values(Arrays.asList(KEY, null)));

        // adders
        assertIllegalArgument(() -> starterHeader().add(EMPTY));
        assertIllegalArgument(() -> starterHeader().add((String) null));
        assertIllegalArgument(() -> starterHeader().add(ANY_VAL, EMPTY));
        assertIllegalArgument(() -> starterHeader().add(ANY_VAL, null));
        assertIllegalArgument(() -> starterHeader().add((Collection<String>) null));
        assertIllegalArgument(() -> starterHeader().add(Arrays.asList(KEY, EMPTY)));
        assertIllegalArgument(() -> starterHeader().add(Arrays.asList(KEY, null)));

        // remove
        assertIllegalArgument(() -> starterHeader().remove(VAL1));
        assertIllegalArgument(() -> starterHeader(VAL1, VAL2).remove(VAL1, VAL2));
        assertIllegalArgument(() -> starterHeader().remove(Collections.singletonList(VAL1)));
        assertIllegalArgument(() -> starterHeader(VAL1, VAL2).remove(Arrays.asList(VAL1, VAL2)));
    }

    @Test
    public void header_getKey_works() {
        assertEquals(KEY, starterHeader().getKey());
    }

    @Test
    public void header_adders_work() {
        Header header = starterHeader();
        assertFalse(header.add(VAL1)); // value is already in there, and duplicates are ignored
        assertHeadersContains(header, VAL1);

        header = starterHeader();
        assertTrue(header.add(VAL2));
        assertHeadersContains(header, VAL1, VAL2);

        header = starterHeader();
        assertTrue(header.add(VAL1, VAL2));
        assertHeadersContains(header, VAL1, VAL2);

        header = starterHeader();
        assertTrue(header.add(VAL2, VAL3));
        assertHeadersContains(header, VAL1, VAL2, VAL3);

        header = starterHeader();
        assertTrue(header.add(Arrays.asList(VAL1, VAL2)));
        assertHeadersContains(header, VAL1, VAL2);

        header = starterHeader();
        assertTrue(header.add(Arrays.asList(VAL2, VAL3)));
        assertHeadersContains(header, VAL1, VAL2, VAL3);
    }

    @Test
    public void header_regularSetters_work() {
        assertHeadersContains(starterHeader(), VAL1);

        Header header = starterHeader();
        header.setValues(VAL2);
        assertHeadersContains(header, VAL2);

        header = starterHeader();
        header.setValues(VAL2, VAL3);
        assertHeadersContains(header, VAL2, VAL3);

        header = starterHeader();
        header.setValues(Collections.singletonList(VAL2));
        assertHeadersContains(header, VAL2);

        header = starterHeader();
        header.setValues(Arrays.asList(VAL2, VAL3));
        assertHeadersContains(header, VAL2, VAL3);
    }

    @Test
    public void header_fluentSetters_work() {
        assertHeadersContains(starterHeader(), VAL1);

        Header header = starterHeader().values(VAL2);
        assertHeadersContains(header, VAL2);

        header = starterHeader().values(VAL2, VAL3);
        assertHeadersContains(header, VAL2, VAL3);

        header = starterHeader().values(Collections.singletonList(VAL2));
        assertHeadersContains(header, VAL2);

        header = starterHeader().values(Arrays.asList(VAL2, VAL3));
        assertHeadersContains(header, VAL2, VAL3);
    }

    @Test
    public void header_removes_work() {
        Header header = starterHeader();
        assertFalse(header.remove(VAL2));
        assertHeadersContains(header, VAL1);

        assertFalse(header.remove(VAL2, VAL3));
        assertHeadersContains(header, VAL1);

        assertFalse(header.remove(Arrays.asList(VAL2, VAL3)));
        assertHeadersContains(header, VAL1);

        header = starterHeader(VAL1, VAL2);
        assertTrue(header.remove(VAL2));
        assertHeadersContains(header, VAL1);

        header = starterHeader(VAL1, VAL2, VAL3);
        assertTrue(header.remove(VAL2, VAL3));
        assertHeadersContains(header, VAL1);

        header = starterHeader(VAL1, VAL2, VAL3);
        assertTrue(header.remove(Arrays.asList(VAL2, VAL3)));
        assertHeadersContains(header, VAL1);
    }

    @Test
    public void header_equalsAndHashcode_work() {
        assertEquals(starterHeader(), starterHeader(VAL1));
        assertNotEquals(starterHeader(), starterHeader(VAL2));
        assertEquals(starterHeader().hashCode(), starterHeader(VAL1).hashCode());
        assertNotEquals(starterHeader().hashCode(), starterHeader(VAL2).hashCode());
    }

    private void assertHeadersContains(Header header, String... values) {
        assertNotNull(header.getValues());
        assertEquals(values.length, header.size());
        for (String v : values) {
            assertTrue(header.getValues().contains(v));
        }
    }

    private Header starterHeader() {
        return new Header(KEY, VAL1);
    }

    private Header starterHeader(String... values) {
        return new Header(KEY, values);
    }
}
