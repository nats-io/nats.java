package io.nats.client.impl;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.*;

public class HeaderTests extends AbstractHeaderTest {

    @Test
    public void keyCannotBeNullOrEmpty() {
        // constructor
        assertIllegalArgument(() -> new Header(null, VAL1));
        assertIllegalArgument(() -> new Header(null, VAL1, VAL2));
        assertIllegalArgument(() -> new Header(null, Collections.singletonList(VAL1)));
        assertIllegalArgument(() -> new Header(EMPTY, VAL1));
        assertIllegalArgument(() -> new Header(EMPTY, VAL1, VAL2));
        assertIllegalArgument(() -> new Header(EMPTY, Collections.singletonList(VAL1)));
    }

    @Test
    public void valuesCannotBeNullOrEmpty() {
        // constructor
        assertIllegalArgument(() -> new Header(KEY1, (String) null));
        assertIllegalArgument(() -> new Header(KEY1, EMPTY));
        assertIllegalArgument(() -> new Header(KEY1, ANY_VAL, EMPTY));
        assertIllegalArgument(() -> new Header(KEY1, ANY_VAL, null));
        assertIllegalArgument(() -> new Header(KEY1, (Collection<String>) null));
        assertIllegalArgument(() -> new Header(KEY1, Arrays.asList(KEY1, EMPTY)));
        assertIllegalArgument(() -> new Header(KEY1, Arrays.asList(KEY1, null)));

        // regular setters
        assertIllegalArgument(() -> testHeader().setValues(EMPTY));
        assertIllegalArgument(() -> testHeader().setValues((String) null));
        assertIllegalArgument(() -> testHeader().setValues(ANY_VAL, EMPTY));
        assertIllegalArgument(() -> testHeader().setValues(ANY_VAL, null));
        assertIllegalArgument(() -> testHeader().setValues((Collection<String>) null));
        assertIllegalArgument(() -> testHeader().setValues(Arrays.asList(KEY1, EMPTY)));
        assertIllegalArgument(() -> testHeader().setValues(Arrays.asList(KEY1, null)));

        // fluent setters
        assertIllegalArgument(() -> testHeader().values(EMPTY));
        assertIllegalArgument(() -> testHeader().values((String) null));
        assertIllegalArgument(() -> testHeader().values(ANY_VAL, EMPTY));
        assertIllegalArgument(() -> testHeader().values(ANY_VAL, null));
        assertIllegalArgument(() -> testHeader().values((Collection<String>) null));
        assertIllegalArgument(() -> testHeader().values(Arrays.asList(KEY1, EMPTY)));
        assertIllegalArgument(() -> testHeader().values(Arrays.asList(KEY1, null)));

        // adders
        assertIllegalArgument(() -> testHeader().add(EMPTY));
        assertIllegalArgument(() -> testHeader().add((String) null));
        assertIllegalArgument(() -> testHeader().add(ANY_VAL, EMPTY));
        assertIllegalArgument(() -> testHeader().add(ANY_VAL, null));
        assertIllegalArgument(() -> testHeader().add((Collection<String>) null));
        assertIllegalArgument(() -> testHeader().add(Arrays.asList(KEY1, EMPTY)));
        assertIllegalArgument(() -> testHeader().add(Arrays.asList(KEY1, null)));

        // remove
        assertIllegalArgument(() -> testHeader().remove(VAL1));
        assertIllegalArgument(() -> testHeader(VAL1, VAL2).remove(VAL1, VAL2));
        assertIllegalArgument(() -> testHeader().remove(Collections.singletonList(VAL1)));
        assertIllegalArgument(() -> testHeader(VAL1, VAL2).remove(Arrays.asList(VAL1, VAL2)));
    }

    @Test
    public void getKey_works() {
        assertEquals(KEY1, testHeader().getKey());
    }

    @Test
    public void adders_work() {
        Header header = testHeader();
        assertFalse(header.add(VAL1)); // value is already in there, and duplicates are ignored
        assertContainsExactly(header, VAL1);

        header = testHeader();
        assertTrue(header.add(VAL2));
        assertContainsExactly(header, VAL1, VAL2);

        header = testHeader();
        assertTrue(header.add(VAL1, VAL2));
        assertContainsExactly(header, VAL1, VAL2);

        header = testHeader();
        assertTrue(header.add(VAL2, VAL3));
        assertContainsExactly(header, VAL1, VAL2, VAL3);

        header = testHeader();
        assertTrue(header.add(Arrays.asList(VAL1, VAL2)));
        assertContainsExactly(header, VAL1, VAL2);

        header = testHeader();
        assertTrue(header.add(Arrays.asList(VAL2, VAL3)));
        assertContainsExactly(header, VAL1, VAL2, VAL3);
    }

    @Test
    public void regularSetters_work() {
        assertContainsExactly(testHeader(), VAL1);

        Header header = testHeader();
        header.setValues(VAL2);
        assertContainsExactly(header, VAL2);

        header = testHeader();
        header.setValues(VAL2, VAL3);
        assertContainsExactly(header, VAL2, VAL3);

        header = testHeader();
        header.setValues(Collections.singletonList(VAL2));
        assertContainsExactly(header, VAL2);

        header = testHeader();
        header.setValues(Arrays.asList(VAL2, VAL3));
        assertContainsExactly(header, VAL2, VAL3);
    }

    @Test
    public void fluentSetters_work() {
        assertContainsExactly(testHeader(), VAL1);

        Header header = testHeader().values(VAL2);
        assertContainsExactly(header, VAL2);

        header = testHeader().values(VAL2, VAL3);
        assertContainsExactly(header, VAL2, VAL3);

        header = testHeader().values(Collections.singletonList(VAL2));
        assertContainsExactly(header, VAL2);

        header = testHeader().values(Arrays.asList(VAL2, VAL3));
        assertContainsExactly(header, VAL2, VAL3);
    }

    @Test
    public void removes_work() {
        Header header = testHeader();
        assertFalse(header.remove(VAL2));
        assertContainsExactly(header, VAL1);

        assertFalse(header.remove(VAL2, VAL3));
        assertContainsExactly(header, VAL1);

        assertFalse(header.remove(Arrays.asList(VAL2, VAL3)));
        assertContainsExactly(header, VAL1);

        header = testHeader(VAL1, VAL2);
        assertTrue(header.remove(VAL2));
        assertContainsExactly(header, VAL1);

        header = testHeader(VAL1, VAL2, VAL3);
        assertTrue(header.remove(VAL2, VAL3));
        assertContainsExactly(header, VAL1);

        header = testHeader(VAL1, VAL2, VAL3);
        assertTrue(header.remove(Arrays.asList(VAL2, VAL3)));
        assertContainsExactly(header, VAL1);
    }

    @Test
    public void equalsAndHashcode_work() {
        assertEquals(testHeader(), testHeader(VAL1));
        assertNotEquals(testHeader(), testHeader(VAL2));
        assertEquals(testHeader().hashCode(), testHeader(VAL1).hashCode());
        assertNotEquals(testHeader().hashCode(), testHeader(VAL2).hashCode());
    }
}
