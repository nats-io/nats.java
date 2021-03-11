// Copyright 2015-2018 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.nats.client;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.junit.jupiter.api.Test;

public class HttpHeadersTests {
    @Test
    public void testEmpty() {
        HttpHeaders headers = new HttpHeaders();
        assertNull(headers.getFirst("TEST"));
        assertEquals(Collections.emptyList(), headers.getAll("TEST"));
        Iterator<HttpHeaders.Entry> iterator = headers.iterator();
        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
        assertEquals("\r\n", headers.toString());
        assertEquals(new HttpHeaders(), headers);
    }

    @Test
    public void testMultiValues() {
        HttpHeaders headers = new HttpHeaders()
            .add("Test", "value1")
            .add("Delete-Me", "later")
            .add("TEST", "value2");
        assertEquals(HttpHeaders.entry("TEST", "value1"), headers.getFirst("TEST"));
        assertEquals(Arrays.asList(
            HttpHeaders.entry("test", "value1"),
            HttpHeaders.entry("TEST", "value2")),
            headers.getAll("TEST"));
        Iterator<HttpHeaders.Entry> iterator = headers.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(HttpHeaders.entry("Test", "value1"), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(HttpHeaders.entry("Delete-Me", "later"), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(HttpHeaders.entry("TEST", "value2"), iterator.next());
        assertNull(iterator.next());
        assertEquals(
            "Test: value1\r\n" +
            "Delete-Me: later\r\n" +
            "TEST: value2\r\n" +
            "\r\n",
            headers.toString());

        HttpHeaders expect = new HttpHeaders()
            .add("Test", "value1")
            .add("DELETE-ME", "later")
            .add("TEST", "value2");
        assertEquals(expect, headers);
        assertEquals(expect.hashCode(), headers.hashCode());

        // Now delete and check again:
        headers.remove("delete-me");

        assertEquals(HttpHeaders.entry("TEST", "value1"), headers.getFirst("TEST"));
        assertEquals(Arrays.asList(
            HttpHeaders.entry("test", "value1"),
            HttpHeaders.entry("test", "value2")),
            headers.getAll("TEST"));
        iterator = headers.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(HttpHeaders.entry("Test", "value1"), iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(HttpHeaders.entry("TEST", "value2"), iterator.next());
        assertNull(iterator.next());
        assertEquals(
            "Test: value1\r\n" +
            "TEST: value2\r\n" +
            "\r\n",
            headers.toString());

        HttpHeaders expect2 = new HttpHeaders()
            .add("Test", "value1")
            .add("TEST", "value2");
        assertEquals(expect2, headers);
        assertEquals(expect2.hashCode(), headers.hashCode());
        assertNotEquals(expect, headers);

        // New delete again and check:
        headers.remove("test");
        assertNull(headers.getFirst("TEST"));
        assertEquals(Collections.emptyList(), headers.getAll("TEST"));
        iterator = headers.iterator();
        assertFalse(iterator.hasNext());
        assertNull(iterator.next());
        assertEquals("\r\n", headers.toString());
        assertEquals(new HttpHeaders(), headers);
    }

    @Test
    public void testNulls() {
        assertThrows(IllegalArgumentException.class, () -> {
            HttpHeaders.entry(null, null);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            HttpHeaders.entry("Test", null);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            HttpHeaders.entry(null, "Test");
        });
        assertThrows(IllegalArgumentException.class, () -> {
            HttpHeaders.entry("", "Test");
        });
    }
}