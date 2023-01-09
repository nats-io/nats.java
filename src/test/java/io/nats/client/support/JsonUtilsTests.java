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

package io.nats.client.support;

import io.nats.client.PurgeOptions;
import io.nats.client.impl.Headers;
import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static io.nats.client.support.ApiConstants.DESCRIPTION;
import static io.nats.client.support.Encoding.jsonDecode;
import static io.nats.client.support.Encoding.jsonEncode;
import static io.nats.client.support.JsonUtils.*;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public final class JsonUtilsTests {
    @Test
    public void testRegex() {
        String json = "{\"ss\": \"s1\", \"s_s\": \"s2\", \"bb\":true, \"b_b\":true, \"ii\":1, \"i_i\":2}";
        Pattern s1Pat = string_pattern("ss");
        Pattern s2Pat = string_pattern("s_s");
        Pattern b1Pat = boolean_pattern("bb");
        Pattern b2Pat = boolean_pattern("b_b");
        Pattern i1Pat = integer_pattern("ii");
        Pattern i2Pat = integer_pattern("i_i");

        assertTrue(s1Pat.matcher(json).find());
        assertTrue(s2Pat.matcher(json).find());
        assertTrue(b1Pat.matcher(json).find());
        assertTrue(b2Pat.matcher(json).find());
        assertTrue(i1Pat.matcher(json).find());
        assertTrue(i2Pat.matcher(json).find());

        assertEquals("s1", readString(json, s1Pat));
        assertEquals("s2", readString(json, s2Pat));
        assertTrue(readBoolean(json, b1Pat));
        assertTrue(readBoolean(json, b2Pat));
        assertEquals(1, readInt(json, i1Pat, -1));
        assertEquals(2, readInt(json, i2Pat, -1));
    }

    @Test
    public void testParseStringArray() {
        List<String> a = getStringList("fieldName", "...\"fieldName\": [\n      ],...");
        assertNotNull(a);
        assertEquals(0, a.size());

        a = getStringList("fieldName", "...\"fieldName\": [\n      \"value0\"\n    ],...");
        assertNotNull(a);
        assertEquals(1, a.size());
        assertEquals("value0", a.get(0));

        a = getStringList("fieldName", "...\"fieldName\": [\r\n      \"value0\",\r      \"value1\"\n    ],...");
        assertNotNull(a);
        assertEquals(2, a.size());
        assertEquals("value0", a.get(0));
        assertEquals("value1", a.get(1));
    }

    @Test
    public void testGetJSONObject() {
        // object is there
        String json = "{\"object\": {\"field\": \"val\"}, \"other\":{}}";
        String object = getJsonObject("object", json);
        assertEquals("{\"field\": \"val\"}", object);

        // object isn't
        json = "{\"other\":{}}";
        object = getJsonObject("object", json);
        assertEquals(EMPTY_JSON, object);

        // object there but incomplete
        json = "{\"object\": {\"field\": \"val\"";
        object = getJsonObject("object", json);
        assertEquals(EMPTY_JSON, object);

        json = dataAsString("StreamInfo.json");
        object = getJsonObject("cluster", json);
        assertFalse(object.contains("placementclstr"));
        assertFalse(object.contains("tags"));
        assertFalse(object.contains("messages"));
        assertTrue(object.contains("clustername"));
        assertTrue(object.contains("replicas"));
    }

    @Test
    public void testGetObjectArray() {
        String json = ResourceUtils.dataAsString("ConsumerListResponse.json");
        List<String> list = getObjectList("consumers", json);
        assertEquals(2, list.size());
    }

    @Test
    public void testBeginEnd() {
        StringBuilder sb = beginJson();
        addField(sb, "name", "value");
        endJson(sb);
        assertEquals("{\"name\":\"value\"}", sb.toString());

        sb = beginFormattedJson();
        addField(sb, "name", "value");
        endFormattedJson(sb);
        assertEquals("{\n    \"name\":\"value\"\n}", sb.toString());

        sb = beginJsonPrefixed(null);
        assertEquals("{", sb.toString());

        sb = beginJsonPrefixed("pre");
        assertEquals("pre {", sb.toString());
    }

    @Test
    public void testAddFields() {
        StringBuilder sb = new StringBuilder();

        addField(sb, "n/a", (String) null);
        assertEquals(0, sb.length());

        addField(sb, "n/a", "");
        assertEquals(0, sb.length());

        addStrings(sb, "n/a", (String[]) null);
        assertEquals(0, sb.length());

        addStrings(sb, "n/a", new String[0]);
        assertEquals(0, sb.length());

        addStrings(sb, "n/a", (List<String>) null);
        assertEquals(0, sb.length());

        addField(sb, "n/a", (JsonSerializable) null);
        assertEquals(0, sb.length());

        addJsons(sb, "n/a", new ArrayList<>());
        assertEquals(0, sb.length());

        addJsons(sb, "n/a", null);
        assertEquals(0, sb.length());

        addDurations(sb, "n/a", null);
        assertEquals(0, sb.length());

        addDurations(sb, "n/a", new ArrayList<>());
        assertEquals(0, sb.length());

        addField(sb, "n/a", (Boolean) null);
        assertEquals(0, sb.length());

        addFldWhenTrue(sb, "n/a", null);
        assertEquals(0, sb.length());

        addFldWhenTrue(sb, "n/a", false);
        assertEquals(0, sb.length());

        addField(sb, "n/a", (Integer) null);
        assertEquals(0, sb.length());

        addField(sb, "n/a", (Long) null);
        assertEquals(0, sb.length());

        //noinspection UnnecessaryBoxing
        addField(sb, "iminusone", new Integer(-1));
        assertEquals(0, sb.length());

        addField(sb, "lminusone", new Long(-1));
        assertEquals(0, sb.length());

        addStrings(sb, "foo", new String[]{"bbb"});
        assertEquals(14, sb.length());

        addField(sb, "zero", 0);
        assertEquals(23, sb.length());

        addField(sb, "lone", 1);
        assertEquals(32, sb.length());

        addField(sb, "lmax", Long.MAX_VALUE);
        assertEquals(59, sb.length());

        addField(sb, "btrue", true);
        assertEquals(72, sb.length());

        addField(sb, "bfalse", false);
        assertEquals(87, sb.length());

        addFieldWhenGtZero(sb, "intnull", (Integer) null);
        assertEquals(87, sb.length());

        addFieldWhenGtZero(sb, "longnull", (Long) null);
        assertEquals(87, sb.length());

        //noinspection UnnecessaryBoxing
        addFieldWhenGtZero(sb, "intnotgt0", new Integer(0));
        assertEquals(87, sb.length());

        addFieldWhenGtZero(sb, "longnotgt0", 0L);
        assertEquals(87, sb.length());

        //noinspection UnnecessaryBoxing
        addFieldWhenGtZero(sb, "intgt0", new Integer(1));
        assertEquals(98, sb.length());

        addFieldWhenGtZero(sb, "longgt0", 1L);
        assertEquals(110, sb.length());

        addField(sb, "null-header", (Headers) null);
        assertEquals(110, sb.length());

        addField(sb, "null-header", new Headers());
        assertEquals(110, sb.length());

        addField(sb, "header", new Headers().add("foo", "bar").add("foo", "baz"));
        assertEquals(141, sb.length());
    }

    static final String EXPECTED_LIST_JSON = "{\"a1\":[\"one\"],\"a2\":[\"two\",\"too\"],\"l1\":[\"one\"],\"l2\":[\"two\",\"too\"],\"j1\":[{\"filter\":\"sub1\",\"keep\":421}],\"j2\":[{\"filter\":\"sub2\",\"seq\":732},{\"filter\":\"sub3\"}],\"d1\":[1000000],\"d2\":[2000000,3000000]}";

    @Test
    public void testLists() {
        StringBuilder sb = beginJson();

        String[] s1 = new String[]{"one"};
        String[] s2 = new String[]{"two", "too"};
        List<String> l1 = Arrays.asList(s1);
        List<String> l2 = Arrays.asList(s2);
        addStrings(sb, "a1", s1);
        addStrings(sb, "a2", s2);
        addStrings(sb, "l1", l1);
        addStrings(sb, "l2", l2);

        PurgeOptions po1 = PurgeOptions.builder().keep(421).subject("sub1").build();
        PurgeOptions po2 = PurgeOptions.builder().sequence(732).subject("sub2").build();
        PurgeOptions po3 = PurgeOptions.builder().subject("sub3").build();

        addJsons(sb, "j1", Collections.singletonList(po1));
        addJsons(sb, "j2", Arrays.asList(po2, po3));

        List<Duration> d1 = Collections.singletonList(Duration.ofMillis(1));
        List<Duration> d2 = Arrays.asList(Duration.ofMillis(2), Duration.ofMillis(3));
        addDurations(sb, "d1", d1);
        addDurations(sb, "d2", d2);

        String json = endJson(sb).toString();
        assertEquals(EXPECTED_LIST_JSON, json);

        List<String> sss = getStringList("a1", json);
        assertEquals(l1, sss);

        sss = getStringList("a2", json);
        assertEquals(l2, sss);

        sss = getStringList("l1", json);
        assertEquals(l1, sss);

        sss = getStringList("l2", json);
        assertEquals(l2, sss);

        List<Duration> ddd = getDurationList("d1", json);
        assertEquals(d1, ddd);

        ddd = getDurationList("d2", json);
        assertEquals(d2, ddd);

        sss = getObjectList("j1", json);
        assertEquals(1, sss.size());
        assertEquals("{\"filter\":\"sub1\",\"keep\":421}", sss.get(0));

        sss = getObjectList("j2", json);
        assertEquals(2, sss.size());
        assertEquals("{\"filter\":\"sub2\",\"seq\":732}", sss.get(0));
        assertEquals("{\"filter\":\"sub3\"}", sss.get(1));
    }

    @Test
    public void testParseDateTime() {
        assertEquals(1611186068, DateTimeUtils.parseDateTime("2021-01-20T23:41:08.579594Z").toEpochSecond());
        assertEquals(1612293508, DateTimeUtils.parseDateTime("2021-02-02T11:18:28.347722551-08:00").toEpochSecond());
        assertEquals(-62135596800L, DateTimeUtils.parseDateTime("anything-not-valid").toEpochSecond());
    }

    @Test
    public void testInteger() {
        Pattern RE = buildPattern("num", FieldType.jsonInteger);
        assertEquals(-1, readInt("\"num\":-1", RE, 0));
        assertEquals(12345678, readInt("\"num\":12345678", RE, 0));
        assertEquals(2147483647, readInt("\"num\":2147483647", RE, 0));
    }

    @Test
    public void testLong() {
        Pattern RE = buildPattern("num", FieldType.jsonInteger);
        assertEquals(-1, readLong("\"num\":-1", RE, 0));
        assertEquals(12345678, readLong("\"num\":12345678", RE, 0));
        assertEquals(9223372036854775807L, readLong("\"num\":9223372036854775807", RE, 0));

        AtomicLong al = new AtomicLong();
        readLong("\"num\":999", RE, al::set);
        assertEquals(999, al.get());

        readLong("\"num\":invalid", RE, al::set);
        assertEquals(999, al.get());

        readLong("\"num\":18446744073709551615", RE, al::set);
        assertEquals(-1, al.get());

        readLong("\"num\":18446744073709551614", RE, al::set);
        assertEquals(-2, al.get());

        al.set(-999);
        readLong("\"num\":18446744073709551616", RE, al::set);
        assertEquals(-999, al.get());

        assertEquals(-1, safeParseLong("18446744073709551615", -999));
        assertEquals(-2, safeParseLong("18446744073709551614", -999));
        assertEquals(-999, safeParseLong("18446744073709551616", -999));
        assertEquals(-999, safeParseLong(null, -999));
        assertEquals(-999, safeParseLong("notanumber", -999));
    }

    @Test
    public void testReads() {
        String json = "\"yes\": true, \"no\": false";
        Pattern YES_RE = buildPattern("yes", FieldType.jsonBoolean);
        Pattern NO_RE = buildPattern("no", FieldType.jsonBoolean);
        Pattern MISSING_RE = buildPattern("x", FieldType.jsonBoolean);

        assertTrue(readBoolean(json, YES_RE));
        assertFalse(readBoolean(json, NO_RE));
        assertFalse((readBoolean(json, MISSING_RE)));
    }

    @Test
    public void testEncodeDecode() {
        _testEncodeDecode("b4\\\\after", "b4\\after", null); // a single slash with a meaningless letter after it
        _testEncodeDecode("b4\\\\tafter", "b4\\tafter", null); // a single slash with a char that can be part of an escape

        _testEncodeDecode("b4\\bafter", "b4\bafter", null);
        _testEncodeDecode("b4\\fafter", "b4\fafter", null);
        _testEncodeDecode("b4\\nafter", "b4\nafter", null);
        _testEncodeDecode("b4\\rafter", "b4\rafter", null);
        _testEncodeDecode("b4\\tafter", "b4\tafter", null);

        _testEncodeDecode("b4\\u0000after", "b4" + (char) 0 + "after", null);
        _testEncodeDecode("b4\\u001fafter", "b4" + (char) 0x1f + "after", "b4\\u001fafter");
        _testEncodeDecode("b4\\u0020after", "b4 after", "b4 after");
        _testEncodeDecode("b4\\u0022after", "b4\"after", "b4\\\"after");
        _testEncodeDecode("b4\\u0027after", "b4'after", "b4'after");
        _testEncodeDecode("b4\\u003dafter", "b4=after", "b4=after");
        _testEncodeDecode("b4\\u003Dafter", "b4=after", "b4=after");
        _testEncodeDecode("b4\\u003cafter", "b4<after", "b4<after");
        _testEncodeDecode("b4\\u003Cafter", "b4<after", "b4<after");
        _testEncodeDecode("b4\\u003eafter", "b4>after", "b4>after");
        _testEncodeDecode("b4\\u003Eafter", "b4>after", "b4>after");
        _testEncodeDecode("b4\\u0060after", "b4`after", "b4`after");
        _testEncodeDecode("b4\\xafter", "b4xafter", "b4xafter"); // unknown escape
        _testEncodeDecode("b4\\", "b4\\", "b4\\\\"); // last char is \

        List<String> utfs = dataAsLines("utf8-only-no-ws-test-strings.txt");
        for (String u : utfs) {
            String uu = "b4\\b\\f\\n\\r\\t" + u + "after";
            _testEncodeDecode(uu, "b4\b\f\n\r\t" + u + "after", uu);
        }
    }

    private void _testEncodeDecode(String encodedInput, String targetDecode, String targetEncode) {
        String decoded = jsonDecode(encodedInput);
        assertEquals(targetDecode, decoded);
        String encoded = jsonEncode(new StringBuilder(), decoded).toString();
        if (targetEncode == null) {
            assertEquals(encodedInput, encoded);
        }
        else {
            assertEquals(targetEncode, encoded);
        }
    }

    @Test
    public void testSimpleMessageBody() {
        assertEquals("{\"number\":1}", new String(simpleMessageBody("number", 1)));
        assertEquals("{\"string\":\"str\"}", new String(simpleMessageBody("string", "str")));
    }

    @Test
    public void testMiscCoverage() {
        Pattern ipattern = integer_pattern("foo");
        Pattern npattern = number_pattern("foo"); // coverage for deprecated

        assertEquals(ipattern.pattern(), npattern.pattern());
        assertEquals(0, getMapOfObjects("\"bad\": ").size());
        assertEquals(0, getMapOfLists("\"bad\": ").size());
        assertEquals("\"field\": ", removeObject("\"field\": ", "notfound"));
    }

    @Test
    public void testReadStringMayHaveQuotes() {
        String json = "{\"num\":1,\"description\":\"q\\\"quoted\\\"q tab\\ttab =\\u003d=\"}";
        assertNull(readStringMayHaveQuotes(json, "NotThere", null));
        assertEquals("q\"quoted\"q tab\ttab ===", readStringMayHaveQuotes(json, DESCRIPTION, null));
    }
}
