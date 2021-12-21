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

import io.nats.client.utils.ResourceUtils;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static io.nats.client.support.Encoding.jsonDecode;
import static io.nats.client.support.Encoding.jsonEncode;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static io.nats.client.utils.ResourceUtils.dataAsString;
import static org.junit.jupiter.api.Assertions.*;

public final class JsonUtilsTests {

    @Test
    public void testParseStringArray() {
        List<String> a = JsonUtils.getStringList("fieldName", "...\"fieldName\": [\n      ],...");
        assertNotNull(a);
        assertEquals(0, a.size());

        a = JsonUtils.getStringList("fieldName", "...\"fieldName\": [\n      \"value0\"\n    ],...");
        assertNotNull(a);
        assertEquals(1, a.size());
        assertEquals("value0", a.get(0));

        a = JsonUtils.getStringList("fieldName", "...\"fieldName\": [\r\n      \"value0\",\r      \"value1\"\n    ],...");
        assertNotNull(a);
        assertEquals(2, a.size());
        assertEquals("value0", a.get(0));
        assertEquals("value1", a.get(1));
    }

    @Test
    public void testGetJSONObject() {
        // object is there
        String json = "{\"object\": {\"field\": \"val\"}, \"other\":{}}";
        String object = JsonUtils.getJsonObject("object", json);
        assertEquals("{\"field\": \"val\"}", object);

        // object isn't
        json = "{\"other\":{}}";
        object = JsonUtils.getJsonObject("object", json);
        assertEquals(JsonUtils.EMPTY_JSON, object);

        // object there but incomplete
        json = "{\"object\": {\"field\": \"val\"";
        object = JsonUtils.getJsonObject("object", json);
        assertEquals(JsonUtils.EMPTY_JSON, object);

        json = dataAsString("StreamInfo.json");
        object = JsonUtils.getJsonObject("cluster", json);
        assertFalse(object.contains("placementclstr"));
        assertFalse(object.contains("tags"));
        assertFalse(object.contains("messages"));
        assertTrue(object.contains("clustername"));
        assertTrue(object.contains("replicas"));
    }

    @Test
    public void testGetObjectArray() {
        String json = ResourceUtils.dataAsString("ConsumerListResponse.json");
        List<String> list = JsonUtils.getObjectList("consumers", json);
        assertEquals(2, list.size());
    }

    @Test
    public void testBeginEnd() {
        StringBuilder sb = JsonUtils.beginJson();
        JsonUtils.addField(sb, "name", "value");
        JsonUtils.endJson(sb);
        assertEquals("{\"name\":\"value\"}", sb.toString());

        sb = JsonUtils.beginFormattedJson();
        JsonUtils.addField(sb, "name", "value");
        JsonUtils.endFormattedJson(sb);
        assertEquals("{\n    \"name\":\"value\"\n}", sb.toString());

        sb = JsonUtils.beginJsonPrefixed(null);
        assertEquals("{", sb.toString());

        sb = JsonUtils.beginJsonPrefixed("pre");
        assertEquals("pre {", sb.toString());
    }

    @Test
    public void testAddFlds() {
        StringBuilder sb = new StringBuilder();

        JsonUtils.addField(sb, "n/a", (String)null);
        assertEquals(0, sb.length());

        JsonUtils.addField(sb, "n/a", "");
        assertEquals(0, sb.length());

        JsonUtils.addStrings(sb, "n/a", (String[])null);
        assertEquals(0, sb.length());

        JsonUtils.addStrings(sb, "n/a", new String[0]);
        assertEquals(0, sb.length());

        JsonUtils.addStrings(sb, "n/a", (List<String>)null);
        assertEquals(0, sb.length());

        JsonUtils.addField(sb, "n/a", (JsonSerializable)null);
        assertEquals(0, sb.length());

        JsonUtils.addJsons(sb, "n/a", new ArrayList<>());
        assertEquals(0, sb.length());

        JsonUtils.addJsons(sb, "n/a", null);
        assertEquals(0, sb.length());

        JsonUtils.addField(sb, "n/a", (Boolean)null);
        assertEquals(0, sb.length());

        JsonUtils.addFldWhenTrue(sb, "n/a", null);
        assertEquals(0, sb.length());

        JsonUtils.addFldWhenTrue(sb, "n/a", false);
        assertEquals(0, sb.length());

        JsonUtils.addField(sb, "n/a", (Integer)null);
        assertEquals(0, sb.length());

        JsonUtils.addField(sb, "n/a", (Long)null);
        assertEquals(0, sb.length());

        //noinspection UnnecessaryBoxing
        JsonUtils.addField(sb, "iminusone", new Integer(-1));
        assertEquals(0, sb.length());

        JsonUtils.addField(sb, "lminusone", new Long(-1));
        assertEquals(0, sb.length());

        JsonUtils.addStrings(sb, "foo", new String[]{"bbb"});
        assertEquals(14, sb.length());

        JsonUtils.addField(sb, "zero", 0);
        assertEquals(23, sb.length());

        JsonUtils.addField(sb, "lone", 1);
        assertEquals(32, sb.length());

        JsonUtils.addField(sb, "lmax", Long.MAX_VALUE);
        assertEquals(59, sb.length());

        JsonUtils.addField(sb, "btrue", true);
        assertEquals(72, sb.length());

        JsonUtils.addField(sb, "bfalse", false);
        assertEquals(87, sb.length());
    }

    @Test
    public void testParseDateTime() {
        assertEquals(1611186068, DateTimeUtils.parseDateTime("2021-01-20T23:41:08.579594Z").toEpochSecond());
        assertEquals(1612293508, DateTimeUtils.parseDateTime("2021-02-02T11:18:28.347722551-08:00").toEpochSecond());
        assertEquals(-62135596800L, DateTimeUtils.parseDateTime("anything-not-valid").toEpochSecond());
    }

    @Test
    public void testInteger() {
        Pattern RE = JsonUtils.buildPattern("num", JsonUtils.FieldType.jsonInteger);
        assertEquals(-1, JsonUtils.readInt("\"num\":-1", RE, 0));
        assertEquals(12345678, JsonUtils.readInt("\"num\":12345678", RE, 0));
        assertEquals(2147483647, JsonUtils.readInt("\"num\":2147483647", RE, 0));
    }

    @Test
    public void testLong() {
        Pattern RE = JsonUtils.buildPattern("num", JsonUtils.FieldType.jsonInteger);
        assertEquals(-1, JsonUtils.readLong("\"num\":-1", RE, 0));
        assertEquals(12345678, JsonUtils.readLong("\"num\":12345678", RE, 0));
        assertEquals(9223372036854775807L, JsonUtils.readLong("\"num\":9223372036854775807", RE, 0));

        AtomicLong al = new AtomicLong();
        JsonUtils.readLong("\"num\":999", RE, al::set);
        assertEquals(999, al.get());

        JsonUtils.readLong("\"num\":invalid", RE, al::set);
        assertEquals(999, al.get());

        JsonUtils.readLong("\"num\":18446744073709551615", RE, al::set);
        assertEquals(-1, al.get());

        JsonUtils.readLong("\"num\":18446744073709551614", RE, al::set);
        assertEquals(-2, al.get());

        al.set(-999);
        JsonUtils.readLong("\"num\":18446744073709551616", RE, al::set);
        assertEquals(-999, al.get());

        assertEquals(-1, JsonUtils.safeParseLong("18446744073709551615", -999));
        assertEquals(-2, JsonUtils.safeParseLong("18446744073709551614", -999));
        assertEquals(-999, JsonUtils.safeParseLong("18446744073709551616", -999));
        assertEquals(-999, JsonUtils.safeParseLong(null, -999));
        assertEquals(-999, JsonUtils.safeParseLong("notanumber", -999));
    }

    @Test
    public void testReads() {
        String json = "\"yes\": true, \"no\": false";
        Pattern YES_RE = JsonUtils.buildPattern("yes", JsonUtils.FieldType.jsonBoolean);
        Pattern NO_RE = JsonUtils.buildPattern("no", JsonUtils.FieldType.jsonBoolean);
        Pattern MISSING_RE = JsonUtils.buildPattern("x", JsonUtils.FieldType.jsonBoolean);

        assertTrue(JsonUtils.readBoolean(json, YES_RE));
        assertFalse(JsonUtils.readBoolean(json, NO_RE));
        assertFalse((JsonUtils.readBoolean(json, MISSING_RE)));
    }

    @Test
    public void testCoverage() {
        Pattern ipattern = JsonUtils.integer_pattern("foo");
        Pattern npattern = JsonUtils.number_pattern("foo"); // coverage for deprecated
        assertEquals(ipattern.pattern(), npattern.pattern());
    }

    @Test
    public void testEncodeDecode() {
        _testEncodeDecode("b4\\\\after",    "b4\\after", null); // a single slash with a meaningless letter after it
        _testEncodeDecode("b4\\\\tafter",    "b4\\tafter", null); // a single slash with a char that can be part of an escape

        _testEncodeDecode("b4\\bafter",     "b4\bafter", null);
        _testEncodeDecode("b4\\fafter",     "b4\fafter", null);
        _testEncodeDecode("b4\\nafter",     "b4\nafter", null);
        _testEncodeDecode("b4\\rafter",     "b4\rafter", null);
        _testEncodeDecode("b4\\tafter",     "b4\tafter", null);

        _testEncodeDecode("b4\\u0000after", "b4" + (char)0 + "after", null);
        _testEncodeDecode("b4\\u001fafter", "b4" + (char)0x1f + "after", "b4\\u001Fafter");
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
        _testEncodeDecode("b4\\xafter",     "b4xafter", "b4xafter"); // unknown escape
        _testEncodeDecode("b4\\",           "b4\\", "b4\\\\"); // last char is \

        List<String> utfs = dataAsLines("utf8-only-no-ws-test-strings.txt");
        for (String u : utfs) {
            String uu = "b4\\b\\f\\n\\r\\t" + u + "after";
            _testEncodeDecode(jsonDecode(uu), "b4\b\f\n\r\t" + u + "after", uu);
        }
    }

    private void _testEncodeDecode(String input, String targetDecode, String targetEncode) {
        String decoded = jsonDecode(input);
        assertEquals(targetDecode, decoded);
        StringBuilder sb = new StringBuilder();
        jsonEncode(sb, decoded);
        String encoded = sb.toString();
        if (targetEncode == null) {
            assertEquals(input, encoded);
        }
        else {
            assertEquals(targetEncode, encoded);
        }
    }
}
