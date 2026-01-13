// Copyright 2023 The NATS Authors
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

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.*;

import static io.nats.client.support.Encoding.jsonEncode;
import static io.nats.client.support.JsonParser.*;
import static io.nats.client.support.JsonParser.Option.KEEP_NULLS;
import static io.nats.client.support.JsonValueUtils.*;
import static io.nats.client.utils.ResourceUtils.dataAsLines;
import static io.nats.client.utils.TestBase.*;
import static org.junit.jupiter.api.Assertions.*;

public final class JsonParsingTests {

    @Test
    public void testStringParsing() {
        List<String> encodeds = new ArrayList<>();
        List<String> decodeds = new ArrayList<>();
        Map<String, JsonValue> oMap = new HashMap<>();
        List<JsonValue> list = new ArrayList<>();

        int x = 0;
        addField(key(x++), "b4\\after", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4/after", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\"after", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\tafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\bafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\fafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\nafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\rafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4\\tafter", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4" + (char) 0 + "after", oMap, list, encodeds, decodeds);
        addField(key(x++), "b4" + (char) 1 + "after", oMap, list, encodeds, decodeds);

        List<String> utfs = dataAsLines("utf8-only-no-ws-test-strings.txt");
        for (String u : utfs) {
            String uu = "b4\b\f\n\r\t" + u + "after";
            addField(key(x++), uu, oMap, list, encodeds, decodeds);
        }

        addField(key(x++), PLAIN, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_SPACE, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_PRINTABLE, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_DOT, oMap, list, encodeds, decodeds);
        addField(key(x++), STAR_NOT_SEGMENT, oMap, list, encodeds, decodeds);
        addField(key(x++), GT_NOT_SEGMENT, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_DASH, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_UNDER, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_DOLLAR, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_LOW, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_127, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_FWD_SLASH, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_BACK_SLASH, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_EQUALS, oMap, list, encodeds, decodeds);
        addField(key(x++), HAS_TIC, oMap, list, encodeds, decodeds);

        for (int i = 0; i < list.size(); i++) {
            JsonValue vi = list.get(i);
            assertEquals(decodeds.get(i), vi.string);
            assertEquals(vi.toJson(), "\"" + encodeds.get(i) + "\"");
            for (int j = 0; j < list.size(); j++) {
                JsonValue vj = list.get(j);
                if (i == j) {
                    assertEquals(vi, vj);
                }
                else {
                    assertNotEquals(vi, vj);
                }
            }
        }
    }

    private void addField(String name, String decoded,
                          Map<String, JsonValue> map, List<JsonValue> list,
                          List<String> encodeds, List<String> decodeds) {
        String enc = jsonEncode(decoded);
        encodeds.add(enc);
        decodeds.add(decoded);
        JsonValue jv = new JsonValue(decoded);
        map.put(name, jv);
        list.add(jv);
    }

    @SuppressWarnings("UnnecessaryUnicodeEscape")
    @Test
    public void testJsonValuePrimitives() throws JsonParseException {
        Map<String, JsonValue> oMap = new HashMap<>();
        oMap.put("trueKey1", new JsonValue(true));
        oMap.put("trueKey2", new JsonValue(Boolean.TRUE));
        oMap.put("falseKey1", new JsonValue(false));
        oMap.put("falseKey2", new JsonValue(Boolean.FALSE));
        oMap.put("stringKey", new JsonValue("hello world!"));
        oMap.put("escapeStringKey", new JsonValue("h\be\tllo w\u1234orld!"));
        oMap.put("nullKey", JsonValue.NULL);
        oMap.put("intKey1", new JsonValue(Integer.MAX_VALUE));
        oMap.put("intKey2", new JsonValue(Integer.MIN_VALUE));
        oMap.put("longKey1", new JsonValue(Long.MAX_VALUE));
        oMap.put("longKey2", new JsonValue(Long.MIN_VALUE));
        oMap.put("doubleKey1", new JsonValue(Double.MAX_VALUE));
        oMap.put("doubleKey2", new JsonValue(Double.MIN_VALUE));
        oMap.put("floatKey1", new JsonValue(Float.MAX_VALUE));
        oMap.put("floatKey2", new JsonValue(Float.MIN_VALUE));
        oMap.put("bigDecimalKey1", new JsonValue(new BigDecimal("9223372036854775807.123")));
        oMap.put("bigDecimalKey2", new JsonValue(new BigDecimal("-9223372036854775808.123")));
        oMap.put("bigIntegerKey1", new JsonValue(new BigInteger("9223372036854775807")));
        oMap.put("bigIntegerKey2", new JsonValue(new BigInteger("-9223372036854775808")));

        // some coverage here
        JsonValue vMap = new JsonValue(oMap);
        JsonValue vMap2 = new JsonValue(oMap);
        assertEquals(vMap.toJson(), vMap.toString());
        assertEquals(vMap, vMap2);

        validateMapTypes(oMap, oMap, true);

        // don't keep nulls
        JsonValue parsed = parse(new JsonValue(oMap).toJson());
        assertNotNull(parsed.map);
        assertEquals(oMap.size() - 1, parsed.map.size());
        validateMapTypes(parsed.map, oMap, false);

        // keep nulls
        parsed = parse(new JsonValue(oMap).toJson(), KEEP_NULLS);
        assertNotNull(parsed.map);
        assertEquals(oMap.size(), parsed.map.size());
        validateMapTypes(parsed.map, oMap, true);
    }

    private static void validateMapTypes(Map<String, JsonValue> map, Map<String, JsonValue> oMap, boolean original) {
        assertEquals(JsonValue.Type.BOOL, map.get("trueKey1").type);
        assertEquals(JsonValue.Type.BOOL, map.get("trueKey2").type);
        assertEquals(JsonValue.Type.BOOL, map.get("falseKey1").type);
        assertEquals(JsonValue.Type.BOOL, map.get("falseKey2").type);
        assertEquals(JsonValue.Type.STRING, map.get("stringKey").type);
        assertEquals(JsonValue.Type.STRING, map.get("escapeStringKey").type);
        assertEquals(JsonValue.Type.INTEGER, map.get("intKey1").type);
        assertEquals(JsonValue.Type.INTEGER, map.get("intKey2").type);
        assertEquals(JsonValue.Type.LONG, map.get("longKey1").type);
        assertEquals(JsonValue.Type.LONG, map.get("longKey2").type);

        assertNotNull(map.get("trueKey1").bool);
        assertNotNull(map.get("trueKey2").bool);
        assertNotNull(map.get("falseKey1").bool);
        assertNotNull(map.get("falseKey2").bool);
        assertNotNull(map.get("stringKey").string);
        assertNotNull(map.get("escapeStringKey").string);
        assertNotNull(map.get("intKey1").i);
        assertNotNull(map.get("intKey2").i);
        assertNotNull(map.get("longKey1").l);
        assertNotNull(map.get("longKey2").l);

        assertEquals(oMap.get("trueKey1"), map.get("trueKey1"));
        assertEquals(oMap.get("trueKey2"), map.get("trueKey2"));
        assertEquals(oMap.get("falseKey1"), map.get("falseKey1"));
        assertEquals(oMap.get("falseKey2"), map.get("falseKey2"));
        assertEquals(oMap.get("stringKey"), map.get("stringKey"));
        assertEquals(oMap.get("escapeStringKey"), map.get("escapeStringKey"));
        assertEquals(oMap.get("intKey1"), map.get("intKey1"));
        assertEquals(oMap.get("intKey2"), map.get("intKey2"));
        assertEquals(oMap.get("longKey1"), map.get("longKey1"));
        assertEquals(oMap.get("longKey2"), map.get("longKey2"));

        if (original) {
            assertNotNull(oMap.get("intKey1").i);
            assertNotNull(oMap.get("intKey2").i);
            assertNotNull(oMap.get("longKey1").l);
            assertNotNull(oMap.get("longKey2").l);
            assertNotNull(oMap.get("doubleKey1").d);
            assertNotNull(oMap.get("doubleKey2").d);
            assertNotNull(oMap.get("floatKey1").f);
            assertNotNull(oMap.get("floatKey2").f);
            assertNotNull(oMap.get("bigDecimalKey1").bd);
            assertNotNull(oMap.get("bigDecimalKey2").bd);
            assertNotNull(oMap.get("bigIntegerKey1").bi);
            assertNotNull(oMap.get("bigIntegerKey2").bi);

            assertEquals(JsonValue.Type.NULL, map.get("nullKey").type);
            assertNull(map.get("nullKey").object);
            assertEquals(oMap.get("nullKey"), map.get("nullKey"));
        }
        else {
            assertNotNull(oMap.get("intKey1").number);
            assertNotNull(oMap.get("intKey2").number);
            assertNotNull(oMap.get("longKey1").number);
            assertNotNull(oMap.get("longKey2").number);
            assertNotNull(oMap.get("doubleKey1").number);
            assertNotNull(oMap.get("doubleKey2").number);
            assertNotNull(oMap.get("floatKey1").number);
            assertNotNull(oMap.get("floatKey2").number);
            assertNotNull(oMap.get("bigDecimalKey1").number);
            assertNotNull(oMap.get("bigDecimalKey2").number);
            assertNotNull(oMap.get("bigIntegerKey1").number);
            assertNotNull(oMap.get("bigIntegerKey2").number);
        }
    }

    @Test
    public void testArray() throws JsonParseException {
        List<JsonValue> list = new ArrayList<>();
        list.add(new JsonValue("string"));
        list.add(new JsonValue(true));
        list.add(JsonValue.NULL);
        list.add(JsonValue.EMPTY_MAP);
        list.add(JsonValue.EMPTY_ARRAY);

        JsonValue root = parse(new JsonValue(list).toJson());
        assertNotNull(root.array);
        assertEquals(list.size(), root.array.size());
        List<JsonValue> array = root.array;
        for (int i = 0; i < array.size(); i++) {
            JsonValue vi = array.get(i);
            JsonValue p = root.array.get(i);
            assertEquals(vi.object, p.object);
            assertTrue(list.contains(vi));
            for (int j = 0; j < array.size(); j++) {
                JsonValue vj = array.get(j);
                if (i == j) {
                    assertEquals(vi, vj);
                }
                else {
                    assertNotEquals(vi, vj);
                }
            }
        }

        list.clear();
        list.add(new JsonValue(1));
        list.add(new JsonValue(Long.MAX_VALUE));
        list.add(new JsonValue(Double.MAX_VALUE));
        list.add(new JsonValue(Float.MAX_VALUE));
        list.add(new JsonValue(new BigDecimal(Double.toString(Double.MAX_VALUE))));
        list.add(new JsonValue(new BigInteger(Long.toString(Long.MAX_VALUE))));

        root = parse(new JsonValue(list).toJson());
        assertNotNull(root.array);
        assertEquals(list.size(), root.array.size());
        array = root.array;
        for (int i = 0; i < array.size(); i++) {
            JsonValue v = array.get(i);
            JsonValue p = root.array.get(i);
            assertEquals(v.object, p.object);
            assertEquals(v.number, p.number);
        }

        Map<String, JsonValue> rootMap = new HashMap<>();
        rootMap.put("list", new JsonValue(list));
        rootMap.put("array", new JsonValue(list.toArray(new JsonValue[0])));
        root = new JsonValue(rootMap);
        List<JsonValue> mappedList = readValue(root, "list").array;

        List<JsonValue> mappedList2 = parse(new JsonValue(mappedList).toJson()).array;
        List<JsonValue> mappedArray = readValue(root, "array").array;
        List<JsonValue> mappedArray2 = parse(new JsonValue(list.toArray(new JsonValue[0])).toJson()).array;
        for (int i = 0; i < list.size(); i++) {
            JsonValue v = list.get(i);
            JsonValue lv = mappedList.get(i);
            JsonValue lv2 = mappedList2.get(i);
            JsonValue av = mappedArray.get(i);
            JsonValue av2 = mappedArray2.get(i);
            assertNotNull(lv);
            assertNotNull(lv2);
            assertNotNull(av);
            assertNotNull(av2);
            assertEquals(v, lv);
            assertEquals(v, av);

            // conversions are not perfect for doubles and floats, but that's a java thing, not a parser thing
            if (v.type == lv2.type) {
                assertEquals(v, lv2);
            }
            if (v.type == av2.type) {
                assertEquals(v, av2);
            }
        }
    }

    @Test
    public void testListReading() {
        List<JsonValue> jvList = new ArrayList<>();
        jvList.add(new JsonValue("string1"));
        jvList.add(new JsonValue("string2"));
        jvList.add(new JsonValue(""));
        jvList.add(new JsonValue(true));
        jvList.add(new JsonValue((String)null));
        jvList.add(JsonValue.NULL);
        jvList.add(JsonValue.EMPTY_MAP);
        jvList.add(JsonValue.EMPTY_ARRAY);
        jvList.add(new JsonValue(Integer.MAX_VALUE));
        jvList.add(new JsonValue(Long.MAX_VALUE));
        Map<String, JsonValue> jvMap = new HashMap<>();
        jvMap.put("list", new JsonValue(jvList));
        JsonValue root = new JsonValue(jvMap);

        List<String> list = readStringList(root, "list");
        assertEquals(3, list.size());
        assertTrue(list.contains("string1"));
        assertTrue(list.contains("string2"));
        assertTrue(list.contains(""));

        list = readStringListIgnoreEmpty(root, "list");
        assertEquals(2, list.size());
        assertTrue(list.contains("string1"));
        assertTrue(list.contains("string2"));

        jvList.remove(0);
        jvList.remove(0);
        jvList.remove(0);
        list = readOptionalStringList(root, "list");
        assertNull(list);

        list = readOptionalStringList(root, "na");
        assertNull(list);

        jvList.clear();
        Duration d0 = Duration.ofNanos(10000000000L);
        Duration d1 = Duration.ofNanos(20000000000L);
        Duration d2 = Duration.ofNanos(30000000000L);

        jvList.add(instance(d0));
        jvList.add(instance(d1));
        jvList.add(instance(d2));
        jvList.add(new JsonValue("not duration nanos"));

        root = new JsonValue(jvMap);

        List<Duration> dlist = readNanosList(root, "list");
        assertEquals(3, dlist.size());
        assertEquals(d0, dlist.get(0));
        assertEquals(d1, dlist.get(1));
        assertEquals(d2, dlist.get(2));
    }

    @Test
    public void testGetIntLong() {
        JsonValue i = new JsonValue(Integer.MAX_VALUE);
        JsonValue li = new JsonValue((long)Integer.MAX_VALUE);
        JsonValue lmax = new JsonValue(Long.MAX_VALUE);
        JsonValue lmin = new JsonValue(Long.MIN_VALUE);
        assertEquals(Integer.MAX_VALUE, getInteger(i));
        assertEquals(Integer.MAX_VALUE, getInteger(li));
        assertNull(getInteger(lmax));
        assertNull(getInteger(lmin));
        assertNull(getInteger(JsonValue.NULL));
        assertNull(getInteger(JsonValue.EMPTY_MAP));
        assertNull(getInteger(JsonValue.EMPTY_ARRAY));

        assertEquals(Integer.MAX_VALUE, getLong(i));
        assertEquals(Integer.MAX_VALUE, getLong(li));
        assertEquals(Long.MAX_VALUE, getLong(lmax));
        assertEquals(Long.MIN_VALUE, getLong(lmin));
        assertNull(getLong(JsonValue.NULL));
        assertNull(getLong(JsonValue.EMPTY_MAP));
        assertNull(getLong(JsonValue.EMPTY_ARRAY));

        assertEquals(Integer.MAX_VALUE, getLong(i, -1));
        assertEquals(Integer.MAX_VALUE, getLong(li, -1));
        assertEquals(Long.MAX_VALUE, getLong(lmax, -1));
        assertEquals(Long.MIN_VALUE, getLong(lmin, -1));
        assertEquals(-1, getLong(JsonValue.NULL, -1));
        assertEquals(-1, getLong(JsonValue.EMPTY_MAP, -1));
        assertEquals(-1, getLong(JsonValue.EMPTY_ARRAY, -1));
    }

    @Test
    public void testConstantsAreReadOnly() {
        assertThrows(UnsupportedOperationException.class, () -> JsonValue.EMPTY_MAP.map.put("foo", null));
        assertThrows(UnsupportedOperationException.class, () -> JsonValue.EMPTY_ARRAY.array.add(null));
    }

    @Test
    public void testNullJsonValue() {
        assertEquals(JsonValue.Type.NULL, JsonValue.NULL.type);
        assertNull(JsonValue.NULL.object);
        assertNull(JsonValue.NULL.map);
        assertNull(JsonValue.NULL.array);
        assertNull(JsonValue.NULL.string);
        assertNull(JsonValue.NULL.bool);
        assertNull(JsonValue.NULL.number);
        assertNull(JsonValue.NULL.i);
        assertNull(JsonValue.NULL.l);
        assertNull(JsonValue.NULL.d);
        assertNull(JsonValue.NULL.f);
        assertNull(JsonValue.NULL.bd);
        assertNull(JsonValue.NULL.bi);
        assertEquals(JsonValue.NULL, new JsonValue((String)null));
        assertEquals(JsonValue.NULL, new JsonValue((Boolean) null));
        assertEquals(JsonValue.NULL, new JsonValue((Map<String, JsonValue>)null));
        assertEquals(JsonValue.NULL, new JsonValue((List<JsonValue>)null));
        assertEquals(JsonValue.NULL, new JsonValue((JsonValue[])null));
        assertEquals(JsonValue.NULL, new JsonValue((BigDecimal)null));
        assertEquals(JsonValue.NULL, new JsonValue((BigInteger) null));
    }

    @Test
    public void testGetMapped() {
        ZonedDateTime zdt = DateTimeUtils.gmtNow();
        Duration dur = Duration.ofNanos(4273);
        Duration dur2 = Duration.ofNanos(7342);

        JsonValue v = new JsonValue(new HashMap<>());
        v.map.put("bool", new JsonValue(Boolean.TRUE));
        v.map.put("string", new JsonValue("hello"));
        v.map.put("int", new JsonValue(Integer.MAX_VALUE));
        v.map.put("long", new JsonValue(Long.MAX_VALUE));
        v.map.put("date", new JsonValue(DateTimeUtils.toRfc3339(zdt)));
        v.map.put("dur", new JsonValue(dur.toNanos()));
        v.map.put("strings", new JsonValue(new JsonValue[]{new JsonValue("s1"), new JsonValue("s2")}));
        v.map.put("durs", new JsonValue(new JsonValue[]{new JsonValue(dur.toNanos()), new JsonValue(dur2.toNanos())}));

        assertNotNull(readValue(v, "string"));
        assertNull(readValue(v, "na"));
        assertEquals(JsonValue.EMPTY_MAP, readObject(v, "na"));
        assertNull(read(null, "na", vv -> vv));
        assertNull(read(JsonValue.NULL, "na", vv -> vv));
        assertNull(read(JsonValue.EMPTY_MAP, "na", vv -> vv));

        assertNull(readDate(null, "na"));
        assertNull(readDate(JsonValue.NULL, "na"));
        assertNull(readDate(JsonValue.EMPTY_MAP, "na"));
        assertEquals(zdt, readDate(v, "date"));
        assertNull(readDate(v, "int"));

        assertFalse(readBoolean(null, "na"));
        assertFalse(readBoolean(null, "na", false));
        assertTrue(readBoolean(null, "na", true));
        assertFalse(readBoolean(JsonValue.NULL, "na"));
        assertFalse(readBoolean(JsonValue.NULL, "na", false));
        assertTrue(readBoolean(JsonValue.NULL, "na", true));
        assertFalse(readBoolean(JsonValue.EMPTY_MAP, "na"));
        assertFalse(readBoolean(JsonValue.EMPTY_MAP, "na", false));
        assertTrue(readBoolean(JsonValue.EMPTY_MAP, "na", true));
        assertFalse(readBoolean(v, "na"));
        assertFalse(readBoolean(v, "na", false));
        assertTrue(readBoolean(v, "na", true));
        assertFalse(readBoolean(v, "int"));
        assertFalse(readBoolean(v, "int", false));
        assertTrue(readBoolean(v, "int", true));

        assertTrue(readBoolean(v, "bool"));
        assertTrue(readBoolean(v, "bool", false));
        assertFalse(readBoolean(v, "na"));
        assertFalse(readBoolean(v, "na", false));
        assertTrue(readBoolean(v, "na", true));

        assertEquals("hello", readString(v, "string"));
        assertEquals("hello", readString(v, "string", null));
        assertNull(readString(v, "na"));
        assertNull(readString(v, "na", null));
        assertEquals("default", readString(v, "na", "default"));
        assertNull(readString(JsonValue.NULL, "na"));
        assertNull(readString(JsonValue.NULL, "na", null));
        assertEquals("default", readString(JsonValue.NULL, "na", "default"));

        assertEquals(zdt, readDate(v, "date"));
        assertNull(readDate(v, "na"));
        assertThrows(DateTimeException.class, () -> readDate(v, "string"));

        assertEquals(Integer.MAX_VALUE, readInteger(v, "int"));
        assertEquals(Integer.MAX_VALUE, readInteger(v, "int", -1));
        assertNull(readInteger(v, "string"));
        assertEquals(-1, readInteger(v, "string", -1));
        assertNull(readInteger(v, "na"));
        assertEquals(-1, readInteger(v, "na", -1));

        assertEquals(Long.MAX_VALUE, readLong(v, "long"));
        assertEquals(Long.MAX_VALUE, readLong(v, "long", -1));
        assertNull(readLong(v, "string"));
        assertEquals(-1, readLong(v, "string", -1));
        assertNull(readLong(v, "na"));
        assertEquals(-1, readLong(v, "na", -1));

        assertEquals(dur, readNanos(v, "dur"));
        assertEquals(dur, readNanos(v, "dur", null));
        assertNull(readNanos(v, "string"));
        assertNull(readNanos(v, "string", null));
        assertEquals(dur2, readNanos(v, "string", dur2));
        assertNull(readNanos(v, "na"));
        assertNull(readNanos(v, "na", null));
        assertEquals(dur2, readNanos(v, "na", dur2));

        // these aren't maps
        JsonValue jvn = new JsonValue(1);
        JsonValue jvs = new JsonValue("s");
        JsonValue jvb = new JsonValue(true);
        JsonValue[] notMaps = new JsonValue[] {JsonValue.NULL, JsonValue.EMPTY_ARRAY, jvn, jvs, jvb};

        for (JsonValue vv : notMaps) {
            assertNull(readValue(vv, "na"));
            assertEquals(JsonValue.EMPTY_MAP, readObject(vv, "na"));
            assertNull(readDate(vv, "na"));
            assertNull(readInteger(vv, "na"));
            assertEquals(-1, readInteger(vv, "na", -1));
            assertNull(readLong(vv, "na"));
            assertEquals(-2, readLong(vv, "na", -2));
            assertFalse(readBoolean(vv, "na"));
            assertNull(readBoolean(vv, "na", null));
            assertTrue(readBoolean(vv, "na", true));
            assertFalse(readBoolean(vv, "na", false));
            assertNull(readNanos(vv, "na"));
            assertEquals(Duration.ZERO, readNanos(vv, "na", Duration.ZERO));
        }
    }

    private void validateParse(JsonValue expected, String json) throws JsonParseException {
        char[] ca = json.toCharArray();
        byte[] ba = json.getBytes();

        assertEquals(expected, parse(json));
        assertEquals(expected, parse(json, 0));
        assertEquals(expected, parse(json, KEEP_NULLS));
        assertEquals(expected, parse(ca));
        assertEquals(expected, parse(ca, 0));
        assertEquals(expected, parse(ca, KEEP_NULLS));
        assertEquals(expected, parse(ba));
        assertEquals(expected, parse(ba, KEEP_NULLS));

        assertEquals(expected, parseUnchecked(json));
        assertEquals(expected, parseUnchecked(json, 0));
        assertEquals(expected, parseUnchecked(json, KEEP_NULLS));
        assertEquals(expected, parseUnchecked(ca));
        assertEquals(expected, parseUnchecked(ca, 0));
        assertEquals(expected, parseUnchecked(ca, KEEP_NULLS));
        assertEquals(expected, parseUnchecked(ba));
        assertEquals(expected, parseUnchecked(ba, KEEP_NULLS));
    }

    @Test
    public void testParsingCoverage() throws JsonParseException {
        validateParse(JsonValue.NULL, "");
        validateParse(JsonValue.EMPTY_MAP, "{}");
        validateParse(JsonValue.EMPTY_ARRAY, "[]");

        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class, () -> parse("{}", -1));
        assertTrue(iae.getMessage().contains("Invalid start index."));

        validateThrows("{", "Text must end with '}'");
        validateThrows("{{", "Cannot directly nest another Object or Array.");
        validateThrows("{[", "Cannot directly nest another Object or Array.");
        validateThrows("{\"foo\":1 ]", "Expected a ',' or '}'.");
        validateThrows("{\"foo\" 1", "Expected a ':' after a key.");
        validateThrows("[\"bad\",", "Unexpected end of data."); // missing close
        validateThrows("[1Z]", "Invalid value.");
        validateThrows("t", "Invalid value.");
        validateThrows("f", "Invalid value.");
        validateThrows("\"u", "Unterminated string.");
        validateThrows("\"u\r", "Unterminated string.");
        validateThrows("\"u\n", "Unterminated string.");
        validateThrows("\"\\x\"", "Illegal escape.");
        validateThrows("\"\\u000", "Illegal escape.");
        validateThrows("\"\\uzzzz", "Illegal escape.");

        JsonValue v = JsonParser.parse((char[])null);
        assertEquals(JsonValue.NULL, v);

        v = parse("{\"foo\":1,}");
        assertEquals(1, v.map.size());
        assertTrue(v.map.containsKey("foo"));
        assertEquals(1, v.map.get("foo").i);

        v = parse("INFO{\"foo\":1,}", 4);
        assertEquals(1, v.map.size());
        assertTrue(v.map.containsKey("foo"));
        assertEquals(1, v.map.get("foo").i);

        v = parse("[\"foo\",]"); // handles dangling commas fine
        assertEquals(1, v.array.size());
        assertEquals("foo", v.array.get(0).string);

        String s = "foo \b \t \n \f \r \" \\ /";
        String j = "\"" + Encoding.jsonEncode(s) + "\"";
        v = parse(j);
        assertNotNull(v.string);
        assertEquals(s, v.string);

        // every constructor
        String json = "{}";
        new JsonParser(json.toCharArray());
        new JsonParser(json.toCharArray(), Option.KEEP_NULLS);
        parse(json.toCharArray());
        parse(json.toCharArray(), 0);
        parse(json.toCharArray(), Option.KEEP_NULLS);
        parse(json.toCharArray(), 0, Option.KEEP_NULLS);
        parse(json);
        parse(json, 0);
        parse(json, Option.KEEP_NULLS);
        parse(json, 0, Option.KEEP_NULLS);
        parse(json.getBytes());
        parse(json.getBytes(), Option.KEEP_NULLS);
        parseUnchecked(json.toCharArray());
        parseUnchecked(json.toCharArray(), 0);
        parseUnchecked(json.toCharArray(), Option.KEEP_NULLS);
        parseUnchecked(json.toCharArray(), 0, Option.KEEP_NULLS);
        parseUnchecked(json);
        parseUnchecked(json, 0);
        parseUnchecked(json, Option.KEEP_NULLS);
        parseUnchecked(json, 0, Option.KEEP_NULLS);
        parseUnchecked(json.getBytes());
        parseUnchecked(json.getBytes(), Option.KEEP_NULLS);
    }

    private void validateThrows(String json, String errorText) {
        // also provides coverage for every constructor
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json.toCharArray())));
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json.toCharArray(), 0)));
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json.toCharArray(), Option.KEEP_NULLS)));
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json.toCharArray(), 0, Option.KEEP_NULLS)));
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json)));
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json, 0)));
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json, Option.KEEP_NULLS)));
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json, 0, Option.KEEP_NULLS)));
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json.getBytes())));
        validateThrowError(errorText, assertThrows(JsonParseException.class, () -> parse(json.getBytes(), Option.KEEP_NULLS)));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json.toCharArray())));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json.toCharArray(), 0)));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json.toCharArray(), Option.KEEP_NULLS)));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json.toCharArray(), 0, Option.KEEP_NULLS)));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json)));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json, 0)));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json, Option.KEEP_NULLS)));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json, 0, Option.KEEP_NULLS)));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json.getBytes())));
        validateThrowError(errorText, assertThrows(RuntimeException.class, () -> parseUnchecked(json.getBytes(), Option.KEEP_NULLS)));
    }

    private static void validateThrowError(String errorText, Exception e) {
        assertTrue(e.getMessage().contains(errorText));
    }

    @Test
    public void testNumberParsing() throws JsonParseException {
        assertEquals(JsonValue.Type.INTEGER, parse("1").type);
        assertEquals(JsonValue.Type.INTEGER, parse(Integer.toString(Integer.MAX_VALUE)).type);
        assertEquals(JsonValue.Type.INTEGER, parse(Integer.toString(Integer.MIN_VALUE)).type);
        assertEquals(JsonValue.Type.LONG, parse(Long.toString((long)Integer.MAX_VALUE + 1)).type);
        assertEquals(JsonValue.Type.LONG, parse(Long.toString((long)Integer.MIN_VALUE - 1)).type);
        assertEquals(JsonValue.Type.DOUBLE, parse("-0").type);
        assertEquals(JsonValue.Type.DOUBLE, parse("-0.0").type);
        assertEquals(JsonValue.Type.DOUBLE, parse("0.1d").type);
        assertEquals(JsonValue.Type.DOUBLE, parse("0.f").type);
        assertEquals(JsonValue.Type.DOUBLE, parse("0.1f").type);
        assertEquals(JsonValue.Type.DOUBLE, parse("-0x1.fffp1").type);
        assertEquals(JsonValue.Type.DOUBLE, parse("0x1.0P-1074").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, parse("0.2").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, parse("244273.456789012345").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, parse("244273.456789012345").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, parse("0.1234567890123456789").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, parse("-24.42e7345").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, parse("-24.42E7345").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, parse("-.01").type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, parse("00.001").type);
        assertEquals(JsonValue.Type.BIG_INTEGER, parse("12345678901234567890").type);

        String str = new BigInteger( Long.toString(Long.MAX_VALUE) ).add( BigInteger.ONE ).toString();
        assertEquals(JsonValue.Type.BIG_INTEGER, parse(str).type);

        validateThrows("-0x123", "Invalid value.");
        JsonParseException e;

        e = assertThrows(JsonParseException.class, () -> parse("-"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> parse("00"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> parse("NaN"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> parse("-NaN"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> parse("Infinity"));
        assertTrue(e.getMessage().contains("Invalid value."));

        e = assertThrows(JsonParseException.class, () -> parse("-Infinity"));
        assertTrue(e.getMessage().contains("Invalid value."));
    }

    @Test
    public void testValueUtilsInstanceDuration() {
        JsonValue v = instance(Duration.ofSeconds(1));
        assertNotNull(v.l);
        assertEquals(1000000000L, v.l);
    }

    static class TestSerializableMap implements JsonSerializable {
        @Override
        @NonNull
        public String toJson() {
            JsonValue v = new JsonValue(new HashMap<>());
            v.map.put("a", new JsonValue("A"));
            v.map.put("b", new JsonValue("B"));
            v.map.put("c", new JsonValue("C"));
            return v.toJson();
        }
    }

    static class TestSerializableList implements JsonSerializable {
        @Override
        @NonNull
        public String toJson() {
            JsonValue v = new JsonValue(new ArrayList<>());
            v.array.add(new JsonValue("X"));
            v.array.add(new JsonValue("Y"));
            v.array.add(new JsonValue("Z"));
            return v.toJson();
        }
    }

    @Test
    public void testValueUtilsInstanceList() {
        List<Object> list = new ArrayList<>();
        list.add("Hello");
        list.add("");
        list.add('c');
        list.add(1);
        list.add(1L);
        list.add(1D);
        list.add(1F);
        list.add(new BigDecimal("1.0"));
        list.add(new BigInteger("1"));
        list.add(true);
        list.add(new HashMap<>());
        list.add(new ArrayList<>());
        list.add(new HashSet<>());
        list.add(new TestSerializableMap());
        list.add(new TestSerializableList());
        list.add(null);
        JsonValue v = instance(list);
        assertNotNull(v.array);
        assertEquals(16, v.array.size());
        assertEquals(JsonValue.Type.STRING, v.array.get(0).type);
        assertEquals(JsonValue.Type.NULL, v.array.get(1).type);
        assertEquals(JsonValue.Type.STRING, v.array.get(2).type);
        assertEquals(JsonValue.Type.INTEGER, v.array.get(3).type);
        assertEquals(JsonValue.Type.LONG, v.array.get(4).type);
        assertEquals(JsonValue.Type.DOUBLE, v.array.get(5).type);
        assertEquals(JsonValue.Type.FLOAT, v.array.get(6).type);
        assertEquals(JsonValue.Type.BIG_DECIMAL, v.array.get(7).type);
        assertEquals(JsonValue.Type.BIG_INTEGER, v.array.get(8).type);
        assertEquals(JsonValue.Type.BOOL, v.array.get(9).type);
        assertEquals(JsonValue.Type.MAP, v.array.get(10).type);
        assertEquals(JsonValue.Type.ARRAY, v.array.get(11).type);
        assertEquals(JsonValue.Type.ARRAY, v.array.get(12).type);
        assertEquals(JsonValue.Type.MAP, v.array.get(13).type);
        assertEquals(JsonValue.Type.ARRAY, v.array.get(14).type);
        assertEquals(JsonValue.Type.NULL, v.array.get(15).type);
    }

    @Test
    public void testValueUtilsInstanceMap() {
        Map<Object, Object> map = new HashMap<>();
        map.put("string", "Hello");
        map.put("char", 'c');
        map.put("int", 1);
        map.put("long", Long.MAX_VALUE);
        map.put("double", 1D);
        map.put("float", 1F);
        map.put("bd", new BigDecimal("1.0"));
        map.put("bi", new BigInteger(Long.toString(Long.MAX_VALUE)));
        map.put("bool", true);
        map.put("map", new HashMap<>());
        map.put("list", new ArrayList<>());
        map.put("set", new HashSet<>());
        map.put("smap", new TestSerializableMap());
        map.put("slist", new TestSerializableList());
        map.put("jv", JsonValue.EMPTY_MAP);
        map.put("null", null);
        map.put("jvNull", JsonValue.NULL);
        map.put("empty_is_null", "");
        validateMap(true, false, instance(map));
    }

    @Test
    public void testValueUtilsMapBuilder() {
        MapBuilder builder = mapBuilder()
            .put("string", "Hello")
            .put("char", 'c')
            .put("int", 1)
            .put("long", Long.MAX_VALUE)
            .put("double", 1D)
            .put("float", 1F)
            .put("bd", new BigDecimal("1.0"))
            .put("bi", new BigInteger(Long.toString(Long.MAX_VALUE)))
            .put("bool", true)
            .put("map", new HashMap<>())
            .put("list", new ArrayList<>())
            .put("set", new HashSet<>())
            .put("smap", new TestSerializableMap())
            .put("slist", new TestSerializableList())
            .put("jv", JsonValue.EMPTY_MAP)
            .put("null", null)
            .put("jvNull", JsonValue.NULL)
            .put("empty_is_null", "");
        validateMap(false, false, builder.toJsonValue());
        //noinspection deprecation
        validateMap(false, false, builder.getJsonValue()); // coverage for deprecated
        validateMap(false, true, JsonParser.parseUnchecked(builder.toJson()));
    }

    private static void validateMap(boolean checkNull, boolean parsed, JsonValue v) {
        assertNotNull(v.map);
        assertEquals(JsonValue.Type.STRING, v.map.get("string").type);
        assertEquals(JsonValue.Type.STRING, v.map.get("char").type);
        assertEquals(JsonValue.Type.INTEGER, v.map.get("int").type);
        assertEquals(JsonValue.Type.LONG, v.map.get("long").type);
        if (parsed) {
            assertEquals(JsonValue.Type.BIG_DECIMAL, v.map.get("double").type);
            assertEquals(JsonValue.Type.BIG_DECIMAL, v.map.get("float").type);
            assertEquals(JsonValue.Type.LONG, v.map.get("bi").type);
        }
        else {
            assertEquals(JsonValue.Type.DOUBLE, v.map.get("double").type);
            assertEquals(JsonValue.Type.FLOAT, v.map.get("float").type);
            assertEquals(JsonValue.Type.BIG_INTEGER, v.map.get("bi").type);
        }
        assertEquals(JsonValue.Type.BIG_DECIMAL, v.map.get("bd").type);
        assertEquals(JsonValue.Type.BOOL, v.map.get("bool").type);
        assertEquals(JsonValue.Type.MAP, v.map.get("map").type);
        assertEquals(JsonValue.Type.ARRAY, v.map.get("list").type);
        assertEquals(JsonValue.Type.ARRAY, v.map.get("set").type);
        assertEquals(JsonValue.Type.MAP, v.map.get("smap").type);
        assertEquals(JsonValue.Type.ARRAY, v.map.get("slist").type);
        assertEquals(JsonValue.Type.MAP, v.map.get("jv").type);
        if (checkNull) {
            assertEquals(18, v.map.size());
            assertEquals(JsonValue.Type.NULL, v.map.get("null").type);
            assertEquals(JsonValue.Type.NULL, v.map.get("jvNull").type);
            assertEquals(JsonValue.Type.NULL, v.map.get("empty_is_null").type);
        }
        else {
            assertEquals(15, v.map.size());
        }
    }

    @Test
    public void testValueUtilsInstanceArray() {
        List<Object> list = new ArrayList<>();
        list.add("Hello");
        list.add('c');
        list.add(1);
        list.add(Long.MAX_VALUE);
        list.add(1D);
        list.add(1F);
        list.add(new BigDecimal("1.0"));
        list.add(new BigInteger(Long.toString(Long.MAX_VALUE)));
        list.add(true);
        list.add(new HashMap<>());
        list.add(new ArrayList<>());
        list.add(new TestSerializableMap());
        list.add(new TestSerializableList());
        list.add(JsonValue.EMPTY_MAP);
        list.add(null);
        list.add(JsonValue.NULL);
        validateArray(true, false, instance(list));
    }

    @Test
    public void testValueUtilsArrayBuilder() {
        ArrayBuilder builder = arrayBuilder()
            .add("Hello")
            .add('c')
            .add(1)
            .add(Long.MAX_VALUE)
            .add(1D)
            .add(1F)
            .add(new BigDecimal("1.0"))
            .add(new BigInteger(Long.toString(Long.MAX_VALUE)))
            .add(true)
            .add(new HashMap<>())
            .add(new ArrayList<>())
            .add(new TestSerializableMap())
            .add(new TestSerializableList())
            .add(JsonValue.EMPTY_MAP)
            .add(null)
            .add(JsonValue.NULL);
        validateArray(false, false, builder.toJsonValue());
        //noinspection deprecation
        validateArray(false, false, builder.getJsonValue()); // coverage for deprecated
        validateArray(false, true, JsonParser.parseUnchecked(builder.toJson()));
    }

    private static void validateArray(boolean checkNull, boolean parsed, JsonValue v) {
        assertNotNull(v.array);
        assertEquals(JsonValue.Type.STRING, v.array.get(0).type);
        assertEquals(JsonValue.Type.STRING, v.array.get(1).type);
        assertEquals(JsonValue.Type.INTEGER, v.array.get(2).type);
        assertEquals(JsonValue.Type.LONG, v.array.get(3).type);
        if (parsed) {
            assertEquals(JsonValue.Type.BIG_DECIMAL, v.array.get(4).type);
            assertEquals(JsonValue.Type.BIG_DECIMAL, v.array.get(5).type);
            assertEquals(JsonValue.Type.LONG, v.array.get(7).type);
        }
        else {
            assertEquals(JsonValue.Type.DOUBLE, v.array.get(4).type);
            assertEquals(JsonValue.Type.FLOAT, v.array.get(5).type);
            assertEquals(JsonValue.Type.BIG_INTEGER, v.array.get(7).type);
        }
        assertEquals(JsonValue.Type.BIG_DECIMAL, v.array.get(6).type);
        assertEquals(JsonValue.Type.BOOL, v.array.get(8).type);
        assertEquals(JsonValue.Type.MAP, v.array.get(9).type);
        assertEquals(JsonValue.Type.ARRAY, v.array.get(10).type);
        assertEquals(JsonValue.Type.MAP, v.array.get(11).type);
        assertEquals(JsonValue.Type.ARRAY, v.array.get(12).type);
        assertEquals(JsonValue.Type.MAP, v.array.get(13).type);
        if (checkNull) {
            assertEquals(16, v.array.size());
            assertEquals(JsonValue.Type.NULL, v.array.get(14).type);
            assertEquals(JsonValue.Type.NULL, v.array.get(15).type);
        }
        else {
            assertEquals(14, v.array.size());
        }
    }

    @Test
    public void testReadStringStringMap() {
        JsonValue jv = mapBuilder()
            .put("stringString", mapBuilder().put("a", "A").put("b", "B").toJsonValue())
            .put("empty", new HashMap<>())
            .put("string", "string")
            .toJsonValue();

        assertNull(readStringStringMap(jv, "string"));
        assertNull(readStringStringMap(jv, "empty"));
        Map<String, String> stringString = readStringStringMap(jv, "stringString");
        assertNotNull(stringString);
        assertEquals(2, stringString.size());
        assertEquals("A", stringString.get("a"));
        assertEquals("B", stringString.get("b"));
    }

    @Test
    public void testJsonParseExceptionCoverage() {
        JsonParseException m = new JsonParseException("message constructor");
        assertTrue(m.getMessage().contains("message constructor"));

        Exception e = new Exception("foo");
        JsonParseException mt = new JsonParseException("message throwable constructor", e);
        assertNotNull(mt.getCause());
        assertTrue(mt.getCause().getMessage().contains("foo"));
        assertTrue(mt.getMessage().contains("message throwable constructor"));

        JsonParseException t = new JsonParseException(e);
        assertNotNull(t.getCause());
        assertTrue(t.getCause().getMessage().contains("foo"));
        assertTrue(t.getMessage().contains("java.lang.Exception"));
        assertTrue(t.getMessage().contains("foo"));
    }
}
